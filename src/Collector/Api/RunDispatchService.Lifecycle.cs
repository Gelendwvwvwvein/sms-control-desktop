using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Collector.Data;
using Collector.Data.Entities;
using Collector.Services;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed partial class RunDispatchService
{
    private static async Task<(int Recovered, int ToSent, int ToRetry, int ToFailed)> RecoverInterruptedJobsAsync(
        AppDbContext db,
        long runSessionId,
        DateTime nowUtc,
        CancellationToken cancellationToken)
    {
        var interruptedJobs = await db.RunJobs
            .Where(x => x.RunSessionId == runSessionId)
            .Where(x => x.Status == JobStatusRunning)
            .ToListAsync(cancellationToken);

        if (interruptedJobs.Count == 0)
        {
            return (0, 0, 0, 0);
        }

        var interruptedJobIds = interruptedJobs.Select(x => x.Id).ToList();
        var sentMessageTimes = await db.Messages
            .AsNoTracking()
            .Where(x => x.RunJobId.HasValue && interruptedJobIds.Contains(x.RunJobId.Value))
            .Where(x => x.Direction == MessageDirectionOut && x.GatewayStatus == MessageGatewayStatusSent)
            .GroupBy(x => x.RunJobId!.Value)
            .Select(g => new { RunJobId = g.Key, SentAtUtc = g.Max(m => m.CreatedAtUtc) })
            .ToDictionaryAsync(x => x.RunJobId, x => x.SentAtUtc, cancellationToken);

        var toSent = 0;
        var toRetry = 0;
        var toFailed = 0;

        foreach (var job in interruptedJobs)
        {
            if (sentMessageTimes.TryGetValue(job.Id, out var sentAtUtc))
            {
                job.Status = JobStatusSent;
                job.SentAtUtc = sentAtUtc;
                job.PlannedAtUtc = sentAtUtc;
                job.LastErrorCode = null;
                job.LastErrorDetail = "Восстановлено после рестарта: найдено ранее отправленное сообщение.";
                toSent++;
                continue;
            }

            job.LastErrorCode = "RUN_INTERRUPTED";
            job.LastErrorDetail = "Задача была в running и возвращена в retry после восстановления цикла.";
            if (job.Attempts >= job.MaxAttempts)
            {
                job.Status = JobStatusFailed;
                toFailed++;
            }
            else
            {
                job.Status = JobStatusRetry;
                job.PlannedAtUtc = nowUtc.AddSeconds(10);
                toRetry++;
            }
        }

        db.RunJobs.UpdateRange(interruptedJobs);
        await db.SaveChangesAsync(cancellationToken);
        return (interruptedJobs.Count, toSent, toRetry, toFailed);
    }

    private async Task TryCompleteSessionAsync(
        AppDbContext db,
        RunSessionRecord runSession,
        DateTime nowUtc,
        CancellationToken cancellationToken)
    {
        await db.Entry(runSession).ReloadAsync(cancellationToken);
        if (!string.Equals(runSession.Status, SessionStatusRunning, StringComparison.Ordinal))
        {
            return;
        }

        var hasPending = await db.RunJobs
            .Where(x => x.RunSessionId == runSession.Id)
            .AnyAsync(
                x => x.Status == JobStatusQueued || x.Status == JobStatusRetry || x.Status == JobStatusRunning,
                cancellationToken);

        if (hasPending)
        {
            return;
        }

        runSession.Status = SessionStatusCompleted;
        runSession.FinishedAtUtc = nowUtc;
        if (string.IsNullOrWhiteSpace(runSession.Notes))
        {
            runSession.Notes = "Сессия завершена: все задачи перешли в sent/failed.";
        }

        db.RunSessions.Update(runSession);
        AddEvent(
            db,
            runSession.Id,
            runJobId: null,
            eventType: "run_completed",
            severity: "info",
            message: $"Сессия #{runSession.Id} завершена.",
            payload: new { runSessionId = runSession.Id, finishedAtUtc = nowUtc });

        await db.SaveChangesAsync(cancellationToken);
        runCancellationCoordinator.ClearSession(runSession.Id);
    }

    private async Task FinalizeAttemptAsync(
        AppDbContext db,
        RunSessionRecord runSession,
        AppSettingsDto settings,
        RunJobRecord job,
        SenderChannelRecord? channel,
        DateTime attemptTimeUtc,
        DispatchAttemptResult result,
        CancellationToken cancellationToken)
    {
        var runSessionId = runSession.Id;
        var attemptsBefore = Math.Max(0, job.Attempts);
        if (result.CountAsAttempt)
        {
            job.Attempts = attemptsBefore + 1;
        }
        else
        {
            job.Attempts = attemptsBefore;
        }

        job.LastErrorCode = result.Success ? null : result.Code;
        job.LastErrorDetail = result.Success ? null : result.Detail;
        if (result.TemplateId.HasValue && result.TemplateId.Value > 0)
        {
            job.TemplateId = result.TemplateId.Value;
        }

        if (result.Success)
        {
            var existingSentAtUtc = await TryGetExistingSentMessageTimeAsync(db, job.Id, cancellationToken);
            if (existingSentAtUtc.HasValue)
            {
                job.Status = JobStatusSent;
                job.SentAtUtc = existingSentAtUtc.Value;
                job.PlannedAtUtc = existingSentAtUtc.Value;
                job.LastErrorCode = null;
                job.LastErrorDetail = "Повторная отправка пропущена: задача уже отмечена как отправленная.";

                if (channel is not null)
                {
                    await UpsertClientChannelBindingAsync(db, job, channel.Id, existingSentAtUtc.Value, cancellationToken);
                }

                AddEvent(
                    db,
                    runSessionId,
                    job.Id,
                    "job_deduplicated",
                    "info",
                    $"Задача #{job.Id} уже имела sent-сообщение, повторная отправка пропущена.",
                    new
                    {
                        runJobId = job.Id,
                        dedupeSource = "messages",
                        sentAtUtc = existingSentAtUtc.Value
                    });

                db.RunJobs.Update(job);
                await db.SaveChangesAsync(cancellationToken);
                return;
            }

            job.Status = JobStatusSent;
            job.SentAtUtc = attemptTimeUtc;
            job.PlannedAtUtc = attemptTimeUtc;

            if (channel is not null)
            {
                await UpsertClientChannelBindingAsync(db, job, channel.Id, attemptTimeUtc, cancellationToken);
            }
            var commentOutcome = await TryWriteContractCommentAfterSendAsync(
                db,
                settings,
                runSession,
                job,
                result,
                cancellationToken);

            db.Messages.Add(new MessageRecord
            {
                RunJobId = job.Id,
                ClientPhone = job.Phone,
                Direction = MessageDirectionOut,
                Text = string.IsNullOrWhiteSpace(result.MessageText)
                    ? $"[auto-dispatch] job:{job.Id}"
                    : result.MessageText,
                GatewayStatus = JobStatusSent,
                CreatedAtUtc = attemptTimeUtc,
                MetaJson = JsonSerializer.Serialize(new
                {
                    runSessionId,
                    runJobId = job.Id,
                    channelId = channel?.Id ?? 0,
                    templateId = result.TemplateId,
                    usedMessageOverride = result.UsedMessageOverride,
                    commentAttempted = commentOutcome.Attempted,
                    commentSuccess = commentOutcome.Success,
                    commentCode = commentOutcome.Code,
                    commentDetail = commentOutcome.Detail,
                    statusCode = result.StatusCode,
                    responseBody = result.ResponseBody,
                    error = result.Error
                })
            });

            AddEvent(
                db,
                runSessionId,
                job.Id,
                "job_sent",
                "info",
                $"Задача #{job.Id} успешно завершена.",
                new
                {
                    runJobId = job.Id,
                    channelId = channel?.Id ?? 0,
                    templateId = result.TemplateId,
                    usedMessageOverride = result.UsedMessageOverride,
                    commentAttempted = commentOutcome.Attempted,
                    commentSuccess = commentOutcome.Success,
                    commentCode = commentOutcome.Code
                });
        }
        else
        {
            if (channel is not null && string.Equals(result.Code, "GATEWAY_SEND_FAILED", StringComparison.Ordinal))
            {
                EventService.Append(
                    db,
                    category: "device",
                    eventType: "channel_send_failed",
                    severity: "warning",
                    message: $"Канал #{channel.Id}: ошибка отправки через gateway.{BuildLogDetailSuffix(result.Detail)}",
                    payload: new
                    {
                        runSessionId,
                        runJobId = job.Id,
                        channelId = channel.Id,
                        channelName = channel.Name,
                        endpoint = channel.Endpoint,
                        code = result.Code,
                        detail = result.Detail,
                        statusCode = result.StatusCode,
                        responseBody = result.ResponseBody,
                        error = result.Error,
                        transient = result.IsTransient
                    },
                    runSessionId: runSessionId,
                    runJobId: job.Id);
            }

            if (string.Equals(result.Code, JobErrorStoppedByStopList, StringComparison.Ordinal) ||
                string.Equals(result.Code, JobErrorStoppedByOperator, StringComparison.Ordinal))
            {
                job.Status = JobStatusStopped;
                job.SentAtUtc = null;
                job.PlannedAtUtc = attemptTimeUtc;

                AddEvent(
                    db,
                    runSessionId,
                    job.Id,
                    "job_stopped",
                    "warning",
                    string.Equals(result.Code, JobErrorStoppedByStopList, StringComparison.Ordinal)
                        ? $"Задача #{job.Id} остановлена: номер находится в стоп-листе."
                        : $"Задача #{job.Id} остановлена оператором во время попытки отправки.",
                    new
                    {
                        runJobId = job.Id,
                        code = result.Code,
                        detail = result.Detail
                    });
            }
            else if (!result.IsTransient || job.Attempts >= job.MaxAttempts)
            {
                job.Status = JobStatusFailed;
                job.SentAtUtc = null;

                AddEvent(
                    db,
                    runSessionId,
                    job.Id,
                    "job_failed",
                    "error",
                    $"Задача #{job.Id} завершилась ошибкой: {result.Code}.{BuildLogDetailSuffix(result.Detail)}",
                    new
                    {
                        runJobId = job.Id,
                        code = result.Code,
                        detail = result.Detail,
                        templateId = result.TemplateId,
                        usedMessageOverride = result.UsedMessageOverride
                    });

                alertService.RaiseSmsSendError(
                    db,
                    runSessionId,
                    job.Id,
                    job.ExternalClientId,
                    job.ClientFio,
                    job.Phone,
                    channel?.Id,
                    channel?.Name ?? string.Empty,
                    result.TemplateId,
                    result.UsedMessageOverride,
                    result.Code,
                    result.Detail);
            }
            else
            {
                job.Status = JobStatusRetry;
                if (result.NextPlannedAtUtc.HasValue)
                {
                    job.PlannedAtUtc = result.NextPlannedAtUtc.Value;
                }
                else
                {
                    job.PlannedAtUtc = result.CountAsAttempt
                        ? attemptTimeUtc + BuildBackoff(job.Attempts)
                        : attemptTimeUtc.AddSeconds(30);
                }
                job.SentAtUtc = null;

                AddEvent(
                    db,
                    runSessionId,
                    job.Id,
                    "job_retry",
                    "warning",
                    $"Задача #{job.Id} переведена в retry: {result.Code}.{BuildLogDetailSuffix(result.Detail)}",
                    new
                    {
                        runJobId = job.Id,
                        code = result.Code,
                        detail = result.Detail,
                        templateId = result.TemplateId,
                        usedMessageOverride = result.UsedMessageOverride,
                        nextTryAtUtc = job.PlannedAtUtc
                    });
            }
        }

        if (channel is not null)
        {
            var hadChannelError = string.Equals(channel.Status, ChannelStatusError, StringComparison.OrdinalIgnoreCase) ||
                                  channel.FailStreak > 0 ||
                                  channel.Alerted;
            if (result.Success)
            {
                channel.Status = ChannelStatusOnline;
                channel.FailStreak = 0;
                channel.Alerted = false;
                if (hadChannelError)
                {
                    var resolved = await alertService.ResolveChannelAlertsAsync(
                        db,
                        channel.Id,
                        "Канал восстановлен после успешной отправки.",
                        cancellationToken);
                    if (resolved > 0)
                    {
                        AddEvent(
                            db,
                            runSessionId,
                            job.Id,
                            "channel_recovered",
                            "info",
                            $"Канал #{channel.Id} восстановлен, закрыто уведомлений: {resolved}.",
                            new { channelId = channel.Id, resolved });
                    }
                }
            }
            else if (IsChannelStateNeutralError(result.Code))
            {
                channel.FailStreak = Math.Max(0, channel.FailStreak);
            }
            else
            {
                var wasError = string.Equals(channel.Status, ChannelStatusError, StringComparison.OrdinalIgnoreCase);
                channel.FailStreak = Math.Max(1, channel.FailStreak + 1);
                if (channel.FailStreak >= 3)
                {
                    channel.Status = ChannelStatusError;
                    channel.Alerted = true;
                    await alertService.RaiseChannelErrorAsync(
                        db,
                        channel,
                        result.Code,
                        result.Detail,
                        runSessionId,
                        job.Id,
                        cancellationToken);

                    if (!wasError)
                    {
                        AddEvent(
                            db,
                            runSessionId,
                            job.Id,
                            "channel_error",
                            "error",
                            $"Канал #{channel.Id} переведен в статус error.",
                            new
                            {
                                channelId = channel.Id,
                                failStreak = channel.FailStreak,
                                code = result.Code
                            });
                    }
                }
            }

            db.SenderChannels.Update(channel);
        }

        db.RunJobs.Update(job);
        await db.SaveChangesAsync(cancellationToken);
    }

    private async Task<bool> TryPromoteAlreadySentJobAsync(
        AppDbContext db,
        long runSessionId,
        RunJobRecord job,
        CancellationToken cancellationToken)
    {
        var sentAtUtc = await TryGetExistingSentMessageTimeAsync(db, job.Id, cancellationToken);
        if (!sentAtUtc.HasValue)
        {
            return false;
        }

        if (string.Equals(job.Status, JobStatusSent, StringComparison.OrdinalIgnoreCase) &&
            job.SentAtUtc.HasValue)
        {
            return true;
        }

        job.Status = JobStatusSent;
        job.SentAtUtc = sentAtUtc.Value;
        job.PlannedAtUtc = sentAtUtc.Value;
        job.LastErrorCode = null;
        job.LastErrorDetail = "Повторная обработка пропущена: найдено ранее отправленное сообщение.";
        db.RunJobs.Update(job);

        AddEvent(
            db,
            runSessionId,
            job.Id,
            "job_deduplicated",
            "info",
            $"Задача #{job.Id} переведена в sent по существующему отправленному сообщению.",
            new
            {
                runJobId = job.Id,
                dedupeSource = "messages",
                sentAtUtc = sentAtUtc.Value
            });

        await db.SaveChangesAsync(cancellationToken);
        return true;
    }

    private static Task<DateTime?> TryGetExistingSentMessageTimeAsync(
        AppDbContext db,
        long runJobId,
        CancellationToken cancellationToken)
    {
        return db.Messages
            .AsNoTracking()
            .Where(x => x.RunJobId == runJobId)
            .Where(x => x.Direction == MessageDirectionOut && x.GatewayStatus == MessageGatewayStatusSent)
            .OrderByDescending(x => x.CreatedAtUtc)
            .Select(x => (DateTime?)x.CreatedAtUtc)
            .FirstOrDefaultAsync(cancellationToken);
    }

    private static Task<bool> IsSessionStillRunningAsync(
        AppDbContext db,
        long runSessionId,
        CancellationToken cancellationToken)
    {
        return db.RunSessions
            .AsNoTracking()
            .AnyAsync(
                x => x.Id == runSessionId && x.Status == SessionStatusRunning,
                cancellationToken);
    }

    private static Task<bool> IsPhoneInActiveStopListAsync(
        AppDbContext db,
        string? phone,
        CancellationToken cancellationToken)
    {
        var normalizedPhone = NormalizePhone(phone);
        if (string.IsNullOrWhiteSpace(normalizedPhone))
        {
            return Task.FromResult(false);
        }

        return db.StopList
            .AsNoTracking()
            .AnyAsync(x => x.IsActive && x.Phone == normalizedPhone, cancellationToken);
    }

    private static async Task UpsertClientChannelBindingAsync(
        AppDbContext db,
        RunJobRecord job,
        long channelId,
        DateTime nowUtc,
        CancellationToken cancellationToken)
    {
        if (channelId <= 0 || string.IsNullOrWhiteSpace(job.ExternalClientId))
        {
            return;
        }

        var externalClientId = job.ExternalClientId.Trim();
        var normalizedPhone = NormalizePhone(job.Phone);

        var binding = await db.ClientChannelBindings
            .FirstOrDefaultAsync(x => x.ExternalClientId == externalClientId, cancellationToken);

        if (binding is null)
        {
            db.ClientChannelBindings.Add(new ClientChannelBindingRecord
            {
                ExternalClientId = externalClientId,
                Phone = normalizedPhone,
                ChannelId = channelId,
                CreatedAtUtc = nowUtc,
                UpdatedAtUtc = nowUtc,
                LastUsedAtUtc = nowUtc
            });
            return;
        }

        binding.Phone = normalizedPhone;
        binding.ChannelId = channelId;
        binding.UpdatedAtUtc = nowUtc;
        binding.LastUsedAtUtc = nowUtc;
        db.ClientChannelBindings.Update(binding);
    }

    private static string NormalizePhone(string? rawPhone)
    {
        return PhoneNormalizer.Normalize(rawPhone, coerceRussianLocalNumbers: true);
    }

    private static TimeSpan BuildBackoff(int attempts)
    {
        var seconds = attempts switch
        {
            <= 1 => 10,
            2 => 20,
            _ => 30
        };

        return TimeSpan.FromSeconds(seconds);
    }

}
