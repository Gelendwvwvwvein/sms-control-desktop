using System.Text.Json;
using System.Text.Json.Nodes;
using System.Globalization;
using Collector.Data;
using Collector.Data.Entities;
using Collector.Services;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed class RunDispatchService(
    SettingsStore settingsStore,
    AlertService alertService,
    RuleEngineService ruleEngine,
    DebtCacheService debtCacheService,
    RocketmanCommentService rocketmanCommentService)
{
    private const string RunModeLive = "live";
    private const string SessionStatusRunning = "running";
    private const string SessionStatusCompleted = "completed";

    private const string JobStatusQueued = "queued";
    private const string JobStatusRunning = "running";
    private const string JobStatusRetry = "retry";
    private const string JobStatusStopped = "stopped";
    private const string JobStatusSent = "sent";
    private const string JobStatusFailed = "failed";
    private const string JobErrorStoppedByOperator = "RUN_STOPPED_BY_OPERATOR";
    private const string DebtStatusReady = "ready";
    private const string MessageDirectionOut = "out";
    private const string MessageGatewayStatusSent = "sent";

    private const string ChannelStatusError = "error";
    private const string ChannelStatusOnline = "online";
    private const string ChannelStatusOffline = "offline";
    private const string DeliveryTypeSms = "sms";
    private const string PreviewStatusReady = "ready";
    private const string PreviewStatusError = "error";
    private const string PayloadFieldMessageOverride = "messageOverrideText";
    private const string PayloadFieldCardUrl = "cardUrl";
    private const int CommentWriteMaxAttempts = 3;
    private const int CommentWriteTimeoutMs = 20000;
    private const int DispatchAttemptTimeoutSeconds = 120;
    private const int LiveDebtFreshTtlMinutes = 20;
    private const int IdleDebtPrefetchCandidatesLimit = 24;
    private const int IdleDebtPrefetchTimeoutMs = 20000;
    private const int IdleDebtPrefetchSuccessCooldownSeconds = 45;
    private const int IdleDebtPrefetchTransientFailureCooldownMinutes = 5;
    private const int IdleDebtPrefetchNonRetryFailureCooldownMinutes = 30;

    private readonly Dictionary<long, DateTime> _channelCooldownUntilUtc = new();
    private readonly Dictionary<string, DateTime> _debtPrefetchCooldownUntilUtc = new(StringComparer.Ordinal);
    private readonly object _cooldownLock = new();
    private readonly object _debtPrefetchLock = new();
    private readonly object _replanSignalLock = new();
    private readonly TraccarHttpSmsSender _traccarSender = new(new HttpClient());
    private string _lastPlanningChannelsSignature = string.Empty;
    private bool _forceRebalance;
    private string _forceRebalanceReason = string.Empty;

    public void NotifyChannelsChanged(string reason)
    {
        lock (_replanSignalLock)
        {
            _forceRebalance = true;
            _forceRebalanceReason = (reason ?? string.Empty).Trim();
            _lastPlanningChannelsSignature = string.Empty;
        }
    }

    public async Task ProcessTickAsync(AppDbContext db, CancellationToken cancellationToken)
    {
        var nowUtc = DateTime.UtcNow;
        var runSession = await db.RunSessions
            .OrderByDescending(x => x.Id)
            .FirstOrDefaultAsync(x => x.Status == SessionStatusRunning, cancellationToken);

        if (runSession is null)
        {
            _lastPlanningChannelsSignature = string.Empty;
            ClearRebalanceSignal();
            return;
        }

        var (recovered, toSent, toRetry, toFailed) = await RecoverInterruptedJobsAsync(db, runSession.Id, nowUtc, cancellationToken);
        if (recovered > 0)
        {
            var sessionAgeHours = runSession.StartedAtUtc.HasValue
                ? (nowUtc - runSession.StartedAtUtc.Value).TotalHours
                : 0;
            var severity = sessionAgeHours > 24 ? "warning" : "info";
            AddEvent(
                db,
                runSession.Id,
                runJobId: null,
                eventType: "run_recovered",
                severity,
                message: $"Восстановление после рестарта: {recovered} задач (→отправлено: {toSent}, →повтор: {toRetry}, →ошибка: {toFailed}). Цикл продолжается.",
                payload: new
                {
                    runSessionId = runSession.Id,
                    recoveredCount = recovered,
                    toSent,
                    toRetry,
                    toFailed,
                    sessionStartedAtUtc = runSession.StartedAtUtc,
                    sessionAgeHours = Math.Round(sessionAgeHours, 1)
                });
            runSession.Notes = $"Восстановлено после рестарта {nowUtc:yyyy-MM-dd HH:mm} UTC: {recovered} задач. " + (runSession.Notes ?? "");
            db.RunSessions.Update(runSession);
            await db.SaveChangesAsync(cancellationToken);
        }

        var settings = await settingsStore.GetAsync(db, cancellationToken);
        var availableChannels = await db.SenderChannels
            .Where(x => x.Status != ChannelStatusError && x.Status != ChannelStatusOffline)
            .OrderBy(x => x.FailStreak)
            .ThenBy(x => x.Id)
            .ToListAsync(cancellationToken);

        var channelsSignature = BuildChannelsSignature(availableChannels);
        var signatureChanged = !string.Equals(_lastPlanningChannelsSignature, channelsSignature, StringComparison.Ordinal);
        var forcedRebalance = TryConsumeRebalanceSignal(out var forceReason);
        if (signatureChanged || forcedRebalance)
        {
            var updatedJobs = await RebalancePendingJobsAsync(
                db,
                runSession.Id,
                availableChannels,
                settings,
                nowUtc,
                cancellationToken);

            if (forcedRebalance)
            {
                AddEvent(
                    db,
                    runSession.Id,
                    runJobId: null,
                    eventType: "queue_rebalance_signal",
                    severity: "info",
                    message: string.IsNullOrWhiteSpace(forceReason)
                        ? "Получен сигнал на пересчет очереди по каналам."
                        : $"Получен сигнал на пересчет очереди по каналам: {forceReason}.",
                    payload: new
                    {
                        runSessionId = runSession.Id,
                        reason = forceReason,
                        updatedJobs
                    });
                await db.SaveChangesAsync(cancellationToken);
            }

            _lastPlanningChannelsSignature = channelsSignature;
        }

        if (availableChannels.Count == 0)
        {
            var dueWithoutChannel = await db.RunJobs
                .Where(x => x.RunSessionId == runSession.Id)
                .Where(x => x.Status == JobStatusQueued || x.Status == JobStatusRetry)
                .Where(x => x.PlannedAtUtc <= nowUtc)
                .OrderBy(x => x.PlannedAtUtc)
                .ThenBy(x => x.Id)
                .FirstOrDefaultAsync(cancellationToken);

            if (dueWithoutChannel is not null)
            {
                await FinalizeAttemptAsync(
                    db,
                    runSession,
                    settings,
                    dueWithoutChannel,
                    channel: null,
                    nowUtc,
                    new DispatchAttemptResult
                    {
                        Success = false,
                        IsTransient = true,
                        Code = "CHANNEL_UNAVAILABLE",
                        Detail = "Нет доступных Android-каналов в статусе online/unknown.",
                        CountAsAttempt = false
                    },
                    cancellationToken);
            }

            await TryCompleteSessionAsync(db, runSession, nowUtc, cancellationToken);
            return;
        }

        var maxAttemptsThisTick = Math.Max(1, availableChannels.Count);
        var processedDueJobThisTick = false;
        for (var i = 0; i < maxAttemptsThisTick; i++)
        {
            if (!await IsSessionStillRunningAsync(db, runSession.Id, cancellationToken))
            {
                break;
            }

            var tickNow = DateTime.UtcNow;
            var dueJob = await db.RunJobs
                .Where(x => x.RunSessionId == runSession.Id)
                .Where(x => x.Status == JobStatusQueued || x.Status == JobStatusRetry)
                .Where(x => x.PlannedAtUtc <= tickNow)
                .OrderBy(x => x.PlannedAtUtc)
                .ThenBy(x => x.Id)
                .FirstOrDefaultAsync(cancellationToken);

            if (dueJob is null)
            {
                if (!processedDueJobThisTick)
                {
                    await TryPrefetchDebtDuringIdleAsync(db, runSession, settings, tickNow, cancellationToken);
                }
                break;
            }

            processedDueJobThisTick = true;

            if (await TryPromoteAlreadySentJobAsync(db, runSession.Id, dueJob, cancellationToken))
            {
                continue;
            }

            if (!string.Equals(dueJob.DeliveryType, DeliveryTypeSms, StringComparison.Ordinal))
            {
                await FinalizeAttemptAsync(
                    db,
                    runSession,
                    settings,
                    dueJob,
                    channel: null,
                    tickNow,
                    new DispatchAttemptResult
                    {
                        Success = false,
                        IsTransient = false,
                        Code = "DELIVERY_TYPE_UNSUPPORTED",
                        Detail = $"Тип доставки '{dueJob.DeliveryType}' не поддерживается."
                    },
                    cancellationToken);
                continue;
            }

            var selected = SelectChannel(availableChannels, dueJob.ChannelId, tickNow);
            if (selected.NextAvailableAtUtc > tickNow)
            {
                dueJob.PlannedAtUtc = selected.NextAvailableAtUtc;
                dueJob.LastErrorCode = "CHANNEL_COOLDOWN";
                dueJob.LastErrorDetail = "Ожидание интервала отправки по выбранному каналу.";
                db.RunJobs.Update(dueJob);
                await db.SaveChangesAsync(cancellationToken);
                continue;
            }

            dueJob.Status = JobStatusRunning;
            dueJob.ChannelId = selected.Channel.Id;
            dueJob.LastErrorCode = null;
            dueJob.LastErrorDetail = null;
            db.RunJobs.Update(dueJob);
            await db.SaveChangesAsync(cancellationToken);

            if (!await IsSessionStillRunningAsync(db, runSession.Id, cancellationToken))
            {
                dueJob.Status = JobStatusStopped;
                dueJob.LastErrorCode = JobErrorStoppedByOperator;
                dueJob.LastErrorDetail = "Остановлено оператором до фактической отправки.";
                db.RunJobs.Update(dueJob);
                await db.SaveChangesAsync(cancellationToken);
                break;
            }

            var attemptTimeUtc = DateTime.UtcNow;
            DispatchAttemptResult attemptResult;
            try
            {
                using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                attemptCts.CancelAfter(TimeSpan.FromSeconds(DispatchAttemptTimeoutSeconds));
                attemptResult = await ExecuteAttemptAsync(
                    db,
                    settings,
                    runSession,
                    dueJob,
                    selected.Channel,
                    attemptCts.Token);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                attemptResult = new DispatchAttemptResult
                {
                    Success = false,
                    IsTransient = true,
                    CountAsAttempt = true,
                    Code = "DISPATCH_ATTEMPT_TIMEOUT",
                    Detail = $"Превышен лимит времени попытки отправки ({DispatchAttemptTimeoutSeconds} сек)."
                };
            }
            catch (Exception ex)
            {
                attemptResult = new DispatchAttemptResult
                {
                    Success = false,
                    IsTransient = true,
                    CountAsAttempt = true,
                    Code = "DISPATCH_ATTEMPT_EXCEPTION",
                    Detail = $"Неперехваченное исключение попытки отправки: {ex.Message}"
                };
            }

            await FinalizeAttemptAsync(db, runSession, settings, dueJob, selected.Channel, attemptTimeUtc, attemptResult, cancellationToken);

            if (attemptResult.Success)
            {
                SetChannelCooldown(selected.Channel.Id, attemptTimeUtc.AddMinutes(Math.Max(1, settings.Gap)));
            }
        }

        await TryCompleteSessionAsync(db, runSession, DateTime.UtcNow, cancellationToken);
    }

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

            if (!result.IsTransient || job.Attempts >= job.MaxAttempts)
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
        var digits = new string((rawPhone ?? string.Empty).Where(char.IsDigit).ToArray());
        if (digits.Length == 10)
        {
            digits = $"7{digits}";
        }
        else if (digits.Length == 11 && digits.StartsWith("8", StringComparison.Ordinal))
        {
            digits = $"7{digits[1..]}";
        }

        if (digits.Length == 0)
        {
            return string.Empty;
        }

        return $"+{digits}";
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

    private async Task<CommentWriteOutcome> TryWriteContractCommentAfterSendAsync(
        AppDbContext db,
        AppSettingsDto settings,
        RunSessionRecord runSession,
        RunJobRecord job,
        DispatchAttemptResult result,
        CancellationToken cancellationToken)
    {
        if (!string.Equals(runSession.Mode, RunModeLive, StringComparison.OrdinalIgnoreCase))
        {
            return CommentWriteOutcome.NotAttempted("COMMENT_SKIPPED_NON_LIVE", "Запись комментария выполняется только для live-сессий.");
        }

        var cardUrl = ExtractPayloadString(job.PayloadJson, PayloadFieldCardUrl);
        if (string.IsNullOrWhiteSpace(cardUrl))
        {
            var detail = "В payload задачи отсутствует cardUrl для записи комментария.";
            AddEvent(
                db,
                runSession.Id,
                job.Id,
                "comment_failed",
                "error",
                $"Задача #{job.Id}: не удалось записать комментарий (нет cardUrl).",
                new
                {
                    runSessionId = runSession.Id,
                    runJobId = job.Id,
                    code = "COMMENT_CARD_URL_MISSING",
                    detail
                });
            alertService.RaiseContractCommentError(
                db,
                runSession.Id,
                job.Id,
                job.ExternalClientId,
                job.Phone,
                string.Empty,
                string.Empty,
                "COMMENT_CARD_URL_MISSING",
                detail);
            return CommentWriteOutcome.AttemptFailed("COMMENT_CARD_URL_MISSING", detail);
        }

        if (string.IsNullOrWhiteSpace(settings.LoginUrl) ||
            string.IsNullOrWhiteSpace(settings.Login) ||
            string.IsNullOrWhiteSpace(settings.Password))
        {
            var detail = "Не заполнены loginUrl/login/password в настройках.";
            AddEvent(
                db,
                runSession.Id,
                job.Id,
                "comment_failed",
                "error",
                $"Задача #{job.Id}: не удалось записать комментарий (неполные настройки).",
                new
                {
                    runSessionId = runSession.Id,
                    runJobId = job.Id,
                    code = "COMMENT_SETTINGS_MISSING",
                    detail
                });
            alertService.RaiseContractCommentError(
                db,
                runSession.Id,
                job.Id,
                job.ExternalClientId,
                job.Phone,
                cardUrl,
                string.Empty,
                "COMMENT_SETTINGS_MISSING",
                detail);
            return CommentWriteOutcome.AttemptFailed("COMMENT_SETTINGS_MISSING", detail);
        }

        var commentText = ResolveCommentText(result);
        if (string.IsNullOrWhiteSpace(commentText))
        {
            var detail = result.TemplateId.HasValue && result.TemplateId.Value > 0
                ? $"Для шаблона id={result.TemplateId.Value} не заполнен комментарий для записи в договор."
                : "Для задачи не определен шаблон с комментарием для записи в договор.";
            AddEvent(
                db,
                runSession.Id,
                job.Id,
                "comment_failed",
                "error",
                $"Задача #{job.Id}: не удалось определить текст комментария для записи в договор.",
                new
                {
                    runSessionId = runSession.Id,
                    runJobId = job.Id,
                    code = "COMMENT_TEXT_RESOLVE_FAILED",
                    detail
                });
            alertService.RaiseContractCommentError(
                db,
                runSession.Id,
                job.Id,
                job.ExternalClientId,
                job.Phone,
                cardUrl,
                string.Empty,
                "COMMENT_TEXT_RESOLVE_FAILED",
                detail);
            return CommentWriteOutcome.AttemptFailed("COMMENT_TEXT_RESOLVE_FAILED", detail);
        }

        RocketmanCommentWriteResult? last = null;
        for (var attempt = 1; attempt <= CommentWriteMaxAttempts; attempt++)
        {
            last = await rocketmanCommentService.WriteCommentAsync(
                settings,
                new RocketmanCommentWriteRequest
                {
                    CardUrl = cardUrl,
                    Comment = commentText,
                    TimeoutMs = CommentWriteTimeoutMs,
                    Headed = false
                },
                cancellationToken);

            if (last.Success)
            {
                AddEvent(
                    db,
                    runSession.Id,
                    job.Id,
                    "comment_saved",
                    "info",
                    $"Задача #{job.Id}: комментарий записан в карточку клиента.",
                    new
                    {
                        runSessionId = runSession.Id,
                        runJobId = job.Id,
                        cardUrl,
                        commentText,
                        code = last.Code,
                        attempt
                    });
                return CommentWriteOutcome.AttemptSuccess(last.Code, last.Message);
            }

            var retryable = IsRetryableCommentError(last.Code);
            AddEvent(
                db,
                runSession.Id,
                job.Id,
                retryable && attempt < CommentWriteMaxAttempts ? "comment_retry" : "comment_attempt_failed",
                retryable ? "warning" : "error",
                $"Задача #{job.Id}: попытка записи комментария {attempt}/{CommentWriteMaxAttempts} завершилась ошибкой {last.Code}.",
                new
                {
                    runSessionId = runSession.Id,
                    runJobId = job.Id,
                    cardUrl,
                    commentText,
                    code = last.Code,
                    detail = last.Message,
                    retryable,
                    attempt
                });

            if (!retryable || attempt >= CommentWriteMaxAttempts)
            {
                break;
            }

            var pauseMs = attempt * 700;
            await Task.Delay(pauseMs, cancellationToken);
        }

        var failedCode = string.IsNullOrWhiteSpace(last?.Code) ? "COMMENT_WRITE_FAILED" : last!.Code.Trim();
        var failedDetail = string.IsNullOrWhiteSpace(last?.Message)
            ? "Не удалось записать комментарий в карточку клиента."
            : last!.Message.Trim();

        AddEvent(
            db,
            runSession.Id,
            job.Id,
            "comment_failed",
            "error",
            $"Задача #{job.Id}: комментарий не записан после {CommentWriteMaxAttempts} попыток.",
            new
            {
                runSessionId = runSession.Id,
                runJobId = job.Id,
                cardUrl,
                commentText,
                code = failedCode,
                detail = failedDetail
            });

        alertService.RaiseContractCommentError(
            db,
            runSession.Id,
            job.Id,
            job.ExternalClientId,
            job.Phone,
            cardUrl,
            commentText,
            failedCode,
            failedDetail);

        return CommentWriteOutcome.AttemptFailed(failedCode, failedDetail);
    }

    private static string ResolveCommentText(DispatchAttemptResult result)
    {
        return (result.TemplateCommentText ?? string.Empty).Trim();
    }

    private static bool IsRetryableCommentError(string? code)
    {
        var normalized = (code ?? string.Empty).Trim().ToUpperInvariant();
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return true;
        }

        return normalized is "COMMENT_TIMEOUT" or "COMMENT_PLAYWRIGHT_ERROR" or "COMMENT_UNEXPECTED_ERROR";
    }

    private static void AddEvent(
        AppDbContext db,
        long runSessionId,
        long? runJobId,
        string eventType,
        string severity,
        string message,
        object payload)
    {
        db.Events.Add(new EventLogRecord
        {
            Category = "dispatch",
            EventType = eventType,
            Severity = severity,
            Message = message,
            RunSessionId = runSessionId,
            RunJobId = runJobId,
            PayloadJson = JsonSerializer.Serialize(payload),
            CreatedAtUtc = DateTime.UtcNow
        });
    }

    private DispatchChannelChoice SelectChannel(
        IReadOnlyList<SenderChannelRecord> channels,
        long? preferredChannelId,
        DateTime nowUtc)
    {
        var preferred = preferredChannelId.HasValue
            ? channels.FirstOrDefault(x => x.Id == preferredChannelId.Value)
            : null;
        if (preferred is not null)
        {
            var preferredNextAvailable = GetChannelNextAvailableAt(preferred.Id, nowUtc);
            DispatchChannelChoice? bestOther = null;
            foreach (var channel in channels)
            {
                if (channel.Id == preferred.Id) continue;
                var nextAvailableAt = GetChannelNextAvailableAt(channel.Id, nowUtc);
                if (bestOther is null ||
                    nextAvailableAt < bestOther.NextAvailableAtUtc ||
                    (nextAvailableAt == bestOther.NextAvailableAtUtc && channel.FailStreak < bestOther.Channel.FailStreak) ||
                    (nextAvailableAt == bestOther.NextAvailableAtUtc && channel.FailStreak == bestOther.Channel.FailStreak && channel.Id < bestOther.Channel.Id))
                {
                    bestOther = new DispatchChannelChoice(channel, nextAvailableAt);
                }
            }

            if (bestOther is not null && bestOther.NextAvailableAtUtc < preferredNextAvailable)
            {
                return bestOther;
            }

            return new DispatchChannelChoice(preferred, preferredNextAvailable);
        }

        DispatchChannelChoice? best = null;
        foreach (var channel in channels)
        {
            var nextAvailableAt = GetChannelNextAvailableAt(channel.Id, nowUtc);
            if (best is null ||
                nextAvailableAt < best.NextAvailableAtUtc ||
                (nextAvailableAt == best.NextAvailableAtUtc && channel.FailStreak < best.Channel.FailStreak) ||
                (nextAvailableAt == best.NextAvailableAtUtc && channel.FailStreak == best.Channel.FailStreak && channel.Id < best.Channel.Id))
            {
                best = new DispatchChannelChoice(channel, nextAvailableAt);
            }
        }

        return best ?? new DispatchChannelChoice(channels[0], nowUtc);
    }

    private DateTime GetChannelNextAvailableAt(long channelId, DateTime nowUtc)
    {
        lock (_cooldownLock)
        {
            if (!_channelCooldownUntilUtc.TryGetValue(channelId, out var value))
            {
                return nowUtc;
            }

            return value > nowUtc ? value : nowUtc;
        }
    }

    private void SetChannelCooldown(long channelId, DateTime valueUtc)
    {
        lock (_cooldownLock)
        {
            _channelCooldownUntilUtc[channelId] = valueUtc;
        }
    }

    private async Task<int> RebalancePendingJobsAsync(
        AppDbContext db,
        long runSessionId,
        IReadOnlyList<SenderChannelRecord> availableChannels,
        AppSettingsDto settings,
        DateTime nowUtc,
        CancellationToken cancellationToken)
    {
        if (availableChannels.Count == 0)
        {
            return 0;
        }

        var pending = await db.RunJobs
            .Where(x => x.RunSessionId == runSessionId)
            .Where(x => x.Status == JobStatusQueued || x.Status == JobStatusRetry)
            .OrderBy(x => x.PlannedAtUtc)
            .ThenBy(x => x.Id)
            .ToListAsync(cancellationToken);
        if (pending.Count == 0)
        {
            return 0;
        }

        var gapMinutes = Math.Max(1, settings.Gap);
        var workStart = ParseHm(settings.WorkWindowStart, new TimeOnly(8, 0));
        var workEnd = ParseHm(settings.WorkWindowEnd, new TimeOnly(21, 0));
        if (workEnd <= workStart)
        {
            workStart = new TimeOnly(8, 0);
            workEnd = new TimeOnly(21, 0);
        }

        var channels = availableChannels
            .OrderBy(x => x.FailStreak)
            .ThenBy(x => x.Id)
            .ToList();
        var nextAvailableByChannel = channels.ToDictionary(
            x => x.Id,
            x => GetChannelNextAvailableAt(x.Id, nowUtc));

        var changed = 0;
        foreach (var job in pending)
        {
            SenderChannelRecord? selected = null;
            DateTime selectedNextAt = DateTime.MaxValue;
            foreach (var channel in channels)
            {
                var nextAt = nextAvailableByChannel[channel.Id];
                if (selected is null ||
                    nextAt < selectedNextAt ||
                    (nextAt == selectedNextAt && channel.FailStreak < selected.FailStreak) ||
                    (nextAt == selectedNextAt && channel.FailStreak == selected.FailStreak && channel.Id < selected.Id))
                {
                    selected = channel;
                    selectedNextAt = nextAt;
                }
            }

            if (selected is null)
            {
                continue;
            }

            var plannedAtUtc = AlignToWorkWindowUtc(selectedNextAt, job.TzOffset, workStart, workEnd);
            var channelChanged = job.ChannelId != selected.Id;
            var plannedChanged = Math.Abs((job.PlannedAtUtc - plannedAtUtc).TotalSeconds) >= 1;
            if (channelChanged || plannedChanged)
            {
                job.ChannelId = selected.Id;
                job.PlannedAtUtc = plannedAtUtc;
                changed++;
            }

            nextAvailableByChannel[selected.Id] = plannedAtUtc.AddMinutes(gapMinutes);
        }

        if (changed == 0)
        {
            return 0;
        }

        db.RunJobs.UpdateRange(pending);
        AddEvent(
            db,
            runSessionId,
            runJobId: null,
            eventType: "queue_rebalanced_channels",
            severity: "info",
            message: $"Очередь переназначена по каналам: обновлено задач {changed}.",
            payload: new
            {
                runSessionId,
                updatedJobs = changed,
                channels = channels.Select(x => new { x.Id, x.Name, x.Status }).ToList()
            });
        await db.SaveChangesAsync(cancellationToken);
        return changed;
    }

    private bool TryConsumeRebalanceSignal(out string reason)
    {
        lock (_replanSignalLock)
        {
            if (!_forceRebalance)
            {
                reason = string.Empty;
                return false;
            }

            _forceRebalance = false;
            reason = _forceRebalanceReason;
            _forceRebalanceReason = string.Empty;
            return true;
        }
    }

    private void ClearRebalanceSignal()
    {
        lock (_replanSignalLock)
        {
            _forceRebalance = false;
            _forceRebalanceReason = string.Empty;
        }
    }

    private async Task TryPrefetchDebtDuringIdleAsync(
        AppDbContext db,
        RunSessionRecord runSession,
        AppSettingsDto settings,
        DateTime nowUtc,
        CancellationToken cancellationToken)
    {
        if (!string.Equals(runSession.Mode, RunModeLive, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(settings.LoginUrl) ||
            string.IsNullOrWhiteSpace(settings.Login) ||
            string.IsNullOrWhiteSpace(settings.Password))
        {
            return;
        }

        var candidates = await db.RunJobs
            .AsNoTracking()
            .Where(x => x.RunSessionId == runSession.Id)
            .Where(x => x.Status == JobStatusQueued || x.Status == JobStatusRetry)
            .Where(x => !string.IsNullOrWhiteSpace(x.ExternalClientId))
            .OrderBy(x => x.PlannedAtUtc)
            .ThenBy(x => x.Id)
            .Select(x => new { x.Id, x.ExternalClientId, x.PlannedAtUtc })
            .Take(IdleDebtPrefetchCandidatesLimit)
            .ToListAsync(cancellationToken);

        if (candidates.Count == 0)
        {
            return;
        }

        var externalClientIds = candidates
            .Select(x => NormalizeExternalClientId(x.ExternalClientId))
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        if (externalClientIds.Count == 0)
        {
            return;
        }

        var cacheByExternalClientId = await db.ClientDebtCache
            .AsNoTracking()
            .Where(x => externalClientIds.Contains(x.ExternalClientId))
            .ToDictionaryAsync(x => x.ExternalClientId, StringComparer.Ordinal, cancellationToken);

        foreach (var candidate in candidates)
        {
            var externalClientId = NormalizeExternalClientId(candidate.ExternalClientId);
            if (string.IsNullOrWhiteSpace(externalClientId))
            {
                continue;
            }

            if (!CanPrefetchDebtNow(externalClientId, nowUtc))
            {
                continue;
            }

            cacheByExternalClientId.TryGetValue(externalClientId, out var cache);
            if (IsDebtCacheFresh(cache, nowUtc))
            {
                SetDebtPrefetchCooldown(externalClientId, nowUtc.AddSeconds(IdleDebtPrefetchSuccessCooldownSeconds), nowUtc);
                continue;
            }

            try
            {
                var fetchResult = await debtCacheService.FetchByExternalClientIdAsync(
                    db,
                    externalClientId,
                    settings,
                    new ClientDebtFetchRequestDto
                    {
                        TimeoutMs = IdleDebtPrefetchTimeoutMs,
                        Headed = false
                    },
                    cancellationToken);

                var success = fetchResult.Success && !string.IsNullOrWhiteSpace(fetchResult.Debt?.ExactTotalRaw);
                if (success)
                {
                    SetDebtPrefetchCooldown(externalClientId, nowUtc.AddSeconds(IdleDebtPrefetchSuccessCooldownSeconds), nowUtc);
                }
                else
                {
                    var nonRetryable = IsDebtPrefetchNonRetryableFailure(fetchResult.Code);
                    var backoffMinutes = nonRetryable
                        ? IdleDebtPrefetchNonRetryFailureCooldownMinutes
                        : IdleDebtPrefetchTransientFailureCooldownMinutes;
                    SetDebtPrefetchCooldown(externalClientId, nowUtc.AddMinutes(backoffMinutes), nowUtc);
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                SetDebtPrefetchCooldown(
                    externalClientId,
                    nowUtc.AddMinutes(IdleDebtPrefetchTransientFailureCooldownMinutes),
                    nowUtc);

                AddEvent(
                    db,
                    runSession.Id,
                    runJobId: candidate.Id,
                    eventType: "debt_prefetch_failed",
                    severity: "warning",
                    message: $"Не удалось выполнить prefetch суммы долга для задачи #{candidate.Id}.",
                    payload: new
                    {
                        runSessionId = runSession.Id,
                        runJobId = candidate.Id,
                        externalClientId,
                        error = ex.Message
                    });
                await db.SaveChangesAsync(cancellationToken);
            }

            // Ограничиваемся одним prefetch-запросом за idle-тик.
            break;
        }
    }

    private bool CanPrefetchDebtNow(string externalClientId, DateTime nowUtc)
    {
        lock (_debtPrefetchLock)
        {
            if (_debtPrefetchCooldownUntilUtc.TryGetValue(externalClientId, out var cooldownUntilUtc) &&
                cooldownUntilUtc > nowUtc)
            {
                return false;
            }

            if (_debtPrefetchCooldownUntilUtc.Count > 2048)
            {
                var staleKeys = _debtPrefetchCooldownUntilUtc
                    .Where(x => x.Value <= nowUtc)
                    .Take(512)
                    .Select(x => x.Key)
                    .ToList();
                foreach (var key in staleKeys)
                {
                    _debtPrefetchCooldownUntilUtc.Remove(key);
                }
            }

            return true;
        }
    }

    private void SetDebtPrefetchCooldown(string externalClientId, DateTime cooldownUntilUtc, DateTime nowUtc)
    {
        lock (_debtPrefetchLock)
        {
            _debtPrefetchCooldownUntilUtc[externalClientId] = cooldownUntilUtc;
            if (_debtPrefetchCooldownUntilUtc.Count <= 4096)
            {
                return;
            }

            var staleKeys = _debtPrefetchCooldownUntilUtc
                .Where(x => x.Value <= nowUtc)
                .Take(1024)
                .Select(x => x.Key)
                .ToList();
            foreach (var key in staleKeys)
            {
                _debtPrefetchCooldownUntilUtc.Remove(key);
            }
        }
    }

    private static bool IsDebtPrefetchNonRetryableFailure(string? code)
    {
        var normalizedCode = (code ?? string.Empty).Trim();
        return string.Equals(normalizedCode, "DEBT_FETCH_SETTINGS_MISSING", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(normalizedCode, "DEBT_CARD_URL_MISSING", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(normalizedCode, "CLIENT_NOT_FOUND", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsDebtCacheFresh(ClientDebtCacheRecord? cache, DateTime nowUtc)
    {
        if (cache is null)
        {
            return false;
        }

        if (!string.Equals(cache.Status, DebtStatusReady, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(cache.ExactTotalRaw))
        {
            return false;
        }

        var refreshedAtUtc = cache.LastFetchedAtUtc ?? cache.UpdatedAtUtc;
        return refreshedAtUtc >= nowUtc.AddMinutes(-LiveDebtFreshTtlMinutes);
    }

    private static string NormalizeExternalClientId(string? externalClientId)
    {
        return (externalClientId ?? string.Empty).Trim();
    }

    private async Task<DispatchAttemptResult> ExecuteAttemptAsync(
        AppDbContext db,
        AppSettingsDto settings,
        RunSessionRecord runSession,
        RunJobRecord job,
        SenderChannelRecord channel,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (string.IsNullOrWhiteSpace(job.Phone))
        {
            return new DispatchAttemptResult
            {
                Success = false,
                IsTransient = false,
                Code = "PHONE_EMPTY",
                Detail = "У клиента отсутствует номер телефона."
            };
        }

        if (string.IsNullOrWhiteSpace(channel.Endpoint) || string.IsNullOrWhiteSpace(channel.Token))
        {
            return new DispatchAttemptResult
            {
                Success = false,
                IsTransient = false,
                Code = "CHANNEL_CONFIG_INVALID",
                Detail = "У канала не задан endpoint или token."
            };
        }

        if (job.TzOffset is < -12 or > 14)
        {
            return new DispatchAttemptResult
            {
                Success = false,
                IsTransient = false,
                CountAsAttempt = false,
                Code = "TIMEZONE_OFFSET_INVALID",
                Detail = $"Некорректный timezoneOffset у клиента: {job.TzOffset}."
            };
        }

        if (!TryParseWorkWindowStrict(settings.WorkWindowStart, settings.WorkWindowEnd, out var workStart, out var workEnd))
        {
            return new DispatchAttemptResult
            {
                Success = false,
                IsTransient = false,
                CountAsAttempt = false,
                Code = "WORK_WINDOW_CONFIG_INVALID",
                Detail = "Некорректно задано рабочее окно в настройках (ожидается HH:mm, end > start)."
            };
        }

        var nowUtc = DateTime.UtcNow;
        var nextAllowedUtc = AlignToWorkWindowUtc(nowUtc, job.TzOffset, workStart, workEnd);
        if (nextAllowedUtc > nowUtc.AddSeconds(1))
        {
            return new DispatchAttemptResult
            {
                Success = false,
                IsTransient = true,
                CountAsAttempt = false,
                Code = "WORK_WINDOW_WAIT",
                Detail = $"Локальное время клиента вне рабочего окна. Следующая попытка: {nextAllowedUtc:O}",
                NextPlannedAtUtc = nextAllowedUtc
            };
        }

        var payloadTotal = ExtractPayloadString(job.PayloadJson, "totalWithCommissionRaw");
        var liveMode = string.Equals(runSession.Mode, RunModeLive, StringComparison.OrdinalIgnoreCase);
        var shouldFetchDebt = string.IsNullOrWhiteSpace(payloadTotal);

        if (liveMode)
        {
            var normalizedExternalClientId = NormalizeExternalClientId(job.ExternalClientId);
            if (!string.IsNullOrWhiteSpace(normalizedExternalClientId))
            {
                var cache = await db.ClientDebtCache
                    .AsNoTracking()
                    .FirstOrDefaultAsync(x => x.ExternalClientId == normalizedExternalClientId, cancellationToken);

                if (IsDebtCacheFresh(cache, nowUtc))
                {
                    shouldFetchDebt = false;
                    var cachedExactRaw = (cache?.ExactTotalRaw ?? string.Empty).Trim();
                    if (!string.IsNullOrWhiteSpace(cachedExactRaw) &&
                        !string.Equals(payloadTotal, cachedExactRaw, StringComparison.Ordinal))
                    {
                        job.PayloadJson = UpsertPayloadField(job.PayloadJson, "totalWithCommissionRaw", cachedExactRaw);
                        db.RunJobs.Update(job);
                        await db.SaveChangesAsync(cancellationToken);
                    }
                }
                else
                {
                    shouldFetchDebt = true;
                }
            }
            else
            {
                shouldFetchDebt = true;
            }
        }

        if (shouldFetchDebt)
        {
            var debtFetch = await debtCacheService.FetchByExternalClientIdAsync(
                db,
                job.ExternalClientId,
                settings,
                new ClientDebtFetchRequestDto
                {
                    TimeoutMs = 30000,
                    Headed = false
                },
                cancellationToken);

            if (!debtFetch.Success || string.IsNullOrWhiteSpace(debtFetch.Debt?.ExactTotalRaw))
            {
                return BuildDebtFetchFailureResult(debtFetch);
            }

            job.PayloadJson = UpsertPayloadField(job.PayloadJson, "totalWithCommissionRaw", debtFetch.Debt.ExactTotalRaw);
            db.RunJobs.Update(job);
            if (liveMode)
            {
                AddEvent(
                    db,
                    runSession.Id,
                    job.Id,
                    "debt_refreshed",
                    "info",
                    $"Задача #{job.Id}: сумма долга обновлена из карточки клиента.",
                    new
                    {
                        runSessionId = runSession.Id,
                        runJobId = job.Id,
                        externalClientId = job.ExternalClientId,
                        totalWithCommissionRaw = debtFetch.Debt.ExactTotalRaw
                    });
            }
            await db.SaveChangesAsync(cancellationToken);
        }

        var activeTemplates = await ruleEngine.GetActiveTemplatesAsync(db, cancellationToken);
        var messageOverride = ExtractPayloadString(job.PayloadJson, PayloadFieldMessageOverride);
        var usesMessageOverride = !string.IsNullOrWhiteSpace(messageOverride);
        var rendered = !usesMessageOverride
            ? ruleEngine.BuildDispatchMessage(activeTemplates, job, settings.DebtBufferAmount)
            : new RuleEngineMessageResult
            {
                TemplateId = job.TemplateId,
                MessageText = messageOverride,
                UsedFallback = false,
                ErrorCode = string.Empty,
                ErrorMessage = string.Empty
            };

        if (!string.IsNullOrWhiteSpace(rendered.ErrorCode))
        {
            job.PreviewStatus = PreviewStatusError;
            job.PreviewText = string.Empty;
            job.PreviewVariablesJson = "{}";
            job.PreviewUpdatedAtUtc = DateTime.UtcNow;
            job.PreviewErrorCode = rendered.ErrorCode;
            job.PreviewErrorDetail = rendered.ErrorMessage;
            return new DispatchAttemptResult
            {
                Success = false,
                IsTransient = false,
                CountAsAttempt = false,
                Code = rendered.ErrorCode,
                Detail = rendered.ErrorMessage
            };
        }

        var previewTemplate = rendered.TemplateId.HasValue && rendered.TemplateId.Value > 0
            ? activeTemplates.FirstOrDefault(x => x.Id == rendered.TemplateId.Value)
            : null;
        var resolvedTemplateKind = (previewTemplate?.Kind ?? string.Empty).Trim();
        var resolvedTemplateComment = (previewTemplate?.CommentText ?? string.Empty).Trim();
        var previewPayloadTotal = ExtractPayloadString(job.PayloadJson, "totalWithCommissionRaw");
        var approxDebtText = RuleEngineService.BuildApproxDebtText(job.PayloadJson, settings.DebtBufferAmount);
        job.PreviewStatus = PreviewStatusReady;
        job.PreviewText = rendered.MessageText;
        job.PreviewVariablesJson = JsonSerializer.Serialize(new
        {
            fullFio = (job.ClientFio ?? string.Empty).Trim(),
            totalWithCommissionRaw = previewPayloadTotal,
            approxDebtText,
            templateId = rendered.TemplateId,
            templateKind = previewTemplate?.Kind ?? string.Empty,
            templateName = previewTemplate?.Name ?? string.Empty,
            messageOverride = usesMessageOverride
        });
        job.PreviewUpdatedAtUtc = DateTime.UtcNow;
        job.PreviewErrorCode = string.Empty;
        job.PreviewErrorDetail = string.Empty;

        var sendResult = await _traccarSender.SendAsync(new TraccarSmsSendRequest
        {
            Url = channel.Endpoint,
            Token = channel.Token,
            To = job.Phone,
            Message = rendered.MessageText,
            TimeoutMs = 15000
        }, cancellationToken);

        if (sendResult.Success)
        {
            return new DispatchAttemptResult
            {
                Success = true,
                IsTransient = false,
                Code = "SENT",
                Detail = sendResult.Detail,
                MessageText = rendered.MessageText,
                TemplateId = rendered.TemplateId,
                TemplateKind = resolvedTemplateKind,
                TemplateCommentText = resolvedTemplateComment,
                UsedMessageOverride = usesMessageOverride,
                StatusCode = sendResult.StatusCode,
                ResponseBody = sendResult.ResponseBody,
                Error = sendResult.Error
            };
        }

        return new DispatchAttemptResult
        {
            Success = false,
            IsTransient = IsTransientGatewayError(sendResult.StatusCode),
            Code = "GATEWAY_SEND_FAILED",
            Detail = BuildGatewayFailureDetail(sendResult),
            MessageText = rendered.MessageText,
            TemplateId = rendered.TemplateId,
            TemplateKind = resolvedTemplateKind,
            TemplateCommentText = resolvedTemplateComment,
            UsedMessageOverride = usesMessageOverride,
            StatusCode = sendResult.StatusCode,
            ResponseBody = sendResult.ResponseBody,
            Error = sendResult.Error
        };
    }

    private static bool IsChannelStateNeutralError(string? code)
    {
        return string.Equals(code, "CHANNEL_UNAVAILABLE", StringComparison.Ordinal);
    }

    private static string BuildGatewayFailureDetail(TraccarSmsSendResult sendResult)
    {
        var normalizedDetail = TruncateForLog(sendResult.Detail, 240);
        var normalizedBody = TruncateForLog(sendResult.ResponseBody, 240);
        var normalizedError = TruncateForLog(sendResult.Error, 200);
        var statusPart = sendResult.StatusCode > 0 ? $"HTTP {sendResult.StatusCode}" : string.Empty;

        var parts = new[]
        {
            normalizedDetail,
            statusPart,
            string.IsNullOrWhiteSpace(normalizedBody) ? string.Empty : $"Ответ gateway: {normalizedBody}",
            string.IsNullOrWhiteSpace(normalizedError) ? string.Empty : $"Ошибка транспорта: {normalizedError}"
        };

        var composed = string.Join(
            ". ",
            parts.Where(x => !string.IsNullOrWhiteSpace(x)));

        return string.IsNullOrWhiteSpace(composed)
            ? "Ошибка отправки через gateway."
            : composed;
    }

    private static string TruncateForLog(string? value, int maxLen)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        var normalized = value
            .Replace('\r', ' ')
            .Replace('\n', ' ')
            .Trim();
        if (normalized.Length > maxLen)
        {
            normalized = $"{normalized[..maxLen]}...";
        }

        return normalized;
    }

    private static string BuildLogDetailSuffix(string? detail)
    {
        var raw = TruncateForLog(detail, 240);
        if (string.IsNullOrWhiteSpace(raw))
        {
            return string.Empty;
        }

        return $" Детали: {raw}";
    }

    private static bool IsTransientGatewayError(int statusCode)
    {
        if (statusCode <= 0) return true;
        return statusCode == 408 || statusCode == 429 || statusCode >= 500;
    }

    private static DispatchAttemptResult BuildDebtFetchFailureResult(ClientDebtFetchResultDto result)
    {
        var code = string.IsNullOrWhiteSpace(result.Code) ? "DEBT_FETCH_FAILED" : result.Code.Trim();
        var detail = string.IsNullOrWhiteSpace(result.Message)
            ? "Не удалось получить сумму долга из карточки клиента."
            : result.Message.Trim();

        var nonRetryable = string.Equals(code, "DEBT_FETCH_SETTINGS_MISSING", StringComparison.OrdinalIgnoreCase) ||
                           string.Equals(code, "DEBT_CARD_URL_MISSING", StringComparison.OrdinalIgnoreCase) ||
                           string.Equals(code, "CLIENT_NOT_FOUND", StringComparison.OrdinalIgnoreCase);

        return new DispatchAttemptResult
        {
            Success = false,
            IsTransient = !nonRetryable,
            CountAsAttempt = !nonRetryable,
            Code = code,
            Detail = detail
        };
    }

    private static string BuildChannelsSignature(IReadOnlyList<SenderChannelRecord> availableChannels)
    {
        if (availableChannels.Count == 0)
        {
            return "none";
        }

        return string.Join(
            "|",
            availableChannels
                .OrderBy(x => x.Id)
                .Select(x => $"{x.Id}:{x.Status}:{x.FailStreak}"));
    }

    private static TimeOnly ParseHm(string? raw, TimeOnly fallback)
    {
        if (TimeOnly.TryParseExact(
                (raw ?? string.Empty).Trim(),
                "HH:mm",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var parsed))
        {
            return parsed;
        }

        return fallback;
    }

    private static bool TryParseWorkWindowStrict(
        string? startRaw,
        string? endRaw,
        out TimeOnly start,
        out TimeOnly end)
    {
        start = default;
        end = default;

        if (!TimeOnly.TryParseExact(
                (startRaw ?? string.Empty).Trim(),
                "HH:mm",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out start))
        {
            return false;
        }

        if (!TimeOnly.TryParseExact(
                (endRaw ?? string.Empty).Trim(),
                "HH:mm",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out end))
        {
            return false;
        }

        return end > start;
    }

    private static DateTime AlignToWorkWindowUtc(DateTime utc, int tzOffsetFromMoscow, TimeOnly start, TimeOnly end)
    {
        return GetWindowSlotUtc(utc, tzOffsetFromMoscow, start, end);
    }

    private static DateTime GetWindowSlotUtc(DateTime utc, int tzOffsetFromMoscow, TimeOnly start, TimeOnly end)
    {
        // timezone_offset в snapshot задается относительно Москвы.
        var clientUtcOffset = TimeSpan.FromHours(3 + tzOffsetFromMoscow);
        var local = utc + clientUtcOffset;
        var localDate = DateOnly.FromDateTime(local);
        var localTime = TimeOnly.FromDateTime(local);

        DateTime localPlanned;
        if (localTime < start)
        {
            localPlanned = localDate.ToDateTime(start);
        }
        else if (localTime >= end)
        {
            localPlanned = localDate.AddDays(1).ToDateTime(start);
        }
        else
        {
            localPlanned = local;
        }

        return DateTime.SpecifyKind(localPlanned - clientUtcOffset, DateTimeKind.Utc);
    }

    private static string ExtractPayloadString(string? payloadJson, string propertyName)
    {
        if (string.IsNullOrWhiteSpace(payloadJson) || string.IsNullOrWhiteSpace(propertyName))
        {
            return string.Empty;
        }

        try
        {
            using var doc = JsonDocument.Parse(payloadJson);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
            {
                return string.Empty;
            }

            if (!doc.RootElement.TryGetProperty(propertyName, out var node))
            {
                return string.Empty;
            }

            if (node.ValueKind == JsonValueKind.String)
            {
                return (node.GetString() ?? string.Empty).Trim();
            }

            if (node.ValueKind == JsonValueKind.Number)
            {
                return node.GetRawText().Trim();
            }
        }
        catch
        {
            return string.Empty;
        }

        return string.Empty;
    }

    private static string UpsertPayloadField(string? payloadJson, string propertyName, string value)
    {
        JsonObject root;
        try
        {
            root = JsonNode.Parse(payloadJson ?? string.Empty) as JsonObject ?? new JsonObject();
        }
        catch
        {
            root = new JsonObject();
        }

        root[propertyName] = value;
        return root.ToJsonString();
    }

    private sealed record DispatchChannelChoice(SenderChannelRecord Channel, DateTime NextAvailableAtUtc);
    private sealed class CommentWriteOutcome
    {
        public bool Attempted { get; init; }
        public bool Success { get; init; }
        public string Code { get; init; } = string.Empty;
        public string Detail { get; init; } = string.Empty;

        public static CommentWriteOutcome NotAttempted(string code, string detail)
        {
            return new CommentWriteOutcome
            {
                Attempted = false,
                Success = false,
                Code = code,
                Detail = detail
            };
        }

        public static CommentWriteOutcome AttemptSuccess(string code, string detail)
        {
            return new CommentWriteOutcome
            {
                Attempted = true,
                Success = true,
                Code = code,
                Detail = detail
            };
        }

        public static CommentWriteOutcome AttemptFailed(string code, string detail)
        {
            return new CommentWriteOutcome
            {
                Attempted = true,
                Success = false,
                Code = code,
                Detail = detail
            };
        }
    }

    private sealed class DispatchAttemptResult
    {
        public bool Success { get; init; }
        public bool IsTransient { get; init; }
        public bool CountAsAttempt { get; init; } = true;
        public string Code { get; init; } = string.Empty;
        public string Detail { get; init; } = string.Empty;
        public string MessageText { get; init; } = string.Empty;
        public long? TemplateId { get; init; }
        public string TemplateKind { get; init; } = string.Empty;
        public string TemplateCommentText { get; init; } = string.Empty;
        public bool UsedMessageOverride { get; init; }
        public int StatusCode { get; init; }
        public string ResponseBody { get; init; } = string.Empty;
        public string Error { get; init; } = string.Empty;
        public DateTime? NextPlannedAtUtc { get; init; }
    }

}
