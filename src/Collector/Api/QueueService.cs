using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Collector.Data;
using Collector.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed partial class QueueService(
    RuleEngineService ruleEngine,
    SettingsStore settingsStore,
    RunLifecycleCoordinator lifecycleCoordinator)
{
    private const string ChannelStatusError = "error";
    private const string ChannelStatusOffline = "offline";
    private const string ChannelStatusOnline = "online";
    private const string RunModeLive = SnapshotModes.Live;
    private const string RunStatusPlanned = RunSessionStatuses.Planned;
    private const string RunStatusRunning = RunSessionStatuses.Running;
    private const string RunStatusStopped = RunSessionStatuses.Stopped;
    private const string RunStatusCompleted = RunSessionStatuses.Completed;
    private const string JobStatusQueued = RunJobStatuses.Queued;
    private const string JobStatusRunning = RunJobStatuses.Running;
    private const string JobStatusRetry = RunJobStatuses.Retry;
    private const string JobStatusStopped = RunJobStatuses.Stopped;
    private const string JobStatusFailed = RunJobStatuses.Failed;
    private const string DeliveryTypeSms = DeliveryTypes.Sms;
    private const int DefaultMaxAttempts = 3;
    private const string PreviewStatusEmpty = PreviewStatuses.Empty;
    private const string PreviewStatusReady = PreviewStatuses.Ready;
    private const string PreviewStatusNeedsDebt = PreviewStatuses.NeedsDebt;
    private const string PreviewStatusError = PreviewStatuses.Error;
    private const string PayloadFieldMessageOverride = PayloadFields.MessageOverrideText;
    private const string QueueExactOverdueLegacyErrorCode = "QUEUE_FILTER_EXACT_DAY_INVALID";
    private const string MessageDirectionOut = MessageDirections.Out;
    private const string MessageGatewayStatusSent = MessageGatewayStatuses.Sent;

    public static ApiErrorDto? ValidateRequest(QueueFilterRequestDto? payload)
    {
        if (payload is null)
        {
            return new ApiErrorDto
            {
                Code = "CFG_REQUIRED_MISSING",
                Message = "Тело запроса пустое."
            };
        }

        var timezoneOffsets = (payload.TimezoneOffsets ?? [])
            .Distinct()
            .ToList();
        if (timezoneOffsets.Any(x => x is < -12 or > 14))
        {
            return new ApiErrorDto
            {
                Code = "QUEUE_FILTER_TZ_INVALID",
                Message = "Часовые пояса должны быть в диапазоне от -12 до +14."
            };
        }

        var exactOverdueError = TryParseExactOverdueFilter(payload.ExactOverdue, payload.ExactDay, out _, out var normalizedExactOverdue);
        if (exactOverdueError is not null)
        {
            return exactOverdueError;
        }

        var overdueRangeError = TryParseRanges(payload.OverdueRanges, out _);
        if (overdueRangeError is not null)
        {
            return overdueRangeError;
        }

        var hasTimezone = timezoneOffsets.Count > 0;
        var hasRanges = (payload.OverdueRanges ?? []).Any(x => !string.IsNullOrWhiteSpace(x));
        var hasExactDay = !string.IsNullOrWhiteSpace(normalizedExactOverdue);
        if (!hasTimezone && !hasRanges && !hasExactDay)
        {
            return new ApiErrorDto
            {
                Code = "QUEUE_FILTER_EMPTY",
                Message = "Нужно выбрать хотя бы один фильтр: часовой пояс, диапазон просрочки или точный день."
            };
        }

        return null;
    }

    public static ApiErrorDto? ValidateRemoveJobsRequest(QueueRemoveJobsRequestDto? payload)
    {
        if (payload is null)
        {
            return new ApiErrorDto
            {
                Code = "CFG_REQUIRED_MISSING",
                Message = "Тело запроса пустое."
            };
        }

        if (payload.RunSessionId.HasValue && payload.RunSessionId.Value <= 0)
        {
            return new ApiErrorDto
            {
                Code = "QUEUE_RUN_SESSION_ID_INVALID",
                Message = "runSessionId должен быть положительным числом."
            };
        }

        var jobIds = (payload.JobIds ?? [])
            .Where(x => x > 0)
            .Distinct()
            .ToList();
        if (jobIds.Count == 0)
        {
            return new ApiErrorDto
            {
                Code = "QUEUE_JOB_IDS_EMPTY",
                Message = "Нужно передать хотя бы один jobId для удаления из плана."
            };
        }

        if (jobIds.Count > 5000)
        {
            return new ApiErrorDto
            {
                Code = "QUEUE_JOB_IDS_TOO_MANY",
                Message = "Слишком много jobId в одном запросе (максимум 5000)."
            };
        }

        return null;
    }

    public static ApiErrorDto? ValidateRetryErrorsRequest(QueueRetryErrorsRequestDto? payload)
    {
        if (payload is null)
        {
            return null;
        }

        if (payload.RunSessionId.HasValue && payload.RunSessionId.Value <= 0)
        {
            return new ApiErrorDto
            {
                Code = "QUEUE_RUN_SESSION_ID_INVALID",
                Message = "runSessionId должен быть положительным числом."
            };
        }

        return null;
    }

    public async Task<QueuePreviewDto> PreviewAsync(AppDbContext db, QueueFilterRequestDto payload, CancellationToken cancellationToken)
    {
        var selection = await SelectRowsAsync(db, payload, cancellationToken);
        return selection.Preview;
    }

    public async Task<QueueForecastDto> ForecastAsync(
        AppDbContext db,
        QueueFilterRequestDto payload,
        AppSettingsDto settings,
        CancellationToken cancellationToken)
    {
        var selection = await SelectRowsAsync(db, payload, cancellationToken);
        var now = DateTime.UtcNow;
        var availableChannels = await GetAvailableChannelsForPlanningAsync(db, cancellationToken);
        var bindings = await GetBindingsByExternalClientIdAsync(db, selection.ReadyRows, cancellationToken);
        var channelCounts = await GetChannelCountsForForecastAsync(db, cancellationToken);
        var schedule = BuildSchedule(selection.ReadyRows, settings, now, availableChannels, bindings);

        return new QueueForecastDto
        {
            Preview = selection.Preview,
            OnlineChannelsCount = channelCounts.OnlineCount,
            ChannelsUsed = schedule.ChannelsUsed,
            GapMinutes = schedule.GapMinutes,
            GapWaitMinutes = schedule.GapWaitMinutes,
            TimezoneWaitMinutes = schedule.TimezoneWaitMinutes,
            TotalWaitMinutes = schedule.TotalWaitMinutes,
            EstimatedDurationMinutes = schedule.EstimatedDurationMinutes,
            EstimatedFinishAtUtc = schedule.EstimatedFinishAtUtc,
            ForecastedAtUtc = now
        };
    }

    public async Task<QueueBuildResultDto> BuildAsync(
        AppDbContext db,
        QueueFilterRequestDto payload,
        AppSettingsDto settings,
        CancellationToken cancellationToken)
    {
        await lifecycleCoordinator.WaitAsync(cancellationToken);
        try
        {
            var runningSessionId = await db.RunSessions
                .AsNoTracking()
                .Where(x => x.Status == RunStatusRunning)
                .OrderByDescending(x => x.Id)
                .Select(x => (long?)x.Id)
                .FirstOrDefaultAsync(cancellationToken);
            if (runningSessionId.HasValue && runningSessionId.Value > 0)
            {
                throw new QueueStateException(
                    "QUEUE_BUILD_BLOCKED_BY_RUNNING_SESSION",
                    $"Нельзя формировать новую очередь во время активного запуска сессии #{runningSessionId.Value}. Сначала остановите её.",
                    409);
            }

            var selection = await SelectRowsAsync(db, payload, cancellationToken);
            var now = DateTime.UtcNow;
            var availableChannels = await GetAvailableChannelsForPlanningAsync(db, cancellationToken);
            var bindings = await GetBindingsByExternalClientIdAsync(db, selection.ReadyRows, cancellationToken);
            var channelCounts = await GetChannelCountsForForecastAsync(db, cancellationToken);
            var schedule = BuildSchedule(selection.ReadyRows, settings, now, availableChannels, bindings);
            var activeTemplates = await ruleEngine.GetActiveTemplatesAsync(db, cancellationToken);
            var debtCacheByExternalClientId = await GetDebtCacheByExternalClientIdAsync(db, selection.ReadyRows, cancellationToken);
            await using var tx = await db.Database.BeginTransactionAsync(cancellationToken);

            var runSession = new RunSessionRecord
            {
                Mode = selection.Snapshot.SourceMode,
                Status = RunStatusPlanned,
                CreatedAtUtc = now,
                StartedAtUtc = null,
                FinishedAtUtc = null,
                SnapshotId = selection.Snapshot.Id,
                FiltersJson = JsonSerializer.Serialize(selection.Preview.AppliedFilter),
                Notes = "Плановая очередь сформирована из snapshot."
            };

            db.RunSessions.Add(runSession);
            await db.SaveChangesAsync(cancellationToken);

            var jobs = selection.ReadyRows
                .Zip(schedule.Items, (row, item) =>
                {
                    var templateId = ruleEngine.ResolveAutoTemplateId(activeTemplates, row.DaysOverdue);
                    var exactDebtRaw = FirstNonEmpty(
                        debtCacheByExternalClientId.TryGetValue(row.ExternalClientId, out var debtCache)
                            ? debtCache.ExactTotalRaw
                            : null,
                        row.TotalWithCommissionRaw);

                    var job = new RunJobRecord
                    {
                        RunSessionId = runSession.Id,
                        ExternalClientId = row.ExternalClientId,
                        ClientFio = row.Fio,
                        Phone = row.Phone,
                        TzOffset = row.TimezoneOffset,
                        DaysOverdue = row.DaysOverdue,
                        TemplateId = templateId,
                        ChannelId = item.ChannelId > 0 ? item.ChannelId : null,
                        DeliveryType = DeliveryTypeSms,
                        Status = JobStatusQueued,
                        Attempts = 0,
                        MaxAttempts = DefaultMaxAttempts,
                        PlannedAtUtc = item.PlannedAtUtc,
                        SentAtUtc = null,
                        LastErrorCode = null,
                        LastErrorDetail = null,
                        PayloadJson = BuildJobPayloadJson(
                            row.CardUrl,
                            exactDebtRaw)
                    };

                    var preview = BuildPreviewForJob(job, activeTemplates, settings, now);
                    ApplyPreviewToJob(job, preview);
                    return job;
                })
                .ToList();

            if (jobs.Count > 0)
            {
                db.RunJobs.AddRange(jobs);
                await db.SaveChangesAsync(cancellationToken);
            }

            await UpsertBindingsFromScheduleAsync(db, selection.ReadyRows, schedule, now, cancellationToken);
            await db.SaveChangesAsync(cancellationToken);

            await tx.CommitAsync(cancellationToken);

            return new QueueBuildResultDto
            {
                RunSessionId = runSession.Id,
                Status = runSession.Status,
                CreatedAtUtc = runSession.CreatedAtUtc,
                CreatedJobs = jobs.Count,
                Preview = selection.Preview,
                Forecast = new QueueForecastDto
                {
                    Preview = selection.Preview,
                    OnlineChannelsCount = channelCounts.OnlineCount,
                    ChannelsUsed = schedule.ChannelsUsed,
                    GapMinutes = schedule.GapMinutes,
                    GapWaitMinutes = schedule.GapWaitMinutes,
                    TimezoneWaitMinutes = schedule.TimezoneWaitMinutes,
                    TotalWaitMinutes = schedule.TotalWaitMinutes,
                    EstimatedDurationMinutes = schedule.EstimatedDurationMinutes,
                    EstimatedFinishAtUtc = schedule.EstimatedFinishAtUtc,
                    ForecastedAtUtc = now
                }
            };
        }
        finally
        {
            lifecycleCoordinator.Release();
        }
    }

    public async Task<QueueListDto> ListAsync(
        AppDbContext db,
        long? runSessionId,
        int limit,
        int offset,
        CancellationToken cancellationToken)
    {
        RunSessionRecord? session;
        if (runSessionId.HasValue && runSessionId.Value > 0)
        {
            session = await db.RunSessions
                .AsNoTracking()
                .FirstOrDefaultAsync(x => x.Id == runSessionId.Value, cancellationToken);
            if (session is null)
            {
                throw new KeyNotFoundException($"Run session с id={runSessionId.Value} не найден.");
            }
        }
        else
        {
            session = await db.RunSessions
                .AsNoTracking()
                .OrderByDescending(x => x.Id)
                .FirstOrDefaultAsync(cancellationToken);
            if (session is null)
            {
                return new QueueListDto { HasSession = false };
            }
        }

        var totalJobs = await db.RunJobs
            .AsNoTracking()
            .CountAsync(x => x.RunSessionId == session.Id, cancellationToken);

        var settings = await settingsStore.GetAsync(db, cancellationToken);

        var rows = await db.RunJobs
            .AsNoTracking()
            .Where(x => x.RunSessionId == session.Id)
            .OrderBy(x => x.PlannedAtUtc)
            .ThenBy(x => x.Id)
            .Skip(offset)
            .Take(limit)
            .ToListAsync(cancellationToken);

        var templateIds = rows
            .Where(x => x.TemplateId.HasValue && x.TemplateId.Value > 0)
            .Select(x => x.TemplateId!.Value)
            .Distinct()
            .ToList();
        var templatesById = templateIds.Count == 0
            ? new Dictionary<long, TemplateRecord>()
            : await db.Templates
                .AsNoTracking()
                .Where(x => templateIds.Contains(x.Id))
                .ToDictionaryAsync(x => x.Id, cancellationToken);

        var channelIds = rows
            .Where(x => x.ChannelId.HasValue && x.ChannelId.Value > 0)
            .Select(x => x.ChannelId!.Value)
            .Distinct()
            .ToList();
        var channelsById = channelIds.Count == 0
            ? new Dictionary<long, SenderChannelRecord>()
            : await db.SenderChannels
                .AsNoTracking()
                .Where(x => channelIds.Contains(x.Id))
                .ToDictionaryAsync(x => x.Id, cancellationToken);

        var externalClientIds = rows
            .Select(x => x.ExternalClientId)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .ToList();
        var debtCacheByExternalClientId = externalClientIds.Count == 0
            ? new Dictionary<string, ClientDebtCacheRecord>(StringComparer.Ordinal)
            : await db.ClientDebtCache
                .AsNoTracking()
                .Where(x => externalClientIds.Contains(x.ExternalClientId))
                .ToDictionaryAsync(x => x.ExternalClientId, cancellationToken);

        var phones = rows
            .Select(x => NormalizePhone(x.Phone))
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .ToList();
        var dialogCountsByPhone = phones.Count == 0
            ? new Dictionary<string, int>(StringComparer.Ordinal)
            : (await db.Messages
                    .AsNoTracking()
                    .Where(x => phones.Contains(x.ClientPhone))
                    .GroupBy(x => x.ClientPhone)
                    .Select(g => new { Phone = g.Key, Count = g.Count() })
                    .ToListAsync(cancellationToken))
                .ToDictionary(x => x.Phone, x => x.Count, StringComparer.Ordinal);

        return new QueueListDto
        {
            HasSession = true,
            Session = new QueueSessionDto
            {
                Id = session.Id,
                Mode = session.Mode,
                Status = session.Status,
                CreatedAtUtc = session.CreatedAtUtc,
                StartedAtUtc = session.StartedAtUtc,
                FinishedAtUtc = session.FinishedAtUtc,
                SnapshotId = session.SnapshotId,
                FiltersJson = session.FiltersJson ?? string.Empty,
                Notes = session.Notes ?? string.Empty
            },
            TotalJobsInSession = totalJobs,
            Items = rows
                .Select(x =>
                    MapJobToDto(
                        x,
                        templatesById,
                        channelsById,
                        debtCacheByExternalClientId.TryGetValue(x.ExternalClientId, out var debtCache) ? debtCache : null,
                        dialogCountsByPhone,
                        settings.DebtBufferAmount))
                .ToList()
        };
    }

    public async Task<QueueRemoveJobsResultDto> RemoveJobsFromPlanAsync(
        AppDbContext db,
        QueueRemoveJobsRequestDto payload,
        CancellationToken cancellationToken)
    {
        var requestedJobIds = (payload.JobIds ?? [])
            .Where(x => x > 0)
            .Distinct()
            .ToList();
        if (requestedJobIds.Count == 0)
        {
            throw new InvalidOperationException("Список задач для удаления пуст.");
        }

        RunSessionRecord? session;
        if (payload.RunSessionId.HasValue && payload.RunSessionId.Value > 0)
        {
            session = await db.RunSessions
                .FirstOrDefaultAsync(x => x.Id == payload.RunSessionId.Value, cancellationToken);
        }
        else
        {
            session = await db.RunSessions
                .OrderByDescending(x => x.Id)
                .FirstOrDefaultAsync(cancellationToken);
        }

        if (session is null)
        {
            throw new KeyNotFoundException("Сессия очереди не найдена.");
        }

        var canEditPlan =
            string.Equals(session.Status, RunStatusPlanned, StringComparison.OrdinalIgnoreCase) ||
            string.Equals(session.Status, RunStatusRunning, StringComparison.OrdinalIgnoreCase);
        if (!canEditPlan)
        {
            throw new InvalidOperationException("Удаление из плана доступно только для сессии со статусом \"planned\" или \"running\".");
        }

        var removableStatuses = new[] { JobStatusQueued, JobStatusRetry, JobStatusStopped };
        var removableQuery = db.RunJobs
            .Where(x =>
                x.RunSessionId == session.Id &&
                requestedJobIds.Contains(x.Id) &&
                removableStatuses.Contains(x.Status));
        var removed = await removableQuery.ExecuteDeleteAsync(cancellationToken);

        var remaining = await db.RunJobs
            .AsNoTracking()
            .CountAsync(x => x.RunSessionId == session.Id, cancellationToken);

        return new QueueRemoveJobsResultDto
        {
            RunSessionId = session.Id,
            Requested = requestedJobIds.Count,
            Removed = removed,
            Skipped = requestedJobIds.Count - removed,
            RemainingJobs = remaining,
            SessionStatus = session.Status
        };
    }

    public async Task<QueueRetryErrorsResultDto> RetryErrorsAsync(
        AppDbContext db,
        QueueRetryErrorsRequestDto payload,
        CancellationToken cancellationToken)
    {
        await lifecycleCoordinator.WaitAsync(cancellationToken);
        try
        {
            RunSessionRecord? session;
            if (payload.RunSessionId.HasValue && payload.RunSessionId.Value > 0)
            {
                session = await db.RunSessions
                    .FirstOrDefaultAsync(x => x.Id == payload.RunSessionId.Value, cancellationToken);
            }
            else
            {
                session = await db.RunSessions
                    .OrderByDescending(x => x.Id)
                    .FirstOrDefaultAsync(cancellationToken);
            }

            if (session is null)
            {
                throw new KeyNotFoundException("Сессия очереди не найдена.");
            }

            if (string.Equals(session.Status, RunStatusRunning, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException("Переотправка ошибок недоступна во время активного запуска.");
            }

            var retryableJobs = await db.RunJobs
                .Where(x => x.RunSessionId == session.Id)
                .Where(x => x.Status == JobStatusFailed || x.Status == JobStatusStopped)
                .ToListAsync(cancellationToken);

            var fromFailed = retryableJobs.Count(x => string.Equals(x.Status, JobStatusFailed, StringComparison.OrdinalIgnoreCase));
            var fromStopped = retryableJobs.Count - fromFailed;
            var nowUtc = DateTime.UtcNow;

            foreach (var job in retryableJobs)
            {
                job.Status = JobStatusRetry;
                job.Attempts = 0;
                job.SentAtUtc = null;
                job.PlannedAtUtc = nowUtc;
                job.LastErrorCode = null;
                job.LastErrorDetail = null;
            }

            if (retryableJobs.Count > 0)
            {
                var settings = await settingsStore.GetAsync(db, cancellationToken);
                await RebuildPersistedPreviewsForJobsAsync(
                    db,
                    retryableJobs,
                    settings,
                    cancellationToken,
                    saveChanges: false);

                if (string.Equals(session.Status, RunStatusCompleted, StringComparison.OrdinalIgnoreCase))
                {
                    session.Status = RunStatusPlanned;
                    session.FinishedAtUtc = null;
                    session.Notes = "После переотправки ошибок сессия снова подготовлена к запуску.";
                    db.RunSessions.Update(session);
                }

                db.RunJobs.UpdateRange(retryableJobs);
                await db.SaveChangesAsync(cancellationToken);
            }

            var remainingFailed = await db.RunJobs
                .AsNoTracking()
                .CountAsync(x => x.RunSessionId == session.Id &&
                                 x.Status == JobStatusFailed,
                    cancellationToken);
            var remainingStopped = await db.RunJobs
                .AsNoTracking()
                .CountAsync(x => x.RunSessionId == session.Id &&
                                 x.Status == JobStatusStopped,
                    cancellationToken);

            return new QueueRetryErrorsResultDto
            {
                RunSessionId = session.Id,
                Retried = retryableJobs.Count,
                FromFailed = fromFailed,
                FromStopped = fromStopped,
                RemainingFailed = remainingFailed,
                RemainingStopped = remainingStopped,
                SessionStatus = session.Status
            };
        }
        finally
        {
            lifecycleCoordinator.Release();
        }
    }

    public async Task<QueueBulkSetTemplateResultDto> BulkSetTemplateAsync(
        AppDbContext db,
        QueueBulkSetTemplateRequest payload,
        CancellationToken cancellationToken)
    {
        var requestedJobIds = (payload.JobIds ?? [])
            .Where(x => x > 0)
            .Distinct()
            .ToList();
        if (requestedJobIds.Count == 0)
        {
            throw new InvalidOperationException("Список задач для назначения шаблона пуст.");
        }

        if (payload.TemplateId <= 0)
        {
            throw new InvalidOperationException("templateId должен быть положительным числом.");
        }

        RunSessionRecord? session;
        if (payload.RunSessionId.HasValue && payload.RunSessionId.Value > 0)
        {
            session = await db.RunSessions
                .FirstOrDefaultAsync(x => x.Id == payload.RunSessionId.Value, cancellationToken);
        }
        else
        {
            session = await db.RunSessions
                .OrderByDescending(x => x.Id)
                .FirstOrDefaultAsync(cancellationToken);
        }

        if (session is null)
        {
            throw new KeyNotFoundException("Сессия очереди не найдена.");
        }

        if (!string.Equals(session.Status, RunStatusPlanned, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("Назначение шаблона доступно только для сессии со статусом \"planned\".");
        }

        var template = await db.Templates
            .AsNoTracking()
            .FirstOrDefaultAsync(x => x.Id == payload.TemplateId && x.Status == "active", cancellationToken);
        if (template is null)
        {
            throw new KeyNotFoundException($"Активный шаблон с id={payload.TemplateId} не найден.");
        }

        var jobs = await db.RunJobs
            .Where(x => x.RunSessionId == session.Id && requestedJobIds.Contains(x.Id))
            .Where(x => string.Equals(x.Status, JobStatusQueued, StringComparison.OrdinalIgnoreCase) ||
                        string.Equals(x.Status, JobStatusRetry, StringComparison.OrdinalIgnoreCase))
            .ToListAsync(cancellationToken);

        var skippedReasons = new List<string>();
        var toUpdate = new List<RunJobRecord>();
        foreach (var job in jobs)
        {
            if (!TemplateService.IsTemplateEligibleForOverdue(template, job.DaysOverdue, allowManualOnly: true))
            {
                skippedReasons.Add($"job #{job.Id}: шаблон «{template.Name}» не подходит для просрочки {job.DaysOverdue} дней");
                continue;
            }
            job.TemplateId = payload.TemplateId;
            toUpdate.Add(job);
        }

        if (toUpdate.Count > 0)
        {
            var now = DateTime.UtcNow;
            var activeTemplates = await ruleEngine.GetActiveTemplatesAsync(db, cancellationToken);
            var settings = await settingsStore.GetAsync(db, cancellationToken);
            foreach (var job in toUpdate)
            {
                var preview = BuildPreviewForJob(job, activeTemplates, settings, now);
                ApplyPreviewToJob(job, preview);
            }
            db.RunJobs.UpdateRange(toUpdate);
            await db.SaveChangesAsync(cancellationToken);
        }

        return new QueueBulkSetTemplateResultDto
        {
            RunSessionId = session.Id,
            Requested = requestedJobIds.Count,
            Applied = toUpdate.Count,
            Skipped = requestedJobIds.Count - toUpdate.Count,
            SkippedReasons = skippedReasons
        };
    }

    public static ApiErrorDto? ValidateBulkSetTemplateRequest(QueueBulkSetTemplateRequest? payload)
    {
        if (payload is null)
        {
            return new ApiErrorDto { Code = "CFG_REQUIRED_MISSING", Message = "Тело запроса пустое." };
        }
        if (payload.TemplateId <= 0)
        {
            return new ApiErrorDto { Code = "QUEUE_TEMPLATE_ID_INVALID", Message = "templateId должен быть положительным числом." };
        }
        var jobIds = (payload.JobIds ?? []).Where(x => x > 0).Distinct().ToList();
        if (jobIds.Count == 0)
        {
            return new ApiErrorDto { Code = "QUEUE_JOB_IDS_EMPTY", Message = "Нужно передать хотя бы один jobId." };
        }
        if (jobIds.Count > 5000)
        {
            return new ApiErrorDto { Code = "QUEUE_JOB_IDS_TOO_MANY", Message = "Слишком много jobId (максимум 5000)." };
        }
        return null;
    }

    public async Task<QueueJobPreviewDto> RebuildJobPreviewAsync(
        AppDbContext db,
        long jobId,
        QueueJobPreviewRequestDto request,
        CancellationToken cancellationToken)
    {
        var job = await db.RunJobs
            .FirstOrDefaultAsync(x => x.Id == jobId, cancellationToken);
        if (job is null)
        {
            throw new KeyNotFoundException($"Задача очереди с id={jobId} не найдена.");
        }

        if (string.IsNullOrWhiteSpace(TryExtractPayloadTotal(job.PayloadJson)))
        {
            var debtCache = await db.ClientDebtCache
                .AsNoTracking()
                .FirstOrDefaultAsync(
                    x => x.ExternalClientId == job.ExternalClientId &&
                         x.Status == DebtStatuses.Ready &&
                         !string.IsNullOrWhiteSpace(x.ExactTotalRaw),
                    cancellationToken);
            if (debtCache is not null)
            {
                job.PayloadJson = UpsertPayloadString(job.PayloadJson, PayloadFields.TotalWithCommissionRaw, debtCache.ExactTotalRaw);
            }
        }

        var now = DateTime.UtcNow;
        var activeTemplates = await ruleEngine.GetActiveTemplatesAsync(db, cancellationToken);
        var settings = await settingsStore.GetAsync(db, cancellationToken);
        var preview = BuildPreviewForJob(job, activeTemplates, settings, now);

        if (request.Persist)
        {
            ApplyPreviewToJob(job, preview);
            db.RunJobs.Update(job);
            await db.SaveChangesAsync(cancellationToken);
        }

        return new QueueJobPreviewDto
        {
            JobId = job.Id,
            RunSessionId = job.RunSessionId,
            ExternalClientId = job.ExternalClientId,
            Phone = job.Phone,
            TemplateId = preview.TemplateId,
            TemplateName = preview.TemplateName,
            TemplateKind = preview.TemplateKind,
            Status = preview.Status,
            Text = preview.Text,
            VariablesJson = preview.VariablesJson,
            UpdatedAtUtc = preview.UpdatedAtUtc,
            ErrorCode = preview.ErrorCode,
            ErrorDetail = preview.ErrorDetail
        };
    }

    public async Task<int> RebuildPersistedPreviewsForOpenSessionsAsync(
        AppDbContext db,
        AppSettingsDto settings,
        CancellationToken cancellationToken)
    {
        var editableSessionStatuses = new[] { RunStatusPlanned, RunStatusRunning, RunStatusStopped };
        var previewableJobStatuses = new[] { JobStatusQueued, JobStatusRetry, JobStatusStopped };

        var openSessionIds = await db.RunSessions
            .AsNoTracking()
            .Where(x => editableSessionStatuses.Contains(x.Status))
            .Select(x => x.Id)
            .ToListAsync(cancellationToken);
        if (openSessionIds.Count == 0)
        {
            return 0;
        }

        var jobs = await db.RunJobs
            .Where(x => openSessionIds.Contains(x.RunSessionId))
            .Where(x => previewableJobStatuses.Contains(x.Status))
            .ToListAsync(cancellationToken);
        return await RebuildPersistedPreviewsForJobsAsync(db, jobs, settings, cancellationToken);
    }

    public async Task<int> RebuildPersistedPreviewsForExternalClientIdAsync(
        AppDbContext db,
        string externalClientId,
        AppSettingsDto settings,
        CancellationToken cancellationToken,
        bool saveChanges = true)
    {
        var normalizedExternalClientId = (externalClientId ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(normalizedExternalClientId))
        {
            return 0;
        }

        var previewableJobStatuses = new[] { JobStatusQueued, JobStatusRetry, JobStatusStopped, JobStatusRunning };
        var jobs = await db.RunJobs
            .Where(x => x.ExternalClientId == normalizedExternalClientId)
            .Where(x => previewableJobStatuses.Contains(x.Status))
            .ToListAsync(cancellationToken);

        return await RebuildPersistedPreviewsForJobsAsync(
            db,
            jobs,
            settings,
            cancellationToken,
            saveChanges);
    }

    public async Task<int> RebuildPersistedPreviewsForTemplateChangeAsync(
        AppDbContext db,
        AppSettingsDto settings,
        long templateId,
        bool includeFallbackCandidates,
        CancellationToken cancellationToken)
    {
        if (templateId <= 0)
        {
            return 0;
        }

        var editableSessionStatuses = new[] { RunStatusPlanned, RunStatusRunning, RunStatusStopped };
        var previewableJobStatuses = new[] { JobStatusQueued, JobStatusRetry, JobStatusStopped };

        var openSessionIds = await db.RunSessions
            .AsNoTracking()
            .Where(x => editableSessionStatuses.Contains(x.Status))
            .Select(x => x.Id)
            .ToListAsync(cancellationToken);
        if (openSessionIds.Count == 0)
        {
            return 0;
        }

        IQueryable<RunJobRecord> query = db.RunJobs
            .Where(x => openSessionIds.Contains(x.RunSessionId))
            .Where(x => previewableJobStatuses.Contains(x.Status));

        if (!includeFallbackCandidates)
        {
            query = query.Where(x => x.TemplateId == templateId);
        }
        else
        {
            var activeTemplateIds = await db.Templates
                .AsNoTracking()
                .Where(x => x.Status == "active")
                .Select(x => x.Id)
                .ToListAsync(cancellationToken);

            if (activeTemplateIds.Count > 0)
            {
                query = query.Where(x =>
                    x.TemplateId == templateId ||
                    !x.TemplateId.HasValue ||
                    !activeTemplateIds.Contains(x.TemplateId.Value));
            }
        }

        var jobs = await query.ToListAsync(cancellationToken);
        return await RebuildPersistedPreviewsForJobsAsync(db, jobs, settings, cancellationToken);
    }

    private async Task<int> RebuildPersistedPreviewsForJobsAsync(
        AppDbContext db,
        List<RunJobRecord> jobs,
        AppSettingsDto settings,
        CancellationToken cancellationToken,
        bool saveChanges = true)
    {
        if (jobs.Count == 0)
        {
            return 0;
        }

        var externalClientIds = jobs
            .Select(x => x.ExternalClientId)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .ToList();

        var exactDebtByExternalClientId = externalClientIds.Count == 0
            ? new Dictionary<string, string>()
            : await db.ClientDebtCache
                .AsNoTracking()
                .Where(x => externalClientIds.Contains(x.ExternalClientId))
                .Where(x => x.Status == DebtStatuses.Ready && !string.IsNullOrWhiteSpace(x.ExactTotalRaw))
                .ToDictionaryAsync(x => x.ExternalClientId, x => x.ExactTotalRaw, cancellationToken);

        var activeTemplates = await ruleEngine.GetActiveTemplatesAsync(db, cancellationToken);
        var nowUtc = DateTime.UtcNow;

        foreach (var job in jobs)
        {
            if (string.IsNullOrWhiteSpace(TryExtractPayloadTotal(job.PayloadJson)) &&
                !string.IsNullOrWhiteSpace(job.ExternalClientId) &&
                exactDebtByExternalClientId.TryGetValue(job.ExternalClientId, out var cachedExactTotalRaw) &&
                !string.IsNullOrWhiteSpace(cachedExactTotalRaw))
            {
                job.PayloadJson = UpsertPayloadString(job.PayloadJson, PayloadFields.TotalWithCommissionRaw, cachedExactTotalRaw);
            }

            var preview = BuildPreviewForJob(job, activeTemplates, settings, nowUtc);
            ApplyPreviewToJob(job, preview);
        }

        db.RunJobs.UpdateRange(jobs);
        if (saveChanges)
        {
            await db.SaveChangesAsync(cancellationToken);
        }

        return jobs.Count;
    }

    public async Task<QueueJobMessageOverrideDto> SetJobMessageOverrideAsync(
        AppDbContext db,
        long jobId,
        QueueJobMessageOverrideRequestDto request,
        CancellationToken cancellationToken)
    {
        var job = await db.RunJobs
            .FirstOrDefaultAsync(x => x.Id == jobId, cancellationToken);
        if (job is null)
        {
            throw new KeyNotFoundException($"Задача очереди с id={jobId} не найдена.");
        }

        if (!string.Equals(job.Status, JobStatusQueued, StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(job.Status, JobStatusRetry, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("Редактирование текста доступно только для задач в статусе «queued» или «retry».");
        }

        var text = (request.Text ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(text))
        {
            throw new InvalidOperationException("Текст сообщения не должен быть пустым.");
        }

        job.PayloadJson = UpsertPayloadString(job.PayloadJson, PayloadFieldMessageOverride, text);

        if (string.IsNullOrWhiteSpace(TryExtractPayloadTotal(job.PayloadJson)))
        {
            var debtCache = await db.ClientDebtCache
                .AsNoTracking()
                .FirstOrDefaultAsync(
                    x => x.ExternalClientId == job.ExternalClientId &&
                         x.Status == DebtStatuses.Ready &&
                         !string.IsNullOrWhiteSpace(x.ExactTotalRaw),
                    cancellationToken);
            if (debtCache is not null)
            {
                job.PayloadJson = UpsertPayloadString(job.PayloadJson, PayloadFields.TotalWithCommissionRaw, debtCache.ExactTotalRaw);
            }
        }

        var now = DateTime.UtcNow;
        var activeTemplates = await ruleEngine.GetActiveTemplatesAsync(db, cancellationToken);
        var settings = await settingsStore.GetAsync(db, cancellationToken);
        var preview = BuildPreviewForJob(job, activeTemplates, settings, now);
        ApplyPreviewToJob(job, preview);

        db.RunJobs.Update(job);
        await db.SaveChangesAsync(cancellationToken);

        return BuildMessageOverrideDto(job, true, text, preview);
    }

    public async Task<QueueJobMessageOverrideDto> ClearJobMessageOverrideAsync(
        AppDbContext db,
        long jobId,
        CancellationToken cancellationToken)
    {
        var job = await db.RunJobs
            .FirstOrDefaultAsync(x => x.Id == jobId, cancellationToken);
        if (job is null)
        {
            throw new KeyNotFoundException($"Задача очереди с id={jobId} не найдена.");
        }

        if (!string.Equals(job.Status, JobStatusQueued, StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(job.Status, JobStatusRetry, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("Сброс текста доступен только для задач в статусе «queued» или «retry».");
        }

        job.PayloadJson = RemovePayloadString(job.PayloadJson, PayloadFieldMessageOverride);

        if (string.IsNullOrWhiteSpace(TryExtractPayloadTotal(job.PayloadJson)))
        {
            var debtCache = await db.ClientDebtCache
                .AsNoTracking()
                .FirstOrDefaultAsync(
                    x => x.ExternalClientId == job.ExternalClientId &&
                         x.Status == DebtStatuses.Ready &&
                         !string.IsNullOrWhiteSpace(x.ExactTotalRaw),
                    cancellationToken);
            if (debtCache is not null)
            {
                job.PayloadJson = UpsertPayloadString(job.PayloadJson, PayloadFields.TotalWithCommissionRaw, debtCache.ExactTotalRaw);
            }
        }

        var now = DateTime.UtcNow;
        var activeTemplates = await ruleEngine.GetActiveTemplatesAsync(db, cancellationToken);
        var settings = await settingsStore.GetAsync(db, cancellationToken);
        var preview = BuildPreviewForJob(job, activeTemplates, settings, now);
        ApplyPreviewToJob(job, preview);

        db.RunJobs.Update(job);
        await db.SaveChangesAsync(cancellationToken);

        return BuildMessageOverrideDto(job, false, string.Empty, preview);
    }
}
