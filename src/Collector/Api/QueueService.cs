using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Collector.Data;
using Collector.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed partial class QueueService(RuleEngineService ruleEngine, SettingsStore settingsStore)
{
    private const string ChannelStatusError = "error";
    private const string ChannelStatusOffline = "offline";
    private const string ChannelStatusOnline = "online";
    private const string RunModeLive = "live";
    private const string RunStatusPlanned = "planned";
    private const string RunStatusRunning = "running";
    private const string JobStatusQueued = "queued";
    private const string JobStatusRetry = "retry";
    private const string JobStatusStopped = "stopped";
    private const string JobStatusFailed = "failed";
    private const string DeliveryTypeSms = "sms";
    private const int DefaultMaxAttempts = 3;
    private const string PreviewStatusEmpty = "empty";
    private const string PreviewStatusReady = "ready";
    private const string PreviewStatusNeedsDebt = "needs_debt";
    private const string PreviewStatusError = "error";
    private const string PayloadFieldMessageOverride = "messageOverrideText";
    private const string QueueExactOverdueLegacyErrorCode = "QUEUE_FILTER_EXACT_DAY_INVALID";
    private const string MessageDirectionOut = "out";
    private const string MessageGatewayStatusSent = "sent";

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
                    row.TotalWithCommissionRaw,
                    debtCacheByExternalClientId.TryGetValue(row.ExternalClientId, out var debtCache)
                        ? debtCache.ExactTotalRaw
                        : null);

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

                var preview = BuildPreviewForJob(job, activeTemplates, now);
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
                        dialogCountsByPhone))
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
            foreach (var job in toUpdate)
            {
                var preview = BuildPreviewForJob(job, activeTemplates, now);
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
                         x.Status == "ready" &&
                         !string.IsNullOrWhiteSpace(x.ExactTotalRaw),
                    cancellationToken);
            if (debtCache is not null)
            {
                job.PayloadJson = UpsertPayloadString(job.PayloadJson, "totalWithCommissionRaw", debtCache.ExactTotalRaw);
            }
        }

        var now = DateTime.UtcNow;
        var activeTemplates = await ruleEngine.GetActiveTemplatesAsync(db, cancellationToken);
        var preview = BuildPreviewForJob(job, activeTemplates, now);

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
                         x.Status == "ready" &&
                         !string.IsNullOrWhiteSpace(x.ExactTotalRaw),
                    cancellationToken);
            if (debtCache is not null)
            {
                job.PayloadJson = UpsertPayloadString(job.PayloadJson, "totalWithCommissionRaw", debtCache.ExactTotalRaw);
            }
        }

        var now = DateTime.UtcNow;
        var activeTemplates = await ruleEngine.GetActiveTemplatesAsync(db, cancellationToken);
        var preview = BuildPreviewForJob(job, activeTemplates, now);
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
                         x.Status == "ready" &&
                         !string.IsNullOrWhiteSpace(x.ExactTotalRaw),
                    cancellationToken);
            if (debtCache is not null)
            {
                job.PayloadJson = UpsertPayloadString(job.PayloadJson, "totalWithCommissionRaw", debtCache.ExactTotalRaw);
            }
        }

        var now = DateTime.UtcNow;
        var activeTemplates = await ruleEngine.GetActiveTemplatesAsync(db, cancellationToken);
        var preview = BuildPreviewForJob(job, activeTemplates, now);
        ApplyPreviewToJob(job, preview);

        db.RunJobs.Update(job);
        await db.SaveChangesAsync(cancellationToken);

        return BuildMessageOverrideDto(job, false, string.Empty, preview);
    }

    private async Task<SelectionResult> SelectRowsAsync(AppDbContext db, QueueFilterRequestDto payload, CancellationToken cancellationToken)
    {
        var snapshot = await ResolveSnapshotAsync(db, payload.SnapshotId, cancellationToken);
        var settings = await settingsStore.GetAsync(db, cancellationToken);

        var parseError = TryParseRanges(payload.OverdueRanges, out var overdueRanges);
        if (parseError is not null)
        {
            throw new InvalidOperationException(parseError.Message);
        }

        var exactOverdueError = TryParseExactOverdueFilter(payload.ExactOverdue, payload.ExactDay, out var exactOverdueRange, out var normalizedExactOverdue);
        if (exactOverdueError is not null)
        {
            throw new InvalidOperationException(exactOverdueError.Message);
        }

        var timezoneOffsets = (payload.TimezoneOffsets ?? [])
            .Distinct()
            .OrderBy(x => x)
            .ToList();

        var snapshotRowsQuery = db.ClientSnapshotRows
            .AsNoTracking()
            .Where(x => x.SnapshotId == snapshot.Id);
        var totalRowsInSnapshot = await snapshotRowsQuery.CountAsync(cancellationToken);

        IQueryable<ClientSnapshotRow> filtered = snapshotRowsQuery;
        if (timezoneOffsets.Count > 0)
        {
            var tzSet = timezoneOffsets.ToHashSet();
            filtered = filtered.Where(x => tzSet.Contains(x.TimezoneOffset));
        }

        if (exactOverdueRange.HasValue)
        {
            var exactOverdue = exactOverdueRange.Value;
            filtered = filtered.Where(x => x.DaysOverdue >= exactOverdue.From && x.DaysOverdue <= exactOverdue.To);
        }
        else if (overdueRanges.Count > 0)
        {
            var overdueDays = overdueRanges
                .SelectMany(r => Enumerable.Range(r.From, (r.To - r.From) + 1))
                .Distinct()
                .ToList();
            filtered = filtered.Where(x => overdueDays.Contains(x.DaysOverdue));
        }

        var matched = await filtered
            .OrderBy(x => x.Id)
            .ToListAsync(cancellationToken);

        var stopPhones = await db.StopList
            .AsNoTracking()
            .Where(x => x.IsActive)
            .Select(x => x.Phone)
            .ToListAsync(cancellationToken);
        var stopSet = stopPhones
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(NormalizePhone)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .ToHashSet(StringComparer.Ordinal);

        var excludedByMissingPhone = matched.Count(x => string.IsNullOrWhiteSpace(x.Phone));
        var excludedByStopList = matched.Count(x => !string.IsNullOrWhiteSpace(x.Phone) && stopSet.Contains(NormalizePhone(x.Phone)));

        var readyCandidates = matched
            .Where(x => !string.IsNullOrWhiteSpace(x.Phone))
            .Where(x => !stopSet.Contains(NormalizePhone(x.Phone)))
            .ToList();
        var recentSmsSet = await GetRecentSentPhoneSetAsync(
            db,
            readyCandidates,
            settings.RecentSmsCooldownDays,
            cancellationToken);
        var excludedByRecentSms = readyCandidates.Count(x => recentSmsSet.Contains(NormalizePhone(x.Phone)));
        var readyRows = readyCandidates
            .Where(x => !recentSmsSet.Contains(NormalizePhone(x.Phone)))
            .ToList();

        var appliedFilter = new QueueAppliedFilterDto
        {
            SnapshotId = snapshot.Id,
            TimezoneOffsets = timezoneOffsets,
            OverdueRanges = exactOverdueRange.HasValue
                ? []
                : overdueRanges.Select(r => $"{r.From}-{r.To}").ToList(),
            ExactDay = exactOverdueRange is { From: var from, To: var to } && from == to
                ? from
                : null,
            ExactOverdue = normalizedExactOverdue,
            RecentSmsCooldownDays = settings.RecentSmsCooldownDays
        };

        var preview = new QueuePreviewDto
        {
            SnapshotId = snapshot.Id,
            SourceMode = snapshot.SourceMode,
            TotalRowsInSnapshot = totalRowsInSnapshot,
            MatchedByFilter = matched.Count,
            ExcludedByStopList = excludedByStopList,
            ExcludedByMissingPhone = excludedByMissingPhone,
            ExcludedByRecentSms = excludedByRecentSms,
            ReadyRows = readyRows.Count,
            CanBuild = readyRows.Count > 0,
            AppliedFilter = appliedFilter
        };

        return new SelectionResult
        {
            Snapshot = snapshot,
            ReadyRows = readyRows,
            Preview = preview
        };
    }

    private static QueueJobDto MapJobToDto(
        RunJobRecord row,
        IReadOnlyDictionary<long, TemplateRecord> templatesById,
        IReadOnlyDictionary<long, SenderChannelRecord> channelsById,
        ClientDebtCacheRecord? debtCache,
        IReadOnlyDictionary<string, int> dialogCountsByPhone)
    {
        TemplateRecord? template = null;
        if (row.TemplateId.HasValue && row.TemplateId.Value > 0)
        {
            templatesById.TryGetValue(row.TemplateId.Value, out template);
        }

        SenderChannelRecord? channel = null;
        if (row.ChannelId.HasValue && row.ChannelId.Value > 0)
        {
            channelsById.TryGetValue(row.ChannelId.Value, out channel);
        }

        var normalizedPhone = NormalizePhone(row.Phone);
        var dialogCount = !string.IsNullOrWhiteSpace(normalizedPhone) &&
                          dialogCountsByPhone.TryGetValue(normalizedPhone, out var foundCount)
            ? foundCount
            : 0;

        var exactTotalRaw = FirstNonEmpty(debtCache?.ExactTotalRaw, TryExtractPayloadTotal(row.PayloadJson));
        var approxDebtText = FirstNonEmpty(debtCache?.ApproxTotalText, BuildApproxDebtText(exactTotalRaw));
        var messageOverrideText = TryExtractPayloadString(row.PayloadJson, PayloadFieldMessageOverride);

        return new QueueJobDto
        {
            Id = row.Id,
            RunSessionId = row.RunSessionId,
            ExternalClientId = row.ExternalClientId,
            ClientFio = row.ClientFio,
            Phone = row.Phone,
            TzOffset = row.TzOffset,
            DaysOverdue = row.DaysOverdue,
            TemplateId = row.TemplateId,
            TemplateName = template?.Name ?? string.Empty,
            TemplateKind = template?.Kind ?? string.Empty,
            TemplateStatus = template?.Status ?? string.Empty,
            ChannelId = row.ChannelId,
            ChannelName = channel?.Name ?? string.Empty,
            ChannelStatus = channel?.Status ?? string.Empty,
            DeliveryType = row.DeliveryType,
            Status = row.Status,
            Attempts = row.Attempts,
            MaxAttempts = row.MaxAttempts,
            PlannedAtUtc = row.PlannedAtUtc,
            SentAtUtc = row.SentAtUtc,
            LastErrorCode = row.LastErrorCode ?? string.Empty,
            LastErrorDetail = row.LastErrorDetail ?? string.Empty,
            CardUrl = TryExtractCardUrl(row.PayloadJson),
            TotalWithCommissionRaw = exactTotalRaw,
            DebtApproxText = approxDebtText,
            DebtApproxValue = debtCache?.ApproxTotalValue,
            DebtStatus = FirstNonEmpty(debtCache?.Status, string.IsNullOrWhiteSpace(exactTotalRaw) ? "empty" : "ready"),
            DebtSource = FirstNonEmpty(debtCache?.Source, string.IsNullOrWhiteSpace(exactTotalRaw) ? string.Empty : "payload"),
            DebtUpdatedAtUtc = debtCache?.UpdatedAtUtc,
            DebtErrorCode = debtCache?.LastErrorCode ?? string.Empty,
            DebtErrorDetail = debtCache?.LastErrorDetail ?? string.Empty,
            PreviewStatus = FirstNonEmpty(row.PreviewStatus, string.IsNullOrWhiteSpace(row.PreviewText) ? PreviewStatusEmpty : PreviewStatusReady),
            PreviewText = row.PreviewText ?? string.Empty,
            PreviewVariablesJson = row.PreviewVariablesJson ?? string.Empty,
            PreviewUpdatedAtUtc = row.PreviewUpdatedAtUtc,
            PreviewErrorCode = row.PreviewErrorCode ?? string.Empty,
            PreviewErrorDetail = row.PreviewErrorDetail ?? string.Empty,
            HasMessageOverride = !string.IsNullOrWhiteSpace(messageOverrideText),
            MessageOverrideText = messageOverrideText,
            DialogStatus = dialogCount > 0 ? "has_history" : "none",
            DialogMessagesCount = dialogCount
        };
    }

    private static string TryExtractCardUrl(string? payloadJson)
    {
        if (string.IsNullOrWhiteSpace(payloadJson))
        {
            return string.Empty;
        }

        try
        {
            using var doc = JsonDocument.Parse(payloadJson);
            if (!doc.RootElement.TryGetProperty("cardUrl", out var cardUrlEl))
            {
                return string.Empty;
            }

            var value = cardUrlEl.GetString() ?? string.Empty;
            return value.Trim();
        }
        catch
        {
            return string.Empty;
        }
    }

    private static string TryExtractPayloadTotal(string? payloadJson)
    {
        if (string.IsNullOrWhiteSpace(payloadJson))
        {
            return string.Empty;
        }

        try
        {
            using var doc = JsonDocument.Parse(payloadJson);
            if (!doc.RootElement.TryGetProperty("totalWithCommissionRaw", out var totalEl))
            {
                return string.Empty;
            }

            return totalEl.ValueKind switch
            {
                JsonValueKind.String => (totalEl.GetString() ?? string.Empty).Trim(),
                JsonValueKind.Number => totalEl.GetRawText().Trim(),
                _ => string.Empty
            };
        }
        catch
        {
            return string.Empty;
        }
    }

    private static string TryExtractPayloadString(string? payloadJson, string field)
    {
        if (string.IsNullOrWhiteSpace(payloadJson) || string.IsNullOrWhiteSpace(field))
        {
            return string.Empty;
        }

        try
        {
            using var doc = JsonDocument.Parse(payloadJson);
            if (!doc.RootElement.TryGetProperty(field, out var node))
            {
                return string.Empty;
            }

            return node.ValueKind switch
            {
                JsonValueKind.String => (node.GetString() ?? string.Empty).Trim(),
                JsonValueKind.Number => node.GetRawText().Trim(),
                _ => string.Empty
            };
        }
        catch
        {
            return string.Empty;
        }
    }

    private static string BuildJobPayloadJson(string? cardUrl, string? totalWithCommissionRaw)
    {
        return JsonSerializer.Serialize(new
        {
            cardUrl = (cardUrl ?? string.Empty).Trim(),
            totalWithCommissionRaw = (totalWithCommissionRaw ?? string.Empty).Trim()
        });
    }

    private static string UpsertPayloadString(string? payloadJson, string field, string value)
    {
        JsonObject root;
        if (string.IsNullOrWhiteSpace(payloadJson))
        {
            root = new JsonObject();
        }
        else
        {
            try
            {
                root = JsonNode.Parse(payloadJson) as JsonObject ?? new JsonObject();
            }
            catch
            {
                root = new JsonObject();
            }
        }

        root[field] = value;
        return root.ToJsonString();
    }

    private static string RemovePayloadString(string? payloadJson, string field)
    {
        JsonObject root;
        if (string.IsNullOrWhiteSpace(payloadJson))
        {
            root = new JsonObject();
        }
        else
        {
            try
            {
                root = JsonNode.Parse(payloadJson) as JsonObject ?? new JsonObject();
            }
            catch
            {
                root = new JsonObject();
            }
        }

        root.Remove(field);
        return root.ToJsonString();
    }

    private JobPreviewResult BuildPreviewForJob(
        RunJobRecord job,
        IReadOnlyList<TemplateRecord> activeTemplates,
        DateTime nowUtc)
    {
        var messageOverrideText = TryExtractPayloadString(job.PayloadJson, PayloadFieldMessageOverride);
        if (!string.IsNullOrWhiteSpace(messageOverrideText))
        {
            var totalRawOverride = TryExtractPayloadTotal(job.PayloadJson);
            var approxDebtTextOverride = RuleEngineService.BuildApproxDebtText(job.PayloadJson);
            var templateOverride = job.TemplateId.HasValue && job.TemplateId.Value > 0
                ? activeTemplates.FirstOrDefault(x => x.Id == job.TemplateId.Value)
                : null;

            var variablesJsonOverride = JsonSerializer.Serialize(new
            {
                fullFio = (job.ClientFio ?? string.Empty).Trim(),
                totalWithCommissionRaw = totalRawOverride,
                approxDebtText = approxDebtTextOverride,
                templateId = templateOverride?.Id,
                templateKind = templateOverride?.Kind ?? string.Empty,
                templateName = templateOverride?.Name ?? string.Empty,
                messageOverride = true
            });

            return new JobPreviewResult
            {
                Status = PreviewStatusReady,
                Text = messageOverrideText,
                VariablesJson = variablesJsonOverride,
                UpdatedAtUtc = nowUtc,
                ErrorCode = string.Empty,
                ErrorDetail = string.Empty,
                TemplateId = templateOverride?.Id,
                TemplateName = templateOverride?.Name ?? string.Empty,
                TemplateKind = templateOverride?.Kind ?? string.Empty
            };
        }

        var rendered = ruleEngine.BuildDispatchMessage(activeTemplates, job);
        if (!string.IsNullOrWhiteSpace(rendered.ErrorCode))
        {
            return new JobPreviewResult
            {
                Status = PreviewStatusError,
                Text = string.Empty,
                VariablesJson = "{}",
                UpdatedAtUtc = nowUtc,
                ErrorCode = rendered.ErrorCode,
                ErrorDetail = rendered.ErrorMessage,
                TemplateId = null,
                TemplateName = string.Empty,
                TemplateKind = string.Empty
            };
        }

        var template = rendered.TemplateId.HasValue && rendered.TemplateId.Value > 0
            ? activeTemplates.FirstOrDefault(x => x.Id == rendered.TemplateId.Value)
            : null;

        var totalRaw = TryExtractPayloadTotal(job.PayloadJson);
        var approxDebtText = RuleEngineService.BuildApproxDebtText(job.PayloadJson);
        var status = string.IsNullOrWhiteSpace(totalRaw)
            ? PreviewStatusNeedsDebt
            : PreviewStatusReady;

        var errorCode = status == PreviewStatusNeedsDebt
            ? "PREVIEW_DEBT_MISSING"
            : string.Empty;
        var errorDetail = status == PreviewStatusNeedsDebt
            ? "Точная сумма долга пока не загружена. Превью построено без нее."
            : string.Empty;

        var variablesJson = JsonSerializer.Serialize(new
        {
            fullFio = (job.ClientFio ?? string.Empty).Trim(),
            totalWithCommissionRaw = totalRaw,
            approxDebtText,
            templateId = rendered.TemplateId,
            templateKind = template?.Kind ?? string.Empty,
            templateName = template?.Name ?? string.Empty
        });

        return new JobPreviewResult
        {
            Status = status,
            Text = rendered.MessageText,
            VariablesJson = variablesJson,
            UpdatedAtUtc = nowUtc,
            ErrorCode = errorCode,
            ErrorDetail = errorDetail,
            TemplateId = rendered.TemplateId,
            TemplateName = template?.Name ?? string.Empty,
            TemplateKind = template?.Kind ?? string.Empty
        };
    }

    private static void ApplyPreviewToJob(RunJobRecord job, JobPreviewResult preview)
    {
        job.PreviewStatus = preview.Status;
        job.PreviewText = preview.Text;
        job.PreviewVariablesJson = preview.VariablesJson;
        job.PreviewUpdatedAtUtc = preview.UpdatedAtUtc;
        job.PreviewErrorCode = preview.ErrorCode;
        job.PreviewErrorDetail = preview.ErrorDetail;
    }

    private static QueueJobMessageOverrideDto BuildMessageOverrideDto(
        RunJobRecord job,
        bool hasMessageOverride,
        string messageOverrideText,
        JobPreviewResult preview)
    {
        return new QueueJobMessageOverrideDto
        {
            JobId = job.Id,
            RunSessionId = job.RunSessionId,
            ExternalClientId = job.ExternalClientId,
            Phone = job.Phone,
            HasMessageOverride = hasMessageOverride,
            MessageOverrideText = messageOverrideText,
            Preview = new QueueJobPreviewDto
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
            }
        };
    }

    private static string BuildApproxDebtText(string exactTotalRaw)
    {
        if (string.IsNullOrWhiteSpace(exactTotalRaw))
        {
            return string.Empty;
        }

        var payload = JsonSerializer.Serialize(new
        {
            totalWithCommissionRaw = exactTotalRaw
        });
        return RuleEngineService.BuildApproxDebtText(payload);
    }

    private static string FirstNonEmpty(params string?[] values)
    {
        foreach (var value in values)
        {
            if (!string.IsNullOrWhiteSpace(value))
            {
                return value.Trim();
            }
        }

        return string.Empty;
    }

    private static string NormalizePhone(string? rawPhone)
    {
        var digits = new string((rawPhone ?? string.Empty).Where(char.IsDigit).ToArray());
        if (digits.Length == 0)
        {
            return string.Empty;
        }

        return $"+{digits}";
    }

    private static ApiErrorDto? TryParseRanges(IEnumerable<string>? rawRanges, out List<OverdueRange> ranges)
    {
        ranges = [];
        var uniqueRanges = (rawRanges ?? [])
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => x.Trim())
            .Distinct(StringComparer.Ordinal)
            .ToList();

        foreach (var raw in uniqueRanges)
        {
            var singleMatch = OverdueSingleDayRegex().Match(raw);
            if (singleMatch.Success)
            {
                var day = int.Parse(singleMatch.Groups[1].Value, CultureInfo.InvariantCulture);
                if (day < 0)
                {
                    return new ApiErrorDto
                    {
                        Code = "QUEUE_FILTER_RANGE_INVALID",
                        Message = $"Некорректное значение просрочки: '{raw}'."
                    };
                }

                ranges.Add(new OverdueRange(day, day));
                continue;
            }

            var match = OverdueRangeRegex().Match(raw);
            if (!match.Success)
            {
                return new ApiErrorDto
                {
                    Code = "QUEUE_FILTER_RANGE_INVALID",
                    Message = $"Некорректное значение просрочки: '{raw}'. Ожидается формат 'N' или 'N-M'."
                };
            }

            var from = int.Parse(match.Groups[1].Value);
            var to = int.Parse(match.Groups[2].Value);
            if (from < 0 || to < 0 || from > to)
            {
                return new ApiErrorDto
                {
                    Code = "QUEUE_FILTER_RANGE_INVALID",
                    Message = $"Некорректный диапазон просрочки: '{raw}'."
                };
            }

            ranges.Add(new OverdueRange(from, to));
        }

        ranges = ranges
            .OrderBy(x => x.From)
            .ThenBy(x => x.To)
            .ToList();

        return null;
    }

    private static ApiErrorDto? TryParseExactOverdueFilter(
        string? rawExactOverdue,
        int? legacyExactDay,
        out OverdueRange? range,
        out string normalized)
    {
        range = null;
        normalized = string.Empty;

        var raw = (rawExactOverdue ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(raw))
        {
            var singleMatch = OverdueSingleDayRegex().Match(raw);
            if (singleMatch.Success)
            {
                var day = int.Parse(singleMatch.Groups[1].Value, CultureInfo.InvariantCulture);
                range = new OverdueRange(day, day);
                normalized = day.ToString(CultureInfo.InvariantCulture);
                return null;
            }

            var rangeMatch = OverdueRangeRegex().Match(raw);
            if (!rangeMatch.Success)
            {
                return new ApiErrorDto
                {
                    Code = QueueExactOverdueLegacyErrorCode,
                    Message = $"Некорректное значение точной просрочки: '{raw}'. Ожидается формат 'N' или 'N-M'."
                };
            }

            var from = int.Parse(rangeMatch.Groups[1].Value, CultureInfo.InvariantCulture);
            var to = int.Parse(rangeMatch.Groups[2].Value, CultureInfo.InvariantCulture);
            if (from < 0 || to < 0 || from > to)
            {
                return new ApiErrorDto
                {
                    Code = QueueExactOverdueLegacyErrorCode,
                    Message = $"Некорректное значение точной просрочки: '{raw}'."
                };
            }

            range = new OverdueRange(from, to);
            normalized = $"{from.ToString(CultureInfo.InvariantCulture)}-{to.ToString(CultureInfo.InvariantCulture)}";
            return null;
        }

        if (!legacyExactDay.HasValue)
        {
            return null;
        }

        if (legacyExactDay.Value < 0)
        {
            return new ApiErrorDto
            {
                Code = QueueExactOverdueLegacyErrorCode,
                Message = "Точное значение просрочки должно быть >= 0."
            };
        }

        range = new OverdueRange(legacyExactDay.Value, legacyExactDay.Value);
        normalized = legacyExactDay.Value.ToString(CultureInfo.InvariantCulture);
        return null;
    }

    // Kept temporarily for migration safety; new calls use TryParseExactOverdueFilter.
    private static ApiErrorDto? TryParseExactOverdueFilterLegacy(
        string? rawExactOverdue,
        int? legacyExactDay,
        out OverdueRange? range,
        out string normalized)
    {
        range = null;
        normalized = string.Empty;

        var raw = (rawExactOverdue ?? string.Empty).Trim();
        if (!string.IsNullOrWhiteSpace(raw))
        {
            var singleMatch = OverdueSingleDayRegex().Match(raw);
            if (singleMatch.Success)
            {
                var day = int.Parse(singleMatch.Groups[1].Value, CultureInfo.InvariantCulture);
                range = new OverdueRange(day, day);
                normalized = day.ToString(CultureInfo.InvariantCulture);
                return null;
            }

            var rangeMatch = OverdueRangeRegex().Match(raw);
            if (!rangeMatch.Success)
            {
                return new ApiErrorDto
                {
                    Code = QueueExactOverdueLegacyErrorCode,
                    Message = $"РќРµРєРѕСЂСЂРµРєС‚РЅРѕРµ Р·РЅР°С‡РµРЅРёРµ С‚РѕС‡РЅРѕР№ РїСЂРѕСЃСЂРѕС‡РєРё: '{raw}'. РћР¶РёРґР°РµС‚СЃСЏ С„РѕСЂРјР°С‚ 'N' РёР»Рё 'N-M'."
                };
            }

            var from = int.Parse(rangeMatch.Groups[1].Value, CultureInfo.InvariantCulture);
            var to = int.Parse(rangeMatch.Groups[2].Value, CultureInfo.InvariantCulture);
            if (from < 0 || to < 0 || from > to)
            {
                return new ApiErrorDto
                {
                    Code = QueueExactOverdueLegacyErrorCode,
                    Message = $"РќРµРєРѕСЂСЂРµРєС‚РЅРѕРµ Р·РЅР°С‡РµРЅРёРµ С‚РѕС‡РЅРѕР№ РїСЂРѕСЃСЂРѕС‡РєРё: '{raw}'."
                };
            }

            range = new OverdueRange(from, to);
            normalized = $"{from.ToString(CultureInfo.InvariantCulture)}-{to.ToString(CultureInfo.InvariantCulture)}";
            return null;
        }

        if (!legacyExactDay.HasValue)
        {
            return null;
        }

        if (legacyExactDay.Value < 0)
        {
            return new ApiErrorDto
            {
                Code = QueueExactOverdueLegacyErrorCode,
                Message = "РўРѕС‡РЅРѕРµ Р·РЅР°С‡РµРЅРёРµ РїСЂРѕСЃСЂРѕС‡РєРё РґРѕР»Р¶РЅРѕ Р±С‹С‚СЊ >= 0."
            };
        }

        range = new OverdueRange(legacyExactDay.Value, legacyExactDay.Value);
        normalized = legacyExactDay.Value.ToString(CultureInfo.InvariantCulture);
        return null;
    }

    private static async Task<ClientSnapshot> ResolveSnapshotAsync(AppDbContext db, long? snapshotId, CancellationToken cancellationToken)
    {
        ClientSnapshot? snapshot;
        if (snapshotId.HasValue && snapshotId.Value > 0)
        {
            snapshot = await db.ClientSnapshots
                .AsNoTracking()
                .FirstOrDefaultAsync(x => x.Id == snapshotId.Value, cancellationToken);
            if (snapshot is null)
            {
                throw new KeyNotFoundException($"Snapshot с id={snapshotId.Value} не найден.");
            }
        }
        else
        {
            snapshot = await db.ClientSnapshots
                .AsNoTracking()
                .OrderByDescending(x => x.Id)
                .FirstOrDefaultAsync(cancellationToken);
            if (snapshot is null)
            {
                throw new KeyNotFoundException("Нет актуализированной базы клиентов. Сначала выполните /api/clients/sync.");
            }
        }

        return snapshot;
    }

    private static ScheduleResult BuildSchedule(
        IReadOnlyList<ClientSnapshotRow> rows,
        AppSettingsDto settings,
        DateTime nowUtc,
        IReadOnlyList<PlanningChannel> channels,
        IReadOnlyDictionary<string, ClientChannelBindingRecord> bindingsByExternalClientId)
    {
        var gapMinutes = settings.Gap <= 0 ? 1 : settings.Gap;
        var workStart = ParseHm(settings.WorkWindowStart, new TimeOnly(8, 0));
        var workEnd = ParseHm(settings.WorkWindowEnd, new TimeOnly(21, 0));
        if (workEnd <= workStart)
        {
            workStart = new TimeOnly(8, 0);
            workEnd = new TimeOnly(21, 0);
        }

        var channelsUsed = Math.Max(1, channels.Count);
        var channelIds = channels.Count == 0
            ? [0L]
            : channels.Select(x => x.Id).ToList();
        var channelIndexById = channelIds
            .Select((id, idx) => new { id, idx })
            .ToDictionary(x => x.id, x => x.idx);

        var channelNextAvailableUtc = Enumerable.Repeat(nowUtc, channelsUsed).ToArray();
        var plannedByIndex = new DateTime[rows.Count];
        var channelByRowIndex = new long[rows.Count];

        var unscheduledGlobal = Enumerable.Range(0, rows.Count).ToHashSet();
        var unassignedGlobal = Enumerable.Range(0, rows.Count).ToHashSet();
        var unscheduledByChannel = Enumerable.Range(0, channelsUsed)
            .Select(_ => new HashSet<int>())
            .ToArray();

        for (var i = 0; i < rows.Count; i++)
        {
            if (!bindingsByExternalClientId.TryGetValue(rows[i].ExternalClientId, out var binding))
            {
                continue;
            }

            if (!channelIndexById.TryGetValue(binding.ChannelId, out var boundChannelIndex))
            {
                continue;
            }

            unscheduledByChannel[boundChannelIndex].Add(i);
            unassignedGlobal.Remove(i);
        }

        while (unscheduledGlobal.Count > 0)
        {
            var channelIndex = GetEarliestChannelIndex(channelNextAvailableUtc);
            var channelBaseUtc = channelNextAvailableUtc[channelIndex];

            var ownBound = unscheduledByChannel[channelIndex];
            var boundCandidate = SelectNextClient(rows, ownBound, channelBaseUtc, workStart, workEnd);
            var unassignedCandidate = SelectNextClient(rows, unassignedGlobal, channelBaseUtc, workStart, workEnd);

            var nextClient = PickPreferredCandidate(boundCandidate, unassignedCandidate);
            if (nextClient.Index < 0)
            {
                break;
            }

            plannedByIndex[nextClient.Index] = nextClient.PlannedAtUtc;
            channelByRowIndex[nextClient.Index] = channelIds[channelIndex];

            unscheduledGlobal.Remove(nextClient.Index);
            unassignedGlobal.Remove(nextClient.Index);
            ownBound.Remove(nextClient.Index);
            channelNextAvailableUtc[channelIndex] = nextClient.PlannedAtUtc.AddMinutes(gapMinutes);
        }

        var items = new List<ScheduledItem>(rows.Count);
        for (var i = 0; i < rows.Count; i++)
        {
            items.Add(new ScheduledItem
            {
                PlannedAtUtc = plannedByIndex[i] == default ? nowUtc : plannedByIndex[i],
                ChannelId = channelByRowIndex[i]
            });
        }

        var channelNextNoWindowUtc = Enumerable.Repeat(nowUtc, channelsUsed).ToArray();
        var plannedNoWindow = new List<DateTime>(rows.Count);
        for (var i = 0; i < rows.Count; i++)
        {
            var channelId = channelByRowIndex[i];
            if (!channelIndexById.TryGetValue(channelId, out var chIndex))
            {
                chIndex = GetEarliestChannelIndex(channelNextNoWindowUtc);
            }

            var plannedAt = channelNextNoWindowUtc[chIndex];
            plannedNoWindow.Add(plannedAt);
            channelNextNoWindowUtc[chIndex] = plannedAt.AddMinutes(gapMinutes);
        }

        var estimatedFinishAtUtc = items.Count == 0 ? nowUtc : items.Max(x => x.PlannedAtUtc);
        var baselineFinishAtUtc = plannedNoWindow.Count == 0 ? nowUtc : plannedNoWindow.Max();

        var estimatedDurationMinutes = (int)Math.Max(0, Math.Ceiling((estimatedFinishAtUtc - nowUtc).TotalMinutes));
        var baselineDurationMinutes = (int)Math.Max(0, Math.Ceiling((baselineFinishAtUtc - nowUtc).TotalMinutes));
        var timezoneWaitMinutes = Math.Max(0, estimatedDurationMinutes - baselineDurationMinutes);
        var gapWaitMinutes = baselineDurationMinutes;
        var totalWaitMinutes = estimatedDurationMinutes;

        return new ScheduleResult
        {
            Items = items,
            ChannelsUsed = channelsUsed,
            GapMinutes = gapMinutes,
            TimezoneWaitMinutes = (int)Math.Max(0, timezoneWaitMinutes),
            GapWaitMinutes = gapWaitMinutes,
            TotalWaitMinutes = totalWaitMinutes,
            EstimatedDurationMinutes = estimatedDurationMinutes,
            EstimatedFinishAtUtc = estimatedFinishAtUtc
        };
    }

    private static NextClientSlot SelectNextClient(
        IReadOnlyList<ClientSnapshotRow> rows,
        HashSet<int> unscheduled,
        DateTime channelBaseUtc,
        TimeOnly workStart,
        TimeOnly workEnd,
        HashSet<int>? excluded = null)
    {
        var best = new NextClientSlot(
            Index: -1,
            PlannedAtUtc: DateTime.MaxValue,
            WindowEndUtc: DateTime.MaxValue,
            IsInWindowNow: false,
            RemainingWindow: TimeSpan.MaxValue);

        foreach (var idx in unscheduled)
        {
            if (excluded is not null && excluded.Contains(idx))
            {
                continue;
            }

            var slot = GetWindowSlotUtc(channelBaseUtc, rows[idx].TimezoneOffset, workStart, workEnd);
            var isInWindowNow = slot.PlannedAtUtc == channelBaseUtc;
            var remainingWindow = isInWindowNow
                ? slot.WindowEndUtc - channelBaseUtc
                : TimeSpan.MaxValue;

            var candidate = new NextClientSlot(
                Index: idx,
                PlannedAtUtc: slot.PlannedAtUtc,
                WindowEndUtc: slot.WindowEndUtc,
                IsInWindowNow: isInWindowNow,
                RemainingWindow: remainingWindow);

            if (ShouldPreferCandidate(candidate, best))
            {
                best = candidate;
            }
        }

        return best;
    }

    private static NextClientSlot PickPreferredCandidate(NextClientSlot first, NextClientSlot second)
    {
        if (first.Index < 0) return second;
        if (second.Index < 0) return first;
        return ShouldPreferCandidate(second, first) ? second : first;
    }

    private static bool ShouldPreferCandidate(NextClientSlot candidate, NextClientSlot currentBest)
    {
        if (currentBest.Index < 0) return true;

        // 1) В приоритете клиенты, которые уже находятся в рабочем окне.
        if (candidate.IsInWindowNow != currentBest.IsInWindowNow)
        {
            return candidate.IsInWindowNow;
        }

        // 2) Среди "сейчас-в-окне" приоритет у тех, чье окно закроется раньше.
        if (candidate.IsInWindowNow && candidate.RemainingWindow != currentBest.RemainingWindow)
        {
            return candidate.RemainingWindow < currentBest.RemainingWindow;
        }

        // 3) Дальше — максимально ранний момент отправки.
        if (candidate.PlannedAtUtc != currentBest.PlannedAtUtc)
        {
            return candidate.PlannedAtUtc < currentBest.PlannedAtUtc;
        }

        // 4) При равенстве plannedAt — более раннее закрытие окна.
        if (candidate.WindowEndUtc != currentBest.WindowEndUtc)
        {
            return candidate.WindowEndUtc < currentBest.WindowEndUtc;
        }

        // 5) Детерминированный tie-break.
        return candidate.Index < currentBest.Index;
    }

    private static int GetEarliestChannelIndex(DateTime[] nextAvailableUtc)
    {
        var bestIndex = 0;
        var bestAt = nextAvailableUtc[0];
        for (var i = 1; i < nextAvailableUtc.Length; i++)
        {
            if (nextAvailableUtc[i] < bestAt)
            {
                bestAt = nextAvailableUtc[i];
                bestIndex = i;
            }
        }

        return bestIndex;
    }

    private static async Task<List<PlanningChannel>> GetAvailableChannelsForPlanningAsync(
        AppDbContext db,
        CancellationToken cancellationToken)
    {
        return await db.SenderChannels
            .AsNoTracking()
            .Where(x => x.Status != ChannelStatusError && x.Status != ChannelStatusOffline)
            .OrderBy(x => x.FailStreak)
            .ThenBy(x => x.Id)
            .Select(x => new PlanningChannel
            {
                Id = x.Id
            })
            .ToListAsync(cancellationToken);
    }

    private static async Task<Dictionary<string, ClientChannelBindingRecord>> GetBindingsByExternalClientIdAsync(
        AppDbContext db,
        IReadOnlyList<ClientSnapshotRow> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return new Dictionary<string, ClientChannelBindingRecord>(StringComparer.Ordinal);
        }

        var externalIds = rows
            .Select(x => x.ExternalClientId)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .ToList();
        if (externalIds.Count == 0)
        {
            return new Dictionary<string, ClientChannelBindingRecord>(StringComparer.Ordinal);
        }

        var bindings = await db.ClientChannelBindings
            .AsNoTracking()
            .Where(x => externalIds.Contains(x.ExternalClientId))
            .ToListAsync(cancellationToken);

        return bindings
            .GroupBy(x => x.ExternalClientId, StringComparer.Ordinal)
            .ToDictionary(
                x => x.Key,
                x => x.OrderByDescending(v => v.UpdatedAtUtc).ThenByDescending(v => v.Id).First(),
                StringComparer.Ordinal);
    }

    private static async Task<Dictionary<string, ClientDebtCacheRecord>> GetDebtCacheByExternalClientIdAsync(
        AppDbContext db,
        IReadOnlyList<ClientSnapshotRow> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return new Dictionary<string, ClientDebtCacheRecord>(StringComparer.Ordinal);
        }

        var externalIds = rows
            .Select(x => x.ExternalClientId)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .ToList();
        if (externalIds.Count == 0)
        {
            return new Dictionary<string, ClientDebtCacheRecord>(StringComparer.Ordinal);
        }

        var cached = await db.ClientDebtCache
            .AsNoTracking()
            .Where(x => externalIds.Contains(x.ExternalClientId))
            .ToListAsync(cancellationToken);

        return cached
            .GroupBy(x => x.ExternalClientId, StringComparer.Ordinal)
            .ToDictionary(
                x => x.Key,
                x => x.OrderByDescending(v => v.UpdatedAtUtc).ThenByDescending(v => v.Id).First(),
                StringComparer.Ordinal);
    }

    private static async Task<HashSet<string>> GetRecentSentPhoneSetAsync(
        AppDbContext db,
        IReadOnlyList<ClientSnapshotRow> rows,
        int cooldownDays,
        CancellationToken cancellationToken)
    {
        if (cooldownDays <= 0 || rows.Count == 0)
        {
            return new HashSet<string>(StringComparer.Ordinal);
        }

        var phones = rows
            .Select(x => NormalizePhone(x.Phone))
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .ToList();
        if (phones.Count == 0)
        {
            return new HashSet<string>(StringComparer.Ordinal);
        }

        var thresholdUtc = GetRecentSmsThresholdUtc(cooldownDays);
        var recentPhones = await db.Messages
            .AsNoTracking()
            .Where(x => x.Direction == MessageDirectionOut)
            .Where(x => x.GatewayStatus == MessageGatewayStatusSent)
            .Where(x => x.CreatedAtUtc >= thresholdUtc)
            .Where(x => phones.Contains(x.ClientPhone))
            .Select(x => x.ClientPhone)
            .Distinct()
            .ToListAsync(cancellationToken);

        return recentPhones
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(NormalizePhone)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .ToHashSet(StringComparer.Ordinal);
    }

    private static DateTime GetRecentSmsThresholdUtc(int cooldownDays)
    {
        var localZone = TimeZoneInfo.Local;
        var nowLocal = TimeZoneInfo.ConvertTimeFromUtc(DateTime.UtcNow, localZone);
        var thresholdDate = DateOnly.FromDateTime(nowLocal).AddDays(-(cooldownDays - 1));
        var thresholdLocal = thresholdDate.ToDateTime(TimeOnly.MinValue, DateTimeKind.Unspecified);
        return TimeZoneInfo.ConvertTimeToUtc(thresholdLocal, localZone);
    }

    private static async Task UpsertBindingsFromScheduleAsync(
        AppDbContext db,
        IReadOnlyList<ClientSnapshotRow> rows,
        ScheduleResult schedule,
        DateTime nowUtc,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0 || schedule.Items.Count == 0)
        {
            return;
        }

        var boundRows = rows
            .Select((row, idx) => new { row, item = schedule.Items[idx] })
            .Where(x => x.item.ChannelId > 0)
            .ToList();
        if (boundRows.Count == 0)
        {
            return;
        }

        var externalIds = boundRows
            .Select(x => x.row.ExternalClientId)
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Distinct(StringComparer.Ordinal)
            .ToList();
        if (externalIds.Count == 0)
        {
            return;
        }

        var existing = await db.ClientChannelBindings
            .Where(x => externalIds.Contains(x.ExternalClientId))
            .ToListAsync(cancellationToken);
        var existingByExternal = existing
            .GroupBy(x => x.ExternalClientId, StringComparer.Ordinal)
            .ToDictionary(
                x => x.Key,
                x => x.OrderByDescending(v => v.UpdatedAtUtc).ThenByDescending(v => v.Id).First(),
                StringComparer.Ordinal);

        foreach (var pair in boundRows)
        {
            var externalClientId = pair.row.ExternalClientId;
            if (string.IsNullOrWhiteSpace(externalClientId))
            {
                continue;
            }

            var normalizedPhone = NormalizePhone(pair.row.Phone);
            if (existingByExternal.TryGetValue(externalClientId, out var current))
            {
                var changed = current.ChannelId != pair.item.ChannelId ||
                              !string.Equals(current.Phone, normalizedPhone, StringComparison.Ordinal);
                if (!changed)
                {
                    continue;
                }

                current.ChannelId = pair.item.ChannelId;
                current.Phone = normalizedPhone;
                current.UpdatedAtUtc = nowUtc;
                db.ClientChannelBindings.Update(current);
                continue;
            }

            var created = new ClientChannelBindingRecord
            {
                ExternalClientId = externalClientId,
                Phone = normalizedPhone,
                ChannelId = pair.item.ChannelId,
                CreatedAtUtc = nowUtc,
                UpdatedAtUtc = nowUtc,
                LastUsedAtUtc = null
            };
            db.ClientChannelBindings.Add(created);
        }
    }

    private static async Task<(int OnlineCount, int AvailableCount)> GetChannelCountsForForecastAsync(
        AppDbContext db,
        CancellationToken cancellationToken)
    {
        var statuses = await db.SenderChannels
            .AsNoTracking()
            .Select(x => x.Status)
            .ToListAsync(cancellationToken);

        var online = statuses.Count(x => string.Equals(x, ChannelStatusOnline, StringComparison.OrdinalIgnoreCase));
        var available = statuses.Count(x =>
            !string.Equals(x, ChannelStatusError, StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(x, ChannelStatusOffline, StringComparison.OrdinalIgnoreCase));

        return (online, available);
    }

    private static DateTime AlignToWorkWindowUtc(DateTime utc, int tzOffsetFromMoscow, TimeOnly start, TimeOnly end)
    {
        return GetWindowSlotUtc(utc, tzOffsetFromMoscow, start, end).PlannedAtUtc;
    }

    private static WindowSlot GetWindowSlotUtc(DateTime utc, int tzOffsetFromMoscow, TimeOnly start, TimeOnly end)
    {
        // timezone_offset in snapshot is from Moscow, so convert to UTC offset via MSK(+3).
        var clientUtcOffset = TimeSpan.FromHours(3 + tzOffsetFromMoscow);
        var local = utc + clientUtcOffset;
        var localDate = DateOnly.FromDateTime(local);
        var localTime = TimeOnly.FromDateTime(local);

        DateTime localPlanned;
        DateTime localWindowEnd;
        if (localTime < start)
        {
            localPlanned = localDate.ToDateTime(start);
            localWindowEnd = localDate.ToDateTime(end);
        }
        else if (localTime >= end)
        {
            localPlanned = localDate.AddDays(1).ToDateTime(start);
            localWindowEnd = localDate.AddDays(1).ToDateTime(end);
        }
        else
        {
            localPlanned = local;
            localWindowEnd = localDate.ToDateTime(end);
        }

        return new WindowSlot(
            DateTime.SpecifyKind(localPlanned - clientUtcOffset, DateTimeKind.Utc),
            DateTime.SpecifyKind(localWindowEnd - clientUtcOffset, DateTimeKind.Utc));
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

    [GeneratedRegex(@"^\s*(\d{1,3})\s*-\s*(\d{1,3})\s*$", RegexOptions.CultureInvariant)]
    private static partial Regex OverdueRangeRegex();

    [GeneratedRegex(@"^\s*(\d{1,3})\s*$", RegexOptions.CultureInvariant)]
    private static partial Regex OverdueSingleDayRegex();

    private sealed class SelectionResult
    {
        public required ClientSnapshot Snapshot { get; init; }
        public required List<ClientSnapshotRow> ReadyRows { get; init; }
        public required QueuePreviewDto Preview { get; init; }
    }

    private readonly record struct OverdueRange(int From, int To);
    private readonly record struct WindowSlot(DateTime PlannedAtUtc, DateTime WindowEndUtc);
    private readonly record struct NextClientSlot(
        int Index,
        DateTime PlannedAtUtc,
        DateTime WindowEndUtc,
        bool IsInWindowNow,
        TimeSpan RemainingWindow);

    private sealed class PlanningChannel
    {
        public long Id { get; init; }
    }

    private sealed class ScheduledItem
    {
        public DateTime PlannedAtUtc { get; init; }
        public long ChannelId { get; init; }
    }

    private sealed class JobPreviewResult
    {
        public string Status { get; init; } = PreviewStatusEmpty;
        public string Text { get; init; } = string.Empty;
        public string VariablesJson { get; init; } = "{}";
        public DateTime UpdatedAtUtc { get; init; }
        public string ErrorCode { get; init; } = string.Empty;
        public string ErrorDetail { get; init; } = string.Empty;
        public long? TemplateId { get; init; }
        public string TemplateName { get; init; } = string.Empty;
        public string TemplateKind { get; init; } = string.Empty;
    }

    private sealed class ScheduleResult
    {
        public List<ScheduledItem> Items { get; init; } = [];
        public int ChannelsUsed { get; init; }
        public int GapMinutes { get; init; }
        public int TimezoneWaitMinutes { get; init; }
        public int GapWaitMinutes { get; init; }
        public int TotalWaitMinutes { get; init; }
        public int EstimatedDurationMinutes { get; init; }
        public DateTime EstimatedFinishAtUtc { get; init; }
    }
}
