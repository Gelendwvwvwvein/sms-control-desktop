using System.Text.Json;
using Collector.Data;
using Collector.Data.Entities;
using Microsoft.EntityFrameworkCore;
using System.Globalization;

namespace Collector.Api;

public sealed class RunService(SettingsStore settingsStore)
{
    private const string SnapshotModeLive = "live";
    private const string SessionStatusPlanned = "planned";
    private const string SessionStatusRunning = "running";
    private const string SessionStatusStopped = "stopped";

    private const string JobStatusQueued = "queued";
    private const string JobStatusRunning = "running";
    private const string JobStatusRetry = "retry";
    private const string JobStatusStopped = "stopped";
    private const string JobStatusSent = "sent";
    private const string JobStatusFailed = "failed";
    private const string JobErrorStoppedByOperator = "RUN_STOPPED_BY_OPERATOR";

    public async Task<RunStatusDto> GetStatusAsync(
        AppDbContext db,
        long? runSessionId,
        CancellationToken cancellationToken)
    {
        var latestSnapshot = await db.ClientSnapshots
            .AsNoTracking()
            .OrderByDescending(x => x.Id)
            .Select(x => new { x.Id, x.CreatedAtUtc })
            .FirstOrDefaultAsync(cancellationToken);

        var runningSession = await db.RunSessions
            .AsNoTracking()
            .Where(x => x.Status == SessionStatusRunning)
            .OrderByDescending(x => x.Id)
            .FirstOrDefaultAsync(cancellationToken);

        RunSessionRecord? selectedSession;
        if (runSessionId.HasValue && runSessionId.Value > 0)
        {
            selectedSession = await db.RunSessions
                .AsNoTracking()
                .FirstOrDefaultAsync(x => x.Id == runSessionId.Value, cancellationToken);

            if (selectedSession is null)
            {
                throw new RunStateException(
                    "RUN_SESSION_NOT_FOUND",
                    $"Сессия запуска с id={runSessionId.Value} не найдена.",
                    404);
            }
        }
        else
        {
            selectedSession = runningSession ?? await db.RunSessions
                .AsNoTracking()
                .OrderByDescending(x => x.Id)
                .FirstOrDefaultAsync(cancellationToken);
        }

        var status = new RunStatusDto
        {
            HasSnapshot = latestSnapshot is not null,
            LatestSnapshotId = latestSnapshot?.Id ?? 0,
            LatestSnapshotCreatedAtUtc = latestSnapshot?.CreatedAtUtc,
            HasRunningSession = runningSession is not null,
            RunningSessionId = runningSession?.Id ?? 0,
            CanStop = runningSession is not null
        };

        RunSessionSummaryDto? summary = null;
        if (selectedSession is not null)
        {
            summary = await BuildSessionSummaryAsync(db, selectedSession, cancellationToken);
            status.HasSession = true;
            status.Session = summary;
        }

        var settings = await settingsStore.GetAsync(db, cancellationToken);
        var startBlock = EvaluateStartBlock(status, summary, settings.AllowLiveDispatch);
        status.CanStart = string.IsNullOrEmpty(startBlock.Code);
        status.StartBlockCode = startBlock.Code;
        status.StartBlockMessage = startBlock.Message;

        return status;
    }

    public async Task<RunCommandResultDto> StartAsync(
        AppDbContext db,
        RunStartRequestDto payload,
        CancellationToken cancellationToken)
    {
        var request = payload ?? new RunStartRequestDto();
        var now = DateTime.UtcNow;
        var current = await GetStatusAsync(db, request.RunSessionId, cancellationToken);
        if (!current.CanStart)
        {
            throw BuildStartBlockedException(current.StartBlockCode, current.StartBlockMessage);
        }

        var sessionId = request.RunSessionId ?? current.Session.Id;
        var session = await db.RunSessions.FirstOrDefaultAsync(x => x.Id == sessionId, cancellationToken);
        if (session is null)
        {
            throw new RunStateException(
                "RUN_SESSION_NOT_FOUND",
                $"Сессия запуска с id={sessionId} не найдена.",
                404);
        }

        var settings = await settingsStore.GetAsync(db, cancellationToken);
        if (!TryParseWorkWindowStrict(settings.WorkWindowStart, settings.WorkWindowEnd, out _, out _))
        {
            throw new RunStateException(
                "RUN_WORK_WINDOW_INVALID",
                "Некорректно задано рабочее окно в настройках. Укажите формат HH:mm и убедитесь, что конец больше начала.",
                409);
        }

        var wasStopped = string.Equals(session.Status, SessionStatusStopped, StringComparison.Ordinal);
        session.Status = SessionStatusRunning;
        session.StartedAtUtc = now;
        session.FinishedAtUtc = null;
        session.Notes = "Запуск активен.";

        if (wasStopped)
        {
            await RestoreStoppedJobsToQueueAsync(db, session.Id, cancellationToken);
        }

        await RebasePendingJobsToStartMomentAsync(db, session.Id, now, settings, cancellationToken);

        db.RunSessions.Update(session);
        AddRunEvent(
            db,
            runSessionId: session.Id,
            eventType: "run_started",
            severity: "info",
            message: $"Запуск сессии #{session.Id}.",
            payload: new { sessionId = session.Id });

        await db.SaveChangesAsync(cancellationToken);

        return new RunCommandResultDto
        {
            Action = "start",
            Message = $"Сессия #{session.Id} переведена в состояние \"running\".",
            ChangedAtUtc = now,
            Status = await GetStatusAsync(db, session.Id, cancellationToken)
        };
    }

    public async Task<RunCommandResultDto> StopAsync(
        AppDbContext db,
        RunStopRequestDto payload,
        CancellationToken cancellationToken)
    {
        var request = payload ?? new RunStopRequestDto();
        var now = DateTime.UtcNow;

        RunSessionRecord? session;
        if (request.RunSessionId.HasValue && request.RunSessionId.Value > 0)
        {
            session = await db.RunSessions.FirstOrDefaultAsync(x => x.Id == request.RunSessionId.Value, cancellationToken);
            if (session is null)
            {
                throw new RunStateException(
                    "RUN_SESSION_NOT_FOUND",
                    $"Сессия запуска с id={request.RunSessionId.Value} не найдена.",
                    404);
            }
        }
        else
        {
            session = await db.RunSessions
                .OrderByDescending(x => x.Id)
                .FirstOrDefaultAsync(x => x.Status == SessionStatusRunning, cancellationToken);

            if (session is null)
            {
                throw new RunStateException(
                    "RUN_NOT_RUNNING",
                    "Нет активной сессии в состоянии \"running\". Останавливать нечего.",
                    409);
            }
        }

        if (!string.Equals(session.Status, SessionStatusRunning, StringComparison.Ordinal))
        {
            throw new RunStateException(
                "RUN_NOT_RUNNING",
                $"Сессия #{session.Id} имеет статус \"{session.Status}\". Остановка доступна только для \"running\".",
                409);
        }

        session.Status = SessionStatusStopped;
        session.FinishedAtUtc = now;
        var normalizedReason = NormalizeReason(request.Reason);
        session.Notes = !string.IsNullOrEmpty(normalizedReason)
            ? $"Остановлено оператором: {normalizedReason}"
            : "Остановлено оператором.";

        var stopDetail = !string.IsNullOrEmpty(normalizedReason)
            ? $"Остановлено оператором: {normalizedReason}"
            : "Остановлено оператором.";
        var pendingJobs = await db.RunJobs
            .Where(x => x.RunSessionId == session.Id)
            .Where(x => x.Status == JobStatusQueued || x.Status == JobStatusRetry)
            .ToListAsync(cancellationToken);

        foreach (var job in pendingJobs)
        {
            job.Status = JobStatusStopped;
            job.LastErrorCode = JobErrorStoppedByOperator;
            job.LastErrorDetail = stopDetail;
        }

        if (pendingJobs.Count > 0)
        {
            db.RunJobs.UpdateRange(pendingJobs);
        }

        db.RunSessions.Update(session);
        AddRunEvent(
            db,
            runSessionId: session.Id,
            eventType: "run_stopped",
            severity: "warning",
            message: $"Остановка сессии #{session.Id}.",
            payload: new
            {
                sessionId = session.Id,
                reason = normalizedReason,
                stoppedJobs = pendingJobs.Count
            });

        await db.SaveChangesAsync(cancellationToken);

        return new RunCommandResultDto
        {
            Action = "stop",
            Message = $"Сессия #{session.Id} переведена в состояние \"stopped\".",
            ChangedAtUtc = now,
            Status = await GetStatusAsync(db, session.Id, cancellationToken)
        };
    }

    public static ApiErrorDto? ValidateStartRequest(RunStartRequestDto? payload)
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
                Code = "RUN_SESSION_ID_INVALID",
                Message = "runSessionId должен быть положительным числом."
            };
        }

        return null;
    }

    public static ApiErrorDto? ValidateStopRequest(RunStopRequestDto? payload)
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
                Code = "RUN_SESSION_ID_INVALID",
                Message = "runSessionId должен быть положительным числом."
            };
        }

        if (!string.IsNullOrWhiteSpace(payload.Reason) && payload.Reason.Trim().Length > 512)
        {
            return new ApiErrorDto
            {
                Code = "RUN_STOP_REASON_TOO_LONG",
                Message = "reason не должен превышать 512 символов."
            };
        }

        return null;
    }

    public async Task<RunHistoryListDto> ListHistoryAsync(
        AppDbContext db,
        int limit,
        int offset,
        CancellationToken cancellationToken)
    {
        var protectedIds = await GetProtectedHistorySessionIdsAsync(db, cancellationToken);
        var query = db.RunSessions
            .AsNoTracking()
            .Where(x => !protectedIds.Contains(x.Id));

        var total = await query.CountAsync(cancellationToken);
        var sessions = await query
            .OrderByDescending(x => x.Id)
            .Skip(offset)
            .Take(limit)
            .ToListAsync(cancellationToken);

        var items = new List<RunSessionSummaryDto>(sessions.Count);
        foreach (var session in sessions)
        {
            items.Add(await BuildSessionSummaryAsync(db, session, cancellationToken));
        }

        return new RunHistoryListDto
        {
            Total = total,
            Items = items
        };
    }

    public async Task<RunHistoryClearResultDto> ClearHistoryAsync(
        AppDbContext db,
        CancellationToken cancellationToken)
    {
        var protectedIds = await GetProtectedHistorySessionIdsAsync(db, cancellationToken);
        var deletableSessionIds = await db.RunSessions
            .AsNoTracking()
            .Where(x => !protectedIds.Contains(x.Id))
            .Select(x => x.Id)
            .ToListAsync(cancellationToken);

        if (deletableSessionIds.Count == 0)
        {
            return new RunHistoryClearResultDto
            {
                DeletedSessions = 0,
                DeletedEvents = 0,
                ProtectedSessions = protectedIds.Count
            };
        }

        var deletedEvents = await db.Events
            .Where(x => x.RunSessionId.HasValue && deletableSessionIds.Contains(x.RunSessionId.Value))
            .ExecuteDeleteAsync(cancellationToken);
        var deletedSessions = await db.RunSessions
            .Where(x => deletableSessionIds.Contains(x.Id))
            .ExecuteDeleteAsync(cancellationToken);

        return new RunHistoryClearResultDto
        {
            DeletedSessions = deletedSessions,
            DeletedEvents = deletedEvents,
            ProtectedSessions = protectedIds.Count
        };
    }

    private static async Task<RunSessionSummaryDto> BuildSessionSummaryAsync(
        AppDbContext db,
        RunSessionRecord session,
        CancellationToken cancellationToken)
    {
        var counters = await db.RunJobs
            .AsNoTracking()
            .Where(x => x.RunSessionId == session.Id)
            .GroupBy(x => x.Status)
            .Select(g => new { Status = g.Key, Count = g.Count() })
            .ToListAsync(cancellationToken);

        var byStatus = counters.ToDictionary(x => x.Status, x => x.Count, StringComparer.Ordinal);
        var totalJobs = counters.Sum(x => x.Count);

        return new RunSessionSummaryDto
        {
            Id = session.Id,
            Mode = session.Mode,
            Status = session.Status,
            CreatedAtUtc = session.CreatedAtUtc,
            StartedAtUtc = session.StartedAtUtc,
            FinishedAtUtc = session.FinishedAtUtc,
            SnapshotId = session.SnapshotId,
            Notes = session.Notes ?? string.Empty,
            TotalJobs = totalJobs,
            QueuedJobs = ReadCount(byStatus, JobStatusQueued),
            RunningJobs = ReadCount(byStatus, JobStatusRunning),
            RetryJobs = ReadCount(byStatus, JobStatusRetry),
            StoppedJobs = ReadCount(byStatus, JobStatusStopped),
            SentJobs = ReadCount(byStatus, JobStatusSent),
            FailedJobs = ReadCount(byStatus, JobStatusFailed)
        };
    }

    private static async Task<HashSet<long>> GetProtectedHistorySessionIdsAsync(
        AppDbContext db,
        CancellationToken cancellationToken)
    {
        var result = new HashSet<long>();

        var runningSessionId = await db.RunSessions
            .AsNoTracking()
            .Where(x => x.Status == SessionStatusRunning)
            .OrderByDescending(x => x.Id)
            .Select(x => (long?)x.Id)
            .FirstOrDefaultAsync(cancellationToken);
        if (runningSessionId.HasValue && runningSessionId.Value > 0)
        {
            result.Add(runningSessionId.Value);
        }

        var latestPlannedSessionId = await db.RunSessions
            .AsNoTracking()
            .Where(x => x.Status == SessionStatusPlanned)
            .OrderByDescending(x => x.Id)
            .Select(x => (long?)x.Id)
            .FirstOrDefaultAsync(cancellationToken);
        if (latestPlannedSessionId.HasValue && latestPlannedSessionId.Value > 0)
        {
            result.Add(latestPlannedSessionId.Value);
        }

        return result;
    }

    private static (string Code, string Message) EvaluateStartBlock(
        RunStatusDto status,
        RunSessionSummaryDto? session,
        bool allowLiveDispatch)
    {
        var isStoppedSession = session is not null &&
                               string.Equals(session.Status, SessionStatusStopped, StringComparison.Ordinal);

        if (status.HasRunningSession)
        {
            return (
                "RUN_ALREADY_RUNNING",
                $"Уже есть активная сессия #{status.RunningSessionId} в состоянии \"running\".");
        }

        if (session is null || session.Id <= 0)
        {
            return (
                "RUN_QUEUE_NOT_FOUND",
                "Нет сформированной очереди. Сначала выполните /api/queue/build.");
        }

        if (!string.Equals(session.Status, SessionStatusPlanned, StringComparison.Ordinal) &&
            !string.Equals(session.Status, SessionStatusStopped, StringComparison.Ordinal))
        {
            return (
                "RUN_START_STATE_INVALID",
                $"Сессию со статусом \"{session.Status}\" нельзя запустить. Допустимо: \"planned\" или \"stopped\".");
        }

        if (session.TotalJobs <= 0)
        {
            return (
                "RUN_QUEUE_EMPTY",
                "Сессия не содержит задач для отправки.");
        }

        if (!allowLiveDispatch &&
            string.Equals(session.Mode, SnapshotModeLive, StringComparison.OrdinalIgnoreCase))
        {
            return (
                "RUN_LIVE_DISPATCH_BLOCKED",
                "Реальная рассылка в LIVE-режиме заблокирована в настройках. Включите переключатель в разделе «Настройки» и сохраните изменения.");
        }

        if (!status.HasSnapshot)
        {
            return (
                "RUN_SNAPSHOT_MISSING",
                "Нет актуализированной базы клиентов. Сначала выполните /api/clients/sync.");
        }

        if (!session.SnapshotId.HasValue || session.SnapshotId.Value <= 0)
        {
            return (
                "RUN_SESSION_SNAPSHOT_MISSING",
                "У выбранной сессии не указан snapshot. Сформируйте очередь заново.");
        }

        if (!isStoppedSession && status.LatestSnapshotId > session.SnapshotId.Value)
        {
            return (
                "RUN_QUEUE_STALE",
                "Плановая очередь устарела после новой актуализации базы. Сформируйте очередь заново.");
        }

        return (string.Empty, string.Empty);
    }

    private static RunStateException BuildStartBlockedException(string code, string message)
    {
        var statusCode = code switch
        {
            "RUN_QUEUE_NOT_FOUND" => 404,
            "RUN_ALREADY_RUNNING" => 409,
            "RUN_START_STATE_INVALID" => 409,
            "RUN_QUEUE_STALE" => 409,
            "RUN_QUEUE_EMPTY" => 409,
            "RUN_LIVE_DISPATCH_BLOCKED" => 409,
            "RUN_WORK_WINDOW_INVALID" => 409,
            _ => 400
        };

        return new RunStateException(code, message, statusCode);
    }

    private static int ReadCount(Dictionary<string, int> counters, string status)
    {
        return counters.TryGetValue(status, out var count) ? count : 0;
    }

    private static async Task RebasePendingJobsToStartMomentAsync(
        AppDbContext db,
        long runSessionId,
        DateTime startAtUtc,
        AppSettingsDto settings,
        CancellationToken cancellationToken)
    {
        var pending = await db.RunJobs
            .Where(x => x.RunSessionId == runSessionId &&
                        (x.Status == JobStatusQueued || x.Status == JobStatusRetry))
            .OrderBy(x => x.PlannedAtUtc)
            .ThenBy(x => x.Id)
            .ToListAsync(cancellationToken);
        if (pending.Count == 0)
        {
            return;
        }

        if (!TryParseWorkWindowStrict(settings.WorkWindowStart, settings.WorkWindowEnd, out var workStart, out var workEnd))
        {
            return;
        }

        var gapMinutes = Math.Max(1, settings.Gap);
        var availableChannels = await db.SenderChannels
            .AsNoTracking()
            .Where(x => x.Status != "error" && x.Status != "offline")
            .OrderBy(x => x.FailStreak)
            .ThenBy(x => x.Id)
            .Select(x => x.Id)
            .ToListAsync(cancellationToken);

        if (availableChannels.Count == 0)
        {
            foreach (var job in pending)
            {
                job.PlannedAtUtc = AlignToWorkWindowUtc(startAtUtc, job.TzOffset, workStart, workEnd);
            }

            db.RunJobs.UpdateRange(pending);
            return;
        }

        var nextAvailableByChannel = availableChannels.ToDictionary(x => x, _ => startAtUtc);

        foreach (var job in pending)
        {
            var selectedChannelId = job.ChannelId.HasValue && nextAvailableByChannel.ContainsKey(job.ChannelId.Value)
                ? job.ChannelId.Value
                : SelectEarliestChannel(nextAvailableByChannel);

            var channelBaseUtc = nextAvailableByChannel[selectedChannelId];
            var plannedAtUtc = AlignToWorkWindowUtc(channelBaseUtc, job.TzOffset, workStart, workEnd);
            job.ChannelId = selectedChannelId;
            job.PlannedAtUtc = plannedAtUtc;
            nextAvailableByChannel[selectedChannelId] = plannedAtUtc.AddMinutes(gapMinutes);
        }

        db.RunJobs.UpdateRange(pending);
    }

    private static long SelectEarliestChannel(Dictionary<long, DateTime> nextAvailableByChannel)
    {
        var selected = 0L;
        var selectedAt = DateTime.MaxValue;
        foreach (var pair in nextAvailableByChannel)
        {
            if (selected == 0L || pair.Value < selectedAt || (pair.Value == selectedAt && pair.Key < selected))
            {
                selected = pair.Key;
                selectedAt = pair.Value;
            }
        }

        return selected;
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

    private static async Task RestoreStoppedJobsToQueueAsync(
        AppDbContext db,
        long runSessionId,
        CancellationToken cancellationToken)
    {
        var stoppedJobs = await db.RunJobs
            .Where(x => x.RunSessionId == runSessionId &&
                        (x.Status == JobStatusStopped || x.Status == JobStatusRunning))
            .ToListAsync(cancellationToken);
        if (stoppedJobs.Count == 0)
        {
            return;
        }

        foreach (var job in stoppedJobs)
        {
            job.Status = JobStatusQueued;
            if (string.Equals(job.LastErrorCode, JobErrorStoppedByOperator, StringComparison.Ordinal))
            {
                job.LastErrorCode = null;
                job.LastErrorDetail = null;
            }
        }

        db.RunJobs.UpdateRange(stoppedJobs);
        await db.SaveChangesAsync(cancellationToken);
    }

    private static string NormalizeReason(string? reason)
    {
        var normalized = (reason ?? string.Empty).Trim();
        if (normalized.Length > 512)
        {
            normalized = normalized[..512];
        }

        return normalized;
    }

    private static void AddRunEvent(
        AppDbContext db,
        long runSessionId,
        string eventType,
        string severity,
        string message,
        object payload)
    {
        db.Events.Add(new EventLogRecord
        {
            Category = "run",
            EventType = eventType,
            Severity = severity,
            Message = message,
            RunSessionId = runSessionId,
            RunJobId = null,
            PayloadJson = JsonSerializer.Serialize(payload),
            CreatedAtUtc = DateTime.UtcNow
        });
    }
}

public sealed class RunStateException : Exception
{
    public RunStateException(string code, string message, int httpStatusCode)
        : base(message)
    {
        Code = code;
        HttpStatusCode = httpStatusCode;
    }

    public string Code { get; }
    public int HttpStatusCode { get; }
}
