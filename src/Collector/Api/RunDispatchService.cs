using System.Text.Json;
using System.Text.Json.Nodes;
using System.Globalization;
using Collector.Data;
using Collector.Data.Entities;
using Collector.Services;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed partial class RunDispatchService(
    SettingsStore settingsStore,
    AlertService alertService,
    RuleEngineService ruleEngine,
    DebtCacheService debtCacheService,
    RocketmanCommentService rocketmanCommentService,
    RunCancellationCoordinator runCancellationCoordinator)
{
    private const string RunModeLive = SnapshotModes.Live;
    private const string SessionStatusRunning = RunSessionStatuses.Running;
    private const string SessionStatusCompleted = RunSessionStatuses.Completed;

    private const string JobStatusQueued = RunJobStatuses.Queued;
    private const string JobStatusRunning = RunJobStatuses.Running;
    private const string JobStatusRetry = RunJobStatuses.Retry;
    private const string JobStatusStopped = RunJobStatuses.Stopped;
    private const string JobStatusSent = RunJobStatuses.Sent;
    private const string JobStatusFailed = RunJobStatuses.Failed;
    private const string JobErrorStoppedByOperator = RunJobErrorCodes.StoppedByOperator;
    private const string JobErrorStoppedByStopList = RunJobErrorCodes.StoppedByStopList;
    private const string DebtStatusReady = DebtStatuses.Ready;
    private const string MessageDirectionOut = MessageDirections.Out;
    private const string MessageGatewayStatusSent = MessageGatewayStatuses.Sent;

    private const string ChannelStatusError = ChannelStatuses.Error;
    private const string ChannelStatusOnline = ChannelStatuses.Online;
    private const string ChannelStatusOffline = ChannelStatuses.Offline;
    private const string DeliveryTypeSms = DeliveryTypes.Sms;
    private const string PreviewStatusReady = PreviewStatuses.Ready;
    private const string PreviewStatusError = PreviewStatuses.Error;
    private const string PayloadFieldMessageOverride = PayloadFields.MessageOverrideText;
    private const string PayloadFieldCardUrl = PayloadFields.CardUrl;
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
                using var attemptCts = runCancellationCoordinator.CreateAttemptScope(runSession.Id, cancellationToken);
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
                var stoppedByOperator = runCancellationCoordinator.IsSessionCancellationRequested(runSession.Id);
                attemptResult = stoppedByOperator
                    ? new DispatchAttemptResult
                    {
                        Success = false,
                        IsTransient = false,
                        CountAsAttempt = false,
                        Code = JobErrorStoppedByOperator,
                        Detail = "Остановлено оператором во время попытки отправки."
                    }
                    : new DispatchAttemptResult
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
}
