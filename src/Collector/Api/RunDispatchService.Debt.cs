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

}
