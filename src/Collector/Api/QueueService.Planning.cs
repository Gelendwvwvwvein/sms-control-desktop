using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Collector.Data;
using Collector.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed partial class QueueService
{
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

}
