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

}
