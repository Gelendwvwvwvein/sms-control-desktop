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
