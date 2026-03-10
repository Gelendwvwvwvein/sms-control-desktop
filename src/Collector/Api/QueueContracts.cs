namespace Collector.Api;

public sealed class QueueFilterRequestDto
{
    public long? SnapshotId { get; set; }
    public List<int> TimezoneOffsets { get; set; } = [];
    public List<string> OverdueRanges { get; set; } = [];
    public int? ExactDay { get; set; }
    public string ExactOverdue { get; set; } = string.Empty;
}

public sealed class QueueAppliedFilterDto
{
    public long SnapshotId { get; set; }
    public List<int> TimezoneOffsets { get; set; } = [];
    public List<string> OverdueRanges { get; set; } = [];
    public int? ExactDay { get; set; }
    public string ExactOverdue { get; set; } = string.Empty;
    public int RecentSmsCooldownDays { get; set; }
}

public sealed class QueuePreviewDto
{
    public long SnapshotId { get; set; }
    public string SourceMode { get; set; } = SnapshotModes.Live;
    public int TotalRowsInSnapshot { get; set; }
    public int MatchedByFilter { get; set; }
    public int ExcludedByStopList { get; set; }
    public int ExcludedByMissingPhone { get; set; }
    public int ExcludedByRecentSms { get; set; }
    public int ReadyRows { get; set; }
    public bool CanBuild { get; set; }
    public QueueAppliedFilterDto AppliedFilter { get; set; } = new();
}

public sealed class QueueForecastDto
{
    public QueuePreviewDto Preview { get; set; } = new();
    public int OnlineChannelsCount { get; set; }
    public int ChannelsUsed { get; set; }
    public int GapMinutes { get; set; }
    public int TimezoneWaitMinutes { get; set; }
    public int GapWaitMinutes { get; set; }
    public int TotalWaitMinutes { get; set; }
    public int EstimatedDurationMinutes { get; set; }
    public DateTime EstimatedFinishAtUtc { get; set; }
    public DateTime ForecastedAtUtc { get; set; }
}

public sealed class QueueBuildResultDto
{
    public long RunSessionId { get; set; }
    public string Status { get; set; } = RunSessionStatuses.Planned;
    public DateTime CreatedAtUtc { get; set; }
    public int CreatedJobs { get; set; }
    public QueuePreviewDto Preview { get; set; } = new();
    public QueueForecastDto Forecast { get; set; } = new();
}

public sealed class QueueSessionDto
{
    public long Id { get; set; }
    public string Mode { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; }
    public DateTime? StartedAtUtc { get; set; }
    public DateTime? FinishedAtUtc { get; set; }
    public long? SnapshotId { get; set; }
    public string FiltersJson { get; set; } = string.Empty;
    public string Notes { get; set; } = string.Empty;
}

public sealed class QueueJobDto
{
    public long Id { get; set; }
    public long RunSessionId { get; set; }
    public string ExternalClientId { get; set; } = string.Empty;
    public string ClientFio { get; set; } = string.Empty;
    public string Phone { get; set; } = string.Empty;
    public int TzOffset { get; set; }
    public int DaysOverdue { get; set; }
    public long? TemplateId { get; set; }
    public string TemplateName { get; set; } = string.Empty;
    public string TemplateKind { get; set; } = string.Empty;
    public string TemplateStatus { get; set; } = string.Empty;
    public long? ChannelId { get; set; }
    public string ChannelName { get; set; } = string.Empty;
    public string ChannelStatus { get; set; } = string.Empty;
    public string DeliveryType { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public int Attempts { get; set; }
    public int MaxAttempts { get; set; }
    public DateTime PlannedAtUtc { get; set; }
    public DateTime? SentAtUtc { get; set; }
    public string LastErrorCode { get; set; } = string.Empty;
    public string LastErrorDetail { get; set; } = string.Empty;
    public string CardUrl { get; set; } = string.Empty;
    public string TotalWithCommissionRaw { get; set; } = string.Empty;
    public string DebtApproxText { get; set; } = string.Empty;
    public int? DebtApproxValue { get; set; }
    public string DebtStatus { get; set; } = DebtStatuses.Empty;
    public string DebtSource { get; set; } = string.Empty;
    public DateTime? DebtUpdatedAtUtc { get; set; }
    public string DebtErrorCode { get; set; } = string.Empty;
    public string DebtErrorDetail { get; set; } = string.Empty;
    public string PreviewStatus { get; set; } = string.Empty;
    public string PreviewText { get; set; } = string.Empty;
    public string PreviewVariablesJson { get; set; } = string.Empty;
    public DateTime? PreviewUpdatedAtUtc { get; set; }
    public string PreviewErrorCode { get; set; } = string.Empty;
    public string PreviewErrorDetail { get; set; } = string.Empty;
    public bool HasMessageOverride { get; set; }
    public string MessageOverrideText { get; set; } = string.Empty;
    public string DialogStatus { get; set; } = DialogStatuses.None;
    public int DialogMessagesCount { get; set; }
}

public sealed class QueueListDto
{
    public bool HasSession { get; set; }
    public QueueSessionDto Session { get; set; } = new();
    public int TotalJobsInSession { get; set; }
    public List<QueueJobDto> Items { get; set; } = [];
}

public sealed class QueueRemoveJobsRequestDto
{
    public long? RunSessionId { get; set; }
    public List<long> JobIds { get; set; } = [];
}

public sealed class QueueRemoveJobsResultDto
{
    public long RunSessionId { get; set; }
    public int Requested { get; set; }
    public int Removed { get; set; }
    public int Skipped { get; set; }
    public int RemainingJobs { get; set; }
    public string SessionStatus { get; set; } = string.Empty;
}

public sealed class QueueBulkSetTemplateRequest
{
    public long? RunSessionId { get; set; }
    public List<long> JobIds { get; set; } = [];
    public long TemplateId { get; set; }
}

public sealed class QueueBulkSetTemplateResultDto
{
    public long RunSessionId { get; set; }
    public int Requested { get; set; }
    public int Applied { get; set; }
    public int Skipped { get; set; }
    public List<string> SkippedReasons { get; set; } = [];
}

public sealed class QueueRetryErrorsRequestDto
{
    public long? RunSessionId { get; set; }
}

public sealed class QueueRetryErrorsResultDto
{
    public long RunSessionId { get; set; }
    public int Retried { get; set; }
    public int FromFailed { get; set; }
    public int FromStopped { get; set; }
    public int RemainingFailed { get; set; }
    public int RemainingStopped { get; set; }
    public string SessionStatus { get; set; } = string.Empty;
}

public sealed class QueueJobPreviewRequestDto
{
    public bool Persist { get; set; } = true;
}

public sealed class QueueJobMessageOverrideRequestDto
{
    public string Text { get; set; } = string.Empty;
}

public sealed class QueueJobPreviewDto
{
    public long JobId { get; set; }
    public long RunSessionId { get; set; }
    public string ExternalClientId { get; set; } = string.Empty;
    public string Phone { get; set; } = string.Empty;
    public long? TemplateId { get; set; }
    public string TemplateName { get; set; } = string.Empty;
    public string TemplateKind { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public string Text { get; set; } = string.Empty;
    public string VariablesJson { get; set; } = string.Empty;
    public DateTime UpdatedAtUtc { get; set; }
    public string ErrorCode { get; set; } = string.Empty;
    public string ErrorDetail { get; set; } = string.Empty;
}

public sealed class QueueJobMessageOverrideDto
{
    public long JobId { get; set; }
    public long RunSessionId { get; set; }
    public string ExternalClientId { get; set; } = string.Empty;
    public string Phone { get; set; } = string.Empty;
    public bool HasMessageOverride { get; set; }
    public string MessageOverrideText { get; set; } = string.Empty;
    public QueueJobPreviewDto Preview { get; set; } = new();
}
