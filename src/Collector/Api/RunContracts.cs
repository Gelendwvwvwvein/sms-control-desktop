namespace Collector.Api;

public sealed class RunStartRequestDto
{
    public long? RunSessionId { get; set; }
}

public sealed class RunStopRequestDto
{
    public long? RunSessionId { get; set; }
    public string Reason { get; set; } = string.Empty;
}

public sealed class RunSessionSummaryDto
{
    public long Id { get; set; }
    public string Mode { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; }
    public DateTime? StartedAtUtc { get; set; }
    public DateTime? FinishedAtUtc { get; set; }
    public long? SnapshotId { get; set; }
    public string Notes { get; set; } = string.Empty;
    public int TotalJobs { get; set; }
    public int QueuedJobs { get; set; }
    public int RunningJobs { get; set; }
    public int RetryJobs { get; set; }
    public int StoppedJobs { get; set; }
    public int SentJobs { get; set; }
    public int FailedJobs { get; set; }
}

public sealed class RunStatusDto
{
    public bool HasSnapshot { get; set; }
    public long LatestSnapshotId { get; set; }
    public DateTime? LatestSnapshotCreatedAtUtc { get; set; }
    public bool HasSession { get; set; }
    public RunSessionSummaryDto Session { get; set; } = new();
    public bool HasRunningSession { get; set; }
    public long RunningSessionId { get; set; }
    public bool CanStart { get; set; }
    public bool CanStop { get; set; }
    public string StartBlockCode { get; set; } = string.Empty;
    public string StartBlockMessage { get; set; } = string.Empty;
}

public sealed class RunCommandResultDto
{
    public string Action { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
    public DateTime ChangedAtUtc { get; set; }
    public RunStatusDto Status { get; set; } = new();
}

public sealed class RunHistoryListDto
{
    public int Total { get; set; }
    public List<RunSessionSummaryDto> Items { get; set; } = [];
}

public sealed class RunHistoryClearResultDto
{
    public int DeletedSessions { get; set; }
    public int DeletedEvents { get; set; }
    public int ProtectedSessions { get; set; }
}
