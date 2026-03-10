namespace Collector.Api;

public sealed class ClientsSyncResultDto
{
    public long SnapshotId { get; set; }
    public string SourceMode { get; set; } = SnapshotModes.Live;
    public int TotalRows { get; set; }
    public DateTime StartedAtUtc { get; set; }
    public DateTime FinishedAtUtc { get; set; }
    public long DurationMs { get; set; }
    public bool CardTotalsSkipped { get; set; }
}

public sealed class ClientsSyncStatusDto
{
    public bool HasSnapshot { get; set; }
    public long SnapshotId { get; set; }
    public string SourceMode { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; }
    public int TotalRows { get; set; }
    public string Notes { get; set; } = string.Empty;
}

public sealed class ClientListItemDto
{
    public long Id { get; set; }
    public long SnapshotId { get; set; }
    public string ExternalClientId { get; set; } = string.Empty;
    public string Fio { get; set; } = string.Empty;
    public string Phone { get; set; } = string.Empty;
    public int TimezoneOffset { get; set; }
    public int DaysOverdue { get; set; }
    public bool ContractBlueFlag { get; set; }
    public string CardUrl { get; set; } = string.Empty;
    public string TotalWithCommissionRaw { get; set; } = string.Empty;
    public string DebtApproxText { get; set; } = string.Empty;
    public int? DebtApproxValue { get; set; }
    public string DebtStatus { get; set; } = DebtStatuses.Empty;
    public string DebtSource { get; set; } = string.Empty;
    public DateTime? DebtUpdatedAtUtc { get; set; }
    public string DebtErrorCode { get; set; } = string.Empty;
    public string DebtErrorDetail { get; set; } = string.Empty;
    public DateTime CollectedAtUtc { get; set; }
    public string DialogStatus { get; set; } = DialogStatuses.None;
    public bool InPlan { get; set; }
    public long? InPlanRunSessionId { get; set; }
}

public sealed class ClientsListDto
{
    public ClientsSyncStatusDto Snapshot { get; set; } = new();
    public int TotalRowsInSnapshot { get; set; }
    public List<ClientListItemDto> Items { get; set; } = [];
}
