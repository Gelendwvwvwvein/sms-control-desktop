namespace Collector.Data.Entities;

public sealed class SettingRecord
{
    public required string Key { get; set; }
    public required string ValueJson { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
}

public sealed class ClientSnapshot
{
    public long Id { get; set; }
    public required string SourceMode { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public int TotalRows { get; set; }
    public string? Notes { get; set; }
    public List<ClientSnapshotRow> Rows { get; set; } = [];
}

public sealed class ClientSnapshotRow
{
    public long Id { get; set; }
    public long SnapshotId { get; set; }
    public required string ExternalClientId { get; set; }
    public required string Fio { get; set; }
    public required string Phone { get; set; }
    public int TimezoneOffset { get; set; }
    public int DaysOverdue { get; set; }
    public string? ContractStatus { get; set; }
    public string? CardUrl { get; set; }
    public string? TotalWithCommissionRaw { get; set; }
    public DateTime CollectedAtUtc { get; set; }
    public ClientSnapshot Snapshot { get; set; } = null!;
}

public sealed class TemplateRecord
{
    public long Id { get; set; }
    public required string Name { get; set; }
    public required string Kind { get; set; }
    public required string OverdueMode { get; set; }
    public int? OverdueFromDays { get; set; }
    public int? OverdueToDays { get; set; }
    public int? OverdueExactDay { get; set; }
    public bool AutoAssign { get; set; }
    public string? CommentText { get; set; }
    public required string Status { get; set; }
    public required string Text { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
}

public sealed class ManualReplyPresetRecord
{
    public long Id { get; set; }
    public required string Title { get; set; }
    public required string Text { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
}

public sealed class StopListRecord
{
    public long Id { get; set; }
    public required string Phone { get; set; }
    public string? Reason { get; set; }
    public required string Source { get; set; }
    public DateTime AddedAtUtc { get; set; }
    public bool IsActive { get; set; }
}

public sealed class SenderChannelRecord
{
    public long Id { get; set; }
    public required string Name { get; set; }
    public required string Endpoint { get; set; }
    public required string Token { get; set; }
    public string? SimPhone { get; set; }
    public required string Status { get; set; }
    public DateTime? LastCheckedAtUtc { get; set; }
    public int FailStreak { get; set; }
    public bool Alerted { get; set; }
}

public sealed class ClientChannelBindingRecord
{
    public long Id { get; set; }
    public required string ExternalClientId { get; set; }
    public required string Phone { get; set; }
    public long ChannelId { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
    public DateTime? LastUsedAtUtc { get; set; }
}

public sealed class ClientDebtCacheRecord
{
    public long Id { get; set; }
    public required string ExternalClientId { get; set; }
    public required string Phone { get; set; }
    public required string CardUrl { get; set; }
    public required string ExactTotalRaw { get; set; }
    public required string ApproxTotalText { get; set; }
    public int? ApproxTotalValue { get; set; }
    public required string Status { get; set; }
    public required string Source { get; set; }
    public DateTime? LastFetchedAtUtc { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
    public string? LastErrorCode { get; set; }
    public string? LastErrorDetail { get; set; }
}

public sealed class RunSessionRecord
{
    public long Id { get; set; }
    public required string Mode { get; set; }
    public required string Status { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public DateTime? StartedAtUtc { get; set; }
    public DateTime? FinishedAtUtc { get; set; }
    public long? SnapshotId { get; set; }
    public string? FiltersJson { get; set; }
    public string? Notes { get; set; }
    public ClientSnapshot? Snapshot { get; set; }
    public List<RunJobRecord> Jobs { get; set; } = [];
}

public sealed class RunJobRecord
{
    public long Id { get; set; }
    public long RunSessionId { get; set; }
    public required string ExternalClientId { get; set; }
    public required string ClientFio { get; set; }
    public required string Phone { get; set; }
    public int TzOffset { get; set; }
    public int DaysOverdue { get; set; }
    public long? TemplateId { get; set; }
    public long? ChannelId { get; set; }
    public required string DeliveryType { get; set; }
    public required string Status { get; set; }
    public int Attempts { get; set; }
    public int MaxAttempts { get; set; }
    public DateTime PlannedAtUtc { get; set; }
    public DateTime? SentAtUtc { get; set; }
    public string? LastErrorCode { get; set; }
    public string? LastErrorDetail { get; set; }
    public string? PayloadJson { get; set; }
    public string? PreviewStatus { get; set; }
    public string? PreviewText { get; set; }
    public string? PreviewVariablesJson { get; set; }
    public DateTime? PreviewUpdatedAtUtc { get; set; }
    public string? PreviewErrorCode { get; set; }
    public string? PreviewErrorDetail { get; set; }
    public RunSessionRecord RunSession { get; set; } = null!;
}

public sealed class MessageRecord
{
    public long Id { get; set; }
    public long? RunJobId { get; set; }
    public required string ClientPhone { get; set; }
    public required string Direction { get; set; }
    public required string Text { get; set; }
    public string? GatewayStatus { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public string? MetaJson { get; set; }
}

public sealed class ManualDialogDraftRecord
{
    public long Id { get; set; }
    public required string Phone { get; set; }
    public required string Text { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
}

public sealed class AuditLogRecord
{
    public long Id { get; set; }
    public required string Category { get; set; }
    public required string Action { get; set; }
    public string? EntityId { get; set; }
    public required string Actor { get; set; }
    public string? DetailsJson { get; set; }
    public DateTime CreatedAtUtc { get; set; }
}

public sealed class AlertRecord
{
    public long Id { get; set; }
    public required string Level { get; set; }
    public required string Text { get; set; }
    public required string Status { get; set; }
    public long? ChannelId { get; set; }
    public DateTime CreatedAtUtc { get; set; }
    public DateTime? ClosedAtUtc { get; set; }
    public string? MetaJson { get; set; }
}

public sealed class EventLogRecord
{
    public long Id { get; set; }
    public required string Category { get; set; }
    public required string EventType { get; set; }
    public required string Severity { get; set; }
    public required string Message { get; set; }
    public long? RunSessionId { get; set; }
    public long? RunJobId { get; set; }
    public string? PayloadJson { get; set; }
    public DateTime CreatedAtUtc { get; set; }
}
