namespace Collector.Api;

public sealed class DialogSummaryDto
{
    public string DialogId { get; set; } = string.Empty;
    public string Phone { get; set; } = string.Empty;
    public string Fio { get; set; } = string.Empty;
    public DateTime LastMessageAtUtc { get; set; }
    public string LastDirection { get; set; } = string.Empty;
    public string LastText { get; set; } = string.Empty;
    public int TotalMessages { get; set; }
    public bool HasIncoming { get; set; }
    public long LastOutgoingChannelId { get; set; }
    public string LastOutgoingChannelName { get; set; } = string.Empty;
}

public sealed class DialogListDto
{
    public int TotalDialogs { get; set; }
    public List<DialogSummaryDto> Items { get; set; } = [];
}

public sealed class DialogMessageDto
{
    public long Id { get; set; }
    public string Phone { get; set; } = string.Empty;
    public string Direction { get; set; } = string.Empty;
    public string Text { get; set; } = string.Empty;
    public string GatewayStatus { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; }
    public string MetaJson { get; set; } = string.Empty;
}

public sealed class DialogMessagesDto
{
    public string DialogId { get; set; } = string.Empty;
    public string Phone { get; set; } = string.Empty;
    public int TotalMessages { get; set; }
    public List<DialogMessageDto> Items { get; set; } = [];
}

public sealed class DialogManualSendRequest
{
    public string Text { get; set; } = string.Empty;
    public long? ChannelId { get; set; }
    public int? TimezoneOffset { get; set; }
}

public sealed class DialogManualSendResultDto
{
    public bool Success { get; set; }
    public string Code { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
    public string Phone { get; set; } = string.Empty;
    public long ChannelId { get; set; }
    public string ChannelName { get; set; } = string.Empty;
    public int StatusCode { get; set; }
    public long MessageId { get; set; }
}

public sealed class DialogDeleteResultDto
{
    public string Phone { get; set; } = string.Empty;
    public int DeletedMessages { get; set; }
}

public sealed class DialogPruneRequest
{
    public int OlderThanDays { get; set; } = 30;
}

public sealed class DialogPruneResultDto
{
    public int OlderThanDays { get; set; }
    public DateTime ThresholdUtc { get; set; }
    public int DeletedMessages { get; set; }
}

public sealed class DialogDraftDto
{
    public string Phone { get; set; } = string.Empty;
    public string Text { get; set; } = string.Empty;
    public bool Exists { get; set; }
    public DateTime? UpdatedAtUtc { get; set; }
}

public sealed class DialogDraftUpsertRequest
{
    public string Text { get; set; } = string.Empty;
}

public sealed class DialogDraftDeleteResultDto
{
    public string Phone { get; set; } = string.Empty;
    public bool Deleted { get; set; }
}
