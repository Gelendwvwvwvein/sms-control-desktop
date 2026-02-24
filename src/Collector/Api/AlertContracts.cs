namespace Collector.Api;

public sealed class AlertDto
{
    public long Id { get; set; }
    public string Level { get; set; } = "error";
    public string Text { get; set; } = string.Empty;
    public string Status { get; set; } = "active";
    public long? ChannelId { get; set; }
    public string ChannelName { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; }
    public DateTime? ClosedAtUtc { get; set; }
    public string MetaJson { get; set; } = string.Empty;
}

public sealed class AlertListDto
{
    public int Total { get; set; }
    public int ActiveCount { get; set; }
    public int ResolvedCount { get; set; }
    public int IrrelevantCount { get; set; }
    public List<AlertDto> Items { get; set; } = [];
}

public sealed class AlertStatusPatchRequest
{
    public string Status { get; set; } = string.Empty;
}
