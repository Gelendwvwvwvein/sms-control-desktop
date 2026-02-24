namespace Collector.Api;

public sealed class EventLogItemDto
{
    public long Id { get; set; }
    public string Category { get; set; } = string.Empty;
    public string EventType { get; set; } = string.Empty;
    public string Severity { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
    public long? RunSessionId { get; set; }
    public long? RunJobId { get; set; }
    public string PayloadJson { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; }
}

public sealed class EventLogListDto
{
    public int Total { get; set; }
    public List<EventLogItemDto> Items { get; set; } = [];
}
