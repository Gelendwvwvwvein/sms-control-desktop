namespace Collector.Api;

public sealed class AuditLogDto
{
    public long Id { get; set; }
    public string Category { get; set; } = string.Empty;
    public string Action { get; set; } = string.Empty;
    public string EntityId { get; set; } = string.Empty;
    public string Actor { get; set; } = string.Empty;
    public string DetailsJson { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; }
}

public sealed class AuditListDto
{
    public int Total { get; set; }
    public List<AuditLogDto> Items { get; set; } = [];
}
