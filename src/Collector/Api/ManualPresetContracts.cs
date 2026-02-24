namespace Collector.Api;

public sealed class ManualPresetDto
{
    public long Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public string Text { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
}

public sealed class ManualPresetUpsertRequest
{
    public string Title { get; set; } = string.Empty;
    public string Text { get; set; } = string.Empty;
}
