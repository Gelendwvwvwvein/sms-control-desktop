namespace Collector.Api;

public sealed class StopListDto
{
    public long Id { get; set; }
    public string Phone { get; set; } = string.Empty;
    public string Reason { get; set; } = string.Empty;
    public string Source { get; set; } = string.Empty;
    public DateTime AddedAtUtc { get; set; }
    public bool IsActive { get; set; }
}

public sealed class StopListUpsertRequest
{
    public string Phone { get; set; } = string.Empty;
    public string Reason { get; set; } = string.Empty;
    public string Source { get; set; } = "manual";
    public bool IsActive { get; set; } = true;
}

public sealed class StopListBulkAddRequest
{
    public List<string> Phones { get; set; } = [];
    public string? Reason { get; set; }
    public string? Source { get; set; }
}

public sealed class StopListBulkRemoveRequest
{
    public List<string> Phones { get; set; } = [];
}

public sealed class StopListBulkDeactivateRequest
{
    public List<long> Ids { get; set; } = [];
}

public sealed class StopListBulkResultDto
{
    public int Requested { get; set; }
    public int Added { get; set; }
    public int Removed { get; set; }
    public int Skipped { get; set; }
    public List<string> InvalidPhones { get; set; } = [];
}
