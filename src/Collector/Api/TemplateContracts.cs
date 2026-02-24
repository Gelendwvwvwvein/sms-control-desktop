namespace Collector.Api;

public sealed class TemplateTypeMetaDto
{
    public string Kind { get; set; } = string.Empty;
    public string Label { get; set; } = string.Empty;
    public string RangeText { get; set; } = string.Empty;
    public int MinOverdueDays { get; set; }
    public int MaxOverdueDays { get; set; }
    public bool AutoAssign { get; set; }
    public string RuleHint { get; set; } = string.Empty;
    public int SortOrder { get; set; }
}

public sealed class TemplateDto
{
    public long Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Kind { get; set; } = string.Empty;
    public string KindLabel { get; set; } = string.Empty;
    public string RangeText { get; set; } = string.Empty;
    public int MinOverdueDays { get; set; }
    public int MaxOverdueDays { get; set; }
    public bool AutoAssign { get; set; }
    public string Status { get; set; } = "draft";
    public string Text { get; set; } = string.Empty;
    public DateTime CreatedAtUtc { get; set; }
    public DateTime UpdatedAtUtc { get; set; }
}

public sealed class TemplateUpsertRequest
{
    public string Name { get; set; } = string.Empty;
    public string Kind { get; set; } = string.Empty;
    public string Status { get; set; } = "draft";
    public string Text { get; set; } = string.Empty;
}

public sealed class TemplateStatusPatchRequest
{
    public string Status { get; set; } = string.Empty;
}
