namespace Collector.Api;

public sealed class RocketmanCommentWriteRequest
{
    public string CardUrl { get; set; } = string.Empty;
    public string Comment { get; set; } = string.Empty;
    public int TimeoutMs { get; set; } = 30000;
    public bool Headed { get; set; }
}

public sealed class RocketmanCommentWriteResult
{
    public bool Success { get; set; }
    public string Code { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
    public string CardUrl { get; set; } = string.Empty;
    public string CurrentUrl { get; set; } = string.Empty;
    public int TimeoutMs { get; set; }
}

public sealed class RocketmanCardTotalRequest
{
    public string CardUrl { get; set; } = string.Empty;
    public int TimeoutMs { get; set; } = 30000;
    public bool Headed { get; set; }
}

public sealed class RocketmanCardTotalResult
{
    public bool Success { get; set; }
    public string Code { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
    public string CardUrl { get; set; } = string.Empty;
    public string CurrentUrl { get; set; } = string.Empty;
    public string TotalWithCommissionRaw { get; set; } = string.Empty;
    public int TimeoutMs { get; set; }
}
