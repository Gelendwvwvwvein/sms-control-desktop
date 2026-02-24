namespace Collector.Api;

public sealed class ClientDebtStateDto
{
    public bool FoundClient { get; set; }
    public string ExternalClientId { get; set; } = string.Empty;
    public string Phone { get; set; } = string.Empty;
    public string CardUrl { get; set; } = string.Empty;
    public string ExactTotalRaw { get; set; } = string.Empty;
    public string ApproxTotalText { get; set; } = string.Empty;
    public int? ApproxTotalValue { get; set; }
    public string Status { get; set; } = "empty";
    public string Source { get; set; } = string.Empty;
    public DateTime? LastFetchedAtUtc { get; set; }
    public DateTime? UpdatedAtUtc { get; set; }
    public string LastErrorCode { get; set; } = string.Empty;
    public string LastErrorDetail { get; set; } = string.Empty;
}

public sealed class ClientDebtFetchRequestDto
{
    public int TimeoutMs { get; set; } = 30000;
    public bool Headed { get; set; }
}

public sealed class ClientDebtFetchResultDto
{
    public bool Success { get; set; }
    public string Code { get; set; } = string.Empty;
    public string Message { get; set; } = string.Empty;
    public ClientDebtStateDto Debt { get; set; } = new();
}
