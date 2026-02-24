namespace Collector.Api;

public sealed class CommentRulesDto
{
    public string Sms2 { get; set; } = "смс2";
    public string Sms3 { get; set; } = "смс3";
    public string Ka1 { get; set; } = "смс от ка";
    public string KaN { get; set; } = "смс ка{n}";
    public string KaFinal { get; set; } = "смс ка фин";
}

public sealed class AppSettingsDto
{
    public string LoginUrl { get; set; } = "https://rocketman.ru/manager/auth/login";
    public string Login { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public int Gap { get; set; } = 8;
    public bool AllowLiveDispatch { get; set; } = true;
    public string WorkWindowStart { get; set; } = "08:00";
    public string WorkWindowEnd { get; set; } = "21:00";
    public CommentRulesDto CommentRules { get; set; } = new();
}

public sealed class ApiErrorDto
{
    public required string Code { get; init; }
    public required string Message { get; init; }
    /// <summary>Severity: critical, warning, info.</summary>
    public string? Severity { get; init; }
    /// <summary>Можно ли повторить операцию с backoff.</summary>
    public bool? Retryable { get; init; }
    /// <summary>Рекомендуемое действие для оператора.</summary>
    public string? OperatorAction { get; init; }
}
