namespace Collector.Api;

public sealed class ChannelDto
{
    public long Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public string Endpoint { get; set; } = string.Empty;
    public string TokenMasked { get; set; } = string.Empty;
    public string SimPhone { get; set; } = string.Empty;
    public string Status { get; set; } = "unknown";
    public DateTime? LastCheckedAtUtc { get; set; }
    public int FailStreak { get; set; }
    public bool Alerted { get; set; }
}

public sealed class CreateChannelRequest
{
    public string Name { get; set; } = string.Empty;
    public string Endpoint { get; set; } = string.Empty;
    public string Token { get; set; } = string.Empty;
    public string SimPhone { get; set; } = string.Empty;
}

public sealed class UpdateChannelRequest
{
    public string Name { get; set; } = string.Empty;
    public string Endpoint { get; set; } = string.Empty;
    public string Token { get; set; } = string.Empty;
    public string SimPhone { get; set; } = string.Empty;
}

public sealed class ChannelStatusPatchRequest
{
    public string Status { get; set; } = string.Empty;
}

public sealed class ChannelCheckResultDto
{
    public long ChannelId { get; set; }
    public string Status { get; set; } = "unknown";
    public int FailStreak { get; set; }
    public DateTime CheckedAtUtc { get; set; }
    public string Detail { get; set; } = string.Empty;
}

public sealed class BulkChannelCheckResultDto
{
    public int Total { get; set; }
    public int Online { get; set; }
    public int Error { get; set; }
    public List<ChannelCheckResultDto> Results { get; set; } = [];
}
