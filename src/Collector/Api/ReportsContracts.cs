namespace Collector.Api;

public sealed class WeeklyReportDayDto
{
    public DateTime DateUtc { get; set; }
    public string Label { get; set; } = string.Empty;
    public int Sent { get; set; }
    public int Failed { get; set; }
    public int Total => Sent + Failed;
}

public sealed class WeeklyReportDto
{
    public DateTime GeneratedAtUtc { get; set; }
    public int SentToday { get; set; }
    public int FailedToday { get; set; }
    public int StopListCount { get; set; }
    public List<WeeklyReportDayDto> Days { get; set; } = [];
}

