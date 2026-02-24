using Collector.Data;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed class ReportsService
{
    private const string MessageDirectionOut = "out";
    private const string MessageStatusSent = "sent";
    private const string MessageStatusFailed = "failed";

    public async Task<WeeklyReportDto> GetWeeklyAsync(AppDbContext db, CancellationToken cancellationToken)
    {
        var nowUtc = DateTime.UtcNow;
        var todayUtc = nowUtc.Date;
        var startUtc = todayUtc.AddDays(-6);
        var endUtcExclusive = todayUtc.AddDays(1);

        var messages = await db.Messages
            .AsNoTracking()
            .Where(x => x.Direction == MessageDirectionOut)
            .Where(x => x.CreatedAtUtc >= startUtc && x.CreatedAtUtc < endUtcExclusive)
            .Select(x => new { x.CreatedAtUtc, x.GatewayStatus })
            .ToListAsync(cancellationToken);

        var byDate = new Dictionary<DateTime, (int Sent, int Failed)>();
        for (var day = startUtc; day <= todayUtc; day = day.AddDays(1))
        {
            byDate[day] = (0, 0);
        }

        foreach (var msg in messages)
        {
            var date = msg.CreatedAtUtc.Date;
            if (!byDate.TryGetValue(date, out var counters))
            {
                continue;
            }

            if (string.Equals(msg.GatewayStatus, MessageStatusSent, StringComparison.OrdinalIgnoreCase))
            {
                counters.Sent += 1;
            }
            else if (string.Equals(msg.GatewayStatus, MessageStatusFailed, StringComparison.OrdinalIgnoreCase))
            {
                counters.Failed += 1;
            }

            byDate[date] = counters;
        }

        var days = byDate
            .OrderBy(x => x.Key)
            .Select(x => new WeeklyReportDayDto
            {
                DateUtc = x.Key,
                Label = x.Key.ToString("dd.MM"),
                Sent = x.Value.Sent,
                Failed = x.Value.Failed
            })
            .ToList();

        var today = byDate.TryGetValue(todayUtc, out var todayCounters)
            ? todayCounters
            : (Sent: 0, Failed: 0);

        var stopListCount = await db.StopList
            .AsNoTracking()
            .CountAsync(x => x.IsActive, cancellationToken);

        return new WeeklyReportDto
        {
            GeneratedAtUtc = nowUtc,
            SentToday = today.Sent,
            FailedToday = today.Failed,
            StopListCount = stopListCount,
            Days = days
        };
    }
}
