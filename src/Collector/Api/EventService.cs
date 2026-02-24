using Collector.Data;
using Collector.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed class EventService
{
    public static ApiErrorDto? ValidateRunSessionId(long? runSessionId)
    {
        if (!runSessionId.HasValue)
        {
            return null;
        }

        if (runSessionId.Value <= 0)
        {
            return new ApiErrorDto
            {
                Code = "EVENTS_RUN_SESSION_ID_INVALID",
                Message = "Параметр runSessionId должен быть числом > 0."
            };
        }

        return null;
    }

    public static ApiErrorDto? ValidateSinceId(long? sinceId)
    {
        if (!sinceId.HasValue)
        {
            return null;
        }

        if (sinceId.Value < 0)
        {
            return new ApiErrorDto
            {
                Code = "EVENTS_SINCE_ID_INVALID",
                Message = "Параметр sinceId должен быть числом >= 0."
            };
        }

        return null;
    }

    public async Task<EventLogListDto> ListAsync(
        AppDbContext db,
        long? runSessionId,
        int limit,
        int offset,
        CancellationToken cancellationToken)
    {
        var query = BuildBaseQuery(db, runSessionId);
        var total = await query.CountAsync(cancellationToken);

        var rows = await query
            .OrderByDescending(x => x.Id)
            .Skip(offset)
            .Take(limit)
            .ToListAsync(cancellationToken);

        return new EventLogListDto
        {
            Total = total,
            Items = rows
                .OrderBy(x => x.Id)
                .Select(MapToDto)
                .ToList()
        };
    }

    public async Task<List<EventLogItemDto>> ListLatestAsync(
        AppDbContext db,
        long? runSessionId,
        int historyLimit,
        CancellationToken cancellationToken)
    {
        var rows = await BuildBaseQuery(db, runSessionId)
            .OrderByDescending(x => x.Id)
            .Take(historyLimit)
            .ToListAsync(cancellationToken);

        return rows
            .OrderBy(x => x.Id)
            .Select(MapToDto)
            .ToList();
    }

    public async Task<List<EventLogItemDto>> ListAfterIdAsync(
        AppDbContext db,
        long afterId,
        long? runSessionId,
        int take,
        CancellationToken cancellationToken)
    {
        if (take <= 0)
        {
            return [];
        }

        var query = BuildBaseQuery(db, runSessionId)
            .Where(x => x.Id > afterId);

        var rows = await query
            .OrderBy(x => x.Id)
            .Take(take)
            .ToListAsync(cancellationToken);

        return rows.Select(MapToDto).ToList();
    }

    private static IQueryable<EventLogRecord> BuildBaseQuery(AppDbContext db, long? runSessionId)
    {
        var query = db.Events.AsNoTracking();
        if (runSessionId.HasValue && runSessionId.Value > 0)
        {
            query = query.Where(x => x.RunSessionId == runSessionId.Value);
        }

        return query;
    }

    private static EventLogItemDto MapToDto(EventLogRecord record)
    {
        return new EventLogItemDto
        {
            Id = record.Id,
            Category = record.Category ?? string.Empty,
            EventType = record.EventType ?? string.Empty,
            Severity = record.Severity ?? string.Empty,
            Message = record.Message ?? string.Empty,
            RunSessionId = record.RunSessionId,
            RunJobId = record.RunJobId,
            PayloadJson = record.PayloadJson ?? string.Empty,
            CreatedAtUtc = record.CreatedAtUtc
        };
    }
}
