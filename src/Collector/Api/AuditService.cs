using System.Text.Json;
using Collector.Data;
using Collector.Data.Entities;
using Microsoft.AspNetCore.Http;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed class AuditService
{
    private static readonly string[] AuditedPrefixes =
    [
        "/api/templates",
        "/api/stop-list",
        "/api/channels",
        "/api/alerts"
    ];

    private static readonly HashSet<string> AuditedMethods = new(StringComparer.OrdinalIgnoreCase)
    {
        "POST",
        "PUT",
        "PATCH",
        "DELETE"
    };

    public bool ShouldAudit(HttpRequest request)
    {
        if (!AuditedMethods.Contains(request.Method))
        {
            return false;
        }

        var path = request.Path.Value ?? string.Empty;
        if (string.IsNullOrWhiteSpace(path))
        {
            return false;
        }

        return AuditedPrefixes.Any(prefix => path.StartsWith(prefix, StringComparison.OrdinalIgnoreCase));
    }

    public async Task LogHttpMutationAsync(
        AppDbContext db,
        HttpContext context,
        CancellationToken cancellationToken)
    {
        var request = context.Request;
        var path = request.Path.Value ?? string.Empty;
        if (string.IsNullOrWhiteSpace(path))
        {
            return;
        }

        var category = ResolveCategory(path);
        var entityId = ResolveEntityId(context);
        var actor = ResolveActor(context);

        var payload = new
        {
            method = request.Method,
            path,
            query = request.QueryString.HasValue ? request.QueryString.Value : string.Empty,
            statusCode = context.Response.StatusCode
        };

        db.AuditLogs.Add(new AuditLogRecord
        {
            Category = category,
            Action = $"{request.Method.ToUpperInvariant()} {path}",
            EntityId = entityId,
            Actor = actor,
            DetailsJson = JsonSerializer.Serialize(payload),
            CreatedAtUtc = DateTime.UtcNow
        });
        await db.SaveChangesAsync(cancellationToken);
    }

    public async Task<AuditListDto> ListAsync(
        AppDbContext db,
        string? category,
        int limit,
        int offset,
        CancellationToken cancellationToken)
    {
        var normalizedCategory = (category ?? string.Empty).Trim().ToLowerInvariant();

        var query = db.AuditLogs.AsNoTracking();
        if (!string.IsNullOrWhiteSpace(normalizedCategory))
        {
            query = query.Where(x => x.Category == normalizedCategory);
        }

        var total = await query.CountAsync(cancellationToken);
        var rows = await query
            .OrderByDescending(x => x.CreatedAtUtc)
            .ThenByDescending(x => x.Id)
            .Skip(offset)
            .Take(limit)
            .ToListAsync(cancellationToken);

        return new AuditListDto
        {
            Total = total,
            Items = rows.Select(x => new AuditLogDto
            {
                Id = x.Id,
                Category = x.Category,
                Action = x.Action,
                EntityId = x.EntityId ?? string.Empty,
                Actor = x.Actor,
                DetailsJson = x.DetailsJson ?? string.Empty,
                CreatedAtUtc = x.CreatedAtUtc
            }).ToList()
        };
    }

    private static string ResolveCategory(string path)
    {
        if (path.StartsWith("/api/templates", StringComparison.OrdinalIgnoreCase)) return "templates";
        if (path.StartsWith("/api/stop-list", StringComparison.OrdinalIgnoreCase)) return "stop-list";
        if (path.StartsWith("/api/channels", StringComparison.OrdinalIgnoreCase)) return "channels";
        if (path.StartsWith("/api/alerts", StringComparison.OrdinalIgnoreCase)) return "alerts";
        return "other";
    }

    private static string ResolveEntityId(HttpContext context)
    {
        if (context.Request.RouteValues.TryGetValue("id", out var id) && id is not null)
        {
            return id.ToString() ?? string.Empty;
        }

        if (context.Request.RouteValues.TryGetValue("phone", out var phone) && phone is not null)
        {
            return phone.ToString() ?? string.Empty;
        }

        return string.Empty;
    }

    private static string ResolveActor(HttpContext context)
    {
        var actorHeader = context.Request.Headers["X-Operator"].ToString();
        var actor = string.IsNullOrWhiteSpace(actorHeader) ? "employee" : actorHeader.Trim();
        if (actor.Length > 128)
        {
            actor = actor[..128];
        }

        return actor;
    }
}
