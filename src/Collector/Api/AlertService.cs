using System.Text.Json;
using Collector.Data;
using Collector.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed class AlertService
{
    public const string StatusActive = "active";
    public const string StatusResolved = "resolved";
    public const string StatusIrrelevant = "irrelevant";
    private const string LevelError = "error";

    public static ApiErrorDto? ValidateStatusFilter(string? status)
    {
        var normalized = NormalizeStatusFilter(status);
        if (normalized is "all" or StatusActive or StatusResolved or StatusIrrelevant)
        {
            return null;
        }

        return new ApiErrorDto
        {
            Code = "ALERT_STATUS_FILTER_INVALID",
            Message = "Параметр status должен быть одним из: all, active, resolved, irrelevant."
        };
    }

    public static ApiErrorDto? ValidateStatusPatchRequest(AlertStatusPatchRequest? payload)
    {
        if (payload is null)
        {
            return new ApiErrorDto
            {
                Code = "CFG_REQUIRED_MISSING",
                Message = "Тело запроса пустое."
            };
        }

        var normalized = NormalizeStatus(payload.Status);
        if (normalized is StatusActive or StatusResolved or StatusIrrelevant)
        {
            return null;
        }

        return new ApiErrorDto
        {
            Code = "ALERT_STATUS_INVALID",
            Message = "Разрешены статусы: active, resolved, irrelevant."
        };
    }

    public async Task<AlertListDto> ListAsync(
        AppDbContext db,
        string? statusFilter,
        int limit,
        int offset,
        CancellationToken cancellationToken)
    {
        var normalizedFilter = NormalizeStatusFilter(statusFilter);
        var baseQuery = db.Alerts.AsNoTracking();
        var total = await baseQuery.CountAsync(cancellationToken);
        var activeCount = await baseQuery.CountAsync(x => x.Status == StatusActive, cancellationToken);
        var resolvedCount = await baseQuery.CountAsync(x => x.Status == StatusResolved, cancellationToken);
        var irrelevantCount = await baseQuery.CountAsync(x => x.Status == StatusIrrelevant, cancellationToken);

        var listQuery = baseQuery;
        if (!string.Equals(normalizedFilter, "all", StringComparison.Ordinal))
        {
            listQuery = listQuery.Where(x => x.Status == normalizedFilter);
        }

        var rows = await listQuery
            .OrderByDescending(x => x.CreatedAtUtc)
            .ThenByDescending(x => x.Id)
            .Skip(offset)
            .Take(limit)
            .ToListAsync(cancellationToken);

        var channelIds = rows
            .Where(x => x.ChannelId.HasValue && x.ChannelId.Value > 0)
            .Select(x => x.ChannelId!.Value)
            .Distinct()
            .ToList();

        var channelNames = channelIds.Count == 0
            ? new Dictionary<long, string>()
            : await db.SenderChannels
                .AsNoTracking()
                .Where(x => channelIds.Contains(x.Id))
                .ToDictionaryAsync(x => x.Id, x => x.Name, cancellationToken);

        return new AlertListDto
        {
            Total = total,
            ActiveCount = activeCount,
            ResolvedCount = resolvedCount,
            IrrelevantCount = irrelevantCount,
            Items = rows.Select(x => new AlertDto
            {
                Id = x.Id,
                Level = x.Level,
                Text = x.Text,
                Status = x.Status,
                ChannelId = x.ChannelId,
                ChannelName = x.ChannelId.HasValue && channelNames.TryGetValue(x.ChannelId.Value, out var name)
                    ? name
                    : string.Empty,
                CreatedAtUtc = x.CreatedAtUtc,
                ClosedAtUtc = x.ClosedAtUtc,
                MetaJson = x.MetaJson ?? string.Empty
            }).ToList()
        };
    }

    public async Task<AlertDto?> UpdateStatusAsync(
        AppDbContext db,
        long alertId,
        string nextStatus,
        CancellationToken cancellationToken)
    {
        var normalized = NormalizeStatus(nextStatus);
        var alert = await db.Alerts.FirstOrDefaultAsync(x => x.Id == alertId, cancellationToken);
        if (alert is null)
        {
            return null;
        }

        alert.Status = normalized;
        alert.ClosedAtUtc = normalized == StatusActive ? null : DateTime.UtcNow;
        db.Alerts.Update(alert);
        await db.SaveChangesAsync(cancellationToken);

        var channelName = string.Empty;
        if (alert.ChannelId.HasValue && alert.ChannelId.Value > 0)
        {
            channelName = await db.SenderChannels
                .AsNoTracking()
                .Where(x => x.Id == alert.ChannelId.Value)
                .Select(x => x.Name)
                .FirstOrDefaultAsync(cancellationToken) ?? string.Empty;
        }

        return new AlertDto
        {
            Id = alert.Id,
            Level = alert.Level,
            Text = alert.Text,
            Status = alert.Status,
            ChannelId = alert.ChannelId,
            ChannelName = channelName,
            CreatedAtUtc = alert.CreatedAtUtc,
            ClosedAtUtc = alert.ClosedAtUtc,
            MetaJson = alert.MetaJson ?? string.Empty
        };
    }

    public async Task RaiseChannelErrorAsync(
        AppDbContext db,
        SenderChannelRecord channel,
        string code,
        string detail,
        long? runSessionId,
        long? runJobId,
        CancellationToken cancellationToken)
    {
        if (channel.Id <= 0)
        {
            return;
        }

        var nowUtc = DateTime.UtcNow;
        var normalizedCode = string.IsNullOrWhiteSpace(code) ? "CHANNEL_ERROR" : code.Trim();
        var text = BuildChannelErrorText(channel.Name, normalizedCode);
        var metaJson = JsonSerializer.Serialize(new
        {
            kind = "channel_device_error",
            channelId = channel.Id,
            channelName = channel.Name,
            code = normalizedCode,
            detail = detail ?? string.Empty,
            runSessionId = runSessionId ?? 0,
            runJobId = runJobId ?? 0,
            generatedAtUtc = nowUtc
        });

        var active = await db.Alerts
            .FirstOrDefaultAsync(
                x => x.ChannelId == channel.Id && x.Status == StatusActive,
                cancellationToken);

        if (active is null)
        {
            db.Alerts.Add(new AlertRecord
            {
                Level = LevelError,
                Text = text,
                Status = StatusActive,
                ChannelId = channel.Id,
                CreatedAtUtc = nowUtc,
                ClosedAtUtc = null,
                MetaJson = metaJson
            });
            return;
        }

        active.Level = LevelError;
        active.Text = text;
        active.Status = StatusActive;
        active.ClosedAtUtc = null;
        active.MetaJson = metaJson;
        db.Alerts.Update(active);
    }

    public async Task<int> ResolveChannelAlertsAsync(
        AppDbContext db,
        long channelId,
        string reason,
        CancellationToken cancellationToken)
    {
        if (channelId <= 0)
        {
            return 0;
        }

        var activeAlerts = await db.Alerts
            .Where(x => x.ChannelId == channelId && x.Status == StatusActive)
            .ToListAsync(cancellationToken);

        if (activeAlerts.Count == 0)
        {
            return 0;
        }

        var nowUtc = DateTime.UtcNow;
        foreach (var alert in activeAlerts)
        {
            alert.Status = StatusResolved;
            alert.ClosedAtUtc = nowUtc;
            if (!string.IsNullOrWhiteSpace(reason))
            {
                alert.MetaJson = JsonSerializer.Serialize(new
                {
                    kind = "channel_device_error",
                    channelId,
                    resolvedAtUtc = nowUtc,
                    resolvedReason = reason.Trim()
                });
            }
        }

        db.Alerts.UpdateRange(activeAlerts);
        return activeAlerts.Count;
    }

    private static string NormalizeStatus(string? status)
    {
        return (status ?? string.Empty).Trim().ToLowerInvariant();
    }

    private static string NormalizeStatusFilter(string? status)
    {
        var normalized = NormalizeStatus(status);
        return string.IsNullOrWhiteSpace(normalized) ? StatusActive : normalized;
    }

    private static string BuildChannelErrorText(string channelName, string code)
    {
        var name = string.IsNullOrWhiteSpace(channelName) ? "Канал" : channelName.Trim();
        return $"{name}: ошибка устройства ({code}). Проверьте телефон и SMS gateway.";
    }

    public void RaiseContractCommentError(
        AppDbContext db,
        long runSessionId,
        long runJobId,
        string externalClientId,
        string phone,
        string cardUrl,
        string commentText,
        string code,
        string detail)
    {
        var normalizedCode = string.IsNullOrWhiteSpace(code) ? "COMMENT_WRITE_FAILED" : code.Trim();
        var nowUtc = DateTime.UtcNow;
        var text = $"Не удалось записать комментарий в договор (job #{runJobId}, code={normalizedCode}).";
        var metaJson = JsonSerializer.Serialize(new
        {
            kind = "contract_comment_error",
            runSessionId,
            runJobId,
            externalClientId = externalClientId ?? string.Empty,
            phone = phone ?? string.Empty,
            cardUrl = cardUrl ?? string.Empty,
            commentText = commentText ?? string.Empty,
            code = normalizedCode,
            detail = detail ?? string.Empty,
            generatedAtUtc = nowUtc
        });

        db.Alerts.Add(new AlertRecord
        {
            Level = LevelError,
            Text = text,
            Status = StatusActive,
            ChannelId = null,
            CreatedAtUtc = nowUtc,
            ClosedAtUtc = null,
            MetaJson = metaJson
        });
    }

    public void RaiseSmsSendError(
        AppDbContext db,
        long runSessionId,
        long runJobId,
        string externalClientId,
        string clientFio,
        string phone,
        long? channelId,
        string channelName,
        long? templateId,
        bool usedMessageOverride,
        string code,
        string detail)
    {
        var normalizedCode = string.IsNullOrWhiteSpace(code) ? "SMS_SEND_FAILED" : code.Trim();
        var nowUtc = DateTime.UtcNow;
        var normalizedPhone = string.IsNullOrWhiteSpace(phone) ? "без номера" : phone.Trim();
        var normalizedFio = string.IsNullOrWhiteSpace(clientFio) ? string.Empty : clientFio.Trim();
        var fioSuffix = string.IsNullOrWhiteSpace(normalizedFio) ? string.Empty : $" ({normalizedFio})";

        var text = $"Не удалось отправить SMS клиенту {normalizedPhone}{fioSuffix} (job #{runJobId}, code={normalizedCode}).";
        if (text.Length > 1024)
        {
            text = text[..1024];
        }

        var normalizedChannelId = channelId.HasValue && channelId.Value > 0 ? channelId : null;
        var metaJson = JsonSerializer.Serialize(new
        {
            kind = "sms_send_error",
            runSessionId,
            runJobId,
            externalClientId = externalClientId ?? string.Empty,
            fio = normalizedFio,
            phone = normalizedPhone,
            channelId = normalizedChannelId,
            channelName = channelName ?? string.Empty,
            templateId = templateId ?? 0,
            usedMessageOverride,
            code = normalizedCode,
            detail = detail ?? string.Empty,
            generatedAtUtc = nowUtc
        });

        db.Alerts.Add(new AlertRecord
        {
            Level = LevelError,
            Text = text,
            Status = StatusActive,
            ChannelId = normalizedChannelId,
            CreatedAtUtc = nowUtc,
            ClosedAtUtc = null,
            MetaJson = metaJson
        });
    }
}
