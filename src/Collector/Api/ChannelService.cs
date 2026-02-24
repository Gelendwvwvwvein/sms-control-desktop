using Collector.Data;
using Collector.Data.Entities;
using Microsoft.EntityFrameworkCore;

namespace Collector.Api;

public sealed class ChannelService(HttpClient httpClient, AlertService alerts, RunDispatchService dispatch)
{
    private const string StatusOnline = "online";
    private const string StatusOffline = "offline";
    private const string StatusError = "error";
    private const string StatusUnknown = "unknown";

    public async Task<List<ChannelDto>> ListAsync(AppDbContext db, CancellationToken cancellationToken)
    {
        var channels = await db.SenderChannels
            .OrderBy(x => x.Id)
            .ToListAsync(cancellationToken);

        return channels.Select(MapToDto).ToList();
    }

    public async Task<ChannelDto> CreateAsync(AppDbContext db, CreateChannelRequest payload, CancellationToken cancellationToken)
    {
        var record = new SenderChannelRecord
        {
            Name = payload.Name.Trim(),
            Endpoint = NormalizeEndpoint(payload.Endpoint),
            Token = payload.Token.Trim(),
            SimPhone = NormalizePhone(payload.SimPhone),
            Status = "unknown",
            LastCheckedAtUtc = null,
            FailStreak = 0,
            Alerted = false
        };

        db.SenderChannels.Add(record);
        await db.SaveChangesAsync(cancellationToken);
        dispatch.NotifyChannelsChanged("канал добавлен оператором");
        return MapToDto(record);
    }

    public async Task<bool> DeleteAsync(AppDbContext db, long channelId, CancellationToken cancellationToken)
    {
        var channel = await db.SenderChannels.FirstOrDefaultAsync(x => x.Id == channelId, cancellationToken);
        if (channel is null) return false;
        db.SenderChannels.Remove(channel);
        await db.SaveChangesAsync(cancellationToken);
        dispatch.NotifyChannelsChanged("канал удален оператором");
        return true;
    }

    public async Task<ChannelDto?> UpdateAsync(
        AppDbContext db,
        long channelId,
        UpdateChannelRequest payload,
        CancellationToken cancellationToken)
    {
        var channel = await db.SenderChannels.FirstOrDefaultAsync(x => x.Id == channelId, cancellationToken);
        if (channel is null) return null;

        channel.Name = payload.Name.Trim();
        channel.Endpoint = NormalizeEndpoint(payload.Endpoint);
        channel.SimPhone = NormalizePhone(payload.SimPhone);

        if (!string.IsNullOrWhiteSpace(payload.Token))
        {
            channel.Token = payload.Token.Trim();
        }

        if (!string.Equals(channel.Status, StatusOffline, StringComparison.OrdinalIgnoreCase))
        {
            channel.Status = StatusUnknown;
            channel.FailStreak = 0;
            channel.Alerted = false;
            channel.LastCheckedAtUtc = null;
            await alerts.ResolveChannelAlertsAsync(db, channel.Id, "Канал перенастроен оператором.", cancellationToken);
        }

        db.SenderChannels.Update(channel);
        await db.SaveChangesAsync(cancellationToken);
        dispatch.NotifyChannelsChanged($"канал #{channelId} перенастроен оператором");
        return MapToDto(channel);
    }

    public async Task<ChannelDto?> UpdateStatusAsync(
        AppDbContext db,
        long channelId,
        string status,
        CancellationToken cancellationToken)
    {
        var channel = await db.SenderChannels.FirstOrDefaultAsync(x => x.Id == channelId, cancellationToken);
        if (channel is null) return null;

        var normalizedStatus = NormalizeStatus(status);
        channel.Status = normalizedStatus;
        channel.LastCheckedAtUtc = DateTime.UtcNow;

        switch (normalizedStatus)
        {
            case StatusOnline:
            case StatusUnknown:
            case StatusOffline:
                channel.FailStreak = 0;
                channel.Alerted = false;
                await alerts.ResolveChannelAlertsAsync(db, channel.Id, "Статус канала изменен оператором.", cancellationToken);
                break;
            case StatusError:
                channel.FailStreak = Math.Max(1, channel.FailStreak);
                channel.Alerted = true;
                await alerts.RaiseChannelErrorAsync(
                    db,
                    channel,
                    code: "CHANNEL_MANUAL_ERROR",
                    detail: "Канал переведен в error вручную.",
                    runSessionId: null,
                    runJobId: null,
                    cancellationToken);
                break;
        }

        db.SenderChannels.Update(channel);
        await db.SaveChangesAsync(cancellationToken);
        dispatch.NotifyChannelsChanged($"канал #{channelId} изменил статус на {normalizedStatus}");
        return MapToDto(channel);
    }

    public async Task<ChannelCheckResultDto?> CheckAsync(AppDbContext db, long channelId, int timeoutMs, CancellationToken cancellationToken)
    {
        var channel = await db.SenderChannels.FirstOrDefaultAsync(x => x.Id == channelId, cancellationToken);
        if (channel is null) return null;
        var stateBefore = BuildStateSignature(channel);
        var result = await CheckInternalAsync(db, channel, timeoutMs, cancellationToken);
        await db.SaveChangesAsync(cancellationToken);
        if (!string.Equals(stateBefore, BuildStateSignature(channel), StringComparison.Ordinal))
        {
            dispatch.NotifyChannelsChanged($"канал #{channelId} обновлен по результату проверки");
        }
        return result;
    }

    public async Task<BulkChannelCheckResultDto> CheckAllAsync(AppDbContext db, int timeoutMs, CancellationToken cancellationToken)
    {
        var channels = await db.SenderChannels.OrderBy(x => x.Id).ToListAsync(cancellationToken);
        var stateBefore = channels.ToDictionary(x => x.Id, BuildStateSignature);
        var output = new BulkChannelCheckResultDto
        {
            Total = channels.Count
        };

        foreach (var channel in channels)
        {
            var item = await CheckInternalAsync(db, channel, timeoutMs, cancellationToken);
            output.Results.Add(item);
        }

        output.Online = output.Results.Count(x => x.Status == "online");
        output.Error = output.Results.Count(x => x.Status == StatusError);
        await db.SaveChangesAsync(cancellationToken);
        var stateChanged = channels.Any(x =>
            stateBefore.TryGetValue(x.Id, out var before) &&
            !string.Equals(before, BuildStateSignature(x), StringComparison.Ordinal));
        if (stateChanged)
        {
            dispatch.NotifyChannelsChanged("каналы обновлены по результату групповой проверки");
        }
        return output;
    }

    private async Task<ChannelCheckResultDto> CheckInternalAsync(
        AppDbContext db,
        SenderChannelRecord channel,
        int timeoutMs,
        CancellationToken cancellationToken)
    {
        var checkedAt = DateTime.UtcNow;
        var detail = string.Empty;
        var ok = false;

        if (string.Equals(channel.Status, StatusOffline, StringComparison.OrdinalIgnoreCase))
        {
            channel.LastCheckedAtUtc = checkedAt;
            db.SenderChannels.Update(channel);
            return new ChannelCheckResultDto
            {
                ChannelId = channel.Id,
                Status = StatusOffline,
                FailStreak = channel.FailStreak,
                CheckedAtUtc = checkedAt,
                Detail = "Проверка пропущена: канал отключен вручную."
            };
        }

        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(timeoutMs));
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

        try
        {
            var response = await httpClient.GetAsync(channel.Endpoint, linkedCts.Token);
            ok = true;
            detail = $"HTTP {(int)response.StatusCode}";
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
        {
            detail = $"Timeout after {timeoutMs}ms";
        }
        catch (Exception ex)
        {
            detail = ex.Message;
        }

        channel.LastCheckedAtUtc = checkedAt;
        if (ok)
        {
            channel.Status = StatusOnline;
            channel.FailStreak = 0;
            channel.Alerted = false;
            await alerts.ResolveChannelAlertsAsync(db, channel.Id, "Канал успешно прошел health-check.", cancellationToken);
        }
        else
        {
            channel.Status = StatusError;
            channel.FailStreak = Math.Max(1, channel.FailStreak + 1);
            channel.Alerted = true;
            await alerts.RaiseChannelErrorAsync(
                db,
                channel,
                code: "CHANNEL_HEALTHCHECK_FAILED",
                detail: string.IsNullOrWhiteSpace(detail) ? "Канал не ответил на проверку." : detail,
                runSessionId: null,
                runJobId: null,
                cancellationToken);
        }

        db.SenderChannels.Update(channel);

        return new ChannelCheckResultDto
        {
            ChannelId = channel.Id,
            Status = channel.Status,
            FailStreak = channel.FailStreak,
            CheckedAtUtc = checkedAt,
            Detail = detail
        };
    }

    public static ApiErrorDto? ValidateCreateRequest(CreateChannelRequest? payload)
    {
        if (payload is null)
        {
            return new ApiErrorDto { Code = "CFG_REQUIRED_MISSING", Message = "Тело запроса пустое." };
        }

        if (string.IsNullOrWhiteSpace(payload.Name))
        {
            return new ApiErrorDto { Code = "CHANNEL_NAME_REQUIRED", Message = "Название канала обязательно." };
        }

        if (string.IsNullOrWhiteSpace(payload.Endpoint))
        {
            return new ApiErrorDto { Code = "CHANNEL_ENDPOINT_REQUIRED", Message = "Endpoint канала обязателен." };
        }

        if (!Uri.TryCreate(payload.Endpoint.Trim(), UriKind.Absolute, out var uri) ||
            (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps))
        {
            return new ApiErrorDto { Code = "CHANNEL_ENDPOINT_INVALID", Message = "Endpoint должен быть валидным URL (http/https)." };
        }

        if (string.IsNullOrWhiteSpace(payload.Token))
        {
            return new ApiErrorDto { Code = "CHANNEL_TOKEN_REQUIRED", Message = "Токен канала обязателен." };
        }

        return null;
    }

    public static ApiErrorDto? ValidateStatusPatchRequest(ChannelStatusPatchRequest? payload)
    {
        if (payload is null)
        {
            return new ApiErrorDto { Code = "CFG_REQUIRED_MISSING", Message = "Тело запроса пустое." };
        }

        var normalizedStatus = NormalizeStatus(payload.Status);
        if (normalizedStatus is StatusOffline or StatusUnknown)
        {
            return null;
        }

        return new ApiErrorDto
        {
            Code = "CHANNEL_STATUS_INVALID",
            Message = "Разрешены только статусы 'offline' (отключить) и 'unknown' (включить)."
        };
    }

    public static ApiErrorDto? ValidateUpdateRequest(UpdateChannelRequest? payload)
    {
        if (payload is null)
        {
            return new ApiErrorDto { Code = "CFG_REQUIRED_MISSING", Message = "Тело запроса пустое." };
        }

        if (string.IsNullOrWhiteSpace(payload.Name))
        {
            return new ApiErrorDto { Code = "CHANNEL_NAME_REQUIRED", Message = "Название канала обязательно." };
        }

        if (string.IsNullOrWhiteSpace(payload.Endpoint))
        {
            return new ApiErrorDto { Code = "CHANNEL_ENDPOINT_REQUIRED", Message = "Endpoint канала обязателен." };
        }

        if (!Uri.TryCreate(payload.Endpoint.Trim(), UriKind.Absolute, out var uri) ||
            (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps))
        {
            return new ApiErrorDto { Code = "CHANNEL_ENDPOINT_INVALID", Message = "Endpoint должен быть валидным URL (http/https)." };
        }

        return null;
    }

    public static int NormalizeTimeoutMs(string? rawTimeout)
    {
        if (!int.TryParse(rawTimeout, out var timeoutMs)) return 5000;
        return Math.Clamp(timeoutMs, 1000, 60000);
    }

    private static string NormalizeEndpoint(string endpoint)
    {
        return endpoint.Trim().TrimEnd('/') + "/";
    }

    private static string NormalizePhone(string phone)
    {
        if (string.IsNullOrWhiteSpace(phone)) return string.Empty;
        var digits = new string(phone.Where(char.IsDigit).ToArray());
        return string.IsNullOrWhiteSpace(digits) ? string.Empty : $"+{digits}";
    }

    private static string NormalizeStatus(string status)
    {
        if (string.IsNullOrWhiteSpace(status)) return string.Empty;
        return status.Trim().ToLowerInvariant();
    }

    private static string BuildStateSignature(SenderChannelRecord channel)
    {
        return $"{channel.Status}:{channel.FailStreak}:{(channel.Alerted ? 1 : 0)}";
    }

    private static ChannelDto MapToDto(SenderChannelRecord x)
    {
        return new ChannelDto
        {
            Id = x.Id,
            Name = x.Name,
            Endpoint = x.Endpoint,
            TokenMasked = MaskToken(x.Token),
            SimPhone = x.SimPhone ?? string.Empty,
            Status = x.Status,
            LastCheckedAtUtc = EnsureUtc(x.LastCheckedAtUtc),
            FailStreak = x.FailStreak,
            Alerted = x.Alerted
        };
    }

    private static DateTime? EnsureUtc(DateTime? value)
    {
        if (!value.HasValue) return null;
        var dt = value.Value;
        return dt.Kind == DateTimeKind.Utc ? dt : DateTime.SpecifyKind(dt, DateTimeKind.Utc);
    }

    private static string MaskToken(string token)
    {
        if (string.IsNullOrWhiteSpace(token)) return string.Empty;
        var trimmed = token.Trim();
        if (trimmed.Length <= 4) return "****";
        return $"{trimmed[..2]}****{trimmed[^2..]}";
    }
}
