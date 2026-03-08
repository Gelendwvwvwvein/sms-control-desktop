using System.Net;
using System.Net.Http.Json;
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
        output.Unknown = output.Results.Count(x => x.Status == StatusUnknown);
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
        var assessment = await CheckGatewayEndpointAsync(
            channel.Endpoint,
            channel.Token,
            cancellationToken,
            linkedCts.Token);
        detail = assessment.Detail;

        channel.LastCheckedAtUtc = checkedAt;
        if (assessment.Status == StatusOnline)
        {
            channel.Status = StatusOnline;
            channel.FailStreak = 0;
            channel.Alerted = false;
            await alerts.ResolveChannelAlertsAsync(db, channel.Id, "Канал успешно прошел health-check.", cancellationToken);
        }
        else if (assessment.Status == StatusUnknown)
        {
            channel.Status = StatusUnknown;
            channel.FailStreak = 0;
            channel.Alerted = false;
            AppendDeviceEvent(
                db,
                eventType: "channel_probe_ambiguous",
                severity: "warning",
                message: $"Канал #{channel.Id}: gateway достижим, но probe-проверка неинформативна.{BuildLogDetailSuffix(detail)}",
                payload: new
                {
                    channelId = channel.Id,
                    channelName = channel.Name,
                    endpoint = channel.Endpoint,
                    status = StatusUnknown,
                    detail
                });
            await alerts.ResolveChannelAlertsAsync(
                db,
                channel.Id,
                "Gateway достижим, но probe-проверка не дала однозначного результата.",
                cancellationToken);
        }
        else
        {
            channel.Status = StatusError;
            channel.FailStreak = Math.Max(1, channel.FailStreak + 1);
            channel.Alerted = true;
            AppendDeviceEvent(
                db,
                eventType: "channel_healthcheck_failed",
                severity: "warning",
                message: $"Канал #{channel.Id}: ошибка проверки подключения устройства.{BuildLogDetailSuffix(detail)}",
                payload: new
                {
                    channelId = channel.Id,
                    channelName = channel.Name,
                    endpoint = channel.Endpoint,
                    status = StatusError,
                    failStreak = channel.FailStreak,
                    detail
                });
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

    private async Task<GatewayHealthAssessment> CheckGatewayEndpointAsync(
        string endpoint,
        string token,
        CancellationToken externalCancellationToken,
        CancellationToken probeCancellationToken)
    {
        if (string.IsNullOrWhiteSpace(endpoint) || string.IsNullOrWhiteSpace(token))
        {
            return GatewayHealthAssessment.Error("У канала не задан endpoint или token.");
        }

        var withToken = await SendProbeAsync(endpoint, token, externalCancellationToken, probeCancellationToken);
        if (!withToken.TransportOk)
        {
            return GatewayHealthAssessment.Error(withToken.ErrorDetail);
        }

        var statusCode = withToken.StatusCode;
        if (!statusCode.HasValue)
        {
            return GatewayHealthAssessment.Error("Проверка канала завершилась без HTTP-статуса.");
        }

        if (statusCode.Value is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
        {
            return GatewayHealthAssessment.Error(
                $"Токен канала отклонен gateway: HTTP {(int)statusCode.Value} ({statusCode.Value}).");
        }

        if (statusCode.Value is HttpStatusCode.NotFound or HttpStatusCode.MethodNotAllowed)
        {
            return GatewayHealthAssessment.Error(
                $"Endpoint канала не подходит для отправки SMS: HTTP {(int)statusCode.Value} ({statusCode.Value}).");
        }

        if (statusCode.Value == HttpStatusCode.InternalServerError)
        {
            return GatewayHealthAssessment.Unknown(
                "Gateway достижим, но вернул HTTP 500 на пустой probe-пэйлоад. " +
                "Для некоторых версий Traccar это означает, что health-check неинформативен, " +
                "хотя реальная отправка SMS работает.");
        }

        if (statusCode.Value == HttpStatusCode.ServiceUnavailable || (int)statusCode.Value >= 500)
        {
            return GatewayHealthAssessment.Error(
                $"Gateway временно недоступен: HTTP {(int)statusCode.Value} ({statusCode.Value}).");
        }

        if (statusCode.Value is HttpStatusCode.BadRequest or HttpStatusCode.UnprocessableEntity)
        {
            var withoutToken = await SendProbeAsync(endpoint, null, externalCancellationToken, probeCancellationToken);
            if (withoutToken.TransportOk &&
                withoutToken.StatusCode is HttpStatusCode.Unauthorized or HttpStatusCode.Forbidden)
            {
                return GatewayHealthAssessment.Online(
                    $"Gateway доступен для отправки: HTTP {(int)statusCode.Value} ({statusCode.Value}) на probe-пэйлоад, токен валиден.");
            }

            return GatewayHealthAssessment.Online(
                $"Gateway ответил HTTP {(int)statusCode.Value} ({statusCode.Value}) на probe-пэйлоад. Маршрут доступен.");
        }

        if ((int)statusCode.Value >= 200 && (int)statusCode.Value < 300)
        {
            var bodySuffix = BuildBodySuffix(withToken.ResponseBody);
            return GatewayHealthAssessment.Online(
                $"Gateway доступен: HTTP {(int)statusCode.Value} ({statusCode.Value}).{bodySuffix}");
        }

        if (statusCode.Value == HttpStatusCode.TooManyRequests)
        {
            return GatewayHealthAssessment.Error(
                $"Gateway ограничил запросы: HTTP {(int)statusCode.Value} ({statusCode.Value}).");
        }

        return GatewayHealthAssessment.Error(
            $"Неожиданный ответ gateway: HTTP {(int)statusCode.Value} ({statusCode.Value}).");
    }

    private async Task<ProbeResult> SendProbeAsync(
        string endpoint,
        string? token,
        CancellationToken externalCancellationToken,
        CancellationToken probeCancellationToken)
    {
        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Post, endpoint);
            if (!string.IsNullOrWhiteSpace(token))
            {
                request.Headers.TryAddWithoutValidation("Authorization", token.Trim());
            }

            // Intentionally invalid payload to avoid sending real SMS during health-check.
            request.Content = JsonContent.Create(new
            {
                to = "",
                message = ""
            });

            using var response = await httpClient.SendAsync(request, probeCancellationToken);
            var body = await response.Content.ReadAsStringAsync(probeCancellationToken);
            return new ProbeResult(
                TransportOk: true,
                StatusCode: response.StatusCode,
                ResponseBody: body,
                ErrorDetail: string.Empty);
        }
        catch (OperationCanceledException) when (externalCancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (OperationCanceledException)
        {
            return new ProbeResult(
                TransportOk: false,
                StatusCode: null,
                ResponseBody: string.Empty,
                ErrorDetail: "Таймаут при проверке канала.");
        }
        catch (Exception ex)
        {
            return new ProbeResult(
                TransportOk: false,
                StatusCode: null,
                ResponseBody: string.Empty,
                ErrorDetail: ex.Message);
        }
    }

    private static string BuildBodySuffix(string responseBody)
    {
        var normalized = (responseBody ?? string.Empty)
            .Replace('\r', ' ')
            .Replace('\n', ' ')
            .Trim();
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return string.Empty;
        }

        const int maxLen = 160;
        if (normalized.Length > maxLen)
        {
            normalized = $"{normalized[..maxLen]}...";
        }

        return $" Ответ: {normalized}";
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

    private static void AppendDeviceEvent(
        AppDbContext db,
        string eventType,
        string severity,
        string message,
        object payload)
    {
        EventService.Append(
            db,
            category: "device",
            eventType: eventType,
            severity: severity,
            message: message,
            payload: payload,
            runSessionId: null,
            runJobId: null);
    }

    private static string BuildLogDetailSuffix(string? detail)
    {
        var normalized = TruncateForLog(detail, 240);
        return string.IsNullOrWhiteSpace(normalized) ? string.Empty : $" Детали: {normalized}";
    }

    private static string TruncateForLog(string? value, int maxLen)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        var normalized = value
            .Replace('\r', ' ')
            .Replace('\n', ' ')
            .Trim();
        if (normalized.Length <= maxLen)
        {
            return normalized;
        }

        return $"{normalized[..maxLen]}...";
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

    private readonly record struct ProbeResult(
        bool TransportOk,
        HttpStatusCode? StatusCode,
        string ResponseBody,
        string ErrorDetail);

    private readonly record struct GatewayHealthAssessment(string Status, string Detail)
    {
        public static GatewayHealthAssessment Online(string detail) => new(StatusOnline, detail);
        public static GatewayHealthAssessment Unknown(string detail) => new(StatusUnknown, detail);
        public static GatewayHealthAssessment Error(string detail) => new(StatusError, detail);
    }
}
