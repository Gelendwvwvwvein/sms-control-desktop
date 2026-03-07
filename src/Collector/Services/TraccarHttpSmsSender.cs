using System.Net;
using System.Net.Http.Headers;
using System.Text.Encodings.Web;
using System.Text.Json;

namespace Collector.Services;

public sealed class TraccarHttpSmsSender
{
    private static readonly JsonSerializerOptions PayloadJsonOptions = new()
    {
        // Keep payload ASCII-safe so gateway behavior does not depend on console/system code pages.
        Encoder = JavaScriptEncoder.Default
    };

    private readonly HttpClient _httpClient;

    public TraccarHttpSmsSender(HttpClient? httpClient = null)
    {
        _httpClient = httpClient ?? new HttpClient();
    }

    public async Task<TraccarSmsSendResult> SendAsync(TraccarSmsSendRequest request, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(request.Url))
            throw new ArgumentException("Gateway URL is required.", nameof(request.Url));
        if (string.IsNullOrWhiteSpace(request.Token))
            throw new ArgumentException("Gateway token is required.", nameof(request.Token));
        if (string.IsNullOrWhiteSpace(request.To))
            throw new ArgumentException("Recipient number is required.", nameof(request.To));
        if (string.IsNullOrWhiteSpace(request.Message))
            throw new ArgumentException("SMS text is required.", nameof(request.Message));

        if (!Uri.TryCreate(request.Url, UriKind.Absolute, out var uri))
            throw new ArgumentException("Gateway URL must be absolute.", nameof(request.Url));

        var msg = new HttpRequestMessage(HttpMethod.Post, uri);
        msg.Headers.TryAddWithoutValidation("Authorization", request.Token);
        msg.Content = BuildSmsPayloadContent(request);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        if (request.TimeoutMs > 0)
        {
            timeoutCts.CancelAfter(request.TimeoutMs);
        }

        try
        {
            using var response = await _httpClient.SendAsync(msg, timeoutCts.Token);
            var body = await response.Content.ReadAsStringAsync(timeoutCts.Token);
            return new TraccarSmsSendResult
            {
                Timestamp = DateTimeOffset.UtcNow,
                Url = request.Url,
                To = request.To,
                Message = request.Message,
                Success = response.IsSuccessStatusCode,
                StatusCode = (int)response.StatusCode,
                Detail = response.IsSuccessStatusCode
                    ? "Traccar принял запрос на отправку SMS."
                    : $"Traccar вернул HTTP {(int)response.StatusCode} ({response.StatusCode}).",
                ResponseBody = body
            };
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return new TraccarSmsSendResult
            {
                Timestamp = DateTimeOffset.UtcNow,
                Url = request.Url,
                To = request.To,
                Message = request.Message,
                Success = false,
                StatusCode = (int)HttpStatusCode.RequestTimeout,
                Detail = "Timeout while sending request to Traccar gateway."
            };
        }
        catch (Exception ex)
        {
            return new TraccarSmsSendResult
            {
                Timestamp = DateTimeOffset.UtcNow,
                Url = request.Url,
                To = request.To,
                Message = request.Message,
                Success = false,
                StatusCode = 0,
                Detail = "Request to Traccar gateway failed.",
                Error = ex.Message
            };
        }
    }

    private static ByteArrayContent BuildSmsPayloadContent(TraccarSmsSendRequest request)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(new
        {
            to = request.To,
            message = request.Message
        }, PayloadJsonOptions);

        var content = new ByteArrayContent(bytes);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/json")
        {
            CharSet = "utf-8"
        };
        return content;
    }
}

public sealed class TraccarSmsSendRequest
{
    public string Url { get; init; } = "";
    public string Token { get; init; } = "";
    public string To { get; init; } = "";
    public string Message { get; init; } = "";
    public int TimeoutMs { get; init; } = 15000;
}

public sealed class TraccarSmsSendResult
{
    public DateTimeOffset Timestamp { get; init; }
    public string Url { get; init; } = "";
    public string To { get; init; } = "";
    public string Message { get; init; } = "";
    public bool Success { get; init; }
    public int StatusCode { get; init; }
    public string Detail { get; init; } = "";
    public string ResponseBody { get; init; } = "";
    public string Error { get; init; } = "";
}
