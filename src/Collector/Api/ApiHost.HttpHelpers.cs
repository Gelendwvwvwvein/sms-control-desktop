using Microsoft.AspNetCore.Http;

namespace Collector.Api;

public static partial class ApiHost
{
    private static bool TryReadClampedIntQuery(
        HttpRequest request,
        string name,
        int defaultValue,
        int minValue,
        int maxValue,
        string errorCode,
        string errorMessage,
        out int value,
        out IResult? error)
    {
        value = defaultValue;
        error = null;

        var raw = request.Query[name].ToString();
        if (string.IsNullOrWhiteSpace(raw))
        {
            return true;
        }

        if (!int.TryParse(raw, out var parsed))
        {
            error = ErrBadRequest(new ApiErrorDto
            {
                Code = errorCode,
                Message = errorMessage
            });
            return false;
        }

        value = Math.Clamp(parsed, minValue, maxValue);
        return true;
    }

    private static bool TryReadNonNegativeIntQuery(
        HttpRequest request,
        string name,
        int defaultValue,
        string errorCode,
        string errorMessage,
        out int value,
        out IResult? error)
    {
        value = defaultValue;
        error = null;

        var raw = request.Query[name].ToString();
        if (string.IsNullOrWhiteSpace(raw))
        {
            return true;
        }

        if (!int.TryParse(raw, out var parsed) || parsed < 0)
        {
            error = ErrBadRequest(new ApiErrorDto
            {
                Code = errorCode,
                Message = errorMessage
            });
            return false;
        }

        value = parsed;
        return true;
    }

    private static bool TryReadOptionalMinLongQuery(
        HttpRequest request,
        string name,
        long minValue,
        string errorCode,
        string errorMessage,
        out long? value,
        out IResult? error)
    {
        value = null;
        error = null;

        var raw = request.Query[name].ToString();
        if (string.IsNullOrWhiteSpace(raw))
        {
            return true;
        }

        if (!long.TryParse(raw, out var parsed) || parsed < minValue)
        {
            error = ErrBadRequest(new ApiErrorDto
            {
                Code = errorCode,
                Message = errorMessage
            });
            return false;
        }

        value = parsed;
        return true;
    }

    private static bool TryReadBoolQuery(
        HttpRequest request,
        string name,
        bool defaultValue,
        string errorCode,
        string errorMessage,
        out bool value,
        out IResult? error)
    {
        value = defaultValue;
        error = null;

        var raw = request.Query[name].ToString();
        if (string.IsNullOrWhiteSpace(raw))
        {
            return true;
        }

        if (!bool.TryParse(raw, out value))
        {
            error = ErrBadRequest(new ApiErrorDto
            {
                Code = errorCode,
                Message = errorMessage
            });
            return false;
        }

        return true;
    }

    private static async Task WriteBadRequestAsync(HttpContext ctx, ApiErrorDto dto, CancellationToken cancellationToken)
    {
        ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
        await ctx.Response.WriteAsJsonAsync(ErrorCatalog.Enrich(dto), cancellationToken);
    }
}
