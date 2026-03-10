using Microsoft.AspNetCore.Http;

namespace Collector.Api;

public static partial class ApiHost
{
    private static ApiErrorDto ApiError(string code, string message) => new()
    {
        Code = code,
        Message = message
    };

    private static async Task<IResult> ExecuteNotFoundAsync<T>(Func<Task<T>> action, string notFoundCode)
    {
        try
        {
            return Results.Ok(await action());
        }
        catch (KeyNotFoundException ex)
        {
            return ErrNotFound(ApiError(notFoundCode, ex.Message));
        }
    }

    private static async Task<IResult> ExecuteNotFoundAndBadRequestAsync<T>(
        Func<Task<T>> action,
        string notFoundCode,
        string invalidCode)
    {
        try
        {
            return Results.Ok(await action());
        }
        catch (KeyNotFoundException ex)
        {
            return ErrNotFound(ApiError(notFoundCode, ex.Message));
        }
        catch (InvalidOperationException ex)
        {
            return ErrBadRequest(ApiError(invalidCode, ex.Message));
        }
    }

    private static async Task<IResult> ExecuteNotFoundAndConflictAsync<T>(
        Func<Task<T>> action,
        string notFoundCode,
        string conflictCode)
    {
        try
        {
            return Results.Ok(await action());
        }
        catch (KeyNotFoundException ex)
        {
            return ErrNotFound(ApiError(notFoundCode, ex.Message));
        }
        catch (InvalidOperationException ex)
        {
            return ErrConflict(ApiError(conflictCode, ex.Message));
        }
    }

    private static async Task<IResult> ExecuteQueueStateAsync<T>(
        Func<Task<T>> action,
        string notFoundCode,
        string invalidCode)
    {
        try
        {
            return Results.Ok(await action());
        }
        catch (QueueStateException ex)
        {
            return Results.Json(ApiError(ex.Code, ex.Message), statusCode: ex.HttpStatusCode);
        }
        catch (KeyNotFoundException ex)
        {
            return ErrNotFound(ApiError(notFoundCode, ex.Message));
        }
        catch (InvalidOperationException ex)
        {
            return ErrBadRequest(ApiError(invalidCode, ex.Message));
        }
    }

    private static async Task<IResult> ExecuteRunStateAsync<T>(Func<Task<T>> action)
    {
        try
        {
            return Results.Ok(await action());
        }
        catch (RunStateException ex)
        {
            return Results.Json(ErrorCatalog.Enrich(ApiError(ex.Code, ex.Message)), statusCode: ex.HttpStatusCode);
        }
    }

    private static IResult TemplateNotFound(long id) =>
        ErrNotFound(ApiError("TEMPLATE_NOT_FOUND", $"Шаблон с id={id} не найден."));
}
