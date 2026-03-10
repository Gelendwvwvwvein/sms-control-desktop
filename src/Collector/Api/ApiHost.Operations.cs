using Collector.Data;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace Collector.Api;

public static partial class ApiHost
{
    private static void MapOperationsEndpoints(WebApplication app)
    {
        app.MapGet("/api/dialogs", async (HttpContext ctx, AppDbContext db, DialogService dialogs, CancellationToken ct) =>
        {
            var search = ctx.Request.Query["search"].ToString();
            if (!TryReadClampedIntQuery(ctx.Request, "limit", 100, 1, 5000, "DIALOGS_LIMIT_INVALID", "Параметр limit должен быть числом.", out var limit, out var parseError))
            {
                return parseError!;
            }

            if (!TryReadNonNegativeIntQuery(ctx.Request, "offset", 0, "DIALOGS_OFFSET_INVALID", "Параметр offset должен быть числом >= 0.", out var offset, out parseError))
            {
                return parseError!;
            }

            var data = await dialogs.ListAsync(db, limit, offset, search, ct);
            return Results.Ok(data);
        });

        app.MapGet("/api/dialogs/by-client/{externalClientId}", async (string externalClientId, HttpContext ctx, AppDbContext db, DialogService dialogs, CancellationToken ct) =>
        {
            if (!TryReadClampedIntQuery(ctx.Request, "limit", 200, 1, 5000, "DIALOGS_LIMIT_INVALID", "Параметр limit должен быть числом.", out var limit, out var parseError))
            {
                return parseError!;
            }

            if (!TryReadNonNegativeIntQuery(ctx.Request, "offset", 0, "DIALOGS_OFFSET_INVALID", "Параметр offset должен быть числом >= 0.", out var offset, out parseError))
            {
                return parseError!;
            }

            var data = await dialogs.GetMessagesByClientExternalIdAsync(db, externalClientId, limit, offset, ct);
            if (data is null)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "DIALOG_NOT_FOUND",
                    Message = $"Диалог для клиента externalClientId='{externalClientId}' не найден."
                });
            }

            return Results.Ok(data);
        });

        app.MapGet("/api/dialogs/by-phone/{phone}/messages", async (string phone, HttpContext ctx, AppDbContext db, DialogService dialogs, CancellationToken ct) =>
        {
            if (!TryReadClampedIntQuery(ctx.Request, "limit", 200, 1, 5000, "DIALOGS_LIMIT_INVALID", "Параметр limit должен быть числом.", out var limit, out var parseError))
            {
                return parseError!;
            }

            if (!TryReadNonNegativeIntQuery(ctx.Request, "offset", 0, "DIALOGS_OFFSET_INVALID", "Параметр offset должен быть числом >= 0.", out var offset, out parseError))
            {
                return parseError!;
            }

            var data = await dialogs.GetMessagesByPhoneAsync(db, phone, limit, offset, ct);
            return Results.Ok(data);
        });

        app.MapGet("/api/dialogs/by-phone/{phone}/draft", async (string phone, AppDbContext db, DialogService dialogs, CancellationToken ct) =>
        {
            var data = await dialogs.GetDraftByPhoneAsync(db, phone, ct);
            return Results.Ok(data);
        });

        app.MapPut("/api/dialogs/by-phone/{phone}/draft", async (string phone, AppDbContext db, DialogService dialogs, DialogDraftUpsertRequest payload, CancellationToken ct) =>
        {
            var validation = DialogService.ValidateDraftUpsertRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var data = await dialogs.UpsertDraftByPhoneAsync(db, phone, payload, ct);
            return Results.Ok(data);
        });

        app.MapDelete("/api/dialogs/by-phone/{phone}/draft", async (string phone, AppDbContext db, DialogService dialogs, CancellationToken ct) =>
        {
            var data = await dialogs.DeleteDraftByPhoneAsync(db, phone, ct);
            return Results.Ok(data);
        });

        app.MapPost("/api/dialogs/by-phone/{phone}/send", async (string phone, AppDbContext db, DialogService dialogs, DialogManualSendRequest payload, CancellationToken ct) =>
        {
            var validation = DialogService.ValidateManualSendRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var result = await dialogs.SendManualAsync(db, phone, payload, ct);
            if (!result.Success)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = result.Code,
                    Message = result.Message
                });
            }

            return Results.Ok(result);
        });

        app.MapDelete("/api/dialogs/by-phone/{phone}", async (string phone, AppDbContext db, DialogService dialogs, CancellationToken ct) =>
        {
            var result = await dialogs.DeleteByPhoneAsync(db, phone, ct);
            return Results.Ok(result);
        });

        app.MapPost("/api/dialogs/prune", async (AppDbContext db, DialogService dialogs, DialogPruneRequest payload, CancellationToken ct) =>
        {
            var validation = DialogService.ValidatePruneRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var result = await dialogs.PruneOlderThanAsync(db, payload.OlderThanDays, ct);
            return Results.Ok(result);
        });

        app.MapGet("/api/stop-list", async (HttpContext ctx, AppDbContext db, StopListService stopList, CancellationToken ct) =>
        {
            if (!TryReadBoolQuery(ctx.Request, "activeOnly", true, "STOP_LIST_ACTIVE_ONLY_INVALID", "Параметр activeOnly должен быть true или false.", out var activeOnly, out var parseError))
            {
                return parseError!;
            }

            var data = await stopList.ListAsync(db, activeOnly, ct);
            return Results.Ok(data);
        });

        app.MapGet("/api/stop-list/by-phone/{phone}", async (string phone, HttpContext ctx, AppDbContext db, StopListService stopList, CancellationToken ct) =>
        {
            var phoneValidation = StopListService.ValidatePhone(phone);
            if (phoneValidation is not null)
            {
                return ErrBadRequest(phoneValidation);
            }

            if (!TryReadBoolQuery(ctx.Request, "activeOnly", true, "STOP_LIST_ACTIVE_ONLY_INVALID", "Параметр activeOnly должен быть true или false.", out var activeOnly, out var parseError))
            {
                return parseError!;
            }

            var data = await stopList.ListByPhoneAsync(db, phone, activeOnly, ct);
            return Results.Ok(data);
        });

        app.MapGet("/api/stop-list/{id:long}", async (long id, AppDbContext db, StopListService stopList, CancellationToken ct) =>
        {
            var data = await stopList.GetByIdAsync(db, id, ct);
            if (data is null)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "STOP_LIST_NOT_FOUND",
                    Message = $"Запись стоп-листа с id={id} не найдена."
                });
            }

            return Results.Ok(data);
        });

        app.MapPost("/api/stop-list", async (AppDbContext db, StopListService stopList, StopListUpsertRequest payload, CancellationToken ct) =>
        {
            var validation = StopListService.ValidateUpsertRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var created = await stopList.CreateOrActivateAsync(db, payload, ct);
            return Results.Ok(created);
        });

        app.MapPut("/api/stop-list/{id:long}", async (long id, AppDbContext db, StopListService stopList, StopListUpsertRequest payload, CancellationToken ct) =>
        {
            var validation = StopListService.ValidateUpsertRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var updated = await stopList.UpdateAsync(db, id, payload, ct);
            if (updated is null)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "STOP_LIST_NOT_FOUND",
                    Message = $"Запись стоп-листа с id={id} не найдена."
                });
            }

            return Results.Ok(updated);
        });

        app.MapDelete("/api/stop-list/{id:long}", async (long id, AppDbContext db, StopListService stopList, CancellationToken ct) =>
        {
            var deleted = await stopList.DeactivateByIdAsync(db, id, ct);
            if (!deleted)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "STOP_LIST_NOT_FOUND",
                    Message = $"Запись стоп-листа с id={id} не найдена."
                });
            }

            return Results.Ok(new { deactivated = true, stopListId = id });
        });

        app.MapDelete("/api/stop-list/by-phone/{phone}", async (string phone, AppDbContext db, StopListService stopList, CancellationToken ct) =>
        {
            var phoneValidation = StopListService.ValidatePhone(phone);
            if (phoneValidation is not null)
            {
                return ErrBadRequest(phoneValidation);
            }

            var deactivatedCount = await stopList.DeactivateByPhoneAsync(db, phone, ct);
            return Results.Ok(new { deactivated = deactivatedCount > 0, phone, deactivatedCount });
        });

        app.MapPost("/api/stop-list/bulk/add", async (AppDbContext db, StopListService stopList, StopListBulkAddRequest payload, CancellationToken ct) =>
        {
            if (payload?.Phones is null || payload.Phones.Count == 0)
            {
                return ErrBadRequest(new ApiErrorDto { Code = "STOP_LIST_PHONES_EMPTY", Message = "Список телефонов пуст." });
            }

            if (payload.Phones.Count > 1000)
            {
                return ErrBadRequest(new ApiErrorDto { Code = "STOP_LIST_PHONES_TOO_MANY", Message = "Слишком много телефонов (максимум 1000)." });
            }

            var result = await stopList.BulkAddAsync(db, payload, ct);
            return Results.Ok(result);
        });

        app.MapPost("/api/stop-list/bulk/remove", async (AppDbContext db, StopListService stopList, StopListBulkRemoveRequest payload, CancellationToken ct) =>
        {
            if (payload?.Phones is null || payload.Phones.Count == 0)
            {
                return ErrBadRequest(new ApiErrorDto { Code = "STOP_LIST_PHONES_EMPTY", Message = "Список телефонов пуст." });
            }

            if (payload.Phones.Count > 1000)
            {
                return ErrBadRequest(new ApiErrorDto { Code = "STOP_LIST_PHONES_TOO_MANY", Message = "Слишком много телефонов (максимум 1000)." });
            }

            var result = await stopList.BulkRemoveByPhonesAsync(db, payload, ct);
            return Results.Ok(result);
        });

        app.MapPost("/api/stop-list/bulk/deactivate", async (AppDbContext db, StopListService stopList, StopListBulkDeactivateRequest payload, CancellationToken ct) =>
        {
            if (payload?.Ids is null || payload.Ids.Count == 0)
            {
                return ErrBadRequest(new ApiErrorDto { Code = "STOP_LIST_IDS_EMPTY", Message = "Список id записей пуст." });
            }

            if (payload.Ids.Count > 1000)
            {
                return ErrBadRequest(new ApiErrorDto { Code = "STOP_LIST_IDS_TOO_MANY", Message = "Слишком много записей (максимум 1000)." });
            }

            var deactivated = await stopList.BulkDeactivateByIdsAsync(db, payload, ct);
            return Results.Ok(new { deactivated, requested = payload.Ids.Count });
        });

        app.MapGet("/api/alerts", async (HttpContext ctx, AppDbContext db, AlertService alerts, CancellationToken ct) =>
        {
            var status = ctx.Request.Query["status"].ToString();
            var statusValidation = AlertService.ValidateStatusFilter(status);
            if (statusValidation is not null)
            {
                return ErrBadRequest(statusValidation);
            }

            if (!TryReadClampedIntQuery(ctx.Request, "limit", 100, 1, 5000, "ALERTS_LIMIT_INVALID", "Параметр limit должен быть числом.", out var limit, out var parseError))
            {
                return parseError!;
            }

            if (!TryReadNonNegativeIntQuery(ctx.Request, "offset", 0, "ALERTS_OFFSET_INVALID", "Параметр offset должен быть числом >= 0.", out var offset, out parseError))
            {
                return parseError!;
            }

            var data = await alerts.ListAsync(db, status, limit, offset, ct);
            return Results.Ok(data);
        });

        app.MapPatch("/api/alerts/{id:long}/status", async (long id, AppDbContext db, AlertService alerts, AlertStatusPatchRequest payload, CancellationToken ct) =>
        {
            var validation = AlertService.ValidateStatusPatchRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var updated = await alerts.UpdateStatusAsync(db, id, payload.Status, ct);
            if (updated is null)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "ALERT_NOT_FOUND",
                    Message = $"Уведомление с id={id} не найдено."
                });
            }

            return Results.Ok(updated);
        });

        app.MapGet("/api/events", async (HttpContext ctx, AppDbContext db, EventService events, CancellationToken ct) =>
        {
            if (!TryReadOptionalMinLongQuery(ctx.Request, "runSessionId", 1, "EVENTS_RUN_SESSION_ID_INVALID", "Параметр runSessionId должен быть числом > 0.", out var runSessionId, out var parseError))
            {
                return parseError!;
            }

            var runSessionValidation = EventService.ValidateRunSessionId(runSessionId);
            if (runSessionValidation is not null)
            {
                return ErrBadRequest(runSessionValidation);
            }

            if (!TryReadClampedIntQuery(ctx.Request, "limit", 200, 1, 5000, "EVENTS_LIMIT_INVALID", "Параметр limit должен быть числом.", out var limit, out parseError))
            {
                return parseError!;
            }

            if (!TryReadNonNegativeIntQuery(ctx.Request, "offset", 0, "EVENTS_OFFSET_INVALID", "Параметр offset должен быть числом >= 0.", out var offset, out parseError))
            {
                return parseError!;
            }

            var data = await events.ListAsync(db, runSessionId, limit, offset, ct);
            return Results.Ok(data);
        });

        app.MapGet("/api/events/run", async (HttpContext ctx, AppDbContext db, EventService events, CancellationToken ct) =>
        {
            if (!TryReadOptionalMinLongQuery(ctx.Request, "runSessionId", 1, "EVENTS_RUN_SESSION_ID_INVALID", "Параметр runSessionId должен быть числом > 0.", out var runSessionId, out var parseError))
            {
                await WriteBadRequestAsync(ctx, new ApiErrorDto
                {
                    Code = "EVENTS_RUN_SESSION_ID_INVALID",
                    Message = "Параметр runSessionId должен быть числом > 0."
                }, ct);
                return;
            }

            var runSessionValidation = EventService.ValidateRunSessionId(runSessionId);
            if (runSessionValidation is not null)
            {
                await WriteBadRequestAsync(ctx, runSessionValidation, ct);
                return;
            }

            if (!TryReadOptionalMinLongQuery(ctx.Request, "sinceId", 0, "EVENTS_SINCE_ID_INVALID", "Параметр sinceId должен быть числом >= 0.", out var sinceId, out parseError))
            {
                await WriteBadRequestAsync(ctx, new ApiErrorDto
                {
                    Code = "EVENTS_SINCE_ID_INVALID",
                    Message = "Параметр sinceId должен быть числом >= 0."
                }, ct);
                return;
            }

            var sinceIdValidation = EventService.ValidateSinceId(sinceId);
            if (sinceIdValidation is not null)
            {
                await WriteBadRequestAsync(ctx, sinceIdValidation, ct);
                return;
            }

            if (!TryReadClampedIntQuery(ctx.Request, "historyLimit", 100, 0, 1000, "EVENTS_HISTORY_LIMIT_INVALID", "Параметр historyLimit должен быть числом.", out var historyLimit, out parseError))
            {
                await WriteBadRequestAsync(ctx, new ApiErrorDto
                {
                    Code = "EVENTS_HISTORY_LIMIT_INVALID",
                    Message = "Параметр historyLimit должен быть числом."
                }, ct);
                return;
            }

            if (!TryReadClampedIntQuery(ctx.Request, "pollMs", 1000, 250, 5000, "EVENTS_POLL_MS_INVALID", "Параметр pollMs должен быть числом.", out var pollMs, out parseError))
            {
                await WriteBadRequestAsync(ctx, new ApiErrorDto
                {
                    Code = "EVENTS_POLL_MS_INVALID",
                    Message = "Параметр pollMs должен быть числом."
                }, ct);
                return;
            }

            if (!TryReadClampedIntQuery(ctx.Request, "heartbeatSec", 15, 5, 60, "EVENTS_HEARTBEAT_SEC_INVALID", "Параметр heartbeatSec должен быть числом.", out var heartbeatSeconds, out parseError))
            {
                await WriteBadRequestAsync(ctx, new ApiErrorDto
                {
                    Code = "EVENTS_HEARTBEAT_SEC_INVALID",
                    Message = "Параметр heartbeatSec должен быть числом."
                }, ct);
                return;
            }

            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, ctx.RequestAborted);
            var streamCt = linkedCts.Token;

            ctx.Response.StatusCode = StatusCodes.Status200OK;
            ctx.Response.Headers.CacheControl = "no-cache";
            ctx.Response.Headers["X-Accel-Buffering"] = "no";
            ctx.Response.ContentType = "text/event-stream";

            await ctx.Response.WriteAsync("retry: 3000\n", streamCt);
            await ctx.Response.WriteAsync(": connected\n\n", streamCt);
            await ctx.Response.Body.FlushAsync(streamCt);

            var lastId = sinceId.GetValueOrDefault();
            if (lastId <= 0 && historyLimit > 0)
            {
                var history = await events.ListLatestAsync(db, runSessionId, historyLimit, streamCt);
                foreach (var item in history)
                {
                    await WriteSseEventAsync(ctx.Response, "run_history", item.Id, item, streamCt);
                    lastId = item.Id;
                }
            }

            var nextHeartbeatAt = DateTime.UtcNow.AddSeconds(heartbeatSeconds);
            try
            {
                while (!streamCt.IsCancellationRequested)
                {
                    var items = await events.ListAfterIdAsync(db, lastId, runSessionId, 250, streamCt);
                    if (items.Count > 0)
                    {
                        foreach (var item in items)
                        {
                            await WriteSseEventAsync(ctx.Response, "run_event", item.Id, item, streamCt);
                            lastId = item.Id;
                        }

                        nextHeartbeatAt = DateTime.UtcNow.AddSeconds(heartbeatSeconds);
                    }
                    else if (DateTime.UtcNow >= nextHeartbeatAt)
                    {
                        await ctx.Response.WriteAsync(": ping\n\n", streamCt);
                        await ctx.Response.Body.FlushAsync(streamCt);
                        nextHeartbeatAt = DateTime.UtcNow.AddSeconds(heartbeatSeconds);
                    }

                    await Task.Delay(pollMs, streamCt);
                }
            }
            catch (OperationCanceledException)
            {
            }
        });
    }
}
