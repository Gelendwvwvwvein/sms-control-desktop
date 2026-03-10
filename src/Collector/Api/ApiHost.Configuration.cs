using Collector.Data;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace Collector.Api;

public static partial class ApiHost
{
    private static void MapConfigurationEndpoints(WebApplication app)
    {
        app.MapGet("/api/settings", async (AppDbContext db, SettingsStore store, CancellationToken ct) =>
        {
            var data = await store.GetAsync(db, ct);
            return Results.Ok(data);
        });

        app.MapPut("/api/settings", async (AppDbContext db, SettingsStore store, QueueService queue, AppSettingsDto payload, CancellationToken ct) =>
        {
            if (payload is null)
            {
                return ErrBadRequest(new ApiErrorDto { Code = "CFG_REQUIRED_MISSING", Message = "Тело запроса пустое." });
            }

            var validationError = ValidateSettings(payload);
            if (validationError is not null)
            {
                return ErrBadRequest(validationError);
            }

            var before = await store.GetAsync(db, ct);
            var saved = await store.SaveAsync(db, payload, ct);
            if (ShouldRebuildPreviewsForSettingsChange(before, saved))
            {
                await queue.RebuildPersistedPreviewsForOpenSessionsAsync(db, saved, ct);
            }

            return Results.Ok(saved);
        });

        app.MapGet("/api/settings/comment-rules", async (AppDbContext db, SettingsStore store, CancellationToken ct) =>
        {
            var data = await store.GetAsync(db, ct);
            return Results.Ok(data.CommentRules);
        });

        app.MapPut("/api/settings/comment-rules", async (AppDbContext db, SettingsStore store, CommentRulesDto payload, CancellationToken ct) =>
        {
            if (payload is null)
            {
                return ErrBadRequest(new ApiErrorDto { Code = "CFG_REQUIRED_MISSING", Message = "Тело запроса пустое." });
            }

            var saved = await store.SaveCommentRulesAsync(db, payload, ct);
            return Results.Ok(saved);
        });

        app.MapGet("/api/reports/weekly", async (AppDbContext db, ReportsService reports, CancellationToken ct) =>
        {
            var data = await reports.GetWeeklyAsync(db, ct);
            return Results.Ok(data);
        });

        app.MapPost("/api/rocketman/comment/test", async (
            AppDbContext db,
            SettingsStore store,
            RocketmanCommentService comments,
            RocketmanCommentWriteRequest payload,
            CancellationToken ct) =>
        {
            if (payload is null)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "CFG_REQUIRED_MISSING",
                    Message = "Тело запроса пустое."
                });
            }

            if (string.IsNullOrWhiteSpace(payload.CardUrl))
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "COMMENT_CARD_URL_REQUIRED",
                    Message = "cardUrl обязателен."
                });
            }

            if (string.IsNullOrWhiteSpace(payload.Comment))
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "COMMENT_TEXT_REQUIRED",
                    Message = "comment обязателен."
                });
            }

            var settings = await store.GetAsync(db, ct);
            if (string.IsNullOrWhiteSpace(settings.LoginUrl) ||
                string.IsNullOrWhiteSpace(settings.Login) ||
                string.IsNullOrWhiteSpace(settings.Password))
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "COMMENT_SETTINGS_MISSING",
                    Message = "Для записи комментария заполните Login URL, логин и пароль в настройках."
                });
            }

            var result = await comments.WriteCommentAsync(settings, payload, ct);
            return result.Success
                ? Results.Ok(result)
                : Results.BadRequest(result);
        });

        app.MapGet("/api/channels", async (AppDbContext db, ChannelService channels, CancellationToken ct) =>
        {
            var data = await channels.ListAsync(db, ct);
            return Results.Ok(data);
        });

        app.MapPost("/api/channels", async (AppDbContext db, ChannelService channels, CreateChannelRequest payload, CancellationToken ct) =>
        {
            var validation = ChannelService.ValidateCreateRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var created = await channels.CreateAsync(db, payload, ct);
            return Results.Ok(created);
        });

        app.MapPut("/api/channels/{id:long}", async (long id, AppDbContext db, ChannelService channels, UpdateChannelRequest payload, CancellationToken ct) =>
        {
            var validation = ChannelService.ValidateUpdateRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var updated = await channels.UpdateAsync(db, id, payload, ct);
            if (updated is null)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "CHANNEL_NOT_FOUND",
                    Message = $"Канал с id={id} не найден."
                });
            }

            return Results.Ok(updated);
        });

        app.MapDelete("/api/channels/{id:long}", async (long id, AppDbContext db, ChannelService channels, CancellationToken ct) =>
        {
            var deleted = await channels.DeleteAsync(db, id, ct);
            if (!deleted)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "CHANNEL_NOT_FOUND",
                    Message = $"Канал с id={id} не найден."
                });
            }

            return Results.Ok(new { deleted = true, channelId = id });
        });

        app.MapPatch("/api/channels/{id:long}/status", async (long id, AppDbContext db, ChannelService channels, ChannelStatusPatchRequest payload, CancellationToken ct) =>
        {
            var validation = ChannelService.ValidateStatusPatchRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var updated = await channels.UpdateStatusAsync(db, id, payload.Status, ct);
            if (updated is null)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "CHANNEL_NOT_FOUND",
                    Message = $"Канал с id={id} не найден."
                });
            }

            return Results.Ok(updated);
        });

        app.MapPost("/api/channels/{id:long}/check", async (long id, HttpContext ctx, AppDbContext db, ChannelService channels, CancellationToken ct) =>
        {
            var timeoutMs = ChannelService.NormalizeTimeoutMs(ctx.Request.Query["timeoutMs"]);
            var result = await channels.CheckAsync(db, id, timeoutMs, ct);
            if (result is null)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "CHANNEL_NOT_FOUND",
                    Message = $"Канал с id={id} не найден."
                });
            }

            return Results.Ok(result);
        });

        app.MapPost("/api/channels/check", async (HttpContext ctx, AppDbContext db, ChannelService channels, CancellationToken ct) =>
        {
            var timeoutMs = ChannelService.NormalizeTimeoutMs(ctx.Request.Query["timeoutMs"]);
            var result = await channels.CheckAllAsync(db, timeoutMs, ct);
            return Results.Ok(result);
        });

        app.MapGet("/api/templates/meta", () => Results.Ok(TemplateService.GetMeta()));

        app.MapGet("/api/templates", async (HttpContext ctx, AppDbContext db, TemplateService templates, CancellationToken ct) =>
        {
            var status = ctx.Request.Query["status"].ToString();
            if (!TemplateService.IsValidStatusFilter(status))
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "TEMPLATE_STATUS_INVALID",
                    Message = "Фильтр status должен быть пустым, 'draft' или 'active'."
                });
            }

            var data = await templates.ListAsync(db, status, ct);
            return Results.Ok(data);
        });

        app.MapGet("/api/templates/active", async (AppDbContext db, TemplateService templates, CancellationToken ct) =>
        {
            var data = await templates.ListAsync(db, "active", ct);
            return Results.Ok(data);
        });

        app.MapGet("/api/templates/{id:long}", async (long id, AppDbContext db, TemplateService templates, CancellationToken ct) =>
        {
            var data = await templates.GetByIdAsync(db, id, ct);
            return data is null ? TemplateNotFound(id) : Results.Ok(data);
        });

        app.MapPost("/api/templates", async (AppDbContext db, TemplateService templates, SettingsStore settingsStore, QueueService queue, TemplateUpsertRequest payload, CancellationToken ct) =>
        {
            var validation = TemplateService.ValidateUpsertRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var created = await templates.CreateAsync(db, payload, ct);
            await RebuildTemplatePreviewsIfNeededAsync(db, settingsStore, queue, before: null, created, ct);

            return Results.Ok(created);
        });

        app.MapPut("/api/templates/{id:long}", async (long id, AppDbContext db, TemplateService templates, SettingsStore settingsStore, QueueService queue, TemplateUpsertRequest payload, CancellationToken ct) =>
        {
            var validation = TemplateService.ValidateUpsertRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var before = await templates.GetByIdAsync(db, id, ct);
            if (before is null)
            {
                return TemplateNotFound(id);
            }

            var updated = await templates.UpdateAsync(db, id, payload, ct);
            if (updated is null)
            {
                return TemplateNotFound(id);
            }

            await RebuildTemplatePreviewsIfNeededAsync(db, settingsStore, queue, before, updated, ct);

            return Results.Ok(updated);
        });

        app.MapPatch("/api/templates/{id:long}/status", async (long id, AppDbContext db, TemplateService templates, SettingsStore settingsStore, QueueService queue, TemplateStatusPatchRequest payload, CancellationToken ct) =>
        {
            var validation = TemplateService.ValidateStatusPatchRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var before = await templates.GetByIdAsync(db, id, ct);
            if (before is null)
            {
                return TemplateNotFound(id);
            }

            var updated = await templates.UpdateStatusAsync(db, id, payload.Status, ct);
            if (updated is null)
            {
                return TemplateNotFound(id);
            }

            await RebuildTemplatePreviewsIfNeededAsync(db, settingsStore, queue, before, updated, ct);

            return Results.Ok(updated);
        });

        app.MapGet("/api/manual-presets", async (AppDbContext db, ManualPresetService presets, CancellationToken ct) =>
        {
            var data = await presets.ListAsync(db, ct);
            return Results.Ok(data);
        });

        app.MapGet("/api/manual-presets/{id:long}", async (long id, AppDbContext db, ManualPresetService presets, CancellationToken ct) =>
        {
            var data = await presets.GetByIdAsync(db, id, ct);
            if (data is null)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "MANUAL_PRESET_NOT_FOUND",
                    Message = $"Типовой ответ с id={id} не найден."
                });
            }

            return Results.Ok(data);
        });

        app.MapPost("/api/manual-presets", async (AppDbContext db, ManualPresetService presets, ManualPresetUpsertRequest payload, CancellationToken ct) =>
        {
            var validation = ManualPresetService.ValidateUpsertRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var created = await presets.CreateAsync(db, payload, ct);
            return Results.Ok(created);
        });

        app.MapPut("/api/manual-presets/{id:long}", async (long id, AppDbContext db, ManualPresetService presets, ManualPresetUpsertRequest payload, CancellationToken ct) =>
        {
            var validation = ManualPresetService.ValidateUpsertRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var updated = await presets.UpdateAsync(db, id, payload, ct);
            if (updated is null)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "MANUAL_PRESET_NOT_FOUND",
                    Message = $"Типовой ответ с id={id} не найден."
                });
            }

            return Results.Ok(updated);
        });

        app.MapDelete("/api/manual-presets/{id:long}", async (long id, AppDbContext db, ManualPresetService presets, CancellationToken ct) =>
        {
            var deleted = await presets.DeleteAsync(db, id, ct);
            if (!deleted)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "MANUAL_PRESET_NOT_FOUND",
                    Message = $"Типовой ответ с id={id} не найден."
                });
            }

            return Results.Ok(new { deleted = true, presetId = id });
        });
    }
}
