using Collector.Data;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Playwright;
using System.Globalization;
using System.Text.Json;

namespace Collector.Api;

public static class ApiHost
{
    public static async Task RunAsync(string[] args, CancellationToken cancellationToken = default)
    {
        var dbPath = GetArg(args, "--db-path");
        if (string.IsNullOrWhiteSpace(dbPath))
        {
            dbPath = Environment.GetEnvironmentVariable("SMS_APP_DB_PATH");
        }

        var resolvedDbPath = DbPathResolver.ResolvePath(dbPath);
        await DatabaseMigrator.MigrateAsync(resolvedDbPath, cancellationToken);

        var portRaw = GetArg(args, "--port");
        var port = int.TryParse(portRaw, out var parsedPort) ? parsedPort : 5057;
        if (HasFlag(args, "--lan"))
        {
            Console.WriteLine("Flag --lan ignored: API is localhost-only.");
        }
        var host = "127.0.0.1";

        var url = $"http://{host}:{port}";

        var builder = WebApplication.CreateBuilder(args);
        builder.WebHost.UseUrls(url);

        builder.Services.AddCors(options =>
        {
            options.AddPolicy("LocalUi", policy =>
            {
                policy
                    .AllowAnyOrigin()
                    .AllowAnyHeader()
                    .AllowAnyMethod();
            });
        });

        builder.Services.AddDbContext<AppDbContext>(options =>
            options.UseSqlite($"Data Source={resolvedDbPath}"));
        builder.Services.AddSingleton<SettingsStore>();
        builder.Services.AddSingleton<TemplateService>();
        builder.Services.AddSingleton<RuleEngineService>();
        builder.Services.AddSingleton<ManualPresetService>();
        builder.Services.AddSingleton<DialogService>();
        builder.Services.AddSingleton<StopListService>();
        builder.Services.AddSingleton<ClientSyncService>();
        builder.Services.AddSingleton<DebtCacheService>();
        builder.Services.AddSingleton<QueueService>();
        builder.Services.AddSingleton<RunService>();
        builder.Services.AddSingleton<AlertService>();
        builder.Services.AddSingleton<EventService>();
        builder.Services.AddSingleton<AuditService>();
        builder.Services.AddSingleton<ReportsService>();
        builder.Services.AddSingleton<RocketmanCommentService>();
        builder.Services.AddSingleton<RunDispatchService>();
        builder.Services.AddHostedService<RunDispatchBackgroundWorker>();
        builder.Services.AddHttpClient<ChannelService>();

        var app = builder.Build();
        app.UseCors("LocalUi");

        app.Use(async (ctx, next) =>
        {
            var auditService = ctx.RequestServices.GetRequiredService<AuditService>();
            var shouldAudit = auditService.ShouldAudit(ctx.Request);
            await next();

            if (!shouldAudit) return;
            if (ctx.Response.StatusCode < 200 || ctx.Response.StatusCode >= 300) return;

            try
            {
                var db = ctx.RequestServices.GetRequiredService<AppDbContext>();
                await auditService.LogHttpMutationAsync(db, ctx, ctx.RequestAborted);
            }
            catch
            {
                // Аудит не должен ломать основной сценарий API.
            }
        });

        app.MapGet("/health", () => Results.Ok(new { status = "ok" }));

        static void ApplyUiNoCacheHeaders(HttpContext ctx)
        {
            ctx.Response.Headers.CacheControl = "no-store, no-cache, must-revalidate";
            ctx.Response.Headers.Pragma = "no-cache";
            ctx.Response.Headers.Expires = "0";
        }

        IResult UiAsset(HttpContext ctx, string path)
        {
            if (!WebUiAssets.TryGet(path, out var data, out var contentType))
            {
                return Results.NotFound();
            }

            ApplyUiNoCacheHeaders(ctx);
            return Results.File(data, contentType);
        }

        app.MapGet("/", (HttpContext ctx) => UiAsset(ctx, "/"));
        app.MapGet("/index.html", (HttpContext ctx) => UiAsset(ctx, "/index.html"));
        app.MapGet("/app.js", (HttpContext ctx) => UiAsset(ctx, "/app.js"));
        app.MapGet("/styles.css", (HttpContext ctx) => UiAsset(ctx, "/styles.css"));
        app.MapGet("/uikit.js", (HttpContext ctx) => UiAsset(ctx, "/uikit.js"));
        app.MapGet("/favicon.svg", (HttpContext ctx) => UiAsset(ctx, "/favicon.svg"));
        app.MapGet("/favicon.ico", (HttpContext ctx) => UiAsset(ctx, "/favicon.ico"));

        app.MapGet("/api/errors/catalog", () =>
        {
            var items = ErrorCatalog.GetAll()
                .Select(kv => new
                {
                    code = kv.Key,
                    severity = kv.Value.Severity,
                    retryable = kv.Value.Retryable,
                    operatorAction = kv.Value.OperatorAction
                })
                .OrderBy(x => x.code)
                .ToList();
            return Results.Ok(new { items });
        });

        app.MapGet("/api/audit", async (HttpContext ctx, AppDbContext db, AuditService audit, CancellationToken ct) =>
        {
            var category = ctx.Request.Query["category"].ToString();
            var limitRaw = ctx.Request.Query["limit"].ToString();
            var offsetRaw = ctx.Request.Query["offset"].ToString();

            var limit = 200;
            if (!string.IsNullOrWhiteSpace(limitRaw))
            {
                if (!int.TryParse(limitRaw, out var parsedLimit))
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "AUDIT_LIMIT_INVALID",
                        Message = "Параметр limit должен быть числом."
                    });
                }

                limit = Math.Clamp(parsedLimit, 1, 5000);
            }

            var offset = 0;
            if (!string.IsNullOrWhiteSpace(offsetRaw))
            {
                if (!int.TryParse(offsetRaw, out var parsedOffset) || parsedOffset < 0)
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "AUDIT_OFFSET_INVALID",
                        Message = "Параметр offset должен быть числом >= 0."
                    });
                }

                offset = parsedOffset;
            }

            var data = await audit.ListAsync(db, category, limit, offset, ct);
            return Results.Ok(data);
        });

        app.MapGet("/api/settings", async (AppDbContext db, SettingsStore store, CancellationToken ct) =>
        {
            var data = await store.GetAsync(db, ct);
            return Results.Ok(data);
        });

        app.MapPut("/api/settings", async (AppDbContext db, SettingsStore store, AppSettingsDto payload, CancellationToken ct) =>
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

            var saved = await store.SaveAsync(db, payload, ct);
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
            if (data is null)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "TEMPLATE_NOT_FOUND",
                    Message = $"Шаблон с id={id} не найден."
                });
            }

            return Results.Ok(data);
        });

        app.MapPost("/api/templates", async (AppDbContext db, TemplateService templates, TemplateUpsertRequest payload, CancellationToken ct) =>
        {
            var validation = TemplateService.ValidateUpsertRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var created = await templates.CreateAsync(db, payload, ct);
            return Results.Ok(created);
        });

        app.MapPut("/api/templates/{id:long}", async (long id, AppDbContext db, TemplateService templates, TemplateUpsertRequest payload, CancellationToken ct) =>
        {
            var validation = TemplateService.ValidateUpsertRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var updated = await templates.UpdateAsync(db, id, payload, ct);
            if (updated is null)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "TEMPLATE_NOT_FOUND",
                    Message = $"Шаблон с id={id} не найден."
                });
            }

            return Results.Ok(updated);
        });

        app.MapPatch("/api/templates/{id:long}/status", async (long id, AppDbContext db, TemplateService templates, TemplateStatusPatchRequest payload, CancellationToken ct) =>
        {
            var validation = TemplateService.ValidateStatusPatchRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var updated = await templates.UpdateStatusAsync(db, id, payload.Status, ct);
            if (updated is null)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "TEMPLATE_NOT_FOUND",
                    Message = $"Шаблон с id={id} не найден."
                });
            }

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

        app.MapGet("/api/dialogs", async (HttpContext ctx, AppDbContext db, DialogService dialogs, CancellationToken ct) =>
        {
            var limitRaw = ctx.Request.Query["limit"].ToString();
            var offsetRaw = ctx.Request.Query["offset"].ToString();
            var search = ctx.Request.Query["search"].ToString();

            var limit = 100;
            if (!string.IsNullOrWhiteSpace(limitRaw))
            {
                if (!int.TryParse(limitRaw, out var parsedLimit))
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "DIALOGS_LIMIT_INVALID",
                        Message = "Параметр limit должен быть числом."
                    });
                }

                limit = Math.Clamp(parsedLimit, 1, 5000);
            }

            var offset = 0;
            if (!string.IsNullOrWhiteSpace(offsetRaw))
            {
                if (!int.TryParse(offsetRaw, out var parsedOffset) || parsedOffset < 0)
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "DIALOGS_OFFSET_INVALID",
                        Message = "Параметр offset должен быть числом >= 0."
                    });
                }

                offset = parsedOffset;
            }

            var data = await dialogs.ListAsync(db, limit, offset, search, ct);
            return Results.Ok(data);
        });

        app.MapGet("/api/dialogs/by-client/{externalClientId}", async (string externalClientId, HttpContext ctx, AppDbContext db, DialogService dialogs, CancellationToken ct) =>
        {
            var limitRaw = ctx.Request.Query["limit"].ToString();
            var offsetRaw = ctx.Request.Query["offset"].ToString();

            var limit = 200;
            if (!string.IsNullOrWhiteSpace(limitRaw))
            {
                if (!int.TryParse(limitRaw, out var parsedLimit))
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "DIALOGS_LIMIT_INVALID",
                        Message = "Параметр limit должен быть числом."
                    });
                }

                limit = Math.Clamp(parsedLimit, 1, 5000);
            }

            var offset = 0;
            if (!string.IsNullOrWhiteSpace(offsetRaw))
            {
                if (!int.TryParse(offsetRaw, out var parsedOffset) || parsedOffset < 0)
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "DIALOGS_OFFSET_INVALID",
                        Message = "Параметр offset должен быть числом >= 0."
                    });
                }

                offset = parsedOffset;
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
            var limitRaw = ctx.Request.Query["limit"].ToString();
            var offsetRaw = ctx.Request.Query["offset"].ToString();

            var limit = 200;
            if (!string.IsNullOrWhiteSpace(limitRaw))
            {
                if (!int.TryParse(limitRaw, out var parsedLimit))
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "DIALOGS_LIMIT_INVALID",
                        Message = "Параметр limit должен быть числом."
                    });
                }

                limit = Math.Clamp(parsedLimit, 1, 5000);
            }

            var offset = 0;
            if (!string.IsNullOrWhiteSpace(offsetRaw))
            {
                if (!int.TryParse(offsetRaw, out var parsedOffset) || parsedOffset < 0)
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "DIALOGS_OFFSET_INVALID",
                        Message = "Параметр offset должен быть числом >= 0."
                    });
                }

                offset = parsedOffset;
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
            var activeOnlyRaw = ctx.Request.Query["activeOnly"].ToString();
            var activeOnly = true;
            if (!string.IsNullOrWhiteSpace(activeOnlyRaw) && !bool.TryParse(activeOnlyRaw, out activeOnly))
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "STOP_LIST_ACTIVE_ONLY_INVALID",
                    Message = "Параметр activeOnly должен быть true или false."
                });
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

            var activeOnlyRaw = ctx.Request.Query["activeOnly"].ToString();
            var activeOnly = true;
            if (!string.IsNullOrWhiteSpace(activeOnlyRaw) && !bool.TryParse(activeOnlyRaw, out activeOnly))
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "STOP_LIST_ACTIVE_ONLY_INVALID",
                    Message = "Параметр activeOnly должен быть true или false."
                });
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

            var limitRaw = ctx.Request.Query["limit"].ToString();
            var offsetRaw = ctx.Request.Query["offset"].ToString();

            var limit = 100;
            if (!string.IsNullOrWhiteSpace(limitRaw))
            {
                if (!int.TryParse(limitRaw, out var parsedLimit))
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "ALERTS_LIMIT_INVALID",
                        Message = "Параметр limit должен быть числом."
                    });
                }

                limit = Math.Clamp(parsedLimit, 1, 5000);
            }

            var offset = 0;
            if (!string.IsNullOrWhiteSpace(offsetRaw))
            {
                if (!int.TryParse(offsetRaw, out var parsedOffset) || parsedOffset < 0)
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "ALERTS_OFFSET_INVALID",
                        Message = "Параметр offset должен быть числом >= 0."
                    });
                }

                offset = parsedOffset;
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
            var runSessionIdRaw = ctx.Request.Query["runSessionId"].ToString();
            long? runSessionId = null;
            if (!string.IsNullOrWhiteSpace(runSessionIdRaw))
            {
                if (!long.TryParse(runSessionIdRaw, out var parsedRunSessionId))
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "EVENTS_RUN_SESSION_ID_INVALID",
                        Message = "Параметр runSessionId должен быть числом > 0."
                    });
                }

                runSessionId = parsedRunSessionId;
            }

            var runSessionValidation = EventService.ValidateRunSessionId(runSessionId);
            if (runSessionValidation is not null)
            {
                return ErrBadRequest(runSessionValidation);
            }

            var limitRaw = ctx.Request.Query["limit"].ToString();
            var offsetRaw = ctx.Request.Query["offset"].ToString();

            var limit = 200;
            if (!string.IsNullOrWhiteSpace(limitRaw))
            {
                if (!int.TryParse(limitRaw, out var parsedLimit))
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "EVENTS_LIMIT_INVALID",
                        Message = "Параметр limit должен быть числом."
                    });
                }

                limit = Math.Clamp(parsedLimit, 1, 5000);
            }

            var offset = 0;
            if (!string.IsNullOrWhiteSpace(offsetRaw))
            {
                if (!int.TryParse(offsetRaw, out var parsedOffset) || parsedOffset < 0)
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "EVENTS_OFFSET_INVALID",
                        Message = "Параметр offset должен быть числом >= 0."
                    });
                }

                offset = parsedOffset;
            }

            var data = await events.ListAsync(db, runSessionId, limit, offset, ct);
            return Results.Ok(data);
        });

        app.MapGet("/api/events/run", async (HttpContext ctx, AppDbContext db, EventService events, CancellationToken ct) =>
        {
            var runSessionIdRaw = ctx.Request.Query["runSessionId"].ToString();
            long? runSessionId = null;
            if (!string.IsNullOrWhiteSpace(runSessionIdRaw))
            {
                if (!long.TryParse(runSessionIdRaw, out var parsedRunSessionId))
                {
                    ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
                    await ctx.Response.WriteAsJsonAsync(ErrorCatalog.Enrich(new ApiErrorDto
                    {
                        Code = "EVENTS_RUN_SESSION_ID_INVALID",
                        Message = "Параметр runSessionId должен быть числом > 0."
                    }), ct);
                    return;
                }

                runSessionId = parsedRunSessionId;
            }

            var runSessionValidation = EventService.ValidateRunSessionId(runSessionId);
            if (runSessionValidation is not null)
            {
                ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
                await ctx.Response.WriteAsJsonAsync(ErrorCatalog.Enrich(runSessionValidation), ct);
                return;
            }

            var sinceIdRaw = ctx.Request.Query["sinceId"].ToString();
            long? sinceId = null;
            if (!string.IsNullOrWhiteSpace(sinceIdRaw))
            {
                if (!long.TryParse(sinceIdRaw, out var parsedSinceId))
                {
                    ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
                    await ctx.Response.WriteAsJsonAsync(ErrorCatalog.Enrich(new ApiErrorDto
                    {
                        Code = "EVENTS_SINCE_ID_INVALID",
                        Message = "Параметр sinceId должен быть числом >= 0."
                    }), ct);
                    return;
                }

                sinceId = parsedSinceId;
            }

            var sinceIdValidation = EventService.ValidateSinceId(sinceId);
            if (sinceIdValidation is not null)
            {
                ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
                await ctx.Response.WriteAsJsonAsync(ErrorCatalog.Enrich(sinceIdValidation), ct);
                return;
            }

            var historyLimit = 100;
            var historyLimitRaw = ctx.Request.Query["historyLimit"].ToString();
            if (!string.IsNullOrWhiteSpace(historyLimitRaw))
            {
                if (!int.TryParse(historyLimitRaw, out var parsedHistoryLimit))
                {
                    ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
                    await ctx.Response.WriteAsJsonAsync(ErrorCatalog.Enrich(new ApiErrorDto
                    {
                        Code = "EVENTS_HISTORY_LIMIT_INVALID",
                        Message = "Параметр historyLimit должен быть числом."
                    }), ct);
                    return;
                }

                historyLimit = Math.Clamp(parsedHistoryLimit, 0, 1000);
            }

            var pollMs = 1000;
            var pollMsRaw = ctx.Request.Query["pollMs"].ToString();
            if (!string.IsNullOrWhiteSpace(pollMsRaw))
            {
                if (!int.TryParse(pollMsRaw, out var parsedPollMs))
                {
                    ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
                    await ctx.Response.WriteAsJsonAsync(ErrorCatalog.Enrich(new ApiErrorDto
                    {
                        Code = "EVENTS_POLL_MS_INVALID",
                        Message = "Параметр pollMs должен быть числом."
                    }), ct);
                    return;
                }

                pollMs = Math.Clamp(parsedPollMs, 250, 5000);
            }

            var heartbeatSeconds = 15;
            var heartbeatRaw = ctx.Request.Query["heartbeatSec"].ToString();
            if (!string.IsNullOrWhiteSpace(heartbeatRaw))
            {
                if (!int.TryParse(heartbeatRaw, out var parsedHeartbeat))
                {
                    ctx.Response.StatusCode = StatusCodes.Status400BadRequest;
                    await ctx.Response.WriteAsJsonAsync(ErrorCatalog.Enrich(new ApiErrorDto
                    {
                        Code = "EVENTS_HEARTBEAT_SEC_INVALID",
                        Message = "Параметр heartbeatSec должен быть числом."
                    }), ct);
                    return;
                }

                heartbeatSeconds = Math.Clamp(parsedHeartbeat, 5, 60);
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
                // клиент закрыл соединение SSE
            }
        });

        app.MapPost("/api/clients/sync", async (AppDbContext db, SettingsStore settingsStore, ClientSyncService clientSync, CancellationToken ct) =>
        {
            var settings = await settingsStore.GetAsync(db, ct);
            var credentialValidation = ClientSyncService.ValidateSyncCredentials(settings.LoginUrl, settings.Login, settings.Password);
            if (credentialValidation is not null)
            {
                return ErrBadRequest(credentialValidation);
            }

            try
            {
                var result = await clientSync.SyncLiveFromRocketmanAsync(
                    db,
                    settings.LoginUrl,
                    settings.Login,
                    settings.Password,
                    ct);

                return Results.Ok(result);
            }
            catch (FileNotFoundException ex)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "SYNC_SELECTORS_NOT_FOUND",
                    Message = ex.Message
                });
            }
            catch (InvalidOperationException ex)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "SYNC_INVALID_STATE",
                    Message = ex.Message
                });
            }
            catch (TimeoutException ex)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "ROCKETMAN_UNAVAILABLE",
                    Message = ex.Message
                });
            }
            catch (PlaywrightException ex)
            {
                if (IsPlaywrightBrowserMissing(ex))
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "SYNC_PLAYWRIGHT_NOT_INSTALLED",
                        Message = "На этом ПК не установлен Chromium для Playwright. Запустите \"Collector.exe --install-playwright\" и повторите актуализацию."
                    });
                }

                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "SYNC_PLAYWRIGHT_ERROR",
                    Message = ex.Message
                });
            }
            catch (Exception ex)
            {
                return Results.Problem(
                    statusCode: 500,
                    title: "Не удалось актуализировать базу клиентов.",
                    detail: ex.Message);
            }
        });

        app.MapGet("/api/clients/sync-status", async (AppDbContext db, ClientSyncService clientSync, CancellationToken ct) =>
        {
            var status = await clientSync.GetLatestSyncStatusAsync(db, ct);
            return Results.Ok(status);
        });

        app.MapGet("/api/clients", async (HttpContext ctx, AppDbContext db, ClientSyncService clientSync, CancellationToken ct) =>
        {
            var snapshotIdRaw = ctx.Request.Query["snapshotId"].ToString();
            var limitRaw = ctx.Request.Query["limit"].ToString();
            var offsetRaw = ctx.Request.Query["offset"].ToString();

            long? snapshotId = null;
            if (!string.IsNullOrWhiteSpace(snapshotIdRaw))
            {
                if (!long.TryParse(snapshotIdRaw, out var parsedSnapshotId) || parsedSnapshotId <= 0)
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "CLIENTS_SNAPSHOT_ID_INVALID",
                        Message = "Параметр snapshotId должен быть положительным числом."
                    });
                }

                snapshotId = parsedSnapshotId;
            }

            var limit = 1000;
            if (!string.IsNullOrWhiteSpace(limitRaw))
            {
                if (!int.TryParse(limitRaw, out var parsedLimit))
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "CLIENTS_LIMIT_INVALID",
                        Message = "Параметр limit должен быть числом."
                    });
                }

                limit = Math.Clamp(parsedLimit, 1, 5000);
            }

            var offset = 0;
            if (!string.IsNullOrWhiteSpace(offsetRaw))
            {
                if (!int.TryParse(offsetRaw, out var parsedOffset) || parsedOffset < 0)
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "CLIENTS_OFFSET_INVALID",
                        Message = "Параметр offset должен быть числом >= 0."
                    });
                }

                offset = parsedOffset;
            }

            var data = await clientSync.ListClientsAsync(db, snapshotId, limit, offset, ct);
            return Results.Ok(data);
        });

        app.MapGet("/api/clients/{externalClientId}/debt", async (
            string externalClientId,
            AppDbContext db,
            DebtCacheService debtCache,
            CancellationToken ct) =>
        {
            var validation = DebtCacheService.ValidateExternalClientId(externalClientId);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            var data = await debtCache.GetByExternalClientIdAsync(db, externalClientId, ct);
            if (data is null)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "CLIENT_NOT_FOUND",
                    Message = $"Клиент с externalClientId='{externalClientId}' не найден."
                });
            }

            return Results.Ok(data);
        });

        app.MapPost("/api/clients/{externalClientId}/debt/fetch", async (
            string externalClientId,
            AppDbContext db,
            SettingsStore settingsStore,
            DebtCacheService debtCache,
            ClientDebtFetchRequestDto payload,
            CancellationToken ct) =>
        {
            var idValidation = DebtCacheService.ValidateExternalClientId(externalClientId);
            if (idValidation is not null)
            {
                return ErrBadRequest(idValidation);
            }

            var requestValidation = DebtCacheService.ValidateFetchRequest(payload);
            if (requestValidation is not null)
            {
                return ErrBadRequest(requestValidation);
            }

            var settings = await settingsStore.GetAsync(db, ct);
            var result = await debtCache.FetchByExternalClientIdAsync(db, externalClientId, settings, payload, ct);
            if (result.Success)
            {
                return Results.Ok(result);
            }

            if (string.Equals(result.Code, "CLIENT_NOT_FOUND", StringComparison.Ordinal))
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = result.Code,
                    Message = result.Message
                });
            }

            return ErrBadRequest(new ApiErrorDto
            {
                Code = result.Code,
                Message = result.Message
            });
        });

        app.MapPost("/api/queue/preview", async (AppDbContext db, QueueService queue, QueueFilterRequestDto payload, CancellationToken ct) =>
        {
            var validation = QueueService.ValidateRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            try
            {
                var data = await queue.PreviewAsync(db, payload, ct);
                return Results.Ok(data);
            }
            catch (KeyNotFoundException ex)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "QUEUE_SNAPSHOT_NOT_FOUND",
                    Message = ex.Message
                });
            }
            catch (InvalidOperationException ex)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "QUEUE_FILTER_INVALID",
                    Message = ex.Message
                });
            }
        });

        app.MapPost("/api/queue/forecast", async (AppDbContext db, SettingsStore settingsStore, QueueService queue, QueueFilterRequestDto payload, CancellationToken ct) =>
        {
            var validation = QueueService.ValidateRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            try
            {
                var settings = await settingsStore.GetAsync(db, ct);
                var data = await queue.ForecastAsync(db, payload, settings, ct);
                return Results.Ok(data);
            }
            catch (KeyNotFoundException ex)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "QUEUE_SNAPSHOT_NOT_FOUND",
                    Message = ex.Message
                });
            }
            catch (InvalidOperationException ex)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "QUEUE_FILTER_INVALID",
                    Message = ex.Message
                });
            }
        });

        app.MapPost("/api/queue/build", async (AppDbContext db, SettingsStore settingsStore, QueueService queue, QueueFilterRequestDto payload, CancellationToken ct) =>
        {
            var validation = QueueService.ValidateRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            try
            {
                var settings = await settingsStore.GetAsync(db, ct);
                var data = await queue.BuildAsync(db, payload, settings, ct);
                return Results.Ok(data);
            }
            catch (KeyNotFoundException ex)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "QUEUE_SNAPSHOT_NOT_FOUND",
                    Message = ex.Message
                });
            }
            catch (InvalidOperationException ex)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "QUEUE_FILTER_INVALID",
                    Message = ex.Message
                });
            }
        });

        app.MapGet("/api/queue", async (HttpContext ctx, AppDbContext db, QueueService queue, CancellationToken ct) =>
        {
            var runSessionIdRaw = ctx.Request.Query["runSessionId"].ToString();
            var limitRaw = ctx.Request.Query["limit"].ToString();
            var offsetRaw = ctx.Request.Query["offset"].ToString();

            long? runSessionId = null;
            if (!string.IsNullOrWhiteSpace(runSessionIdRaw))
            {
                if (!long.TryParse(runSessionIdRaw, out var parsedSessionId) || parsedSessionId <= 0)
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "QUEUE_RUN_SESSION_ID_INVALID",
                        Message = "Параметр runSessionId должен быть положительным числом."
                    });
                }

                runSessionId = parsedSessionId;
            }

            var limit = 1000;
            if (!string.IsNullOrWhiteSpace(limitRaw))
            {
                if (!int.TryParse(limitRaw, out var parsedLimit))
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "QUEUE_LIMIT_INVALID",
                        Message = "Параметр limit должен быть числом."
                    });
                }

                limit = Math.Clamp(parsedLimit, 1, 5000);
            }

            var offset = 0;
            if (!string.IsNullOrWhiteSpace(offsetRaw))
            {
                if (!int.TryParse(offsetRaw, out var parsedOffset) || parsedOffset < 0)
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "QUEUE_OFFSET_INVALID",
                        Message = "Параметр offset должен быть числом >= 0."
                    });
                }

                offset = parsedOffset;
            }

            try
            {
                var data = await queue.ListAsync(db, runSessionId, limit, offset, ct);
                return Results.Ok(data);
            }
            catch (KeyNotFoundException ex)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "QUEUE_RUN_SESSION_NOT_FOUND",
                    Message = ex.Message
                });
            }
        });

        app.MapPost("/api/queue/{jobId:long}/preview/rebuild", async (
            long jobId,
            AppDbContext db,
            QueueService queue,
            QueueJobPreviewRequestDto? payload,
            CancellationToken ct) =>
        {
            if (jobId <= 0)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "QUEUE_JOB_ID_INVALID",
                    Message = "jobId должен быть положительным числом."
                });
            }

            var request = payload ?? new QueueJobPreviewRequestDto();
            try
            {
                var data = await queue.RebuildJobPreviewAsync(db, jobId, request, ct);
                return Results.Ok(data);
            }
            catch (KeyNotFoundException ex)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "QUEUE_JOB_NOT_FOUND",
                    Message = ex.Message
                });
            }
            catch (InvalidOperationException ex)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "QUEUE_PREVIEW_REBUILD_INVALID",
                    Message = ex.Message
                });
            }
        });

        app.MapPut("/api/queue/{jobId:long}/message-override", async (
            long jobId,
            AppDbContext db,
            QueueService queue,
            QueueJobMessageOverrideRequestDto payload,
            CancellationToken ct) =>
        {
            if (jobId <= 0)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "QUEUE_JOB_ID_INVALID",
                    Message = "jobId должен быть положительным числом."
                });
            }

            if (payload is null)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "QUEUE_MESSAGE_OVERRIDE_REQUIRED",
                    Message = "Тело запроса с текстом сообщения обязательно."
                });
            }

            try
            {
                var data = await queue.SetJobMessageOverrideAsync(db, jobId, payload, ct);
                return Results.Ok(data);
            }
            catch (KeyNotFoundException ex)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "QUEUE_JOB_NOT_FOUND",
                    Message = ex.Message
                });
            }
            catch (InvalidOperationException ex)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "QUEUE_MESSAGE_OVERRIDE_INVALID",
                    Message = ex.Message
                });
            }
        });

        app.MapDelete("/api/queue/{jobId:long}/message-override", async (
            long jobId,
            AppDbContext db,
            QueueService queue,
            CancellationToken ct) =>
        {
            if (jobId <= 0)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "QUEUE_JOB_ID_INVALID",
                    Message = "jobId должен быть положительным числом."
                });
            }

            try
            {
                var data = await queue.ClearJobMessageOverrideAsync(db, jobId, ct);
                return Results.Ok(data);
            }
            catch (KeyNotFoundException ex)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "QUEUE_JOB_NOT_FOUND",
                    Message = ex.Message
                });
            }
            catch (InvalidOperationException ex)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "QUEUE_MESSAGE_OVERRIDE_INVALID",
                    Message = ex.Message
                });
            }
        });

        app.MapPost("/api/queue/jobs/remove", async (AppDbContext db, QueueService queue, QueueRemoveJobsRequestDto payload, CancellationToken ct) =>
        {
            var validation = QueueService.ValidateRemoveJobsRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            try
            {
                var data = await queue.RemoveJobsFromPlanAsync(db, payload, ct);
                return Results.Ok(data);
            }
            catch (KeyNotFoundException ex)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "QUEUE_RUN_SESSION_NOT_FOUND",
                    Message = ex.Message
                });
            }
            catch (InvalidOperationException ex)
            {
                return ErrConflict(new ApiErrorDto
                {
                    Code = "QUEUE_REMOVE_INVALID_STATE",
                    Message = ex.Message
                });
            }
        });

        app.MapPost("/api/queue/retry-errors", async (AppDbContext db, QueueService queue, QueueRetryErrorsRequestDto? payload, CancellationToken ct) =>
        {
            var request = payload ?? new QueueRetryErrorsRequestDto();
            var validation = QueueService.ValidateRetryErrorsRequest(request);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            try
            {
                var data = await queue.RetryErrorsAsync(db, request, ct);
                return Results.Ok(data);
            }
            catch (KeyNotFoundException ex)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "QUEUE_RUN_SESSION_NOT_FOUND",
                    Message = ex.Message
                });
            }
            catch (InvalidOperationException ex)
            {
                return ErrConflict(new ApiErrorDto
                {
                    Code = "QUEUE_RETRY_ERRORS_INVALID_STATE",
                    Message = ex.Message
                });
            }
        });

        app.MapPost("/api/queue/bulk/set-template", async (AppDbContext db, QueueService queue, QueueBulkSetTemplateRequest payload, CancellationToken ct) =>
        {
            var validation = QueueService.ValidateBulkSetTemplateRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            try
            {
                var data = await queue.BulkSetTemplateAsync(db, payload, ct);
                return Results.Ok(data);
            }
            catch (KeyNotFoundException ex)
            {
                return ErrNotFound(new ApiErrorDto
                {
                    Code = "QUEUE_BULK_TEMPLATE_NOT_FOUND",
                    Message = ex.Message
                });
            }
            catch (InvalidOperationException ex)
            {
                return ErrBadRequest(new ApiErrorDto
                {
                    Code = "QUEUE_BULK_TEMPLATE_INVALID",
                    Message = ex.Message
                });
            }
        });

        app.MapGet("/api/run/status", async (HttpContext ctx, AppDbContext db, RunService run, CancellationToken ct) =>
        {
            var runSessionIdRaw = ctx.Request.Query["runSessionId"].ToString();
            long? runSessionId = null;
            if (!string.IsNullOrWhiteSpace(runSessionIdRaw))
            {
                if (!long.TryParse(runSessionIdRaw, out var parsedSessionId) || parsedSessionId <= 0)
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "RUN_SESSION_ID_INVALID",
                        Message = "Параметр runSessionId должен быть положительным числом."
                    });
                }

                runSessionId = parsedSessionId;
            }

            try
            {
                var data = await run.GetStatusAsync(db, runSessionId, ct);
                return Results.Ok(data);
            }
            catch (RunStateException ex)
            {
                return Results.Json(ErrorCatalog.Enrich(new ApiErrorDto
                {
                    Code = ex.Code,
                    Message = ex.Message
                }), statusCode: ex.HttpStatusCode);
            }
        });

        app.MapGet("/api/run/history", async (HttpContext ctx, AppDbContext db, RunService run, CancellationToken ct) =>
        {
            var limitRaw = ctx.Request.Query["limit"].ToString();
            var offsetRaw = ctx.Request.Query["offset"].ToString();

            var limit = 50;
            if (!string.IsNullOrWhiteSpace(limitRaw))
            {
                if (!int.TryParse(limitRaw, out var parsedLimit))
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "RUN_HISTORY_LIMIT_INVALID",
                        Message = "Параметр limit должен быть числом."
                    });
                }

                limit = Math.Clamp(parsedLimit, 1, 500);
            }

            var offset = 0;
            if (!string.IsNullOrWhiteSpace(offsetRaw))
            {
                if (!int.TryParse(offsetRaw, out var parsedOffset) || parsedOffset < 0)
                {
                    return ErrBadRequest(new ApiErrorDto
                    {
                        Code = "RUN_HISTORY_OFFSET_INVALID",
                        Message = "Параметр offset должен быть числом >= 0."
                    });
                }

                offset = parsedOffset;
            }

            var data = await run.ListHistoryAsync(db, limit, offset, ct);
            return Results.Ok(data);
        });

        app.MapDelete("/api/run/history", async (AppDbContext db, RunService run, CancellationToken ct) =>
        {
            var data = await run.ClearHistoryAsync(db, ct);
            return Results.Ok(data);
        });

        app.MapPost("/api/run/start", async (AppDbContext db, RunService run, RunStartRequestDto? payload, CancellationToken ct) =>
        {
            var validation = RunService.ValidateStartRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            try
            {
                var data = await run.StartAsync(db, payload ?? new RunStartRequestDto(), ct);
                return Results.Ok(data);
            }
            catch (RunStateException ex)
            {
                return Results.Json(ErrorCatalog.Enrich(new ApiErrorDto
                {
                    Code = ex.Code,
                    Message = ex.Message
                }), statusCode: ex.HttpStatusCode);
            }
        });

        app.MapPost("/api/run/stop", async (AppDbContext db, RunService run, RunStopRequestDto? payload, CancellationToken ct) =>
        {
            var validation = RunService.ValidateStopRequest(payload);
            if (validation is not null)
            {
                return ErrBadRequest(validation);
            }

            try
            {
                var data = await run.StopAsync(db, payload ?? new RunStopRequestDto(), ct);
                return Results.Ok(data);
            }
            catch (RunStateException ex)
            {
                return Results.Json(ErrorCatalog.Enrich(new ApiErrorDto
                {
                    Code = ex.Code,
                    Message = ex.Message
                }), statusCode: ex.HttpStatusCode);
            }
        });

        app.MapPost("/api/app/shutdown", async (AppDbContext db, RunService run, IHostApplicationLifetime lifetime, CancellationToken ct) =>
        {
            try
            {
                var status = await run.GetStatusAsync(db, null, ct);
                if (status.HasRunningSession && status.RunningSessionId > 0)
                {
                    try
                    {
                        await run.StopAsync(db, new RunStopRequestDto
                        {
                            RunSessionId = status.RunningSessionId,
                            Reason = "Остановлено при завершении приложения."
                        }, ct);
                    }
                    catch
                    {
                        // ignore stop errors during shutdown
                    }
                }
            }
            catch
            {
                // ignore status errors during shutdown
            }

            _ = Task.Run(async () =>
            {
                await Task.Delay(300);
                lifetime.StopApplication();
            });

            return Results.Ok(new
            {
                action = "shutdown",
                message = "Приложение завершает работу."
            });
        });

        var localUiUrl = $"http://127.0.0.1:{port}/";
        var hostLifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();
        hostLifetime.ApplicationStarted.Register(() =>
        {
            Console.WriteLine($"Backend API started on {url}");
            Console.WriteLine($"Local UI: {localUiUrl}");
            Console.WriteLine($"DB path: {resolvedDbPath}");
            Console.WriteLine("Available endpoints:");
            Console.WriteLine("  GET /health");
            Console.WriteLine("  GET /api/errors/catalog");
            Console.WriteLine("  GET /api/audit");
            Console.WriteLine("  GET/PUT /api/settings");
            Console.WriteLine("  GET/PUT /api/settings/comment-rules");
            Console.WriteLine("  GET /api/reports/weekly");
            Console.WriteLine("  POST /api/rocketman/comment/test");
            Console.WriteLine("  GET/POST /api/channels");
            Console.WriteLine("  PUT /api/channels/{id}");
            Console.WriteLine("  DELETE /api/channels/{id}");
            Console.WriteLine("  PATCH /api/channels/{id}/status");
            Console.WriteLine("  POST /api/channels/check");
            Console.WriteLine("  POST /api/channels/{id}/check");
            Console.WriteLine("  GET /api/templates/meta");
            Console.WriteLine("  GET /api/templates");
            Console.WriteLine("  GET /api/templates/active");
            Console.WriteLine("  GET /api/templates/{id}");
            Console.WriteLine("  POST /api/templates");
            Console.WriteLine("  PUT /api/templates/{id}");
            Console.WriteLine("  PATCH /api/templates/{id}/status");
            Console.WriteLine("  GET /api/manual-presets");
            Console.WriteLine("  GET /api/manual-presets/{id}");
            Console.WriteLine("  POST /api/manual-presets");
            Console.WriteLine("  PUT /api/manual-presets/{id}");
            Console.WriteLine("  DELETE /api/manual-presets/{id}");
            Console.WriteLine("  GET /api/dialogs");
            Console.WriteLine("  GET /api/dialogs/by-client/{externalClientId}");
            Console.WriteLine("  GET /api/dialogs/by-phone/{phone}/messages");
            Console.WriteLine("  GET /api/dialogs/by-phone/{phone}/draft");
            Console.WriteLine("  PUT /api/dialogs/by-phone/{phone}/draft");
            Console.WriteLine("  DELETE /api/dialogs/by-phone/{phone}/draft");
            Console.WriteLine("  POST /api/dialogs/by-phone/{phone}/send");
            Console.WriteLine("  DELETE /api/dialogs/by-phone/{phone}");
            Console.WriteLine("  POST /api/dialogs/prune");
            Console.WriteLine("  GET /api/stop-list");
            Console.WriteLine("  GET /api/stop-list/by-phone/{phone}");
            Console.WriteLine("  GET /api/stop-list/{id}");
            Console.WriteLine("  POST /api/stop-list");
            Console.WriteLine("  PUT /api/stop-list/{id}");
            Console.WriteLine("  DELETE /api/stop-list/{id}");
            Console.WriteLine("  DELETE /api/stop-list/by-phone/{phone}");
            Console.WriteLine("  POST /api/stop-list/bulk/add");
            Console.WriteLine("  POST /api/stop-list/bulk/remove");
            Console.WriteLine("  POST /api/stop-list/bulk/deactivate");
            Console.WriteLine("  GET /api/alerts");
            Console.WriteLine("  PATCH /api/alerts/{id}/status");
            Console.WriteLine("  GET /api/events");
            Console.WriteLine("  GET /api/events/run");
            Console.WriteLine("  POST /api/clients/sync");
            Console.WriteLine("  GET /api/clients/sync-status");
            Console.WriteLine("  GET /api/clients");
            Console.WriteLine("  GET /api/clients/{externalClientId}/debt");
            Console.WriteLine("  POST /api/clients/{externalClientId}/debt/fetch");
            Console.WriteLine("  POST /api/queue/preview");
            Console.WriteLine("  POST /api/queue/forecast");
            Console.WriteLine("  POST /api/queue/build");
            Console.WriteLine("  GET /api/queue");
            Console.WriteLine("  POST /api/queue/{jobId}/preview/rebuild");
            Console.WriteLine("  PUT /api/queue/{jobId}/message-override");
            Console.WriteLine("  DELETE /api/queue/{jobId}/message-override");
            Console.WriteLine("  POST /api/queue/jobs/remove");
            Console.WriteLine("  POST /api/queue/retry-errors");
            Console.WriteLine("  POST /api/queue/bulk/set-template");
            Console.WriteLine("  GET /api/run/status");
            Console.WriteLine("  GET /api/run/history");
            Console.WriteLine("  DELETE /api/run/history");
            Console.WriteLine("  POST /api/run/start");
            Console.WriteLine("  POST /api/run/stop");
            Console.WriteLine("  POST /api/app/shutdown");
        });
        await app.RunAsync();
    }

    private static async Task WriteSseEventAsync(
        HttpResponse response,
        string eventName,
        long id,
        EventLogItemDto payload,
        CancellationToken cancellationToken)
    {
        var json = JsonSerializer.Serialize(payload);
        await response.WriteAsync($"id: {id}\n", cancellationToken);
        await response.WriteAsync($"event: {eventName}\n", cancellationToken);
        await response.WriteAsync($"data: {json}\n\n", cancellationToken);
        await response.Body.FlushAsync(cancellationToken);
    }

    private static ApiErrorDto? ValidateSettings(AppSettingsDto payload)
    {
        if (payload.Gap <= 0)
        {
            return new ApiErrorDto
            {
                Code = "CFG_INVALID_GAP",
                Message = "Интервал между SMS должен быть больше 0."
            };
        }

        if (payload.RecentSmsCooldownDays < 0 || payload.RecentSmsCooldownDays > 365)
        {
            return new ApiErrorDto
            {
                Code = "CFG_INVALID_RECENT_SMS_COOLDOWN_DAYS",
                Message = "Интервал запрета повторной отправки должен быть в диапазоне от 0 до 365 дней."
            };
        }

        if (!TryParseHm(payload.WorkWindowStart, out var start))
        {
            return new ApiErrorDto
            {
                Code = "CFG_INVALID_WORK_WINDOW_START",
                Message = "Начало рабочего окна должно быть в формате HH:mm."
            };
        }

        if (!TryParseHm(payload.WorkWindowEnd, out var end))
        {
            return new ApiErrorDto
            {
                Code = "CFG_INVALID_WORK_WINDOW_END",
                Message = "Окончание рабочего окна должно быть в формате HH:mm."
            };
        }

        if (end <= start)
        {
            return new ApiErrorDto
            {
                Code = "CFG_INVALID_WORK_WINDOW_RANGE",
                Message = "Окончание рабочего окна должно быть позже начала."
            };
        }

        if (payload.TemplateRuleTypes is null || payload.TemplateRuleTypes.Count == 0)
        {
            return new ApiErrorDto
            {
                Code = "CFG_TEMPLATE_RULE_TYPES_REQUIRED",
                Message = "Нужно передать хотя бы один тип шаблона."
            };
        }

        var usedIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var item in payload.TemplateRuleTypes)
        {
            if (item is null)
            {
                return new ApiErrorDto
                {
                    Code = "CFG_TEMPLATE_RULE_TYPE_INVALID",
                    Message = "Тип шаблона не может быть пустым."
                };
            }

            var id = (item.Id ?? string.Empty).Trim();
            if (string.IsNullOrWhiteSpace(id))
            {
                return new ApiErrorDto
                {
                    Code = "CFG_TEMPLATE_RULE_TYPE_ID_REQUIRED",
                    Message = "У каждого типа шаблона должен быть id."
                };
            }

            if (!usedIds.Add(id))
            {
                return new ApiErrorDto
                {
                    Code = "CFG_TEMPLATE_RULE_TYPE_ID_DUPLICATE",
                    Message = $"Найден дубликат id типа шаблона: {id}."
                };
            }

            if (string.IsNullOrWhiteSpace(item.Name))
            {
                return new ApiErrorDto
                {
                    Code = "CFG_TEMPLATE_RULE_TYPE_NAME_REQUIRED",
                    Message = $"Для типа «{id}» не задано название."
                };
            }

            var manualOnly = item.AutoAssign == false;
            if (manualOnly)
            {
                continue;
            }

            var modeRaw = (item.OverdueMode ?? string.Empty).Trim().ToLowerInvariant();
            var mode = string.IsNullOrWhiteSpace(modeRaw)
                ? TemplateService.OverdueModeRange
                : modeRaw;
            if (mode != TemplateService.OverdueModeRange && mode != TemplateService.OverdueModeExact)
            {
                return new ApiErrorDto
                {
                    Code = "CFG_TEMPLATE_RULE_TYPE_MODE_INVALID",
                    Message = $"Для типа «{item.Name}» режим должен быть '{TemplateService.OverdueModeRange}' или '{TemplateService.OverdueModeExact}'."
                };
            }
            if (mode == TemplateService.OverdueModeExact)
            {
                if (!item.OverdueExactDay.HasValue || item.OverdueExactDay.Value < 0)
                {
                    return new ApiErrorDto
                    {
                        Code = "CFG_TEMPLATE_RULE_TYPE_EXACT_INVALID",
                        Message = $"Для типа «{item.Name}» в режиме exact нужно указать день >= 0."
                    };
                }

                continue;
            }

            if (!item.OverdueFromDays.HasValue || !item.OverdueToDays.HasValue)
            {
                return new ApiErrorDto
                {
                    Code = "CFG_TEMPLATE_RULE_TYPE_RANGE_REQUIRED",
                    Message = $"Для типа «{item.Name}» в режиме range нужно указать «от» и «до»."
                };
            }

            if (item.OverdueFromDays.Value < 0 || item.OverdueToDays.Value < 0)
            {
                return new ApiErrorDto
                {
                    Code = "CFG_TEMPLATE_RULE_TYPE_RANGE_INVALID",
                    Message = $"Для типа «{item.Name}» диапазон должен быть >= 0."
                };
            }

            if (item.OverdueToDays.Value < item.OverdueFromDays.Value)
            {
                return new ApiErrorDto
                {
                    Code = "CFG_TEMPLATE_RULE_TYPE_RANGE_ORDER_INVALID",
                    Message = $"Для типа «{item.Name}» значение «до» не может быть меньше «от»."
                };
            }
        }

        return null;
    }

    private static bool TryParseHm(string? value, out TimeOnly time)
    {
        return TimeOnly.TryParseExact(
            (value ?? string.Empty).Trim(),
            "HH:mm",
            CultureInfo.InvariantCulture,
            DateTimeStyles.None,
            out time);
    }

    private static string GetArg(string[] args, string key)
    {
        var idx = Array.IndexOf(args, key);
        if (idx >= 0 && idx + 1 < args.Length) return args[idx + 1];
        return string.Empty;
    }

    private static bool HasFlag(string[] args, string key)
    {
        return args.Contains(key, StringComparer.Ordinal);
    }

    private static bool IsPlaywrightBrowserMissing(PlaywrightException ex)
    {
        var message = ex.Message ?? string.Empty;
        return message.Contains("Executable doesn't exist", StringComparison.OrdinalIgnoreCase) ||
               message.Contains("Please run the following command", StringComparison.OrdinalIgnoreCase) ||
               message.Contains("ms-playwright", StringComparison.OrdinalIgnoreCase);
    }

    private static IResult ErrBadRequest(ApiErrorDto? dto) =>
        dto == null ? Results.BadRequest() : Results.BadRequest(ErrorCatalog.Enrich(dto));

    private static IResult ErrNotFound(ApiErrorDto dto) =>
        Results.NotFound(ErrorCatalog.Enrich(dto));

    private static IResult ErrConflict(ApiErrorDto dto) =>
        Results.Conflict(ErrorCatalog.Enrich(dto));
}
