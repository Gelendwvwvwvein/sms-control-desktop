using Collector.Data;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Playwright;
using System.Globalization;
using System.Text.Json;

namespace Collector.Api;

public static partial class ApiHost
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

        ConfigureServices(builder.Services, resolvedDbPath);

        var app = builder.Build();
        ConfigureCommonMiddleware(app);
        MapUiEndpoints(app);

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
            if (!TryReadClampedIntQuery(ctx.Request, "limit", 200, 1, 5000, "AUDIT_LIMIT_INVALID", "Параметр limit должен быть числом.", out var limit, out var parseError))
            {
                return parseError!;
            }

            if (!TryReadNonNegativeIntQuery(ctx.Request, "offset", 0, "AUDIT_OFFSET_INVALID", "Параметр offset должен быть числом >= 0.", out var offset, out parseError))
            {
                return parseError!;
            }

            var data = await audit.ListAsync(db, category, limit, offset, ct);
            return Results.Ok(data);
        });

        MapConfigurationEndpoints(app);
        MapOperationsEndpoints(app);
        MapWorkflowEndpoints(app);

        var localUiUrl = $"http://127.0.0.1:{port}/";
        RegisterStartupLogging(app, url, localUiUrl, resolvedDbPath);
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

        if (payload.DebtBufferAmount < 0 || payload.DebtBufferAmount > 1_000_000)
        {
            return new ApiErrorDto
            {
                Code = "CFG_INVALID_DEBT_BUFFER_AMOUNT",
                Message = "Надбавка к сумме долга должна быть в диапазоне от 0 до 1 000 000 руб."
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

    private static bool ShouldRebuildPreviewsForSettingsChange(AppSettingsDto? before, AppSettingsDto? after)
    {
        if (after is null)
        {
            return false;
        }

        if (before is null)
        {
            return true;
        }

        if (before.DebtBufferAmount != after.DebtBufferAmount)
        {
            return true;
        }

        return !AreEquivalentPreviewTemplateRuleTypes(before.TemplateRuleTypes, after.TemplateRuleTypes);
    }

    private static bool AreEquivalentPreviewTemplateRuleTypes(
        IReadOnlyList<TemplateRuleTypeDto>? before,
        IReadOnlyList<TemplateRuleTypeDto>? after)
    {
        var beforeMap = (before ?? [])
            .Where(x => x is not null && !string.IsNullOrWhiteSpace(x.Id))
            .ToDictionary(x => x.Id, StringComparer.OrdinalIgnoreCase);
        var afterMap = (after ?? [])
            .Where(x => x is not null && !string.IsNullOrWhiteSpace(x.Id))
            .ToDictionary(x => x.Id, StringComparer.OrdinalIgnoreCase);

        if (beforeMap.Count != afterMap.Count)
        {
            return false;
        }

        foreach (var (id, beforeItem) in beforeMap)
        {
            if (!afterMap.TryGetValue(id, out var afterItem))
            {
                return false;
            }

            if (!string.Equals(beforeItem.OverdueMode, afterItem.OverdueMode, StringComparison.Ordinal) ||
                beforeItem.AutoAssign != afterItem.AutoAssign ||
                beforeItem.OverdueFromDays != afterItem.OverdueFromDays ||
                beforeItem.OverdueToDays != afterItem.OverdueToDays ||
                beforeItem.OverdueExactDay != afterItem.OverdueExactDay)
            {
                return false;
            }
        }

        return true;
    }

    private static bool ShouldRebuildPreviewsForTemplateChange(TemplateDto? before, TemplateDto? after)
    {
        if (after is null)
        {
            return false;
        }

        var beforeActive = IsActiveTemplateStatus(before?.Status);
        var afterActive = IsActiveTemplateStatus(after.Status);
        if (!beforeActive && !afterActive)
        {
            return false;
        }

        if (before is null)
        {
            return afterActive;
        }

        if (beforeActive != afterActive)
        {
            return true;
        }

        return !string.Equals(before.Text, after.Text, StringComparison.Ordinal) ||
               !string.Equals(before.Name, after.Name, StringComparison.Ordinal) ||
               !string.Equals(before.Kind, after.Kind, StringComparison.Ordinal) ||
               !string.Equals(before.OverdueMode, after.OverdueMode, StringComparison.Ordinal) ||
               before.AutoAssign != after.AutoAssign ||
               before.OverdueFromDays != after.OverdueFromDays ||
               before.OverdueToDays != after.OverdueToDays ||
               before.OverdueExactDay != after.OverdueExactDay;
    }

    private static bool ShouldIncludeFallbackPreviewRebuildForTemplateChange(TemplateDto? before, TemplateDto? after)
    {
        if (after is null)
        {
            return false;
        }

        var beforeActive = IsActiveTemplateStatus(before?.Status);
        var afterActive = IsActiveTemplateStatus(after.Status);
        if (before is null)
        {
            return afterActive;
        }

        if (beforeActive != afterActive)
        {
            return true;
        }

        if (!afterActive)
        {
            return false;
        }

        return !string.Equals(before.Kind, after.Kind, StringComparison.Ordinal) ||
               !string.Equals(before.OverdueMode, after.OverdueMode, StringComparison.Ordinal) ||
               before.AutoAssign != after.AutoAssign ||
               before.OverdueFromDays != after.OverdueFromDays ||
               before.OverdueToDays != after.OverdueToDays ||
               before.OverdueExactDay != after.OverdueExactDay;
    }

    private static async Task RebuildTemplatePreviewsIfNeededAsync(
        AppDbContext db,
        SettingsStore settingsStore,
        QueueService queue,
        TemplateDto? before,
        TemplateDto? after,
        CancellationToken cancellationToken)
    {
        if (!ShouldRebuildPreviewsForTemplateChange(before, after) || after is null)
        {
            return;
        }

        var settings = await settingsStore.GetAsync(db, cancellationToken);
        await queue.RebuildPersistedPreviewsForTemplateChangeAsync(
            db,
            settings,
            after.Id,
            ShouldIncludeFallbackPreviewRebuildForTemplateChange(before, after),
            cancellationToken);
    }

    private static bool IsActiveTemplateStatus(string? status)
    {
        return string.Equals((status ?? string.Empty).Trim(), "active", StringComparison.OrdinalIgnoreCase);
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
