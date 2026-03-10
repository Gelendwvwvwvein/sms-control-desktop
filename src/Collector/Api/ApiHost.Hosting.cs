using Collector.Data;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Collector.Api;

public static partial class ApiHost
{
    private static void ConfigureServices(IServiceCollection services, string resolvedDbPath)
    {
        services.AddCors(options =>
        {
            options.AddPolicy("LocalUi", policy =>
            {
                policy
                    .AllowAnyOrigin()
                    .AllowAnyHeader()
                    .AllowAnyMethod();
            });
        });

        services.AddDbContext<AppDbContext>(options =>
            options.UseSqlite($"Data Source={resolvedDbPath}"));
        services.AddSingleton<SettingsStore>();
        services.AddSingleton<TemplateService>();
        services.AddSingleton<RuleEngineService>();
        services.AddSingleton<RunLifecycleCoordinator>();
        services.AddSingleton<RunCancellationCoordinator>();
        services.AddSingleton<ManualPresetService>();
        services.AddSingleton<DialogService>();
        services.AddSingleton<StopListService>();
        services.AddSingleton<ClientSyncService>();
        services.AddSingleton<DebtCacheService>();
        services.AddSingleton<QueueService>();
        services.AddSingleton<RunService>();
        services.AddSingleton<AlertService>();
        services.AddSingleton<EventService>();
        services.AddSingleton<AuditService>();
        services.AddSingleton<ReportsService>();
        services.AddSingleton<RocketmanCommentService>();
        services.AddSingleton<RunDispatchService>();
        services.AddHostedService<RunDispatchBackgroundWorker>();
        services.AddHttpClient<ChannelService>();
    }

    private static void ConfigureCommonMiddleware(WebApplication app)
    {
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
            }
        });

        app.MapGet("/health", () => Results.Ok(new { status = "ok" }));
    }

    private static void RegisterStartupLogging(WebApplication app, string url, string localUiUrl, string resolvedDbPath)
    {
        var hostLifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();
        hostLifetime.ApplicationStarted.Register(() =>
        {
            Console.WriteLine($"Backend API started on {url}");
            Console.WriteLine($"Local UI: {localUiUrl}");
            Console.WriteLine($"DB path: {resolvedDbPath}");
            Console.WriteLine("Available endpoints:");
            foreach (var line in StartupEndpointDescriptions)
            {
                Console.WriteLine(line);
            }
        });
    }

    private static readonly string[] StartupEndpointDescriptions =
    [
        "  GET /health",
        "  GET /api/errors/catalog",
        "  GET /api/audit",
        "  GET/PUT /api/settings",
        "  GET/PUT /api/settings/comment-rules",
        "  GET /api/reports/weekly",
        "  POST /api/rocketman/comment/test",
        "  GET/POST /api/channels",
        "  PUT /api/channels/{id}",
        "  DELETE /api/channels/{id}",
        "  PATCH /api/channels/{id}/status",
        "  POST /api/channels/check",
        "  POST /api/channels/{id}/check",
        "  GET /api/templates/meta",
        "  GET /api/templates",
        "  GET /api/templates/active",
        "  GET /api/templates/{id}",
        "  POST /api/templates",
        "  PUT /api/templates/{id}",
        "  PATCH /api/templates/{id}/status",
        "  GET /api/manual-presets",
        "  GET /api/manual-presets/{id}",
        "  POST /api/manual-presets",
        "  PUT /api/manual-presets/{id}",
        "  DELETE /api/manual-presets/{id}",
        "  GET /api/dialogs",
        "  GET /api/dialogs/by-client/{externalClientId}",
        "  GET /api/dialogs/by-phone/{phone}/messages",
        "  GET /api/dialogs/by-phone/{phone}/draft",
        "  PUT /api/dialogs/by-phone/{phone}/draft",
        "  DELETE /api/dialogs/by-phone/{phone}/draft",
        "  POST /api/dialogs/by-phone/{phone}/send",
        "  DELETE /api/dialogs/by-phone/{phone}",
        "  POST /api/dialogs/prune",
        "  GET /api/stop-list",
        "  GET /api/stop-list/by-phone/{phone}",
        "  GET /api/stop-list/{id}",
        "  POST /api/stop-list",
        "  PUT /api/stop-list/{id}",
        "  DELETE /api/stop-list/{id}",
        "  DELETE /api/stop-list/by-phone/{phone}",
        "  POST /api/stop-list/bulk/add",
        "  POST /api/stop-list/bulk/remove",
        "  POST /api/stop-list/bulk/deactivate",
        "  GET /api/alerts",
        "  PATCH /api/alerts/{id}/status",
        "  GET /api/events",
        "  GET /api/events/run",
        "  POST /api/clients/sync",
        "  GET /api/clients/sync-status",
        "  GET /api/clients",
        "  GET /api/clients/{externalClientId}/debt",
        "  POST /api/clients/{externalClientId}/debt/fetch",
        "  POST /api/queue/preview",
        "  POST /api/queue/forecast",
        "  POST /api/queue/build",
        "  GET /api/queue",
        "  POST /api/queue/{jobId}/preview/rebuild",
        "  PUT /api/queue/{jobId}/message-override",
        "  DELETE /api/queue/{jobId}/message-override",
        "  POST /api/queue/jobs/remove",
        "  POST /api/queue/retry-errors",
        "  POST /api/queue/bulk/set-template",
        "  GET /api/run/status",
        "  GET /api/run/history",
        "  DELETE /api/run/history",
        "  POST /api/run/start",
        "  POST /api/run/stop",
        "  POST /api/app/shutdown"
    ];
}
