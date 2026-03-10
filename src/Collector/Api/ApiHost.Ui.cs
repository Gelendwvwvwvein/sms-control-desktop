using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace Collector.Api;

public static partial class ApiHost
{
    private static void MapUiEndpoints(WebApplication app)
    {
        app.MapGet("/", (HttpContext ctx) => UiAsset(ctx, "/"));
        app.MapGet("/index.html", (HttpContext ctx) => UiAsset(ctx, "/index.html"));
        app.MapGet("/app.shared.js", (HttpContext ctx) => UiAsset(ctx, "/app.shared.js"));
        app.MapGet("/app.helpers.js", (HttpContext ctx) => UiAsset(ctx, "/app.helpers.js"));
        app.MapGet("/app.actions.js", (HttpContext ctx) => UiAsset(ctx, "/app.actions.js"));
        app.MapGet("/app.render.js", (HttpContext ctx) => UiAsset(ctx, "/app.render.js"));
        app.MapGet("/app.js", (HttpContext ctx) => UiAsset(ctx, "/app.js"));
        app.MapGet("/styles.css", (HttpContext ctx) => UiAsset(ctx, "/styles.css"));
        app.MapGet("/uikit.js", (HttpContext ctx) => UiAsset(ctx, "/uikit.js"));
        app.MapGet("/favicon.svg", (HttpContext ctx) => UiAsset(ctx, "/favicon.svg"));
        app.MapGet("/favicon.ico", (HttpContext ctx) => UiAsset(ctx, "/favicon.ico"));
    }

    private static void ApplyUiNoCacheHeaders(HttpContext ctx)
    {
        ctx.Response.Headers.CacheControl = "no-store, no-cache, must-revalidate";
        ctx.Response.Headers.Pragma = "no-cache";
        ctx.Response.Headers.Expires = "0";
    }

    private static IResult UiAsset(HttpContext ctx, string path)
    {
        if (!WebUiAssets.TryGet(path, out var data, out var contentType))
        {
            return Results.NotFound();
        }

        ApplyUiNoCacheHeaders(ctx);
        return Results.File(data, contentType);
    }
}
