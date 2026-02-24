using System.Reflection;

namespace Collector.Api;

public static class WebUiAssets
{
    private static readonly Dictionary<string, (string ResourceName, string ContentType)> AssetMap =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["/"] = ("Collector.WebUi.index.html", "text/html; charset=utf-8"),
            ["/index.html"] = ("Collector.WebUi.index.html", "text/html; charset=utf-8"),
            ["/app.js"] = ("Collector.WebUi.app.js", "application/javascript; charset=utf-8"),
            ["/styles.css"] = ("Collector.WebUi.styles.css", "text/css; charset=utf-8"),
            ["/uikit.js"] = ("Collector.WebUi.uikit.js", "application/javascript; charset=utf-8"),
            ["/favicon.svg"] = ("Collector.WebUi.favicon.svg", "image/svg+xml"),
            ["/favicon.ico"] = ("Collector.WebUi.favicon.svg", "image/svg+xml")
        };

    private static readonly Dictionary<string, byte[]> Cache = new(StringComparer.OrdinalIgnoreCase);
    private static readonly object Sync = new();

    public static bool TryGet(string path, out byte[] data, out string contentType)
    {
        if (!AssetMap.TryGetValue(path, out var asset))
        {
            data = Array.Empty<byte>();
            contentType = "application/octet-stream";
            return false;
        }

        lock (Sync)
        {
            if (Cache.TryGetValue(asset.ResourceName, out var cached))
            {
                data = cached;
                contentType = asset.ContentType;
                return true;
            }

            var assembly = Assembly.GetExecutingAssembly();
            using var stream = assembly.GetManifestResourceStream(asset.ResourceName);
            if (stream is null)
            {
                data = Array.Empty<byte>();
                contentType = asset.ContentType;
                return false;
            }

            using var ms = new MemoryStream();
            stream.CopyTo(ms);
            cached = ms.ToArray();
            Cache[asset.ResourceName] = cached;
            data = cached;
            contentType = asset.ContentType;
            return true;
        }
    }
}
