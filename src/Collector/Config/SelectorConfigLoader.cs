using System.Reflection;
using System.Text.Json;

namespace Collector.Config;

public static class SelectorConfigLoader
{
    private const string EmbeddedResourceName = "Collector.Config.rocketman.selectors.json";

    public static async Task<SelectorConfig> LoadAsync(string? explicitPath, CancellationToken cancellationToken = default)
    {
        if (!string.IsNullOrWhiteSpace(explicitPath))
        {
            var json = await File.ReadAllTextAsync(explicitPath.Trim(), cancellationToken);
            var configFromFile = JsonSerializer.Deserialize<SelectorConfig>(json);
            if (configFromFile is not null)
            {
                return configFromFile;
            }

            throw new InvalidOperationException("Не удалось прочитать конфиг селекторов Rocketman из указанного пути.");
        }

        var assembly = Assembly.GetExecutingAssembly();
        await using var stream = assembly.GetManifestResourceStream(EmbeddedResourceName);
        if (stream is null)
        {
            throw new InvalidOperationException(
                $"Не найден встроенный ресурс селекторов: {EmbeddedResourceName}.");
        }

        using var reader = new StreamReader(stream);
        var embeddedJson = await reader.ReadToEndAsync(cancellationToken);
        var config = JsonSerializer.Deserialize<SelectorConfig>(embeddedJson);
        if (config is null)
        {
            throw new InvalidOperationException(
                "Не удалось прочитать встроенный конфиг селекторов Rocketman.");
        }

        return config;
    }
}
