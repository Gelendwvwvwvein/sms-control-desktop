namespace Collector.Models;

public sealed class CollectorOptions
{
    public bool Headless { get; init; } = true;
    public int TimeoutMs { get; init; } = 0;
    public int Parallelism { get; init; } = 3;
    public string OutputPath { get; init; } = "out/clients.json";
    public string SelectorsPath { get; init; } = "src/Collector/Config/rocketman.selectors.json";
    public string LoginUrl { get; init; } = "";
    public bool Debug { get; init; }
    public string DebugLogPath { get; init; } = "out/debug.log";
}
