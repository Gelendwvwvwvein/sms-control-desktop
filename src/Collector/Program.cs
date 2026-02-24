using System.Text.Json;
using System.Text.Encodings.Web;
using Collector.Api;
using Collector.Config;
using Collector.Data;
using Collector.Models;
using Collector.Services;
using Microsoft.Playwright;

static string GetArg(string[] args, string key)
{
    var idx = Array.IndexOf(args, key);
    if (idx >= 0 && idx + 1 < args.Length) return args[idx + 1];
    return string.Empty;
}

static bool HasFlag(string[] args, string key) => args.Contains(key);

static string ResolveSelectorsPath(string path)
{
    if (string.IsNullOrWhiteSpace(path)) return path;
    if (Path.IsPathRooted(path)) return path;

    var candidates = new List<string>();
    var cwd = Directory.GetCurrentDirectory();
    candidates.Add(Path.GetFullPath(Path.Combine(cwd, path)));
    candidates.Add(Path.GetFullPath(Path.Combine(cwd, "Config", "rocketman.selectors.json")));

    var dir = cwd;
    for (var i = 0; i < 6; i++)
    {
        candidates.Add(Path.GetFullPath(Path.Combine(dir, path)));
        var parent = Directory.GetParent(dir);
        if (parent == null) break;
        dir = parent.FullName;
    }

    var baseDir = AppContext.BaseDirectory;
    candidates.Add(Path.GetFullPath(Path.Combine(baseDir, "..", "..", "..", "..", path)));

    foreach (var candidate in candidates.Distinct())
    {
        if (File.Exists(candidate)) return candidate;
    }

    return Path.GetFullPath(Path.Combine(cwd, path));
}

var argsList = args;

var phone = GetArg(argsList, "--phone");
var password = GetArg(argsList, "--password");
var loginUrl = GetArg(argsList, "--login-url");
var selectorsPath = GetArg(argsList, "--selectors");
var output = GetArg(argsList, "--output");
var debugLog = GetArg(argsList, "--debug-log");
var dbPath = GetArg(argsList, "--db-path");
var timeoutRaw = GetArg(argsList, "--timeout");
var parallelRaw = GetArg(argsList, "--parallel");
var dbMigrate = HasFlag(argsList, "--db-migrate");
var sendTestSms = HasFlag(argsList, "--send-test-sms");
var smsTo = GetArg(argsList, "--to");
var smsMessage = GetArg(argsList, "--message");
var adbDevice = GetArg(argsList, "--adb-device");
var adbPath = GetArg(argsList, "--adb-path");
var adbTimeoutRaw = GetArg(argsList, "--adb-timeout");
var gatewayUrl = GetArg(argsList, "--gateway-url");
var gatewayToken = GetArg(argsList, "--gateway-token");
var gatewayTimeoutRaw = GetArg(argsList, "--gateway-timeout");
var smsLog = GetArg(argsList, "--sms-log");
var smsSendDelayRaw = GetArg(argsList, "--send-delay-ms");
var smsOpenOnly = HasFlag(argsList, "--open-only");
var headless = !HasFlag(argsList, "--headed");
var debug = HasFlag(argsList, "--debug");
var serveApi = HasFlag(argsList, "--serve");

if (serveApi)
{
    await ApiHost.RunAsync(argsList);
    return;
}

if (dbMigrate)
{
    var migrateResult = await DatabaseMigrator.MigrateAsync(dbPath);
    if (migrateResult.AppliedMigrations.Count > 0)
    {
        Console.WriteLine($"Database migrations applied: {migrateResult.AppliedMigrations.Count}. Path: {migrateResult.DatabasePath}");
        foreach (var migration in migrateResult.AppliedMigrations)
        {
            Console.WriteLine($"  - {migration}");
        }
    }
    else
    {
        Console.WriteLine($"Database is up to date. Path: {migrateResult.DatabasePath}");
    }

    return;
}

if (string.IsNullOrWhiteSpace(gatewayUrl)) gatewayUrl = Environment.GetEnvironmentVariable("SMS_GATEWAY_URL") ?? string.Empty;
if (string.IsNullOrWhiteSpace(gatewayToken)) gatewayToken = Environment.GetEnvironmentVariable("SMS_GATEWAY_TOKEN") ?? string.Empty;

if (sendTestSms)
{
    if (string.IsNullOrWhiteSpace(smsTo) || string.IsNullOrWhiteSpace(smsMessage))
    {
        Console.WriteLine("Missing required args for test SMS. Use --send-test-sms --to --message [--adb-device] [--open-only].");
        return;
    }

    var smsLogPath = string.IsNullOrWhiteSpace(smsLog) ? "out/sms_history.jsonl" : smsLog;
    Directory.CreateDirectory(Path.GetDirectoryName(smsLogPath) ?? ".");

    object result;
    bool success;
    string detail;

    if (!string.IsNullOrWhiteSpace(gatewayUrl) || !string.IsNullOrWhiteSpace(gatewayToken))
    {
        if (string.IsNullOrWhiteSpace(gatewayUrl) || string.IsNullOrWhiteSpace(gatewayToken))
        {
            Console.WriteLine("For gateway mode provide both --gateway-url and --gateway-token.");
            return;
        }

        var sender = new TraccarHttpSmsSender();
        var request = new TraccarSmsSendRequest
        {
            Url = gatewayUrl,
            Token = gatewayToken,
            To = smsTo,
            Message = smsMessage,
            TimeoutMs = int.TryParse(gatewayTimeoutRaw, out var gTimeout) ? gTimeout : 15000
        };

        var traccarResult = await sender.SendAsync(request);
        result = traccarResult;
        success = traccarResult.Success;
        detail = traccarResult.Detail;
    }
    else
    {
        var sender = new AndroidAdbSmsSender(adbPath);
        var request = new AdbSmsSendRequest
        {
            To = smsTo,
            Message = smsMessage,
            DeviceId = string.IsNullOrWhiteSpace(adbDevice) ? null : adbDevice,
            TimeoutMs = int.TryParse(adbTimeoutRaw, out var adbTimeout) ? adbTimeout : 20000,
            SendDelayMs = int.TryParse(smsSendDelayRaw, out var sendDelay) ? Math.Max(0, sendDelay) : 1200,
            OpenOnly = smsOpenOnly
        };

        var adbResult = await sender.SendAsync(request);
        result = adbResult;
        success = adbResult.Success;
        detail = adbResult.Detail;
    }

    await File.AppendAllTextAsync(smsLogPath, JsonSerializer.Serialize(result) + Environment.NewLine);

    Console.WriteLine(success
        ? $"SMS command executed. Log: {smsLogPath}. {detail}"
        : $"SMS command failed. Log: {smsLogPath}. {detail}");
    return;
}

if (string.IsNullOrWhiteSpace(phone)) phone = Environment.GetEnvironmentVariable("ROCKETMAN_PHONE") ?? string.Empty;
if (string.IsNullOrWhiteSpace(password)) password = Environment.GetEnvironmentVariable("ROCKETMAN_PASSWORD") ?? string.Empty;
if (string.IsNullOrWhiteSpace(loginUrl)) loginUrl = Environment.GetEnvironmentVariable("ROCKETMAN_LOGIN_URL") ?? string.Empty;

if (string.IsNullOrWhiteSpace(phone) || string.IsNullOrWhiteSpace(password) || string.IsNullOrWhiteSpace(loginUrl))
{
    Console.WriteLine("Missing required args. Use --phone, --password, --login-url (or env ROCKETMAN_PHONE/ROCKETMAN_PASSWORD/ROCKETMAN_LOGIN_URL).");
    return;
}

var opts = new CollectorOptions
{
    Headless = headless,
    LoginUrl = loginUrl,
    OutputPath = string.IsNullOrWhiteSpace(output) ? "out/clients.json" : output,
    SelectorsPath = string.IsNullOrWhiteSpace(selectorsPath) ? "src/Collector/Config/rocketman.selectors.json" : selectorsPath,
    TimeoutMs = int.TryParse(timeoutRaw, out var t) ? t : 0,
    Parallelism = int.TryParse(parallelRaw, out var p) ? Math.Max(1, p) : 3,
    Debug = debug,
    DebugLogPath = string.IsNullOrWhiteSpace(debugLog) ? "out/debug.log" : debugLog
};

var selectorsResolved = ResolveSelectorsPath(opts.SelectorsPath);
if (!File.Exists(selectorsResolved))
{
    Console.WriteLine($"Selectors file not found: {selectorsResolved}");
    return;
}

var cfg = JsonSerializer.Deserialize<SelectorConfig>(await File.ReadAllTextAsync(selectorsResolved));
if (cfg == null)
{
    Console.WriteLine("Failed to parse selectors config.");
    return;
}

Directory.CreateDirectory(Path.GetDirectoryName(opts.OutputPath) ?? ".");

using var playwright = await Playwright.CreateAsync();
await using var browser = await playwright.Chromium.LaunchAsync(new BrowserTypeLaunchOptions
{
    Headless = opts.Headless
});

var context = await browser.NewContextAsync();
var page = await context.NewPageAsync();

var collector = new RocketmanCollector(cfg, opts);
var results = await collector.CollectAsync(page, context, new Credentials { Phone = phone, Password = password });

await File.WriteAllTextAsync(opts.OutputPath, JsonSerializer.Serialize(results, new JsonSerializerOptions
{
    WriteIndented = true,
    Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
}));

Console.WriteLine($"Done. Collected: {results.Count}. Output: {opts.OutputPath}");
