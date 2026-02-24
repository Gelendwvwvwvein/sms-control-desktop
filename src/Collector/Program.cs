using System.Text.Json;
using System.Text.Encodings.Web;
using System.Reflection;
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
var installPlaywright = HasFlag(argsList, "--install-playwright");
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

if (installPlaywright)
{
    try
    {
        var playwrightAssembly = typeof(IPlaywright).Assembly;
        var programType = playwrightAssembly.GetType("Microsoft.Playwright.Program");
        if (programType is null)
        {
            Console.WriteLine("Playwright CLI class not found in Microsoft.Playwright assembly.");
            return;
        }

        var method = programType.GetMethod(
            "Main",
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static,
            binder: null,
            types: [typeof(string[])],
            modifiers: null);

        if (method is null)
        {
            Console.WriteLine("Playwright CLI entrypoint not found.");
            return;
        }

        var result = method.Invoke(null, [new[] { "install", "chromium" }]);
        var code = result switch
        {
            Task<int> taskInt => await taskInt,
            Task task => await AwaitTaskAsSuccessCodeAsync(task),
            int intCode => intCode,
            null => 0,
            _ => throw new InvalidOperationException(
                $"Unexpected Playwright CLI return type: {result.GetType().FullName}")
        };

        Console.WriteLine(code == 0
            ? "Playwright Chromium installed."
            : $"Playwright install finished with code {code}.");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Playwright install failed: {ex.Message}");
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
    SelectorsPath = selectorsPath,
    TimeoutMs = int.TryParse(timeoutRaw, out var t) ? t : 0,
    Parallelism = int.TryParse(parallelRaw, out var p) ? Math.Max(1, p) : 3,
    Debug = debug,
    DebugLogPath = string.IsNullOrWhiteSpace(debugLog) ? "out/debug.log" : debugLog
};

SelectorConfig cfg;
try
{
    cfg = await SelectorConfigLoader.LoadAsync(opts.SelectorsPath, CancellationToken.None);
}
catch (Exception ex)
{
    Console.WriteLine($"Failed to load selectors config: {ex.Message}");
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

static async Task<int> AwaitTaskAsSuccessCodeAsync(Task task)
{
    await task;
    return 0;
}
