using System.Text.Json;
using System.Text.Encodings.Web;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Diagnostics;
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
ConfigureBundledPlaywrightBrowsers();

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
var desktopMode = HasFlag(argsList, "--desktop");

if (desktopMode)
{
    HideConsoleWindowIfPossible();
    await LaunchDesktopAsync(argsList);
    return;
}

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
            Environment.ExitCode = 1;
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
            Environment.ExitCode = 1;
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

        if (code == 0)
        {
            Console.WriteLine("Playwright Chromium installed.");
        }
        else
        {
            Console.WriteLine($"Playwright install finished with code {code}.");
            Environment.ExitCode = code;
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Playwright install failed: {ex.Message}");
        Environment.ExitCode = 1;
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

static void ConfigureBundledPlaywrightBrowsers()
{
    const string browsersEnvVar = "PLAYWRIGHT_BROWSERS_PATH";
    const string driverEnvVar = "PLAYWRIGHT_DRIVER_SEARCH_PATH";

    var existingBrowsers = Environment.GetEnvironmentVariable(browsersEnvVar);
    if (string.IsNullOrWhiteSpace(existingBrowsers))
    {
        var bundledBrowsersPath = Path.Combine(AppContext.BaseDirectory, "ms-playwright");
        if (Directory.Exists(bundledBrowsersPath))
        {
            Environment.SetEnvironmentVariable(browsersEnvVar, bundledBrowsersPath);
        }
    }

    var existingDriver = Environment.GetEnvironmentVariable(driverEnvVar);
    if (string.IsNullOrWhiteSpace(existingDriver))
    {
        var bundledDriverRoot = Path.Combine(AppContext.BaseDirectory, ".playwright");
        if (Directory.Exists(bundledDriverRoot))
        {
            Environment.SetEnvironmentVariable(driverEnvVar, AppContext.BaseDirectory);
        }
    }
}

static async Task LaunchDesktopAsync(string[] args)
{
    var portRaw = GetArg(args, "--port");
    var port = int.TryParse(portRaw, out var parsedPort) ? parsedPort : 5057;
    port = Math.Clamp(port, 1, 65535);

    var loopbackHost = "127.0.0.1";
    var healthUrl = $"http://{loopbackHost}:{port}/health";
    var uiUrl = $"http://{loopbackHost}:{port}/";

    var backendUp = await IsBackendHealthyAsync(healthUrl);
    if (!backendUp)
    {
        var started = StartServeProcess(args);
        if (started)
        {
            await WaitForBackendHealthyAsync(healthUrl, TimeSpan.FromSeconds(15));
        }
    }

    OpenInDefaultBrowser(uiUrl);
}

static bool StartServeProcess(string[] args)
{
    try
    {
        var processPath = Environment.ProcessPath;
        if (string.IsNullOrWhiteSpace(processPath) || !File.Exists(processPath))
        {
            return false;
        }

        var isDotnetHost = string.Equals(
            Path.GetFileNameWithoutExtension(processPath),
            "dotnet",
            StringComparison.OrdinalIgnoreCase);
        var entryAssemblyName = Assembly.GetEntryAssembly()?.GetName().Name;
        var entryAssemblyPath = string.IsNullOrWhiteSpace(entryAssemblyName)
            ? string.Empty
            : Path.Combine(AppContext.BaseDirectory, $"{entryAssemblyName}.dll");
        var useDotnetDllBootstrap = isDotnetHost &&
                                    !string.IsNullOrWhiteSpace(entryAssemblyPath) &&
                                    File.Exists(entryAssemblyPath);

        var startInfo = new ProcessStartInfo
        {
            FileName = processPath,
            UseShellExecute = false,
            CreateNoWindow = true,
            WorkingDirectory = AppContext.BaseDirectory
        };

        if (useDotnetDllBootstrap)
        {
            startInfo.ArgumentList.Add(entryAssemblyPath!);
        }

        foreach (var arg in BuildServeArguments(args))
        {
            startInfo.ArgumentList.Add(arg);
        }

        _ = Process.Start(startInfo);
        return true;
    }
    catch
    {
        return false;
    }
}

static IReadOnlyList<string> BuildServeArguments(string[] args)
{
    var result = new List<string> { "--serve" };
    for (var i = 0; i < args.Length; i++)
    {
        var arg = args[i];
        if (string.Equals(arg, "--desktop", StringComparison.Ordinal) ||
            string.Equals(arg, "--serve", StringComparison.Ordinal))
        {
            continue;
        }

        if (string.Equals(arg, "--lan", StringComparison.Ordinal))
        {
            result.Add("--lan");
            continue;
        }

        if (string.Equals(arg, "--port", StringComparison.Ordinal) ||
            string.Equals(arg, "--host", StringComparison.Ordinal) ||
            string.Equals(arg, "--db-path", StringComparison.Ordinal))
        {
            if (i + 1 < args.Length)
            {
                result.Add(arg);
                result.Add(args[i + 1]);
                i++;
            }
        }
    }

    return result;
}

static async Task<bool> WaitForBackendHealthyAsync(string healthUrl, TimeSpan timeout)
{
    var startedAt = DateTime.UtcNow;
    while (DateTime.UtcNow - startedAt < timeout)
    {
        if (await IsBackendHealthyAsync(healthUrl))
        {
            return true;
        }

        await Task.Delay(250);
    }

    return await IsBackendHealthyAsync(healthUrl);
}

static async Task<bool> IsBackendHealthyAsync(string healthUrl)
{
    try
    {
        using var httpClient = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(2)
        };
        using var response = await httpClient.GetAsync(healthUrl);
        return response.IsSuccessStatusCode;
    }
    catch
    {
        return false;
    }
}

static void OpenInDefaultBrowser(string url)
{
    try
    {
        _ = Process.Start(new ProcessStartInfo
        {
            FileName = url,
            UseShellExecute = true
        });
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Failed to open browser automatically: {ex.Message}. Open manually: {url}");
    }
}

static void HideConsoleWindowIfPossible()
{
    if (!OperatingSystem.IsWindows())
    {
        return;
    }

    try
    {
        var handle = GetConsoleWindow();
        if (handle != IntPtr.Zero)
        {
            _ = ShowWindow(handle, 0);
        }
    }
    catch
    {
        // ignore errors, desktop mode can continue even if console window stays visible
    }
}

[DllImport("kernel32.dll")]
static extern IntPtr GetConsoleWindow();

[DllImport("user32.dll")]
static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);
