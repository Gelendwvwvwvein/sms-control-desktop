using System.Diagnostics;

namespace Collector.Services;

public sealed class AndroidAdbSmsSender
{
    private readonly string _adbPath;

    public AndroidAdbSmsSender(string adbPath = "adb")
    {
        _adbPath = string.IsNullOrWhiteSpace(adbPath) ? "adb" : adbPath;
    }

    public async Task<AdbSmsSendResult> SendAsync(AdbSmsSendRequest request, CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(request.To))
            throw new ArgumentException("Recipient number is required.", nameof(request.To));
        if (string.IsNullOrWhiteSpace(request.Message))
            throw new ArgumentException("SMS text is required.", nameof(request.Message));

        var check = await CheckDeviceAsync(request.DeviceId, request.TimeoutMs, ct);
        if (!check.Ready)
        {
            return new AdbSmsSendResult
            {
                Timestamp = DateTimeOffset.UtcNow,
                To = request.To,
                Message = request.Message,
                DeviceId = request.DeviceId ?? string.Empty,
                OpenOnly = request.OpenOnly,
                Success = false,
                Detail = check.Detail,
                ComposerStdout = check.Stdout,
                ComposerStderr = check.Stderr
            };
        }

        var composeArgs = BuildArgs(
            request.DeviceId,
            "shell",
            "am",
            "start",
            "-a",
            "android.intent.action.SENDTO",
            "-d",
            $"sms:{request.To}",
            "--es",
            "sms_body",
            request.Message,
            "--ez",
            "exit_on_sent",
            "true");

        var compose = await RunAdbAsync(composeArgs, request.TimeoutMs, ct);
        if (compose.ExitCode != 0)
        {
            return new AdbSmsSendResult
            {
                Timestamp = DateTimeOffset.UtcNow,
                To = request.To,
                Message = request.Message,
                DeviceId = request.DeviceId ?? string.Empty,
                OpenOnly = request.OpenOnly,
                Success = false,
                Detail = "Не удалось открыть экран SMS.",
                ComposerStdout = compose.Stdout,
                ComposerStderr = compose.Stderr
            };
        }

        if (request.OpenOnly)
        {
            return new AdbSmsSendResult
            {
                Timestamp = DateTimeOffset.UtcNow,
                To = request.To,
                Message = request.Message,
                DeviceId = request.DeviceId ?? string.Empty,
                OpenOnly = true,
                Success = true,
                Detail = "Экран SMS открыт. Отправьте сообщение вручную на телефоне.",
                ComposerStdout = compose.Stdout,
                ComposerStderr = compose.Stderr
            };
        }

        await Task.Delay(request.SendDelayMs, ct);
        var send = await RunAdbAsync(BuildArgs(request.DeviceId, "shell", "input", "keyevent", "66"), request.TimeoutMs, ct);

        return new AdbSmsSendResult
        {
            Timestamp = DateTimeOffset.UtcNow,
            To = request.To,
            Message = request.Message,
            DeviceId = request.DeviceId ?? string.Empty,
            OpenOnly = false,
            Success = send.ExitCode == 0,
            Detail = send.ExitCode == 0
                ? "Команда отправки выполнена. Проверьте статус в приложении SMS на телефоне."
                : "Команда отправки не выполнилась.",
            ComposerStdout = compose.Stdout,
            ComposerStderr = compose.Stderr,
            SendStdout = send.Stdout,
            SendStderr = send.Stderr
        };
    }

    private async Task<DeviceCheckResult> CheckDeviceAsync(string? deviceId, int timeoutMs, CancellationToken ct)
    {
        var devices = await RunAdbAsync(new[] { "devices" }, timeoutMs, ct);
        if (devices.ExitCode != 0)
        {
            return DeviceCheckResult.Fail("Команда adb devices завершилась ошибкой.", devices.Stdout, devices.Stderr);
        }

        var readyLines = devices.Stdout
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(l => l.Contains('\t', StringComparison.Ordinal) && l.EndsWith("\tdevice", StringComparison.Ordinal))
            .ToList();

        if (readyLines.Count == 0)
        {
            return DeviceCheckResult.Fail("Не найдено подключенных Android-устройств в статусе device.", devices.Stdout, devices.Stderr);
        }

        if (!string.IsNullOrWhiteSpace(deviceId))
        {
            var found = readyLines.Any(l => l.StartsWith(deviceId + "\t", StringComparison.Ordinal));
            if (!found)
            {
                return DeviceCheckResult.Fail($"Устройство '{deviceId}' не найдено в adb devices.", devices.Stdout, devices.Stderr);
            }
        }

        return DeviceCheckResult.Ok();
    }

    private async Task<AdbCommandResult> RunAdbAsync(IEnumerable<string> args, int timeoutMs, CancellationToken ct)
    {
        using var proc = new Process();
        proc.StartInfo = new ProcessStartInfo
        {
            FileName = _adbPath,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        foreach (var arg in args)
        {
            proc.StartInfo.ArgumentList.Add(arg);
        }

        try
        {
            if (!proc.Start())
            {
                return new AdbCommandResult(-1, string.Empty, "Не удалось запустить adb.");
            }
        }
        catch (Exception ex)
        {
            return new AdbCommandResult(-1, string.Empty, ex.Message);
        }

        var stdoutTask = proc.StandardOutput.ReadToEndAsync();
        var stderrTask = proc.StandardError.ReadToEndAsync();

        try
        {
            if (timeoutMs > 0)
            {
                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                timeoutCts.CancelAfter(timeoutMs);
                await proc.WaitForExitAsync(timeoutCts.Token);
            }
            else
            {
                await proc.WaitForExitAsync(ct);
            }
        }
        catch (OperationCanceledException)
        {
            try { if (!proc.HasExited) proc.Kill(entireProcessTree: true); } catch { /* ignore */ }
            var outText = await stdoutTask;
            var errText = await stderrTask;
            return new AdbCommandResult(-1, outText, "adb timeout exceeded");
        }

        var stdout = await stdoutTask;
        var stderr = await stderrTask;
        return new AdbCommandResult(proc.ExitCode, stdout.Trim(), stderr.Trim());
    }

    private static IReadOnlyList<string> BuildArgs(string? deviceId, params string[] tail)
    {
        var list = new List<string>();
        if (!string.IsNullOrWhiteSpace(deviceId))
        {
            list.Add("-s");
            list.Add(deviceId);
        }

        list.AddRange(tail);
        return list;
    }

    private readonly record struct AdbCommandResult(int ExitCode, string Stdout, string Stderr);

    private readonly record struct DeviceCheckResult(bool Ready, string Detail, string Stdout, string Stderr)
    {
        public static DeviceCheckResult Ok() => new(true, string.Empty, string.Empty, string.Empty);
        public static DeviceCheckResult Fail(string detail, string stdout, string stderr) => new(false, detail, stdout, stderr);
    }
}

public sealed class AdbSmsSendRequest
{
    public string To { get; init; } = "";
    public string Message { get; init; } = "";
    public string? DeviceId { get; init; }
    public int TimeoutMs { get; init; } = 20000;
    public int SendDelayMs { get; init; } = 1200;
    public bool OpenOnly { get; init; }
}

public sealed class AdbSmsSendResult
{
    public DateTimeOffset Timestamp { get; init; }
    public string To { get; init; } = "";
    public string Message { get; init; } = "";
    public string DeviceId { get; init; } = "";
    public bool OpenOnly { get; init; }
    public bool Success { get; init; }
    public string Detail { get; init; } = "";
    public string ComposerStdout { get; init; } = "";
    public string ComposerStderr { get; init; } = "";
    public string SendStdout { get; init; } = "";
    public string SendStderr { get; init; } = "";
}
