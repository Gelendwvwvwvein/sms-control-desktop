namespace Collector.Data;

public static class DbPathResolver
{
    public const string LegacyRelativePath = "out/smscontrol.db";
    public const string AppFolderName = "SmsControl";
    public const string DatabaseFileName = "smscontrol.db";

    public static string ResolvePath(string? inputPath)
    {
        if (!string.IsNullOrWhiteSpace(inputPath))
        {
            return ResolveProvidedPath(inputPath.Trim());
        }

        var envPath = Environment.GetEnvironmentVariable("SMS_APP_DB_PATH");
        if (!string.IsNullOrWhiteSpace(envPath))
        {
            return ResolveProvidedPath(envPath.Trim());
        }

        var persistentPath = ResolvePersistentDefaultPath();
        var legacyPath = ResolveProvidedPath(LegacyRelativePath);
        if (File.Exists(legacyPath) && !File.Exists(persistentPath))
        {
            return legacyPath;
        }

        return persistentPath;
    }

    private static string ResolveProvidedPath(string path)
    {
        if (Path.IsPathRooted(path))
        {
            return path;
        }

        return Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), path));
    }

    private static string ResolvePersistentDefaultPath()
    {
        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        if (string.IsNullOrWhiteSpace(localAppData))
        {
            return ResolveProvidedPath(LegacyRelativePath);
        }

        return Path.Combine(localAppData, AppFolderName, DatabaseFileName);
    }
}
