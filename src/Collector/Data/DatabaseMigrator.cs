using Microsoft.EntityFrameworkCore;

namespace Collector.Data;

public static class DatabaseMigrator
{
    public static async Task<MigrateResult> MigrateAsync(string? dbPath = null, CancellationToken cancellationToken = default)
    {
        var resolvedPath = DbPathResolver.ResolvePath(dbPath);
        EnsureDirectoryOrFallback(ref resolvedPath, dbPath);

        var options = new DbContextOptionsBuilder<AppDbContext>()
            .UseSqlite($"Data Source={resolvedPath}")
            .Options;

        await using var db = new AppDbContext(options);
        var pending = await db.Database.GetPendingMigrationsAsync(cancellationToken);
        var pendingList = pending.ToList();
        await db.Database.MigrateAsync(cancellationToken);

        return new MigrateResult
        {
            DatabasePath = resolvedPath,
            AppliedMigrations = pendingList
        };
    }

    private static void EnsureDirectoryOrFallback(ref string resolvedPath, string? requestedPath)
    {
        try
        {
            EnsureDirectory(resolvedPath);
        }
        catch (Exception ex) when (IsDirectoryAccessError(ex) && string.IsNullOrWhiteSpace(requestedPath))
        {
            var legacyPath = DbPathResolver.ResolvePath(DbPathResolver.LegacyRelativePath);
            EnsureDirectory(legacyPath);
            resolvedPath = legacyPath;
        }
    }

    private static void EnsureDirectory(string dbPath)
    {
        var dbDir = Path.GetDirectoryName(dbPath);
        if (!string.IsNullOrWhiteSpace(dbDir))
        {
            Directory.CreateDirectory(dbDir);
        }
    }

    private static bool IsDirectoryAccessError(Exception ex) =>
        ex is UnauthorizedAccessException ||
        ex is IOException ||
        ex is NotSupportedException;
}

public sealed class MigrateResult
{
    public required string DatabasePath { get; init; }
    public required IReadOnlyList<string> AppliedMigrations { get; init; }
}
