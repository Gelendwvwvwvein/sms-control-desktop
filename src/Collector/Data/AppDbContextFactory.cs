using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace Collector.Data;

public sealed class AppDbContextFactory : IDesignTimeDbContextFactory<AppDbContext>
{
    public AppDbContext CreateDbContext(string[] args)
    {
        var dbPathArg = args.FirstOrDefault(x => x.StartsWith("--db-path=", StringComparison.OrdinalIgnoreCase));
        var dbPath = dbPathArg is null ? null : dbPathArg["--db-path=".Length..];
        var envPath = Environment.GetEnvironmentVariable("SMS_APP_DB_PATH");
        var resolvedPath = DbPathResolver.ResolvePath(dbPath ?? envPath);

        var optionsBuilder = new DbContextOptionsBuilder<AppDbContext>();
        optionsBuilder.UseSqlite($"Data Source={resolvedPath}");
        return new AppDbContext(optionsBuilder.Options);
    }
}
