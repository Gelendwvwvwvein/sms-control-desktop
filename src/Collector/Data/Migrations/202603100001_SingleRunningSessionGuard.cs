using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Collector.Data.Migrations;

[DbContext(typeof(AppDbContext))]
[Migration("202603100001_SingleRunningSessionGuard")]
public partial class SingleRunningSessionGuard : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.Sql(
            """
            UPDATE run_sessions
            SET status = 'stopped',
                finished_at_utc = COALESCE(finished_at_utc, CURRENT_TIMESTAMP),
                notes = CASE
                    WHEN notes IS NULL OR trim(notes) = '' THEN 'Автоматически остановлено при устранении дублирующихся running-сессий.'
                    ELSE notes || ' | Автоматически остановлено при устранении дублирующихся running-сессий.'
                END
            WHERE status = 'running'
              AND id NOT IN (
                  SELECT id
                  FROM run_sessions
                  WHERE status = 'running'
                  ORDER BY id DESC
                  LIMIT 1
              );
            """);

        migrationBuilder.Sql(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS IX_run_sessions_single_running
            ON run_sessions(status)
            WHERE status = 'running';
            """);
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.Sql(
            """
            DROP INDEX IF EXISTS IX_run_sessions_single_running;
            """);
    }
}
