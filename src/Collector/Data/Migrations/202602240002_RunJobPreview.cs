using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Collector.Data.Migrations;

[DbContext(typeof(AppDbContext))]
[Migration("202602240002_RunJobPreview")]
public partial class RunJobPreview : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddColumn<string>(
            name: "preview_status",
            table: "run_jobs",
            type: "TEXT",
            maxLength: 32,
            nullable: true);

        migrationBuilder.AddColumn<string>(
            name: "preview_text",
            table: "run_jobs",
            type: "TEXT",
            nullable: true);

        migrationBuilder.AddColumn<string>(
            name: "preview_variables_json",
            table: "run_jobs",
            type: "TEXT",
            nullable: true);

        migrationBuilder.AddColumn<DateTime>(
            name: "preview_updated_at_utc",
            table: "run_jobs",
            type: "TEXT",
            nullable: true);

        migrationBuilder.AddColumn<string>(
            name: "preview_error_code",
            table: "run_jobs",
            type: "TEXT",
            maxLength: 128,
            nullable: true);

        migrationBuilder.AddColumn<string>(
            name: "preview_error_detail",
            table: "run_jobs",
            type: "TEXT",
            maxLength: 1024,
            nullable: true);
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropColumn(
            name: "preview_status",
            table: "run_jobs");

        migrationBuilder.DropColumn(
            name: "preview_text",
            table: "run_jobs");

        migrationBuilder.DropColumn(
            name: "preview_variables_json",
            table: "run_jobs");

        migrationBuilder.DropColumn(
            name: "preview_updated_at_utc",
            table: "run_jobs");

        migrationBuilder.DropColumn(
            name: "preview_error_code",
            table: "run_jobs");

        migrationBuilder.DropColumn(
            name: "preview_error_detail",
            table: "run_jobs");
    }
}
