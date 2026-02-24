using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Infrastructure;

#nullable disable

namespace Collector.Data.Migrations;

[DbContext(typeof(AppDbContext))]
[Migration("202602190001_InitialSchema")]
public partial class InitialSchema : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "alerts",
            columns: table => new
            {
                id = table.Column<long>(type: "INTEGER", nullable: false)
                    .Annotation("Sqlite:Autoincrement", true),
                level = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                text = table.Column<string>(type: "TEXT", maxLength: 1024, nullable: false),
                status = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                channel_id = table.Column<long>(type: "INTEGER", nullable: true),
                created_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false),
                closed_at_utc = table.Column<DateTime>(type: "TEXT", nullable: true),
                meta_json = table.Column<string>(type: "TEXT", nullable: true)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_alerts", x => x.id);
            });

        migrationBuilder.CreateTable(
            name: "client_snapshots",
            columns: table => new
            {
                id = table.Column<long>(type: "INTEGER", nullable: false)
                    .Annotation("Sqlite:Autoincrement", true),
                source_mode = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                created_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false),
                total_rows = table.Column<int>(type: "INTEGER", nullable: false),
                notes = table.Column<string>(type: "TEXT", nullable: true)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_client_snapshots", x => x.id);
            });

        migrationBuilder.CreateTable(
            name: "events",
            columns: table => new
            {
                id = table.Column<long>(type: "INTEGER", nullable: false)
                    .Annotation("Sqlite:Autoincrement", true),
                category = table.Column<string>(type: "TEXT", maxLength: 64, nullable: false),
                event_type = table.Column<string>(type: "TEXT", maxLength: 64, nullable: false),
                severity = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                message = table.Column<string>(type: "TEXT", maxLength: 2048, nullable: false),
                run_session_id = table.Column<long>(type: "INTEGER", nullable: true),
                run_job_id = table.Column<long>(type: "INTEGER", nullable: true),
                payload_json = table.Column<string>(type: "TEXT", nullable: true),
                created_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_events", x => x.id);
            });

        migrationBuilder.CreateTable(
            name: "manual_reply_presets",
            columns: table => new
            {
                id = table.Column<long>(type: "INTEGER", nullable: false)
                    .Annotation("Sqlite:Autoincrement", true),
                title = table.Column<string>(type: "TEXT", maxLength: 256, nullable: false),
                text = table.Column<string>(type: "TEXT", nullable: false),
                created_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false),
                updated_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_manual_reply_presets", x => x.id);
            });

        migrationBuilder.CreateTable(
            name: "messages",
            columns: table => new
            {
                id = table.Column<long>(type: "INTEGER", nullable: false)
                    .Annotation("Sqlite:Autoincrement", true),
                run_job_id = table.Column<long>(type: "INTEGER", nullable: true),
                client_phone = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                direction = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                text = table.Column<string>(type: "TEXT", nullable: false),
                gateway_status = table.Column<string>(type: "TEXT", maxLength: 64, nullable: true),
                created_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false),
                meta_json = table.Column<string>(type: "TEXT", nullable: true)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_messages", x => x.id);
            });

        migrationBuilder.CreateTable(
            name: "sender_channels",
            columns: table => new
            {
                id = table.Column<long>(type: "INTEGER", nullable: false)
                    .Annotation("Sqlite:Autoincrement", true),
                name = table.Column<string>(type: "TEXT", maxLength: 128, nullable: false),
                endpoint = table.Column<string>(type: "TEXT", maxLength: 1024, nullable: false),
                token = table.Column<string>(type: "TEXT", maxLength: 1024, nullable: false),
                sim_phone = table.Column<string>(type: "TEXT", maxLength: 32, nullable: true),
                status = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                last_checked_at_utc = table.Column<DateTime>(type: "TEXT", nullable: true),
                fail_streak = table.Column<int>(type: "INTEGER", nullable: false),
                alerted = table.Column<bool>(type: "INTEGER", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_sender_channels", x => x.id);
            });

        migrationBuilder.CreateTable(
            name: "settings",
            columns: table => new
            {
                key = table.Column<string>(type: "TEXT", maxLength: 128, nullable: false),
                value_json = table.Column<string>(type: "TEXT", nullable: false),
                updated_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_settings", x => x.key);
            });

        migrationBuilder.CreateTable(
            name: "stop_list",
            columns: table => new
            {
                id = table.Column<long>(type: "INTEGER", nullable: false)
                    .Annotation("Sqlite:Autoincrement", true),
                phone = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                reason = table.Column<string>(type: "TEXT", maxLength: 512, nullable: true),
                source = table.Column<string>(type: "TEXT", maxLength: 64, nullable: false),
                added_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false),
                is_active = table.Column<bool>(type: "INTEGER", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_stop_list", x => x.id);
            });

        migrationBuilder.CreateTable(
            name: "templates",
            columns: table => new
            {
                id = table.Column<long>(type: "INTEGER", nullable: false)
                    .Annotation("Sqlite:Autoincrement", true),
                name = table.Column<string>(type: "TEXT", maxLength: 256, nullable: false),
                kind = table.Column<string>(type: "TEXT", maxLength: 64, nullable: false),
                status = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                text = table.Column<string>(type: "TEXT", nullable: false),
                created_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false),
                updated_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_templates", x => x.id);
            });

        migrationBuilder.CreateTable(
            name: "client_snapshot_rows",
            columns: table => new
            {
                id = table.Column<long>(type: "INTEGER", nullable: false)
                    .Annotation("Sqlite:Autoincrement", true),
                snapshot_id = table.Column<long>(type: "INTEGER", nullable: false),
                external_client_id = table.Column<string>(type: "TEXT", maxLength: 64, nullable: false),
                fio = table.Column<string>(type: "TEXT", maxLength: 256, nullable: false),
                phone = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                timezone_offset = table.Column<int>(type: "INTEGER", nullable: false),
                days_overdue = table.Column<int>(type: "INTEGER", nullable: false),
                contract_status = table.Column<string>(type: "TEXT", maxLength: 64, nullable: true),
                card_url = table.Column<string>(type: "TEXT", maxLength: 1024, nullable: true),
                total_with_commission_raw = table.Column<string>(type: "TEXT", maxLength: 128, nullable: true),
                collected_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_client_snapshot_rows", x => x.id);
                table.ForeignKey(
                    name: "FK_client_snapshot_rows_client_snapshots_snapshot_id",
                    column: x => x.snapshot_id,
                    principalTable: "client_snapshots",
                    principalColumn: "id",
                    onDelete: ReferentialAction.Cascade);
            });

        migrationBuilder.CreateTable(
            name: "run_sessions",
            columns: table => new
            {
                id = table.Column<long>(type: "INTEGER", nullable: false)
                    .Annotation("Sqlite:Autoincrement", true),
                mode = table.Column<string>(type: "TEXT", maxLength: 16, nullable: false),
                status = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                created_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false),
                started_at_utc = table.Column<DateTime>(type: "TEXT", nullable: true),
                finished_at_utc = table.Column<DateTime>(type: "TEXT", nullable: true),
                snapshot_id = table.Column<long>(type: "INTEGER", nullable: true),
                filters_json = table.Column<string>(type: "TEXT", nullable: true),
                notes = table.Column<string>(type: "TEXT", nullable: true)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_run_sessions", x => x.id);
                table.ForeignKey(
                    name: "FK_run_sessions_client_snapshots_snapshot_id",
                    column: x => x.snapshot_id,
                    principalTable: "client_snapshots",
                    principalColumn: "id",
                    onDelete: ReferentialAction.SetNull);
            });

        migrationBuilder.CreateTable(
            name: "run_jobs",
            columns: table => new
            {
                id = table.Column<long>(type: "INTEGER", nullable: false)
                    .Annotation("Sqlite:Autoincrement", true),
                run_session_id = table.Column<long>(type: "INTEGER", nullable: false),
                external_client_id = table.Column<string>(type: "TEXT", maxLength: 64, nullable: false),
                client_fio = table.Column<string>(type: "TEXT", maxLength: 256, nullable: false),
                phone = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                tz_offset = table.Column<int>(type: "INTEGER", nullable: false),
                days_overdue = table.Column<int>(type: "INTEGER", nullable: false),
                template_id = table.Column<long>(type: "INTEGER", nullable: true),
                channel_id = table.Column<long>(type: "INTEGER", nullable: true),
                delivery_type = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                status = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                attempts = table.Column<int>(type: "INTEGER", nullable: false),
                max_attempts = table.Column<int>(type: "INTEGER", nullable: false),
                planned_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false),
                sent_at_utc = table.Column<DateTime>(type: "TEXT", nullable: true),
                last_error_code = table.Column<string>(type: "TEXT", maxLength: 128, nullable: true),
                last_error_detail = table.Column<string>(type: "TEXT", maxLength: 1024, nullable: true),
                payload_json = table.Column<string>(type: "TEXT", nullable: true)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_run_jobs", x => x.id);
                table.ForeignKey(
                    name: "FK_run_jobs_run_sessions_run_session_id",
                    column: x => x.run_session_id,
                    principalTable: "run_sessions",
                    principalColumn: "id",
                    onDelete: ReferentialAction.Cascade);
            });

        migrationBuilder.CreateIndex(
            name: "IX_alerts_channel_id",
            table: "alerts",
            column: "channel_id");

        migrationBuilder.CreateIndex(
            name: "IX_alerts_status",
            table: "alerts",
            column: "status");

        migrationBuilder.CreateIndex(
            name: "IX_client_snapshot_rows_external_client_id",
            table: "client_snapshot_rows",
            column: "external_client_id");

        migrationBuilder.CreateIndex(
            name: "IX_client_snapshot_rows_snapshot_id_phone",
            table: "client_snapshot_rows",
            columns: new[] { "snapshot_id", "phone" });

        migrationBuilder.CreateIndex(
            name: "IX_client_snapshots_created_at_utc",
            table: "client_snapshots",
            column: "created_at_utc");

        migrationBuilder.CreateIndex(
            name: "IX_events_category",
            table: "events",
            column: "category");

        migrationBuilder.CreateIndex(
            name: "IX_events_created_at_utc",
            table: "events",
            column: "created_at_utc");

        migrationBuilder.CreateIndex(
            name: "IX_events_severity",
            table: "events",
            column: "severity");

        migrationBuilder.CreateIndex(
            name: "IX_messages_client_phone",
            table: "messages",
            column: "client_phone");

        migrationBuilder.CreateIndex(
            name: "IX_messages_created_at_utc",
            table: "messages",
            column: "created_at_utc");

        migrationBuilder.CreateIndex(
            name: "IX_run_jobs_external_client_id",
            table: "run_jobs",
            column: "external_client_id");

        migrationBuilder.CreateIndex(
            name: "IX_run_jobs_phone",
            table: "run_jobs",
            column: "phone");

        migrationBuilder.CreateIndex(
            name: "IX_run_jobs_run_session_id_status",
            table: "run_jobs",
            columns: new[] { "run_session_id", "status" });

        migrationBuilder.CreateIndex(
            name: "IX_run_sessions_created_at_utc",
            table: "run_sessions",
            column: "created_at_utc");

        migrationBuilder.CreateIndex(
            name: "IX_run_sessions_snapshot_id",
            table: "run_sessions",
            column: "snapshot_id");

        migrationBuilder.CreateIndex(
            name: "IX_run_sessions_status",
            table: "run_sessions",
            column: "status");

        migrationBuilder.CreateIndex(
            name: "IX_sender_channels_status",
            table: "sender_channels",
            column: "status");

        migrationBuilder.CreateIndex(
            name: "IX_stop_list_phone_is_active",
            table: "stop_list",
            columns: new[] { "phone", "is_active" });

        migrationBuilder.CreateIndex(
            name: "IX_templates_kind",
            table: "templates",
            column: "kind");

        migrationBuilder.CreateIndex(
            name: "IX_templates_status",
            table: "templates",
            column: "status");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(name: "alerts");
        migrationBuilder.DropTable(name: "client_snapshot_rows");
        migrationBuilder.DropTable(name: "events");
        migrationBuilder.DropTable(name: "manual_reply_presets");
        migrationBuilder.DropTable(name: "messages");
        migrationBuilder.DropTable(name: "run_jobs");
        migrationBuilder.DropTable(name: "sender_channels");
        migrationBuilder.DropTable(name: "settings");
        migrationBuilder.DropTable(name: "stop_list");
        migrationBuilder.DropTable(name: "templates");
        migrationBuilder.DropTable(name: "run_sessions");
        migrationBuilder.DropTable(name: "client_snapshots");
    }
}
