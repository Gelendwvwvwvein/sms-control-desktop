using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Collector.Data.Migrations;

[DbContext(typeof(AppDbContext))]
[Migration("202602240003_DraftsAndAudit")]
public partial class DraftsAndAudit : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "manual_dialog_drafts",
            columns: table => new
            {
                id = table.Column<long>(type: "INTEGER", nullable: false)
                    .Annotation("Sqlite:Autoincrement", true),
                phone = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                text = table.Column<string>(type: "TEXT", nullable: false),
                created_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false),
                updated_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_manual_dialog_drafts", x => x.id);
            });

        migrationBuilder.CreateTable(
            name: "audit_logs",
            columns: table => new
            {
                id = table.Column<long>(type: "INTEGER", nullable: false)
                    .Annotation("Sqlite:Autoincrement", true),
                category = table.Column<string>(type: "TEXT", maxLength: 64, nullable: false),
                action = table.Column<string>(type: "TEXT", maxLength: 128, nullable: false),
                entity_id = table.Column<string>(type: "TEXT", maxLength: 128, nullable: true),
                actor = table.Column<string>(type: "TEXT", maxLength: 128, nullable: false),
                details_json = table.Column<string>(type: "TEXT", nullable: true),
                created_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_audit_logs", x => x.id);
            });

        migrationBuilder.CreateIndex(
            name: "IX_manual_dialog_drafts_phone",
            table: "manual_dialog_drafts",
            column: "phone",
            unique: true);

        migrationBuilder.CreateIndex(
            name: "IX_manual_dialog_drafts_updated_at_utc",
            table: "manual_dialog_drafts",
            column: "updated_at_utc");

        migrationBuilder.CreateIndex(
            name: "IX_audit_logs_created_at_utc",
            table: "audit_logs",
            column: "created_at_utc");

        migrationBuilder.CreateIndex(
            name: "IX_audit_logs_category",
            table: "audit_logs",
            column: "category");

        migrationBuilder.CreateIndex(
            name: "IX_audit_logs_actor",
            table: "audit_logs",
            column: "actor");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(name: "manual_dialog_drafts");
        migrationBuilder.DropTable(name: "audit_logs");
    }
}
