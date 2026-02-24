using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Collector.Data.Migrations;

[DbContext(typeof(AppDbContext))]
[Migration("202602240001_ClientDebtCache")]
public partial class ClientDebtCache : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "client_debt_cache",
            columns: table => new
            {
                id = table.Column<long>(type: "INTEGER", nullable: false)
                    .Annotation("Sqlite:Autoincrement", true),
                external_client_id = table.Column<string>(type: "TEXT", maxLength: 64, nullable: false),
                phone = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                card_url = table.Column<string>(type: "TEXT", maxLength: 1024, nullable: false),
                exact_total_raw = table.Column<string>(type: "TEXT", maxLength: 128, nullable: false),
                approx_total_text = table.Column<string>(type: "TEXT", maxLength: 64, nullable: false),
                approx_total_value = table.Column<int>(type: "INTEGER", nullable: true),
                status = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                source = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                last_fetched_at_utc = table.Column<DateTime>(type: "TEXT", nullable: true),
                created_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false),
                updated_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false),
                last_error_code = table.Column<string>(type: "TEXT", maxLength: 128, nullable: true),
                last_error_detail = table.Column<string>(type: "TEXT", maxLength: 1024, nullable: true)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_client_debt_cache", x => x.id);
            });

        migrationBuilder.CreateIndex(
            name: "IX_client_debt_cache_external_client_id",
            table: "client_debt_cache",
            column: "external_client_id",
            unique: true);

        migrationBuilder.CreateIndex(
            name: "IX_client_debt_cache_phone",
            table: "client_debt_cache",
            column: "phone");

        migrationBuilder.CreateIndex(
            name: "IX_client_debt_cache_status",
            table: "client_debt_cache",
            column: "status");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(name: "client_debt_cache");
    }
}
