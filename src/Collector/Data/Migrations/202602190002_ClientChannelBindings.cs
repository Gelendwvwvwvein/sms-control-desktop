using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Collector.Data.Migrations;

[DbContext(typeof(AppDbContext))]
[Migration("202602190002_ClientChannelBindings")]
public partial class ClientChannelBindings : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.CreateTable(
            name: "client_channel_bindings",
            columns: table => new
            {
                id = table.Column<long>(type: "INTEGER", nullable: false)
                    .Annotation("Sqlite:Autoincrement", true),
                external_client_id = table.Column<string>(type: "TEXT", maxLength: 64, nullable: false),
                phone = table.Column<string>(type: "TEXT", maxLength: 32, nullable: false),
                channel_id = table.Column<long>(type: "INTEGER", nullable: false),
                created_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false),
                updated_at_utc = table.Column<DateTime>(type: "TEXT", nullable: false),
                last_used_at_utc = table.Column<DateTime>(type: "TEXT", nullable: true)
            },
            constraints: table =>
            {
                table.PrimaryKey("PK_client_channel_bindings", x => x.id);
            });

        migrationBuilder.CreateIndex(
            name: "IX_client_channel_bindings_channel_id",
            table: "client_channel_bindings",
            column: "channel_id");

        migrationBuilder.CreateIndex(
            name: "IX_client_channel_bindings_external_client_id",
            table: "client_channel_bindings",
            column: "external_client_id",
            unique: true);

        migrationBuilder.CreateIndex(
            name: "IX_client_channel_bindings_phone",
            table: "client_channel_bindings",
            column: "phone");
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropTable(name: "client_channel_bindings");
    }
}
