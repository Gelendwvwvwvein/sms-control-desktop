using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Collector.Data.Migrations;

[DbContext(typeof(AppDbContext))]
[Migration("202602250001_TemplateOverdueAndComments")]
public partial class TemplateOverdueAndComments : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.AddColumn<string>(
            name: "overdue_mode",
            table: "templates",
            type: "TEXT",
            maxLength: 16,
            nullable: false,
            defaultValue: "range");

        migrationBuilder.AddColumn<int>(
            name: "overdue_from_days",
            table: "templates",
            type: "INTEGER",
            nullable: true);

        migrationBuilder.AddColumn<int>(
            name: "overdue_to_days",
            table: "templates",
            type: "INTEGER",
            nullable: true);

        migrationBuilder.AddColumn<int>(
            name: "overdue_exact_day",
            table: "templates",
            type: "INTEGER",
            nullable: true);

        migrationBuilder.AddColumn<bool>(
            name: "auto_assign",
            table: "templates",
            type: "INTEGER",
            nullable: false,
            defaultValue: true);

        migrationBuilder.AddColumn<string>(
            name: "comment_text",
            table: "templates",
            type: "TEXT",
            nullable: true);

        migrationBuilder.Sql(
            """
            UPDATE templates
            SET
                overdue_mode = 'range',
                overdue_exact_day = NULL,
                overdue_from_days = CASE lower(kind)
                    WHEN 'sms1' THEN 3
                    WHEN 'sms1_regular' THEN 3
                    WHEN 'sms2' THEN 6
                    WHEN 'sms3' THEN 21
                    WHEN 'ka1' THEN 30
                    WHEN 'ka2' THEN 46
                    WHEN 'ka_final' THEN 51
                    ELSE 0
                END,
                overdue_to_days = CASE lower(kind)
                    WHEN 'sms1' THEN 5
                    WHEN 'sms1_regular' THEN 5
                    WHEN 'sms2' THEN 20
                    WHEN 'sms3' THEN 29
                    WHEN 'ka1' THEN 45
                    WHEN 'ka2' THEN 50
                    WHEN 'ka_final' THEN 59
                    ELSE 3650
                END,
                auto_assign = CASE lower(kind)
                    WHEN 'sms1_regular' THEN 0
                    ELSE 1
                END,
                comment_text = CASE lower(kind)
                    WHEN 'sms1' THEN 'смс2'
                    WHEN 'sms1_regular' THEN 'смс2'
                    WHEN 'sms2' THEN 'смс2'
                    WHEN 'sms3' THEN 'смс3'
                    WHEN 'ka1' THEN 'смс от ка'
                    WHEN 'ka2' THEN 'смс ка{n}'
                    WHEN 'ka_final' THEN 'смс ка фин'
                    ELSE ''
                END
            ;
            """);

        migrationBuilder.CreateIndex(
            name: "IX_templates_status_auto_assign",
            table: "templates",
            columns: new[] { "status", "auto_assign" });
    }

    protected override void Down(MigrationBuilder migrationBuilder)
    {
        migrationBuilder.DropIndex(
            name: "IX_templates_status_auto_assign",
            table: "templates");

        migrationBuilder.DropColumn(
            name: "overdue_mode",
            table: "templates");

        migrationBuilder.DropColumn(
            name: "overdue_from_days",
            table: "templates");

        migrationBuilder.DropColumn(
            name: "overdue_to_days",
            table: "templates");

        migrationBuilder.DropColumn(
            name: "overdue_exact_day",
            table: "templates");

        migrationBuilder.DropColumn(
            name: "auto_assign",
            table: "templates");

        migrationBuilder.DropColumn(
            name: "comment_text",
            table: "templates");
    }
}
