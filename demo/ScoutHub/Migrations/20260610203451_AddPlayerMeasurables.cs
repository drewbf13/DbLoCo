using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace ScoutHub.Migrations
{
    /// <inheritdoc />
    public partial class AddPlayerMeasurables : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<int>(
                name: "HeightInches",
                table: "Players",
                type: "int",
                nullable: true);

            migrationBuilder.AddColumn<int>(
                name: "WeightPounds",
                table: "Players",
                type: "int",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "HeightInches",
                table: "Players");

            migrationBuilder.DropColumn(
                name: "WeightPounds",
                table: "Players");
        }
    }
}
