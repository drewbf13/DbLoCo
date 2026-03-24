using FluentAssertions;
using SqlClone.Domain.Models;
using SqlClone.Infrastructure.SqlServer;

namespace SqlClone.Tests;

public sealed class SqlTableSeederTests
{
    [Fact]
    public void SelectInheritedParentFilters_WhenWithinLimit_ReturnsOriginalFilters()
    {
        var filters = new List<SqlTableSeeder.InheritedParentFilter>
        {
            new("clause-a", "dbo.Account", 20),
            new("clause-b", "dbo.Customer", 10)
        };

        var selected = SqlTableSeeder.SelectInheritedParentFilters(filters, maxFilterCount: 4);

        selected.Should().Equal(filters);
    }

    [Fact]
    public void SelectInheritedParentFilters_WhenOverLimit_UsesPriorityThenParentName()
    {
        var filters = new List<SqlTableSeeder.InheritedParentFilter>
        {
            new("clause-a", "dbo.Zeta", 20),
            new("clause-b", "dbo.Alpha", 20),
            new("clause-c", "dbo.Beta", 15),
            new("clause-d", "dbo.Gamma", 10)
        };

        var selected = SqlTableSeeder.SelectInheritedParentFilters(filters, maxFilterCount: 2);

        selected.Should().Equal(
            new SqlTableSeeder.InheritedParentFilter("clause-b", "dbo.Alpha", 20),
            new SqlTableSeeder.InheritedParentFilter("clause-a", "dbo.Zeta", 20));
    }

    [Theory]
    [InlineData(SeedStrategy.LinkedServer, true)]
    [InlineData("linkedserver", true)]
    [InlineData("LINKEDSERVER", true)]
    [InlineData(SeedStrategy.BulkCopy, false)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void IsLinkedServerStrategy_ReturnsExpected(string? strategy, bool expected)
    {
        SqlTableSeeder.IsLinkedServerStrategy(strategy).Should().Be(expected);
    }

    [Fact]
    public void BuildLinkedServerOrderByClause_WhenLatestOrderByIsSet_ReturnsItDirectly()
    {
        var table = new SeedTablePlan
        {
            SourceDatabase = "src",
            TargetDatabase = "tgt",
            Schema = "dbo",
            Table = "Orders",
            LatestOrderBy = "CreatedAt DESC"
        };

        var clause = SqlTableSeeder.BuildLinkedServerOrderByClause(["Id", "CreatedAt"], [], table);

        clause.Should().Be("CreatedAt DESC");
    }

    [Fact]
    public void BuildLinkedServerOrderByClause_WhenPrimaryKeyColumnsProvided_UsesPKColumnsDesc()
    {
        var table = new SeedTablePlan
        {
            SourceDatabase = "src",
            TargetDatabase = "tgt",
            Schema = "dbo",
            Table = "Orders"
        };

        var clause = SqlTableSeeder.BuildLinkedServerOrderByClause(["Id", "Name"], ["Id"], table);

        clause.Should().Be("[Id] DESC");
    }

    [Fact]
    public void BuildLinkedServerOrderByClause_WhenCompositePrimaryKey_UsesPKColumnsDesc()
    {
        var table = new SeedTablePlan
        {
            SourceDatabase = "src",
            TargetDatabase = "tgt",
            Schema = "dbo",
            Table = "Orders"
        };

        var clause = SqlTableSeeder.BuildLinkedServerOrderByClause(["OrderId", "LineId", "Name"], ["OrderId", "LineId"], table);

        clause.Should().Be("[OrderId] DESC, [LineId] DESC");
    }

    [Fact]
    public void BuildLinkedServerOrderByClause_WhenNoPKAndNoLatestOrderBy_ReturnsFirstColumnDesc()
    {
        var table = new SeedTablePlan
        {
            SourceDatabase = "src",
            TargetDatabase = "tgt",
            Schema = "dbo",
            Table = "Orders"
        };

        var clause = SqlTableSeeder.BuildLinkedServerOrderByClause(["Id", "Name"], [], table);

        clause.Should().Be("[Id] DESC");
    }

    [Fact]
    public void BuildLinkedServerOrderByClause_WhenNoColumnsAndNoLatestOrderBy_ReturnsFallback()
    {
        var table = new SeedTablePlan
        {
            SourceDatabase = "src",
            TargetDatabase = "tgt",
            Schema = "dbo",
            Table = "Orders"
        };

        var clause = SqlTableSeeder.BuildLinkedServerOrderByClause([], [], table);

        clause.Should().Be("(SELECT NULL)");
    }

    [Fact]
    public void BuildLinkedServerOrderByClause_WhenColumnContainsBracket_EscapesIt()
    {
        var table = new SeedTablePlan
        {
            SourceDatabase = "src",
            TargetDatabase = "tgt",
            Schema = "dbo",
            Table = "Orders"
        };

        var clause = SqlTableSeeder.BuildLinkedServerOrderByClause(["Col]Name"], [], table);

        clause.Should().Be("[Col]]Name] DESC");
    }
}
