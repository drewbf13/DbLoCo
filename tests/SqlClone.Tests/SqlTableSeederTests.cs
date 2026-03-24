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
    public void BuildInheritedParentFilterPriorityLookup_NormalizesAndDropsWhitespaceEntries()
    {
        var lookup = SqlTableSeeder.BuildInheritedParentFilterPriorityLookup([
            " dbo.Customer ",
            "",
            "   ",
            "Orders"
        ]);

        lookup.Should().BeEquivalentTo(["dbo.Customer", "Orders"]);
    }

    [Theory]
    [InlineData("dbo", "Customer", true)]
    [InlineData("sales", "Orders", true)]
    [InlineData("dbo", "Invoice", false)]
    public void IsExplicitlyPrioritizedParent_MatchesSchemaQualifiedOrTableOnlyEntries(
        string schema,
        string table,
        bool expected)
    {
        var configured = SqlTableSeeder.BuildInheritedParentFilterPriorityLookup([
            "dbo.Customer",
            "Orders"
        ]);

        SqlTableSeeder.IsExplicitlyPrioritizedParent(schema, table, configured).Should().Be(expected);
    }

    [Fact]
    public void BuildSeedExecutionLevels_SplitsSameOrderAcrossDifferentGroups()
    {
        var tables = new[]
        {
            new SeedTablePlan { SourceDatabase = "AppDb", TargetDatabase = "AppDb", Schema = "dbo", Table = "A", Order = 10, GroupKey = 1 },
            new SeedTablePlan { SourceDatabase = "AppDb", TargetDatabase = "AppDb", Schema = "dbo", Table = "B", Order = 10, GroupKey = 2 }
        };

        var levels = SqlTableSeeder.BuildSeedExecutionLevels(tables);

        levels.Should().HaveCount(2);
        levels[0].OrderKey.Should().Be(10);
        levels[0].GroupKey.Should().Be(1);
        levels[0].Tables.Select(t => t.Table).Should().Equal("A");
        levels[1].OrderKey.Should().Be(10);
        levels[1].GroupKey.Should().Be(2);
        levels[1].Tables.Select(t => t.Table).Should().Equal("B");
    }

    [Fact]
    public void BuildSeedExecutionLevels_UsesGroupKeyAsFallbackOrderWhenOrderNotSet()
    {
        var tables = new[]
        {
            new SeedTablePlan { SourceDatabase = "AppDb", TargetDatabase = "AppDb", Schema = "dbo", Table = "A", Order = 0, GroupKey = 5 },
            new SeedTablePlan { SourceDatabase = "AppDb", TargetDatabase = "AppDb", Schema = "dbo", Table = "B", Order = 0, GroupKey = 2 }
        };

        var levels = SqlTableSeeder.BuildSeedExecutionLevels(tables);

        levels.Select(level => (level.OrderKey, level.GroupKey)).Should().Equal((2, 2), (5, 5));
    }
}
