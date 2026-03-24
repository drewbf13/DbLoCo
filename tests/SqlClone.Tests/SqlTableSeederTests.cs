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
}
