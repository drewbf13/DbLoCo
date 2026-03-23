using FluentAssertions;
using SqlClone.Infrastructure.SqlServer;

namespace SqlClone.Tests;

public sealed class SourceInspectorTests
{
    [Fact]
    public void BuildDescendingOrderByClause_WithCompositePrimaryKey_FormatsAllColumnsDescending()
    {
        var clause = SourceInspector.BuildDescendingOrderByClause(["TenantId", "OrderId"]);

        clause.Should().Be("[TenantId] DESC, [OrderId] DESC");
    }

    [Fact]
    public void BuildDescendingOrderByClause_EscapesClosingBracketsInIdentifiers()
    {
        var clause = SourceInspector.BuildDescendingOrderByClause(["Col]Name"]);

        clause.Should().Be("[Col]]Name] DESC");
    }
}
