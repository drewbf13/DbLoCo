namespace SqlClone.Domain.Models;

public sealed class ClonePlan
{
    public required string EnvironmentName { get; init; }
    public required string Materializer { get; init; }
    public IReadOnlyList<DatabaseClonePlan> Databases { get; init; } = [];
    public IReadOnlyList<LinkedServerDefinition> LinkedServers { get; init; } = [];
}

public sealed class DatabaseClonePlan
{
    public required string Name { get; init; }
}
