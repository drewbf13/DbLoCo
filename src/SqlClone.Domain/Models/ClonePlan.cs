namespace SqlClone.Domain.Models;

public sealed class ClonePlan
{
    public required string EnvironmentName { get; init; }
    public required string Materializer { get; init; }
    public IReadOnlyList<DatabaseClonePlan> Databases { get; init; } = [];
    public IReadOnlyList<LinkedServerDefinition> LinkedServers { get; init; } = [];
    public MigrationPlan Migration { get; init; } = new();
    public IReadOnlyList<SeedTablePlan> SeedTables { get; init; } = [];
}

public sealed class DatabaseClonePlan
{
    public required string Name { get; init; }
}


public sealed class MigrationPlan
{
    public bool Enabled { get; init; }
    public string GitRepository { get; init; } = string.Empty;
    public string LocalRepositoryPath { get; init; } = string.Empty;
    public string Branch { get; init; } = string.Empty;
    public string BuildCommand { get; init; } = string.Empty;
    public string WorkingDirectory { get; init; } = string.Empty;
}

public sealed class SeedTablePlan
{
    public required string SourceDatabase { get; init; }
    public required string TargetDatabase { get; init; }
    public required string Schema { get; init; }
    public required string Table { get; init; }
    public bool TruncateTarget { get; init; }
    public int? LatestRows { get; init; }
    public string? LatestOrderBy { get; init; }
    public int Order { get; init; }
    public int GroupKey { get; init; }
}
