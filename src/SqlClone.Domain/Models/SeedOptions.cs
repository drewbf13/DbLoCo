namespace SqlClone.Domain.Models;

public sealed class SeedOptions
{
    public string Strategy { get; set; } = SeedStrategy.BulkCopy;
    public string LinkedServerName { get; set; } = string.Empty;
    public bool Enabled { get; set; }
    public string SourceDatabase { get; set; } = string.Empty;
    public List<string> ExcludeSchemas { get; set; } = [];
    public List<string> InheritedParentFilterPriorityTables { get; set; } = [];
    public List<SeedTableOptions> Tables { get; set; } = [];
}

public static class SeedStrategy
{
    public const string BulkCopy = "BulkCopy";
    public const string LinkedServer = "LinkedServer";
}

public sealed class SeedTableOptions
{
    public string SourceDatabase { get; set; } = string.Empty;
    public string TargetDatabase { get; set; } = string.Empty;
    public string Schema { get; set; } = "dbo";
    public string Table { get; set; } = string.Empty;
    public bool TruncateTarget { get; set; }
    public int? LatestRows { get; set; }
    public string? LatestOrderBy { get; set; }
    public int Order { get; set; }
    public int GroupKey { get; set; }
    public List<SeedTableOptions> Children { get; set; } = [];
}
