namespace SqlClone.Domain.Models;

public sealed class SeedOptions
{
    public bool Enabled { get; set; }
    public string SourceDatabase { get; set; } = string.Empty;
    public List<SeedTableOptions> Tables { get; set; } = [];
}

public sealed class SeedTableOptions
{
    public string SourceDatabase { get; set; } = string.Empty;
    public string TargetDatabase { get; set; } = string.Empty;
    public string Schema { get; set; } = "dbo";
    public string Table { get; set; } = string.Empty;
    public bool TruncateTarget { get; set; }
}
