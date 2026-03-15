namespace SqlClone.Domain.Models;

public sealed class ValidationResult
{
    public bool SqlReachable { get; set; }
    public Dictionary<string, bool> Databases { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    public Dictionary<string, bool> LinkedServers { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    public bool IsSuccessful => SqlReachable && Databases.Values.All(x => x) && LinkedServers.Values.All(x => x);
}

public sealed class SourceDatabaseInfo
{
    public required string Name { get; init; }
    public required DateTime CreateDateUtc { get; init; }
    public required string RecoveryModel { get; init; }
    public required string StateDescription { get; init; }
}
