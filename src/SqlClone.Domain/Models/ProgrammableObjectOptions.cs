namespace SqlClone.Domain.Models;

public sealed class ProgrammableObjectOptions
{
    /// <summary>
    /// When enabled, after migration + seed the clone queries the source for functions,
    /// views and stored procedures that the migrated schema is missing and creates them.
    /// This auto-captures any programmable-object drift that EF migrations do not produce.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Source database to read object definitions from. Defaults to Seed.SourceDatabase,
    /// then the first Restore.Databases entry.
    /// </summary>
    public string? SourceDatabase { get; set; }

    /// <summary>
    /// Cloned database the objects are created in. Defaults to the first Restore.Databases
    /// entry, then Seed.SourceDatabase.
    /// </summary>
    public string? TargetDatabase { get; set; }

    /// <summary>
    /// Schemas to skip (in addition to system schemas, which are always skipped).
    /// </summary>
    public List<string> ExcludeSchemas { get; set; } = ["HangFire"];
}
