using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;

namespace SqlClone.Application;

public sealed class ClonePlanFactory : IClonePlanFactory
{
    public ClonePlan CreatePlan(CloneOptions options, string? overrideEnvironment = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new ClonePlan
        {
            EnvironmentName = overrideEnvironment ?? options.EnvironmentName,
            Materializer = options.Restore.Materializer,
            Databases = options.Restore.Databases.Select(name => new DatabaseClonePlan { Name = name }).ToList(),
            LinkedServers = options.LinkedServers.Definitions.ToList(),
            Migration = new MigrationPlan
            {
                Enabled = options.Migration.Enabled,
                GitRepository = options.Migration.GitRepository,
                LocalRepositoryPath = options.Migration.LocalRepositoryPath,
                Branch = options.Migration.Branch,
                BuildCommand = options.Migration.BuildCommand,
                WorkingDirectory = options.Migration.WorkingDirectory
            },
            SeedTables = (options.Seed.Enabled ? options.Seed.Tables : []).Select(table => new SeedTablePlan
            {
                SourceDatabase = string.IsNullOrWhiteSpace(table.SourceDatabase) ? options.Seed.SourceDatabase : table.SourceDatabase,
                TargetDatabase = string.IsNullOrWhiteSpace(table.TargetDatabase) ? options.Seed.SourceDatabase : table.TargetDatabase,
                Schema = string.IsNullOrWhiteSpace(table.Schema) ? "dbo" : table.Schema,
                Table = table.Table,
                TruncateTarget = table.TruncateTarget,
                LatestRows = table.LatestRows is > 0 ? table.LatestRows : null,
                LatestOrderBy = table.LatestOrderBy,
                Order = table.Order,
                GroupKey = table.GroupKey > 0 ? table.GroupKey : 1
            })
            .Where(table => !string.IsNullOrWhiteSpace(table.SourceDatabase) && !string.IsNullOrWhiteSpace(table.TargetDatabase) && !string.IsNullOrWhiteSpace(table.Table))
            .OrderBy(table => table.Order)
            .ThenBy(table => table.GroupKey)
            .ThenBy(table => table.Schema)
            .ThenBy(table => table.Table)
            .ToList()
        };
    }
}
