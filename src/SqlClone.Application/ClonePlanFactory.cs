using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;

namespace SqlClone.Application;

public sealed class ClonePlanFactory : IClonePlanFactory
{
    public ClonePlan CreatePlan(CloneOptions options, string? overrideEnvironment = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        var excludedSchemas = new HashSet<string>(
            options.Seed.ExcludeSchemas
                .Where(schema => !string.IsNullOrWhiteSpace(schema))
                .Select(schema => schema.Trim()),
            StringComparer.OrdinalIgnoreCase);

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
            SeedTables = FlattenSeedTables(options).Select(table => new SeedTablePlan
            {
                SourceDatabase = table.SourceDatabase,
                TargetDatabase = table.TargetDatabase,
                Schema = table.Schema,
                Table = table.Table,
                TruncateTarget = table.TruncateTarget,
                LatestRows = table.LatestRows,
                LatestOrderBy = table.LatestOrderBy,
                Order = table.Order,
                GroupKey = table.GroupKey
            })
            .Where(table =>
                !string.IsNullOrWhiteSpace(table.SourceDatabase)
                && !string.IsNullOrWhiteSpace(table.TargetDatabase)
                && !string.IsNullOrWhiteSpace(table.Table)
                && !excludedSchemas.Contains(table.Schema))
            .OrderBy(table => table.Order)
            .ThenBy(table => table.GroupKey)
            .ThenBy(table => table.Schema)
            .ThenBy(table => table.Table)
            .ToList()
        };
    }

    private static IReadOnlyList<ResolvedSeedTable> FlattenSeedTables(CloneOptions options)
    {
        if (!options.Seed.Enabled)
        {
            return [];
        }

        var resolvedTables = new List<ResolvedSeedTable>();

        foreach (var table in options.Seed.Tables)
        {
            FlattenSeedTableRecursive(
                table,
                parent: null,
                defaultSourceDatabase: options.Seed.SourceDatabase,
                defaultTargetDatabase: options.Seed.SourceDatabase,
                defaultOrder: table.Order,
                defaultGroupKey: table.GroupKey,
                resolvedTables);
        }

        return resolvedTables;
    }

    private static void FlattenSeedTableRecursive(
        SeedTableOptions table,
        ResolvedSeedTable? parent,
        string defaultSourceDatabase,
        string defaultTargetDatabase,
        int defaultOrder,
        int defaultGroupKey,
        ICollection<ResolvedSeedTable> destination)
    {
        var resolvedTable = new ResolvedSeedTable
        {
            SourceDatabase = ResolveValue(table.SourceDatabase, parent?.SourceDatabase, defaultSourceDatabase),
            TargetDatabase = ResolveValue(table.TargetDatabase, parent?.TargetDatabase, defaultTargetDatabase),
            Schema = ResolveValue(table.Schema, parent?.Schema, "dbo"),
            Table = table.Table,
            TruncateTarget = table.TruncateTarget,
            LatestRows = table.LatestRows is > 0 ? table.LatestRows : null,
            LatestOrderBy = table.LatestOrderBy,
            Order = table.Order > 0 ? table.Order : (parent?.Order ?? defaultOrder),
            GroupKey = table.GroupKey > 0 ? table.GroupKey : (parent?.GroupKey ?? (defaultGroupKey > 0 ? defaultGroupKey : 1))
        };

        destination.Add(resolvedTable);

        foreach (var child in table.Children)
        {
            FlattenSeedTableRecursive(
                child,
                resolvedTable,
                defaultSourceDatabase,
                defaultTargetDatabase,
                resolvedTable.Order,
                resolvedTable.GroupKey,
                destination);
        }
    }

    private static string ResolveValue(string? value, string? inheritedValue, string fallback) =>
        !string.IsNullOrWhiteSpace(value)
            ? value
            : !string.IsNullOrWhiteSpace(inheritedValue)
                ? inheritedValue
                : fallback;

    private sealed class ResolvedSeedTable
    {
        public required string SourceDatabase { get; init; }
        public required string TargetDatabase { get; init; }
        public required string Schema { get; init; }
        public required string Table { get; init; }
        public required bool TruncateTarget { get; init; }
        public required int? LatestRows { get; init; }
        public required string? LatestOrderBy { get; init; }
        public required int Order { get; init; }
        public required int GroupKey { get; init; }
    }
}
