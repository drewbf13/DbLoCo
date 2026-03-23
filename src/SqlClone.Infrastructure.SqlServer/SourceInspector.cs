using Microsoft.Extensions.Logging;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;
using System.Text.Json;

namespace SqlClone.Infrastructure.SqlServer;

public sealed class SourceInspector : ISourceInspector
{
    private const int MaxOpenAttempts = 3;
    private const int DefaultLatestRowsLimit = 10_000;
    private readonly SqlConnectionFactory _factory;
    private readonly ILogger<SourceInspector> _logger;

    private const string Sql = """
        SELECT name, create_date, recovery_model_desc, state_desc
        FROM sys.databases
        WHERE name NOT IN ('master', 'model', 'msdb', 'tempdb')
        ORDER BY name;
        """;

    public SourceInspector(SqlConnectionFactory factory, ILogger<SourceInspector> logger)
    {
        _factory = factory;
        _logger = logger;
    }

    public async Task<IReadOnlyList<SourceDatabaseInfo>> GetDatabasesAsync(CancellationToken cancellationToken)
    {
        var databases = new List<SourceDatabaseInfo>();

        await using var connection = _factory.CreateSourceConnection();
        await SqlClientTransientRetry.OpenWithRetryAsync(connection, MaxOpenAttempts, cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = Sql;

        _logger.LogInformation("Inspecting source SQL databases");
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            databases.Add(new SourceDatabaseInfo
            {
                Name = reader.GetString(0),
                CreateDateUtc = DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc),
                RecoveryModel = reader.GetString(2),
                StateDescription = reader.GetString(3)
            });
        }

        return databases;
    }

    public async Task<string> GenerateSeedConfigSectionAsync(
        string sourceDatabase,
        string targetDatabase,
        bool truncateTarget,
        CancellationToken cancellationToken)
    {
        await using var connection = _factory.CreateSourceConnection(sourceDatabase);
        await SqlClientTransientRetry.OpenWithRetryAsync(connection, MaxOpenAttempts, cancellationToken);

        var tables = await GetUserTablesAsync(connection, cancellationToken);
        var foreignKeyDependencies = await GetForeignKeyDependenciesAsync(connection, cancellationToken);
        var primaryKeyColumnsByTable = await GetPrimaryKeyColumnsByTableAsync(connection, cancellationToken);
        var dependencyGroups = TopologicalGroupTables(tables, foreignKeyDependencies);
        var domainGroupKeys = BuildDomainGroupKeys(tables);
        var orderByTableKey = BuildOrderByTableKey(dependencyGroups);
        var nestedTableNodes = BuildNestedSeedTableNodes(tables, foreignKeyDependencies, orderByTableKey);

        var seedTables = nestedTableNodes
            .Select(table => BuildSeedTableOptionsTree(
                table,
                sourceDatabase,
                targetDatabase,
                truncateTarget,
                primaryKeyColumnsByTable,
                domainGroupKeys,
                orderByTableKey,
                applyDefaultLimit: true))
            .ToList();

        var payload = new
        {
            Seed = new SeedOptions
            {
                Enabled = true,
                SourceDatabase = sourceDatabase,
                Tables = seedTables
            }
        };

        return JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
    }

    private static async Task<List<TableNode>> GetUserTablesAsync(Microsoft.Data.SqlClient.SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT s.name, t.name
            FROM sys.tables t
            INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE t.is_ms_shipped = 0
            ORDER BY s.name, t.name;
            """;

        var tables = new List<TableNode>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            tables.Add(new TableNode(reader.GetString(0), reader.GetString(1)));
        }

        return tables;
    }

    private static async Task<List<ForeignKeyDependency>> GetForeignKeyDependenciesAsync(Microsoft.Data.SqlClient.SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT
                ps.name AS ParentSchema,
                pt.name AS ParentTable,
                rs.name AS ReferencedSchema,
                rt.name AS ReferencedTable
            FROM sys.foreign_keys fk
            INNER JOIN sys.tables pt ON fk.parent_object_id = pt.object_id
            INNER JOIN sys.schemas ps ON pt.schema_id = ps.schema_id
            INNER JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
            INNER JOIN sys.schemas rs ON rt.schema_id = rs.schema_id;
            """;

        var dependencies = new List<ForeignKeyDependency>();
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            dependencies.Add(new ForeignKeyDependency(
                new TableNode(reader.GetString(0), reader.GetString(1)),
                new TableNode(reader.GetString(2), reader.GetString(3))));
        }

        return dependencies;
    }

    private static async Task<Dictionary<string, List<string>>> GetPrimaryKeyColumnsByTableAsync(
        Microsoft.Data.SqlClient.SqlConnection connection,
        CancellationToken cancellationToken)
    {
        const string sql = """
            SELECT
                c.TABLE_SCHEMA,
                c.TABLE_NAME,
                k.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
                ON c.CONSTRAINT_NAME = k.CONSTRAINT_NAME
               AND c.TABLE_SCHEMA = k.TABLE_SCHEMA
               AND c.TABLE_NAME = k.TABLE_NAME
            WHERE c.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, k.ORDINAL_POSITION;
            """;

        var primaryKeyColumnsByTable = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        await using var command = connection.CreateCommand();
        command.CommandText = sql;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var tableKey = $"{reader.GetString(0)}.{reader.GetString(1)}";
            if (!primaryKeyColumnsByTable.TryGetValue(tableKey, out var columns))
            {
                columns = [];
                primaryKeyColumnsByTable.Add(tableKey, columns);
            }

            columns.Add(reader.GetString(2));
        }

        return primaryKeyColumnsByTable;
    }

    private static string? BuildPrimaryKeyDescendingOrderByClause(
        IReadOnlyDictionary<string, List<string>> primaryKeyColumnsByTable,
        TableNode table)
    {
        if (!primaryKeyColumnsByTable.TryGetValue(table.Key, out var primaryKeyColumns) || primaryKeyColumns.Count == 0)
        {
            return null;
        }

        return BuildDescendingOrderByClause(primaryKeyColumns);
    }

    internal static string BuildDescendingOrderByClause(IEnumerable<string> columns)
        => string.Join(", ", columns.Select(column => $"[{EscapeIdentifier(column)}] DESC"));

    private static string EscapeIdentifier(string value)
        => value.Replace("]", "]]", StringComparison.Ordinal);

    private static List<List<TableNode>> TopologicalGroupTables(
        IReadOnlyList<TableNode> tables,
        IReadOnlyList<ForeignKeyDependency> dependencies)
    {
        var tableKeyLookup = tables.ToDictionary(t => t.Key, t => t, StringComparer.OrdinalIgnoreCase);
        var inDegree = tables.ToDictionary(t => t.Key, _ => 0, StringComparer.OrdinalIgnoreCase);
        var outgoing = tables.ToDictionary(t => t.Key, _ => new HashSet<string>(StringComparer.OrdinalIgnoreCase), StringComparer.OrdinalIgnoreCase);

        foreach (var dependency in dependencies)
        {
            var parentKey = dependency.Parent.Key;
            var referencedKey = dependency.Referenced.Key;
            if (!tableKeyLookup.ContainsKey(parentKey) || !tableKeyLookup.ContainsKey(referencedKey))
            {
                continue;
            }

            if (parentKey.Equals(referencedKey, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (outgoing[referencedKey].Add(parentKey))
            {
                inDegree[parentKey]++;
            }
        }

        var remainingKeys = new HashSet<string>(tableKeyLookup.Keys, StringComparer.OrdinalIgnoreCase);
        var ready = new SortedSet<string>(
            tables.Where(t => inDegree[t.Key] == 0).Select(t => t.Key),
            StringComparer.OrdinalIgnoreCase);

        var groups = new List<List<TableNode>>();

        while (ready.Count > 0)
        {
            var currentLevelKeys = ready.ToList();
            ready.Clear();

            var currentLevel = currentLevelKeys
                .Select(key => tableKeyLookup[key])
                .ToList();

            groups.Add(currentLevel);

            foreach (var currentKey in currentLevelKeys)
            {
                remainingKeys.Remove(currentKey);

                foreach (var dependentKey in outgoing[currentKey].OrderBy(k => k, StringComparer.OrdinalIgnoreCase))
                {
                    inDegree[dependentKey]--;
                    if (inDegree[dependentKey] == 0)
                    {
                        ready.Add(dependentKey);
                    }
                }
            }
        }

        if (remainingKeys.Count > 0)
        {
            // Cycles (including mutual references) cannot be fully topologically sorted.
            // Keep deterministic output and avoid parallel writes for unresolved tables.
            foreach (var unresolvedKey in remainingKeys.OrderBy(k => k, StringComparer.OrdinalIgnoreCase))
            {
                groups.Add([tableKeyLookup[unresolvedKey]]);
            }
        }

        return groups;
    }

    private static Dictionary<string, int> BuildDomainGroupKeys(IReadOnlyList<TableNode> tables)
    {
        return tables
            .Select(table => table.Schema)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(schema => schema, StringComparer.OrdinalIgnoreCase)
            .Select((schema, index) => new { schema, key = index + 1 })
            .ToDictionary(item => item.schema, item => item.key, StringComparer.OrdinalIgnoreCase);
    }

    private static Dictionary<string, int> BuildOrderByTableKey(IReadOnlyList<IReadOnlyList<TableNode>> dependencyGroups)
    {
        var orderByTableKey = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var groupIndex = 0; groupIndex < dependencyGroups.Count; groupIndex++)
        {
            var order = (groupIndex + 1) * 10;
            foreach (var table in dependencyGroups[groupIndex])
            {
                orderByTableKey[table.Key] = order;
            }
        }

        return orderByTableKey;
    }

    private static List<NestedTableNode> BuildNestedSeedTableNodes(
        IReadOnlyList<TableNode> tables,
        IReadOnlyList<ForeignKeyDependency> dependencies,
        IReadOnlyDictionary<string, int> orderByTableKey)
    {
        var tableByKey = tables.ToDictionary(table => table.Key, table => table, StringComparer.OrdinalIgnoreCase);
        var parentCandidatesByChild = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        var inboundReferenceCountByTable = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach (var dependency in dependencies)
        {
            if (!tableByKey.ContainsKey(dependency.Parent.Key) || !tableByKey.ContainsKey(dependency.Referenced.Key))
            {
                continue;
            }

            if (dependency.Parent.Key.Equals(dependency.Referenced.Key, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            inboundReferenceCountByTable[dependency.Referenced.Key] =
                inboundReferenceCountByTable.TryGetValue(dependency.Referenced.Key, out var currentInboundCount)
                    ? currentInboundCount + 1
                    : 1;

            if (!parentCandidatesByChild.TryGetValue(dependency.Parent.Key, out var candidates))
            {
                candidates = [];
                parentCandidatesByChild.Add(dependency.Parent.Key, candidates);
            }

            candidates.Add(dependency.Referenced.Key);
        }

        var selectedParentByChild = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (childKey, candidateParentKeys) in parentCandidatesByChild)
        {
            var child = tableByKey[childKey];
            var selectedParentKey = candidateParentKeys
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderByDescending(parentKey => ScoreParentTableAffinity(child, tableByKey[parentKey]))
                .ThenBy(parentKey => inboundReferenceCountByTable.TryGetValue(parentKey, out var count) ? count : 0)
                .ThenBy(parentKey => orderByTableKey.TryGetValue(parentKey, out var order) ? order : int.MaxValue)
                .ThenBy(parentKey => parentKey, StringComparer.OrdinalIgnoreCase)
                .First();
            selectedParentByChild[childKey] = selectedParentKey;
        }

        var childrenByParent = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        foreach (var (childKey, parentKey) in selectedParentByChild)
        {
            if (!childrenByParent.TryGetValue(parentKey, out var childList))
            {
                childList = [];
                childrenByParent.Add(parentKey, childList);
            }

            childList.Add(childKey);
        }

        var rootKeys = tables
            .Select(table => table.Key)
            .Where(key => !selectedParentByChild.ContainsKey(key))
            .OrderBy(key => orderByTableKey.TryGetValue(key, out var order) ? order : int.MaxValue)
            .ThenBy(key => key, StringComparer.OrdinalIgnoreCase)
            .ToList();

        return rootKeys
            .Select(rootKey => BuildNestedTree(rootKey, tableByKey, childrenByParent, orderByTableKey))
            .ToList();
    }

    private static int ScoreParentTableAffinity(TableNode child, TableNode candidateParent)
    {
        var childTable = child.Table;
        var parentTable = candidateParent.Table;
        var childTableNormalized = childTable.ToLowerInvariant();
        var parentTableNormalized = parentTable.ToLowerInvariant();

        var score = 0;

        if (childTableNormalized.StartsWith(parentTableNormalized + "_", StringComparison.Ordinal))
        {
            score += 100;
        }
        else if (childTableNormalized.StartsWith(parentTableNormalized, StringComparison.Ordinal))
        {
            score += 50;
        }
        else if (childTableNormalized.Contains("_" + parentTableNormalized + "_", StringComparison.Ordinal)
            || childTableNormalized.EndsWith("_" + parentTableNormalized, StringComparison.Ordinal))
        {
            score += 20;
        }

        if (child.Schema.Equals(candidateParent.Schema, StringComparison.OrdinalIgnoreCase))
        {
            score += 5;
        }

        return score;
    }

    private static NestedTableNode BuildNestedTree(
        string tableKey,
        IReadOnlyDictionary<string, TableNode> tableByKey,
        IReadOnlyDictionary<string, List<string>> childrenByParent,
        IReadOnlyDictionary<string, int> orderByTableKey)
    {
        var childNodes = childrenByParent.TryGetValue(tableKey, out var childKeys)
            ? childKeys
                .OrderBy(key => orderByTableKey.TryGetValue(key, out var order) ? order : int.MaxValue)
                .ThenBy(key => key, StringComparer.OrdinalIgnoreCase)
                .Select(childKey => BuildNestedTree(childKey, tableByKey, childrenByParent, orderByTableKey))
                .ToList()
            : [];

        return new NestedTableNode(tableByKey[tableKey], childNodes);
    }

    private static SeedTableOptions BuildSeedTableOptionsTree(
        NestedTableNode node,
        string sourceDatabase,
        string targetDatabase,
        bool truncateTarget,
        IReadOnlyDictionary<string, List<string>> primaryKeyColumnsByTable,
        IReadOnlyDictionary<string, int> domainGroupKeys,
        IReadOnlyDictionary<string, int> orderByTableKey,
        bool applyDefaultLimit)
    {
        return new SeedTableOptions
        {
            SourceDatabase = sourceDatabase,
            TargetDatabase = targetDatabase,
            Schema = node.Table.Schema,
            Table = node.Table.Table,
            TruncateTarget = truncateTarget,
            LatestRows = applyDefaultLimit ? DefaultLatestRowsLimit : null,
            LatestOrderBy = BuildPrimaryKeyDescendingOrderByClause(primaryKeyColumnsByTable, node.Table),
            GroupKey = domainGroupKeys[node.Table.Schema],
            Order = orderByTableKey[node.Table.Key],
            Children = node.Children
                .Select(child => BuildSeedTableOptionsTree(
                    child,
                    sourceDatabase,
                    targetDatabase,
                    truncateTarget,
                    primaryKeyColumnsByTable,
                    domainGroupKeys,
                    orderByTableKey,
                    applyDefaultLimit: false))
                .ToList()
        };
    }

    private sealed record TableNode(string Schema, string Table)
    {
        public string Key => $"{Schema}.{Table}";
    }

    private sealed record ForeignKeyDependency(TableNode Parent, TableNode Referenced);
    private sealed record NestedTableNode(TableNode Table, List<NestedTableNode> Children);
}
