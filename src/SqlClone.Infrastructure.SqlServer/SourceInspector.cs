using Microsoft.Extensions.Logging;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;
using System.Text.Json;

namespace SqlClone.Infrastructure.SqlServer;

public sealed class SourceInspector : ISourceInspector
{
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
        await connection.OpenAsync(cancellationToken);
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
        await connection.OpenAsync(cancellationToken);

        var tables = await GetUserTablesAsync(connection, cancellationToken);
        var foreignKeyDependencies = await GetForeignKeyDependenciesAsync(connection, cancellationToken);
        var dependencyGroups = TopologicalGroupTables(tables, foreignKeyDependencies);
        var domainGroupKeys = BuildDomainGroupKeys(tables);

        var seedTables = dependencyGroups
            .SelectMany((group, groupIndex) => group
                .OrderBy(table => table.Key, StringComparer.OrdinalIgnoreCase)
                .Select(table => new SeedTableOptions
                {
                    SourceDatabase = sourceDatabase,
                    TargetDatabase = targetDatabase,
                    Schema = table.Schema,
                    Table = table.Table,
                    TruncateTarget = truncateTarget,
                    GroupKey = domainGroupKeys[table.Schema],
                    Order = (groupIndex + 1) * 10
                }))
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

    private sealed record TableNode(string Schema, string Table)
    {
        public string Key => $"{Schema}.{Table}";
    }

    private sealed record ForeignKeyDependency(TableNode Parent, TableNode Referenced);
}
