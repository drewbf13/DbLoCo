using System.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;

namespace SqlClone.Infrastructure.SqlServer;

/// <summary>
/// Creates the source database's functions, views and stored procedures that the migrated
/// clone is missing. EF migrations build tables but do not emit these programmable objects
/// (they are mapped via [DbFunction]/ToView or called via EXEC), so this step queries the
/// source each run and recreates any that are absent — automatically catching migration drift.
/// </summary>
public sealed class SourceProgrammableObjectSynchronizer : IProgrammableObjectSynchronizer
{
    private const int MetadataQueryTimeoutSeconds = 180;
    private const int CreateTimeoutSeconds = 180;

    private readonly SqlConnectionFactory _connectionFactory;
    private readonly CloneOptions _options;
    private readonly ILogger<SourceProgrammableObjectSynchronizer> _logger;

    public SourceProgrammableObjectSynchronizer(
        SqlConnectionFactory connectionFactory,
        IOptions<CloneOptions> options,
        ILogger<SourceProgrammableObjectSynchronizer> logger)
    {
        _connectionFactory = connectionFactory;
        _options = options.Value;
        _logger = logger;
    }

    private sealed record DbObject(int Id, string Schema, string Name, string Type, string? Definition)
    {
        public string Key => $"{Schema}.{Name}";
        public string Kind => Type == "V" ? "view" : Type == "P" ? "procedure" : "function";
        public string DropKeyword => Type == "V" ? "VIEW" : Type == "P" ? "PROCEDURE" : "FUNCTION";
    }

    public async Task SynchronizeAsync(CancellationToken cancellationToken)
    {
        var settings = _options.ProgrammableObjects;
        if (!settings.Enabled)
        {
            _logger.LogInformation("Programmable-object sync disabled; skipping.");
            return;
        }

        var sourceDatabase = Resolve(settings.SourceDatabase, _options.Seed.SourceDatabase, _options.Restore.Databases);
        var targetDatabase = Resolve(settings.TargetDatabase, null, _options.Restore.Databases, _options.Seed.SourceDatabase);
        if (string.IsNullOrWhiteSpace(sourceDatabase) || string.IsNullOrWhiteSpace(targetDatabase))
        {
            _logger.LogWarning(
                "Skipping programmable-object sync: could not resolve source/target database. Set Clone:ProgrammableObjects:SourceDatabase and TargetDatabase.");
            return;
        }

        var excludedSchemas = new HashSet<string>(settings.ExcludeSchemas, StringComparer.OrdinalIgnoreCase) { "sys" };

        await using var source = _connectionFactory.CreateSourceConnection(sourceDatabase);
        await using var target = _connectionFactory.CreateTargetConnection(targetDatabase);
        await SqlClientTransientRetry.OpenWithRetryAsync(source, 3, cancellationToken);
        await SqlClientTransientRetry.OpenWithRetryAsync(target, 3, cancellationToken);

        var sourceObjects = (await LoadObjectsAsync(source, cancellationToken))
            .Where(o => !excludedSchemas.Contains(o.Schema) && o.Definition is not null)
            .ToList();
        var targetKeys = (await LoadObjectsAsync(target, cancellationToken))
            .Select(o => o.Key)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var missing = sourceObjects.Where(o => !targetKeys.Contains(o.Key)).ToList();
        if (missing.Count == 0)
        {
            _logger.LogInformation("Programmable-object sync: clone already has every source function/view/procedure.");
            return;
        }

        _logger.LogInformation(
            "Programmable-object sync: creating {Count} missing object(s) in {Database} ({Breakdown}).",
            missing.Count,
            targetDatabase,
            string.Join(", ", missing.GroupBy(o => o.Kind).Select(g => $"{g.Count()} {g.Key}(s)")));

        // Functions and views require their dependencies to exist at create time, so order them
        // topologically. Procedures use deferred name resolution and can be created in any order.
        var functionsAndViews = OrderByDependencies(
            missing.Where(o => o.Type != "P").ToList(),
            await LoadDependencyEdgesAsync(source, cancellationToken));
        var procedures = missing.Where(o => o.Type == "P").OrderBy(o => o.Key, StringComparer.OrdinalIgnoreCase);

        await ExecuteNonQueryAsync(target, "SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON;", cancellationToken);

        var created = 0;
        var failed = 0;
        foreach (var obj in functionsAndViews.Concat(procedures))
        {
            if (await TryCreateAsync(target, obj, cancellationToken))
            {
                created++;
            }
            else
            {
                failed++;
            }
        }

        if (failed > 0)
        {
            _logger.LogWarning(
                "Programmable-object sync finished: {Created} created, {Failed} failed (objects whose dependencies the migrated schema lacks).",
                created,
                failed);
        }
        else
        {
            _logger.LogInformation("Programmable-object sync finished: {Created} created.", created);
        }
    }

    private async Task<bool> TryCreateAsync(SqlConnection target, DbObject obj, CancellationToken cancellationToken)
    {
        try
        {
            await ExecuteNonQueryAsync(
                target,
                $"DROP {obj.DropKeyword} IF EXISTS [{Escape(obj.Schema)}].[{Escape(obj.Name)}];",
                cancellationToken);
            await ExecuteNonQueryAsync(target, obj.Definition!, cancellationToken);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning("Failed to create {Kind} {Schema}.{Name}: {Error}", obj.Kind, obj.Schema, obj.Name, ex.Message);
            // A failure (e.g. a command timeout) can break the connection; reopen it so one bad
            // object does not cascade into "connection is closed" failures for everything after it.
            await EnsureConnectionOpenAsync(target, cancellationToken);
            return false;
        }
    }

    private async Task EnsureConnectionOpenAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        if (connection.State == ConnectionState.Open)
        {
            return;
        }

        try
        {
            await connection.CloseAsync();
            await SqlClientTransientRetry.OpenWithRetryAsync(connection, 3, cancellationToken);
            await ExecuteNonQueryAsync(connection, "SET QUOTED_IDENTIFIER ON; SET ANSI_NULLS ON;", cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogWarning("Could not reopen the target connection after a failed object create: {Error}", ex.Message);
        }
    }

    private static async Task<List<DbObject>> LoadObjectsAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT o.object_id, s.name, o.name, o.type, OBJECT_DEFINITION(o.object_id)
FROM sys.objects o
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE o.is_ms_shipped = 0
  AND o.type IN ('FN','IF','TF','V','P');";

        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = MetadataQueryTimeoutSeconds;

        var objects = new List<DbObject>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            objects.Add(new DbObject(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.GetString(3).Trim(),
                reader.IsDBNull(4) ? null : reader.GetString(4)));
        }

        return objects;
    }

    private static async Task<List<(int From, int To)>> LoadDependencyEdgesAsync(SqlConnection source, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT DISTINCT d.referencing_id, d.referenced_id
FROM sys.sql_expression_dependencies d
WHERE d.referenced_id IS NOT NULL
  AND d.referencing_id <> d.referenced_id;";

        await using var command = source.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = MetadataQueryTimeoutSeconds;

        var edges = new List<(int From, int To)>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            // 'referenced' must be created before 'referencing'.
            edges.Add((reader.GetInt32(1), reader.GetInt32(0)));
        }

        return edges;
    }

    // Kahn topological sort; nodes with unresolved cycles are appended in a stable order.
    private static List<DbObject> OrderByDependencies(List<DbObject> objects, List<(int From, int To)> edges)
    {
        var byId = objects.ToDictionary(o => o.Id);
        var ids = byId.Keys.ToHashSet();
        var indegree = ids.ToDictionary(id => id, _ => 0);
        var adjacency = ids.ToDictionary(id => id, _ => new List<int>());

        foreach (var (from, to) in edges)
        {
            if (!ids.Contains(from) || !ids.Contains(to))
            {
                continue;
            }

            adjacency[from].Add(to);
            indegree[to]++;
        }

        var queue = new Queue<int>(indegree.Where(kv => kv.Value == 0).Select(kv => kv.Key)
            .OrderBy(id => byId[id].Key, StringComparer.OrdinalIgnoreCase));
        var ordered = new List<DbObject>();
        while (queue.Count > 0)
        {
            var id = queue.Dequeue();
            ordered.Add(byId[id]);
            foreach (var next in adjacency[id])
            {
                if (--indegree[next] == 0)
                {
                    queue.Enqueue(next);
                }
            }
        }

        ordered.AddRange(objects.Where(o => !ordered.Contains(o)));
        return ordered;
    }

    private static async Task ExecuteNonQueryAsync(SqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = CreateTimeoutSeconds;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string Resolve(params object?[] candidates)
    {
        foreach (var candidate in candidates)
        {
            switch (candidate)
            {
                case string s when !string.IsNullOrWhiteSpace(s):
                    return s;
                case IEnumerable<string> list:
                    var first = list.FirstOrDefault(item => !string.IsNullOrWhiteSpace(item));
                    if (!string.IsNullOrWhiteSpace(first))
                    {
                        return first;
                    }
                    break;
            }
        }

        return string.Empty;
    }

    private static string Escape(string value) => value.Replace("]", "]]", StringComparison.Ordinal);
}
