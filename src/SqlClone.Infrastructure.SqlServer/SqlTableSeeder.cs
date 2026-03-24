using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace SqlClone.Infrastructure.SqlServer;

public sealed class SqlTableSeeder : ITableSeeder
{
    private const int MaxConcurrentSeedOperationsPerLevel = 8;
    private const int MetadataQueryTimeoutSeconds = 180;
    private const int SeedSourceQueryTimeoutSeconds = 600;
    private const int MaxSeedAttempts = 3;
    private const int MaxInheritedParentFilters = 4;
    private const int BulkCopyProgressIntervalRows = 10_000;
    private const int BulkCopyTargetBatchBytes = 4 * 1024 * 1024;
    private const int BulkCopyMinBatchRows = 100;
    private const int BulkCopyMaxBatchRows = 10_000;
    private static readonly TimeSpan SeedAttemptTimeout = TimeSpan.FromMinutes(20);

    private readonly SqlConnectionFactory _connectionFactory;
    private readonly SqlExecutionHelper _sql;
    private readonly ILogger<SqlTableSeeder> _logger;
    private readonly SeedOptions _seedOptions;

    public SqlTableSeeder(
        SqlConnectionFactory connectionFactory,
        SqlExecutionHelper sql,
        ILogger<SqlTableSeeder> logger,
        IOptions<CloneOptions> options)
    {
        _connectionFactory = connectionFactory;
        _sql = sql;
        _logger = logger;
        _seedOptions = options.Value.Seed;
    }

    public async Task SeedAsync(IReadOnlyList<SeedTablePlan> tables, CancellationToken cancellationToken)
    {
        var totalTables = tables.Count;
        var tablePlanLookup = tables.ToDictionary(
            table => TableRefKey.Create(table.SourceDatabase, table.Schema, table.Table),
            table => table);
        var completedTables = 0;
        var failedTables = 0;
        var runningTables = 0;
        var failedTableNames = new ConcurrentBag<string>();

        _logger.LogInformation("Starting seed for {TotalTables} table(s).", totalTables);

        var dependencyLevels = tables
            .GroupBy(table => table.Order > 0 ? table.Order : table.GroupKey)
            .OrderBy(group => group.Key)
            .ToList();

        foreach (var dependencyLevel in dependencyLevels)
        {
            cancellationToken.ThrowIfCancellationRequested();

            _logger.LogInformation(
                "Starting seed dependency level {LevelKey} ({TableCount} table(s), max parallel {MaxParallel}).",
                dependencyLevel.Key,
                dependencyLevel.Count(),
                MaxConcurrentSeedOperationsPerLevel);

            using var concurrencyGate = new SemaphoreSlim(MaxConcurrentSeedOperationsPerLevel);
            var levelTasks = dependencyLevel
                .Select(table => SeedTableWithWarningThrottledAsync(table, tablePlanLookup, concurrencyGate, cancellationToken))
                .ToArray();

            await Task.WhenAll(levelTasks);

            _logger.LogInformation(
                "Completed seed dependency level {LevelKey} ({TableCount} table(s)).",
                dependencyLevel.Key,
                dependencyLevel.Count());
        }

        _logger.LogInformation(
            "Seed finished. {Completed}/{Total} table(s) completed, {Failed} failed.",
            completedTables,
            totalTables,
            failedTables);

        if (!failedTableNames.IsEmpty)
        {
            _logger.LogWarning(
                "Failed tables ({FailedCount}): {FailedTables}",
                failedTableNames.Count,
                string.Join(", ", failedTableNames.OrderBy(table => table, StringComparer.OrdinalIgnoreCase)));
        }

        void LogOverallProgress()
        {
            _logger.LogInformation(
                "Seed overall progress: {Completed}/{Total} complete, {Failed} failed, {Running} running.",
                completedTables,
                totalTables,
                failedTables,
                runningTables);
        }

        async Task SeedTableWithWarningThrottledAsync(
            SeedTablePlan table,
            IReadOnlyDictionary<TableRefKey, SeedTablePlan> planLookup,
            SemaphoreSlim concurrencyGate,
            CancellationToken ct)
        {
            await concurrencyGate.WaitAsync(ct);
            Interlocked.Increment(ref runningTables);
            LogOverallProgress();

            try
            {
                var succeeded = await SeedTableWithWarningAsync(table, planLookup, ct);
                if (!succeeded)
                {
                    Interlocked.Increment(ref failedTables);
                    failedTableNames.Add($"{table.TargetDatabase}.{table.Schema}.{table.Table}");
                }
            }
            finally
            {
                Interlocked.Decrement(ref runningTables);
                Interlocked.Increment(ref completedTables);
                LogOverallProgress();
                concurrencyGate.Release();
            }
        }
    }


    private async Task<bool> SeedTableWithWarningAsync(
        SeedTablePlan table,
        IReadOnlyDictionary<TableRefKey, SeedTablePlan> planLookup,
        CancellationToken cancellationToken)
    {
        const bool allowRetry = true;
        var maxInheritedParentFiltersForAttempt = MaxInheritedParentFilters;

        for (var attempt = 1; attempt <= MaxSeedAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                await SeedTableAsync(table, planLookup, maxInheritedParentFiltersForAttempt, cancellationToken);
                return true;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested && attempt < MaxSeedAttempts)
            {
                var delay = TimeSpan.FromSeconds(Math.Pow(2, attempt - 1));
                _logger.LogWarning(
                    "Seed attempt timed out for {SourceDatabase}.{Schema}.{Table} -> {TargetDatabase}.{Schema}.{Table} (attempt {Attempt}/{MaxAttempts}, timeout {TimeoutMinutes} min). Retrying in {DelaySeconds}s.",
                    table.SourceDatabase,
                    table.Schema,
                    table.Table,
                    table.TargetDatabase,
                    table.Schema,
                    table.Table,
                    attempt,
                    MaxSeedAttempts,
                    SeedAttemptTimeout.TotalMinutes,
                    delay.TotalSeconds);

                await Task.Delay(delay, cancellationToken);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex) when (allowRetry && attempt < MaxSeedAttempts && ShouldRetrySeedAttempt(ex))
            {
                SqlConnection.ClearAllPools();
                var delay = TimeSpan.FromSeconds(Math.Pow(2, attempt - 1));
                _logger.LogWarning(
                    ex,
                    "Transient seed failure while seeding {SourceDatabase}.{Schema}.{Table} -> {TargetDatabase}.{Schema}.{Table} (attempt {Attempt}/{MaxAttempts}). Retrying in {DelaySeconds}s.",
                    table.SourceDatabase,
                    table.Schema,
                    table.Table,
                    table.TargetDatabase,
                    table.Schema,
                    table.Table,
                    attempt,
                    MaxSeedAttempts,
                    delay.TotalSeconds);

                await Task.Delay(delay, cancellationToken);
            }
            catch (Exception ex) when (allowRetry && attempt < MaxSeedAttempts && IsForeignKeyConstraintViolation(ex))
            {
                maxInheritedParentFiltersForAttempt = Math.Min(
                    int.MaxValue,
                    maxInheritedParentFiltersForAttempt + MaxInheritedParentFilters);
                _logger.LogWarning(
                    ex,
                    "Foreign key constraint violation while seeding {SourceDatabase}.{Schema}.{Table} -> {TargetDatabase}.{Schema}.{Table} (attempt {Attempt}/{MaxAttempts}). Retrying with inherited parent filter limit increased to {MaxInheritedParentFilters}.",
                    table.SourceDatabase,
                    table.Schema,
                    table.Table,
                    table.TargetDatabase,
                    table.Schema,
                    table.Table,
                    attempt,
                    MaxSeedAttempts,
                    maxInheritedParentFiltersForAttempt);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(
                    ex,
                    "Skipping table {SourceDatabase}.{Schema}.{Table} -> {TargetDatabase}.{Schema}.{Table} after seed failure.",
                    table.SourceDatabase,
                    table.Schema,
                    table.Table,
                    table.TargetDatabase,
                    table.Schema,
                    table.Table);
                return false;
            }
        }

        if (allowRetry)
        {
            _logger.LogWarning(
                "Skipping table {SourceDatabase}.{Schema}.{Table} -> {TargetDatabase}.{Schema}.{Table} after exhausting retry attempts.",
                table.SourceDatabase,
                table.Schema,
                table.Table,
                table.TargetDatabase,
                table.Schema,
                table.Table);
        }

        return false;
    }

    private static bool ShouldRetrySeedAttempt(Exception exception) =>
        SqlClientTransientRetry.IsTransientTransportError(exception)
        || SqlClientTransientRetry.IsTransientSqlError(exception);

    private static bool IsForeignKeyConstraintViolation(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is SqlException sqlException && sqlException.Number == 547)
            {
                return true;
            }
        }

        return false;
    }

    private async Task SeedTableAsync(
        SeedTablePlan table,
        IReadOnlyDictionary<TableRefKey, SeedTablePlan> planLookup,
        int maxInheritedParentFilters,
        CancellationToken cancellationToken)
    {
        if (UsesLinkedServerSeedStrategy())
        {
            await SeedTableViaLinkedServerAsync(table, cancellationToken);
            return;
        }

        using var attemptTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        attemptTimeout.CancelAfter(SeedAttemptTimeout);
        var effectiveCancellationToken = attemptTimeout.Token;

        var stopwatch = Stopwatch.StartNew();
        long copiedRows = 0;

        _logger.LogInformation(
            "Starting seed for {SourceDatabase}.{Schema}.{Table} -> {TargetDatabase}.{Schema}.{Table}.",
            table.SourceDatabase,
            table.Schema,
            table.Table,
            table.TargetDatabase,
            table.Schema,
            table.Table);

        var escapedSchema = EscapeIdentifier(table.Schema);
        var escapedTable = EscapeIdentifier(table.Table);
        var destinationName = $"[{escapedSchema}].[{escapedTable}]";

        await using var source = _connectionFactory.CreateSourceConnection(table.SourceDatabase);
        await using var target = _connectionFactory.CreateTargetConnection(table.TargetDatabase);
        await SqlClientTransientRetry.OpenWithRetryAsync(source, MaxSeedAttempts, effectiveCancellationToken);
        await SqlClientTransientRetry.OpenWithRetryAsync(target, MaxSeedAttempts, effectiveCancellationToken);

        var computedColumns = await LoadComputedColumnListAsync(target, table, effectiveCancellationToken);
        var columnList = (await LoadColumnListAsync(source, table, effectiveCancellationToken))
            .Where(c => !computedColumns.Contains(c, StringComparer.OrdinalIgnoreCase))
            .ToList();
        if (columnList.Count == 0)
        {
            if (computedColumns.Count > 0)
            {
                throw new InvalidOperationException(
                    $"No writable columns found for source table {table.SourceDatabase}.{table.Schema}.{table.Table} after excluding computed columns.");
            }

            throw new InvalidOperationException(
                $"No columns found for source table {table.SourceDatabase}.{table.Schema}.{table.Table}");
        }

        var escapedColumns = string.Join(", ", columnList.Select(c => $"[{EscapeIdentifier(c)}]"));
        var selectionCache = new Dictionary<TableRefKey, TableSelectionQuery>();
        var sourceSelection = await BuildTableSelectionQueryAsync(
            source,
            table,
            planLookup,
            selectionCache,
            new HashSet<TableRefKey>(),
            maxInheritedParentFilters,
            effectiveCancellationToken);
        var sourceQuery = $"SELECT {escapedColumns} FROM ({sourceSelection.Sql}) AS [src];";
        var estimatedRowSizeBytes = await EstimateRowSizeBytesAsync(source, table, columnList, effectiveCancellationToken);
        var bulkCopyBatchSize = ComputeBulkCopyBatchSize(estimatedRowSizeBytes);
        var expectedSourceRowCount = await LoadSourceRowCountAsync(source, sourceSelection.Sql, effectiveCancellationToken);
        await using var sourceCommand = source.CreateCommand();
        sourceCommand.CommandText = sourceQuery;
        sourceCommand.CommandTimeout = SeedSourceQueryTimeoutSeconds;

        await using var reader = await sourceCommand.ExecuteReaderAsync(effectiveCancellationToken);
        var primaryKeyColumns = await LoadPrimaryKeyColumnListAsync(target, table, effectiveCancellationToken);
        var identityColumns = await LoadIdentityColumnListAsync(target, table, effectiveCancellationToken);
        var nonNullableColumns = await LoadNonNullableColumnListAsync(target, table, effectiveCancellationToken);
        var includesIdentityColumns = columnList.Any(c => identityColumns.Contains(c, StringComparer.OrdinalIgnoreCase));
        var missingPrimaryKeyColumns = primaryKeyColumns
            .Where(pk => !columnList.Contains(pk, StringComparer.OrdinalIgnoreCase))
            .ToList();
        var useMergeStrategy = table.TruncateTarget
            && primaryKeyColumns.Count > 0
            && missingPrimaryKeyColumns.Count == 0;

        if (useMergeStrategy)
        {
            var tempTableName = $"#Seed_{Guid.NewGuid():N}";
            await _sql.ExecuteNonQueryAsync(
                target,
                $"SELECT TOP (0) {escapedColumns} INTO {tempTableName} FROM {destinationName};",
                effectiveCancellationToken);

            using (var bulkCopy = CreateBulkCopy(target, tempTableName, columnList, includesIdentityColumns, bulkCopyBatchSize, rowsCopied =>
                   {
                       copiedRows = rowsCopied;
                       _logger.LogInformation(
                           "Seeding progress {SourceDatabase}.{Schema}.{Table}: {RowsCopied} row(s) copied.",
                           table.SourceDatabase,
                           table.Schema,
                           table.Table,
                           rowsCopied);
                   }))
            {
                await bulkCopy.WriteToServerAsync(reader, effectiveCancellationToken);
            }
            copiedRows = Math.Max(copiedRows, expectedSourceRowCount);

            var mergeSql = BuildMergeSql(destinationName, tempTableName, columnList, primaryKeyColumns, nonNullableColumns);
            if (includesIdentityColumns)
            {
                mergeSql = WrapWithIdentityInsert(destinationName, mergeSql);
            }

            await _sql.ExecuteNonQueryAsync(target, mergeSql, effectiveCancellationToken);
            await _sql.ExecuteNonQueryAsync(target, $"DROP TABLE {tempTableName};", effectiveCancellationToken);
        }
        else
        {
            if (table.TruncateTarget)
            {
                if (primaryKeyColumns.Count == 0)
                {
                    _logger.LogWarning(
                        "Table {TargetDatabase}.{Schema}.{Table} requested truncate-style seed but has no primary key. Falling back to TRUNCATE + INSERT.",
                        table.TargetDatabase,
                        table.Schema,
                        table.Table);
                }
                else if (missingPrimaryKeyColumns.Count > 0)
                {
                    _logger.LogWarning(
                        "Table {TargetDatabase}.{Schema}.{Table} requested truncate-style seed but filtered columns excluded primary key columns ({PrimaryKeyColumns}). Falling back to TRUNCATE + INSERT.",
                        table.TargetDatabase,
                        table.Schema,
                        table.Table,
                        string.Join(", ", missingPrimaryKeyColumns.OrderBy(c => c, StringComparer.OrdinalIgnoreCase)));
                }

                await _sql.ExecuteNonQueryAsync(target, $"TRUNCATE TABLE {destinationName};", effectiveCancellationToken);
            }

            using var bulkCopy = CreateBulkCopy(target, destinationName, columnList, includesIdentityColumns, bulkCopyBatchSize, rowsCopied =>
            {
                copiedRows = rowsCopied;
                _logger.LogInformation(
                    "Seeding progress {SourceDatabase}.{Schema}.{Table}: {RowsCopied} row(s) copied.",
                    table.SourceDatabase,
                    table.Schema,
                    table.Table,
                    rowsCopied);
            });
            await bulkCopy.WriteToServerAsync(reader, effectiveCancellationToken);
            copiedRows = Math.Max(copiedRows, expectedSourceRowCount);
        }

        stopwatch.Stop();
        _logger.LogInformation(
            "Seeded {SourceDatabase}.{Schema}.{Table} into {TargetDatabase}.{Schema}.{Table} in {ElapsedMs} ms ({RowsCopied} row(s) copied, est row size {EstimatedRowBytes} B, batch size {BatchSize}).",
            table.SourceDatabase,
            table.Schema,
            table.Table,
            table.TargetDatabase,
            table.Schema,
            table.Table,
            stopwatch.ElapsedMilliseconds,
            copiedRows,
            estimatedRowSizeBytes,
            bulkCopyBatchSize);

        if (computedColumns.Count > 0)
        {
            _logger.LogInformation(
                "Skipped computed columns for {TargetDatabase}.{Schema}.{Table}: {Columns}",
                table.TargetDatabase,
                table.Schema,
                table.Table,
                string.Join(", ", computedColumns.OrderBy(c => c, StringComparer.OrdinalIgnoreCase)));
        }
    }

    private bool UsesLinkedServerSeedStrategy() =>
        IsLinkedServerStrategy(_seedOptions.Strategy);

    internal static bool IsLinkedServerStrategy(string? strategy) =>
        string.Equals(strategy, SeedStrategy.LinkedServer, StringComparison.OrdinalIgnoreCase);

    private async Task SeedTableViaLinkedServerAsync(
        SeedTablePlan table,
        CancellationToken cancellationToken)
    {
        using var attemptTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        attemptTimeout.CancelAfter(SeedAttemptTimeout);
        var effectiveCancellationToken = attemptTimeout.Token;

        var linkedServerName = _seedOptions.LinkedServerName;
        if (string.IsNullOrWhiteSpace(linkedServerName))
        {
            throw new InvalidOperationException(
                $"Seed strategy is '{SeedStrategy.LinkedServer}' but Seed.LinkedServerName is not configured.");
        }

        var stopwatch = Stopwatch.StartNew();
        _logger.LogInformation(
            "Starting linked-server seed for {SourceDatabase}.{Schema}.{Table} -> {TargetDatabase}.{Schema}.{Table} via {LinkedServer}.",
            table.SourceDatabase,
            table.Schema,
            table.Table,
            table.TargetDatabase,
            table.Schema,
            table.Table,
            linkedServerName);

        await using var target = _connectionFactory.CreateTargetConnection(table.TargetDatabase);
        await SqlClientTransientRetry.OpenWithRetryAsync(target, MaxSeedAttempts, effectiveCancellationToken);

        var computedColumns = await LoadComputedColumnListAsync(target, table, effectiveCancellationToken);
        var targetColumns = await LoadColumnListAsync(target, table, effectiveCancellationToken);
        var sourceColumns = await LoadColumnListFromLinkedServerAsync(target, table, linkedServerName, effectiveCancellationToken);

        var columnList = targetColumns
            .Where(column => sourceColumns.Contains(column, StringComparer.OrdinalIgnoreCase))
            .Where(column => !computedColumns.Contains(column, StringComparer.OrdinalIgnoreCase))
            .ToList();

        if (columnList.Count == 0)
        {
            throw new InvalidOperationException(
                $"No compatible writable columns found while seeding {table.SourceDatabase}.{table.Schema}.{table.Table} via linked server '{linkedServerName}'.");
        }

        var escapedSchema = EscapeIdentifier(table.Schema);
        var escapedTable = EscapeIdentifier(table.Table);
        var destinationName = $"[{escapedSchema}].[{escapedTable}]";
        var sourceName = $"[{EscapeIdentifier(linkedServerName)}].[{EscapeIdentifier(table.SourceDatabase)}].[{escapedSchema}].[{escapedTable}]";
        var escapedColumns = string.Join(", ", columnList.Select(c => $"[{EscapeIdentifier(c)}]"));
        var primaryKeyColumns = await LoadPrimaryKeyColumnListAsync(target, table, effectiveCancellationToken);
        var orderByClause = BuildLinkedServerOrderByClause(columnList, primaryKeyColumns, table);
        var sourceSelectionSql = table.LatestRows is > 0
            ? $"SELECT TOP ({table.LatestRows.Value}) {escapedColumns} FROM {sourceName} ORDER BY {orderByClause}"
            : $"SELECT {escapedColumns} FROM {sourceName}";

        var stageTableName = $"#Seed_{Guid.NewGuid():N}";
        var stageAndCountSql = $@"
SELECT {escapedColumns}
INTO {stageTableName}
FROM ({sourceSelectionSql}) AS [src];
SELECT COUNT_BIG(*) FROM {stageTableName};";
        var copiedRows = await ExecuteScalarInt64Async(target, stageAndCountSql, SeedSourceQueryTimeoutSeconds, effectiveCancellationToken);

        if (table.TruncateTarget)
        {
            await _sql.ExecuteNonQueryAsync(target, $"TRUNCATE TABLE {destinationName};", effectiveCancellationToken);
        }

        var identityColumns = await LoadIdentityColumnListAsync(target, table, effectiveCancellationToken);
        var includesIdentityColumns = columnList.Any(c => identityColumns.Contains(c, StringComparer.OrdinalIgnoreCase));
        var insertSql = $@"
INSERT INTO {destinationName} ({escapedColumns})
SELECT {escapedColumns}
FROM {stageTableName};";

        if (includesIdentityColumns)
        {
            insertSql = WrapWithIdentityInsert(destinationName, insertSql);
        }

        await _sql.ExecuteNonQueryAsync(target, insertSql, effectiveCancellationToken);
        await _sql.ExecuteNonQueryAsync(target, $"DROP TABLE {stageTableName};", effectiveCancellationToken);

        stopwatch.Stop();
        _logger.LogInformation(
            "Linked-server seeded {SourceDatabase}.{Schema}.{Table} into {TargetDatabase}.{Schema}.{Table} in {ElapsedMs} ms ({RowsCopied} row(s) copied, strategy {Strategy}, linked server {LinkedServer}).",
            table.SourceDatabase,
            table.Schema,
            table.Table,
            table.TargetDatabase,
            table.Schema,
            table.Table,
            stopwatch.ElapsedMilliseconds,
            copiedRows,
            SeedStrategy.LinkedServer,
            linkedServerName);

        if (computedColumns.Count > 0)
        {
            _logger.LogInformation(
                "Skipped computed columns for {TargetDatabase}.{Schema}.{Table}: {Columns}",
                table.TargetDatabase,
                table.Schema,
                table.Table,
                string.Join(", ", computedColumns.OrderBy(c => c, StringComparer.OrdinalIgnoreCase)));
        }
    }

    private static async Task<List<string>> LoadColumnListAsync(SqlConnection source, SeedTablePlan table, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table
ORDER BY ORDINAL_POSITION;";

        await using var command = source.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = MetadataQueryTimeoutSeconds;
        command.Parameters.AddWithValue("@schema", table.Schema);
        command.Parameters.AddWithValue("@table", table.Table);

        var columns = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add(reader.GetString(0));
        }

        return columns;
    }

    private static async Task<List<string>> LoadColumnListFromLinkedServerAsync(
        SqlConnection target,
        SeedTablePlan table,
        string linkedServerName,
        CancellationToken cancellationToken)
    {
        var sql = $@"
SELECT COLUMN_NAME
FROM [{EscapeIdentifier(linkedServerName)}].[{EscapeIdentifier(table.SourceDatabase)}].INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @schema
  AND TABLE_NAME = @table
ORDER BY ORDINAL_POSITION;";

        await using var command = target.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = MetadataQueryTimeoutSeconds;
        command.Parameters.AddWithValue("@schema", table.Schema);
        command.Parameters.AddWithValue("@table", table.Table);

        var columns = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add(reader.GetString(0));
        }

        return columns;
    }

    internal static string BuildLinkedServerOrderByClause(
        IReadOnlyCollection<string> selectedColumns,
        IReadOnlyCollection<string> primaryKeyColumns,
        SeedTablePlan table)
    {
        if (!string.IsNullOrWhiteSpace(table.LatestOrderBy))
        {
            return table.LatestOrderBy!;
        }

        if (primaryKeyColumns.Count > 0)
        {
            return string.Join(", ", primaryKeyColumns.Select(column => $"[{EscapeIdentifier(column)}] DESC"));
        }

        return selectedColumns
            .Select(column => $"[{EscapeIdentifier(column)}] DESC")
            .FirstOrDefault() ?? "(SELECT NULL)";
    }

    private static async Task<long> ExecuteScalarInt64Async(
        SqlConnection connection,
        string sql,
        int timeoutSeconds,
        CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = timeoutSeconds;
        var result = await command.ExecuteScalarAsync(cancellationToken);
        if (result is null || result is DBNull)
        {
            return 0L;
        }

        return Convert.ToInt64(result);
    }

    private static async Task<long> LoadSourceRowCountAsync(
        SqlConnection source,
        string sourceSelectionSql,
        CancellationToken cancellationToken)
    {
        await using var rowCountCommand = source.CreateCommand();
        rowCountCommand.CommandText = $"SELECT COUNT_BIG(*) FROM ({sourceSelectionSql}) AS [src];";
        rowCountCommand.CommandTimeout = SeedSourceQueryTimeoutSeconds;

        var rowCount = await rowCountCommand.ExecuteScalarAsync(cancellationToken);
        if (rowCount is null || rowCount == DBNull.Value)
        {
            return 0;
        }

        return Convert.ToInt64(rowCount);
    }

    private static async Task<List<string>> LoadPrimaryKeyColumnListAsync(SqlConnection target, SeedTablePlan table, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT k.COLUMN_NAME
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k
    ON c.CONSTRAINT_NAME = k.CONSTRAINT_NAME
   AND c.TABLE_SCHEMA = k.TABLE_SCHEMA
   AND c.TABLE_NAME = k.TABLE_NAME
WHERE c.CONSTRAINT_TYPE = 'PRIMARY KEY'
  AND c.TABLE_SCHEMA = @schema
  AND c.TABLE_NAME = @table
ORDER BY k.ORDINAL_POSITION;";

        await using var command = target.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = MetadataQueryTimeoutSeconds;
        command.Parameters.AddWithValue("@schema", table.Schema);
        command.Parameters.AddWithValue("@table", table.Table);

        var columns = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add(reader.GetString(0));
        }

        return columns;
    }

    private static async Task<List<string>> LoadIdentityColumnListAsync(SqlConnection target, SeedTablePlan table, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT c.name
FROM sys.identity_columns c
INNER JOIN sys.tables t ON c.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @schema
  AND t.name = @table;";

        await using var command = target.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = MetadataQueryTimeoutSeconds;
        command.Parameters.AddWithValue("@schema", table.Schema);
        command.Parameters.AddWithValue("@table", table.Table);

        var columns = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add(reader.GetString(0));
        }

        return columns;
    }

    private static async Task<List<string>> LoadComputedColumnListAsync(SqlConnection target, SeedTablePlan table, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT c.name
FROM sys.columns c
INNER JOIN sys.tables t ON c.object_id = t.object_id
INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @schema
  AND t.name = @table
  AND c.is_computed = 1;";

        await using var command = target.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = MetadataQueryTimeoutSeconds;
        command.Parameters.AddWithValue("@schema", table.Schema);
        command.Parameters.AddWithValue("@table", table.Table);

        var columns = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add(reader.GetString(0));
        }

        return columns;
    }

    private static async Task<List<string>> LoadNonNullableColumnListAsync(SqlConnection target, SeedTablePlan table, CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @schema
  AND TABLE_NAME = @table
  AND IS_NULLABLE = 'NO';";

        await using var command = target.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = MetadataQueryTimeoutSeconds;
        command.Parameters.AddWithValue("@schema", table.Schema);
        command.Parameters.AddWithValue("@table", table.Table);

        var columns = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            columns.Add(reader.GetString(0));
        }

        return columns;
    }

    private async Task<TableSelectionQuery> BuildTableSelectionQueryAsync(
        SqlConnection source,
        SeedTablePlan table,
        IReadOnlyDictionary<TableRefKey, SeedTablePlan> planLookup,
        IDictionary<TableRefKey, TableSelectionQuery> selectionCache,
        ISet<TableRefKey> recursionStack,
        int maxInheritedParentFilters,
        CancellationToken cancellationToken)
    {
        var tableKey = TableRefKey.Create(table.SourceDatabase, table.Schema, table.Table);
        if (selectionCache.TryGetValue(tableKey, out var cachedSelection))
        {
            return cachedSelection;
        }

        if (!recursionStack.Add(tableKey))
        {
            throw new InvalidOperationException(
                $"Detected cyclic dependency while building recursive seed filter for {table.SourceDatabase}.{table.Schema}.{table.Table}.");
        }

        try
        {
            var sourceTableName = $"[{EscapeIdentifier(table.Schema)}].[{EscapeIdentifier(table.Table)}]";
            var fkRelationships = await LoadForeignKeyRelationshipsAsync(source, table, cancellationToken);
            var nullableChildColumns = await LoadNullableColumnLookupAsync(source, table, cancellationToken);
            var inheritedClauses = new List<InheritedParentFilter>();

            foreach (var relationship in fkRelationships)
            {
                var parentKey = TableRefKey.Create(table.SourceDatabase, relationship.ReferencedSchema, relationship.ReferencedTable);
                if (!planLookup.TryGetValue(parentKey, out var parentPlan))
                {
                    continue;
                }

                var parentSelection = await BuildTableSelectionQueryAsync(
                    source,
                    parentPlan,
                    planLookup,
                    selectionCache,
                    recursionStack,
                    maxInheritedParentFilters,
                    cancellationToken);

                if (!parentSelection.IsConstrained)
                {
                    continue;
                }

                var parentColumns = string.Join(", ", relationship.ColumnPairs.Select(pair => $"[parent].[{EscapeIdentifier(pair.ParentColumn)}]"));
                var joinPredicate = string.Join(
                    " AND ",
                    relationship.ColumnPairs.Select(pair =>
                        $"[keys].[{EscapeIdentifier(pair.ParentColumn)}] = [src].[{EscapeIdentifier(pair.ChildColumn)}]"));
                var nullableBypassPredicate = string.Join(
                    " OR ",
                    relationship.ColumnPairs
                        .Where(pair => nullableChildColumns.Contains(pair.ChildColumn))
                        .Select(pair => $"[src].[{EscapeIdentifier(pair.ChildColumn)}] IS NULL"));

                var existsClause =
                    $@"EXISTS (
    SELECT 1
    FROM (
        SELECT DISTINCT {parentColumns}
        FROM ({parentSelection.Sql}) AS [parent]
    ) AS [keys]
    WHERE {joinPredicate}
)";

                var clause = string.IsNullOrWhiteSpace(nullableBypassPredicate)
                    ? existsClause
                    : $"({nullableBypassPredicate} OR {existsClause})";

                inheritedClauses.Add(new InheritedParentFilter(
                    clause,
                    $"{relationship.ReferencedSchema}.{relationship.ReferencedTable}",
                    ComputeInheritedFilterPriorityScore(
                        table,
                        relationship,
                        nullableChildColumns)));
            }

            var selectedInheritedClauses = SelectInheritedParentFilters(inheritedClauses, maxInheritedParentFilters);
            if (inheritedClauses.Count > selectedInheritedClauses.Count)
            {
                _logger.LogWarning(
                    "Selection query for {SourceDatabase}.{Schema}.{Table} has {TotalFilters} constrained parent filter(s). Limiting to {SelectedFilters} to avoid recursive filter explosion. Selected parent tables: {SelectedParents}.",
                    table.SourceDatabase,
                    table.Schema,
                    table.Table,
                    inheritedClauses.Count,
                    selectedInheritedClauses.Count,
                    string.Join(", ", selectedInheritedClauses.Select(filter => filter.ParentTable)));
            }

            var whereClause = selectedInheritedClauses.Count == 0
                ? string.Empty
                : $"{Environment.NewLine}WHERE {string.Join($"{Environment.NewLine}  AND ", selectedInheritedClauses.Select(filter => filter.SqlClause))}";
            var filteredSourceSql = $"SELECT * FROM {sourceTableName} AS [src]{whereClause}";

            var hasLocalLimit = table.LatestRows is > 0;
            var selectionSql = filteredSourceSql;
            if (hasLocalLimit)
            {
                var orderByClause = await ResolveLatestOrderByClauseAsync(source, table, cancellationToken);
                selectionSql = $"SELECT TOP ({table.LatestRows!.Value}) * FROM ({filteredSourceSql}) AS [limited] ORDER BY {orderByClause}";
            }

            var selection = new TableSelectionQuery(selectionSql, hasLocalLimit || selectedInheritedClauses.Count > 0);
            selectionCache[tableKey] = selection;
            return selection;
        }
        finally
        {
            recursionStack.Remove(tableKey);
        }
    }

    private static async Task<IReadOnlyList<ForeignKeyRelationship>> LoadForeignKeyRelationshipsAsync(
        SqlConnection source,
        SeedTablePlan table,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT fk.name,
       rs.name AS referenced_schema,
       rt.name AS referenced_table,
       pc.name AS child_column,
       rc.name AS parent_column
FROM sys.foreign_keys fk
INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
INNER JOIN sys.tables pt ON fk.parent_object_id = pt.object_id
INNER JOIN sys.schemas ps ON pt.schema_id = ps.schema_id
INNER JOIN sys.columns pc ON pc.object_id = pt.object_id AND pc.column_id = fkc.parent_column_id
INNER JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
INNER JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
INNER JOIN sys.columns rc ON rc.object_id = rt.object_id AND rc.column_id = fkc.referenced_column_id
WHERE ps.name = @schema
  AND pt.name = @table
ORDER BY fk.name, fkc.constraint_column_id;";

        await using var command = source.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = MetadataQueryTimeoutSeconds;
        command.Parameters.AddWithValue("@schema", table.Schema);
        command.Parameters.AddWithValue("@table", table.Table);

        var relationships = new Dictionary<string, ForeignKeyRelationship>(StringComparer.OrdinalIgnoreCase);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var fkName = reader.GetString(0);
            if (!relationships.TryGetValue(fkName, out var relationship))
            {
                relationship = new ForeignKeyRelationship(reader.GetString(1), reader.GetString(2), []);
                relationships.Add(fkName, relationship);
            }

            relationship.ColumnPairs.Add(new ForeignKeyColumnPair(reader.GetString(3), reader.GetString(4)));
        }

        return relationships.Values.ToList();
    }

    private static async Task<IReadOnlySet<string>> LoadNullableColumnLookupAsync(
        SqlConnection source,
        SeedTablePlan table,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @schema
  AND TABLE_NAME = @table
  AND IS_NULLABLE = 'YES';";

        await using var command = source.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = MetadataQueryTimeoutSeconds;
        command.Parameters.AddWithValue("@schema", table.Schema);
        command.Parameters.AddWithValue("@table", table.Table);

        var nullableColumns = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            nullableColumns.Add(reader.GetString(0));
        }

        return nullableColumns;
    }

    private static async Task<string> ResolveLatestOrderByClauseAsync(
        SqlConnection source,
        SeedTablePlan table,
        CancellationToken cancellationToken)
    {
        if (!string.IsNullOrWhiteSpace(table.LatestOrderBy))
        {
            return table.LatestOrderBy!;
        }

        var primaryKeys = await LoadPrimaryKeyColumnListAsync(source, table, cancellationToken);
        if (primaryKeys.Count > 0)
        {
            return string.Join(", ", primaryKeys.Select(column => $"[{EscapeIdentifier(column)}] DESC"));
        }

        var columns = await LoadColumnListAsync(source, table, cancellationToken);
        if (columns.Count > 0)
        {
            return $"[{EscapeIdentifier(columns[0])}] DESC";
        }

        throw new InvalidOperationException(
            $"LatestRows configured for {table.SourceDatabase}.{table.Schema}.{table.Table} but no columns were found to order by.");
    }

    private static SqlBulkCopy CreateBulkCopy(
        SqlConnection target,
        string destination,
        IEnumerable<string> columns,
        bool keepIdentity,
        int batchSize,
        Action<long>? onRowsCopied = null)
    {
        var options = keepIdentity ? SqlBulkCopyOptions.KeepIdentity : SqlBulkCopyOptions.Default;
        var bulkCopy = new SqlBulkCopy(target, options, null)
        {
            DestinationTableName = destination,
            BulkCopyTimeout = 600,
            NotifyAfter = BulkCopyProgressIntervalRows,
            BatchSize = batchSize
        };

        if (onRowsCopied is not null)
        {
            bulkCopy.SqlRowsCopied += (_, e) => onRowsCopied(e.RowsCopied);
        }

        foreach (var col in columns)
        {
            bulkCopy.ColumnMappings.Add(col, col);
        }

        return bulkCopy;
    }

    private static async Task<int> EstimateRowSizeBytesAsync(
        SqlConnection source,
        SeedTablePlan table,
        IReadOnlyCollection<string> selectedColumns,
        CancellationToken cancellationToken)
    {
        const string sql = @"
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @schema
  AND TABLE_NAME = @table;";

        await using var command = source.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = MetadataQueryTimeoutSeconds;
        command.Parameters.AddWithValue("@schema", table.Schema);
        command.Parameters.AddWithValue("@table", table.Table);

        var selectedColumnLookup = new HashSet<string>(selectedColumns, StringComparer.OrdinalIgnoreCase);
        var estimatedRowSizeBytes = 0;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var columnName = reader.GetString(0);
            if (!selectedColumnLookup.Contains(columnName))
            {
                continue;
            }

            var dataType = reader.GetString(1);
            int? characterMaximumLength = reader.IsDBNull(2) ? null : reader.GetInt32(2);
            byte? numericPrecision = reader.IsDBNull(3) ? null : reader.GetByte(3);

            estimatedRowSizeBytes += EstimateColumnSizeBytes(dataType, characterMaximumLength, numericPrecision);
        }

        return Math.Max(estimatedRowSizeBytes, 1);
    }

    private static int ComputeBulkCopyBatchSize(int estimatedRowSizeBytes)
    {
        var dynamicBatchSize = BulkCopyTargetBatchBytes / Math.Max(estimatedRowSizeBytes, 1);
        return Math.Clamp(dynamicBatchSize, BulkCopyMinBatchRows, BulkCopyMaxBatchRows);
    }

    private static int EstimateColumnSizeBytes(string dataType, int? characterMaximumLength, byte? numericPrecision)
    {
        var normalizedType = dataType.Trim().ToLowerInvariant();
        return normalizedType switch
        {
            "tinyint" => 1,
            "smallint" => 2,
            "int" => 4,
            "bigint" => 8,
            "bit" => 1,
            "real" => 4,
            "float" => 8,
            "money" => 8,
            "smallmoney" => 4,
            "date" => 3,
            "smalldatetime" => 4,
            "datetime" => 8,
            "datetime2" => 8,
            "datetimeoffset" => 10,
            "time" => 5,
            "uniqueidentifier" => 16,
            "char" => Math.Max(characterMaximumLength ?? 1, 1),
            "nchar" => Math.Max((characterMaximumLength ?? 1) * 2, 2),
            "varchar" => EstimateVariableLengthBytes(characterMaximumLength, isUnicode: false),
            "nvarchar" => EstimateVariableLengthBytes(characterMaximumLength, isUnicode: true),
            "binary" => Math.Max(characterMaximumLength ?? 1, 1),
            "varbinary" => EstimateVariableLengthBytes(characterMaximumLength, isUnicode: false),
            "xml" => 2_048,
            "decimal" or "numeric" => EstimateDecimalBytes(numericPrecision),
            _ => 64
        };
    }

    private static int EstimateVariableLengthBytes(int? characterMaximumLength, bool isUnicode)
    {
        const int assumedVariableLengthBytes = 256;
        var maxLength = characterMaximumLength.GetValueOrDefault(-1);
        if (maxLength <= 0)
        {
            return isUnicode ? assumedVariableLengthBytes * 2 : assumedVariableLengthBytes;
        }

        return isUnicode ? maxLength * 2 : maxLength;
    }

    private static int EstimateDecimalBytes(byte? numericPrecision)
    {
        var precision = numericPrecision.GetValueOrDefault(18);
        return precision switch
        {
            <= 9 => 5,
            <= 19 => 9,
            <= 28 => 13,
            _ => 17
        };
    }

    private static string BuildMergeSql(
        string destinationName,
        string sourceName,
        IReadOnlyList<string> columnList,
        IReadOnlyList<string> primaryKeyColumns,
        IReadOnlyList<string> nonNullableColumns)
    {
        static string Bracket(string column) => $"[{EscapeIdentifier(column)}]";

        var onClause = string.Join(
            " AND ",
            primaryKeyColumns.Select(c => $"target.{Bracket(c)} = source.{Bracket(c)}"));

        var nonPrimaryKeyColumns = columnList.Where(c => !primaryKeyColumns.Contains(c, StringComparer.OrdinalIgnoreCase)).ToList();
        var updateClause = nonPrimaryKeyColumns.Count > 0
            ? $"WHEN MATCHED THEN UPDATE SET {string.Join(", ", nonPrimaryKeyColumns.Select(c => $"target.{Bracket(c)} = COALESCE(source.{Bracket(c)}, target.{Bracket(c)})"))}"
            : string.Empty;

        var requiredInsertColumns = columnList
            .Where(c => nonNullableColumns.Contains(c, StringComparer.OrdinalIgnoreCase))
            .ToList();
        var insertPredicate = requiredInsertColumns.Count > 0
            ? $" AND {string.Join(" AND ", requiredInsertColumns.Select(c => $"source.{Bracket(c)} IS NOT NULL"))}"
            : string.Empty;

        var insertColumns = string.Join(", ", columnList.Select(Bracket));
        var insertValues = string.Join(", ", columnList.Select(c => $"source.{Bracket(c)}"));

        return $@"
MERGE {destinationName} AS target
USING {sourceName} AS source
    ON {onClause}
{updateClause}
WHEN NOT MATCHED BY TARGET{insertPredicate} THEN
    INSERT ({insertColumns})
    VALUES ({insertValues});";
    }

    private static string WrapWithIdentityInsert(string destinationName, string sql) =>
        $@"
SET IDENTITY_INSERT {destinationName} ON;
{sql}
SET IDENTITY_INSERT {destinationName} OFF;";

    private sealed record TableRefKey(string Database, string Schema, string Table)
    {
        public static TableRefKey Create(string database, string schema, string table) =>
            new(database.Trim(), schema.Trim(), table.Trim());
    }

    private sealed record ForeignKeyRelationship(
        string ReferencedSchema,
        string ReferencedTable,
        List<ForeignKeyColumnPair> ColumnPairs);

    private sealed record ForeignKeyColumnPair(string ChildColumn, string ParentColumn);
    internal sealed record InheritedParentFilter(string SqlClause, string ParentTable, int PriorityScore);
    private sealed record TableSelectionQuery(string Sql, bool IsConstrained);

    private static string EscapeIdentifier(string value) => value.Replace("]", "]]", StringComparison.Ordinal);

    internal static IReadOnlyList<InheritedParentFilter> SelectInheritedParentFilters(
        IReadOnlyList<InheritedParentFilter> filters,
        int maxFilterCount)
    {
        if (filters.Count <= maxFilterCount)
        {
            return filters;
        }

        return filters
            .OrderByDescending(filter => filter.PriorityScore)
            .ThenBy(filter => filter.ParentTable, StringComparer.OrdinalIgnoreCase)
            .Take(maxFilterCount)
            .ToList();
    }

    private static int ComputeInheritedFilterPriorityScore(
        SeedTablePlan childTable,
        ForeignKeyRelationship relationship,
        IReadOnlySet<string> nullableChildColumns)
    {
        var score = 0;
        var childTableName = childTable.Table.ToLowerInvariant();
        var parentTableName = relationship.ReferencedTable.ToLowerInvariant();

        if (childTableName.StartsWith(parentTableName, StringComparison.Ordinal)
            || childTableName.Contains("_" + parentTableName + "_", StringComparison.Ordinal)
            || childTableName.EndsWith("_" + parentTableName, StringComparison.Ordinal))
        {
            score += 40;
        }

        score += relationship.ColumnPairs.Count * 10;
        if (relationship.ColumnPairs.All(pair => !nullableChildColumns.Contains(pair.ChildColumn)))
        {
            score += 10;
        }

        if (childTable.Schema.Equals(relationship.ReferencedSchema, StringComparison.OrdinalIgnoreCase))
        {
            score += 5;
        }

        return score;
    }
}
