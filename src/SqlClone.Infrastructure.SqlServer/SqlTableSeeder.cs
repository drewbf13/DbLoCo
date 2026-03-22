using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;
using System.Diagnostics;

namespace SqlClone.Infrastructure.SqlServer;

public sealed class SqlTableSeeder : ITableSeeder
{
    private const int MaxConcurrentSeedOperationsPerLevel = 8;
    private const int MetadataQueryTimeoutSeconds = 180;
    private const int SeedSourceQueryTimeoutSeconds = 600;
    private const int MaxSeedAttempts = 3;
    private const int BulkCopyProgressIntervalRows = 10_000;

    private readonly SqlConnectionFactory _connectionFactory;
    private readonly SqlExecutionHelper _sql;
    private readonly ILogger<SqlTableSeeder> _logger;

    public SqlTableSeeder(
        SqlConnectionFactory connectionFactory,
        SqlExecutionHelper sql,
        ILogger<SqlTableSeeder> logger)
    {
        _connectionFactory = connectionFactory;
        _sql = sql;
        _logger = logger;
    }

    public async Task SeedAsync(IReadOnlyList<SeedTablePlan> tables, CancellationToken cancellationToken)
    {
        var totalTables = tables.Count;
        var completedTables = 0;
        var failedTables = 0;
        var runningTables = 0;

        _logger.LogInformation("Starting seed for {TotalTables} table(s).", totalTables);

        foreach (var dependencyLevel in tables.GroupBy(table => table.Order > 0 ? table.Order : table.GroupKey).OrderBy(group => group.Key))
        {
            cancellationToken.ThrowIfCancellationRequested();

            using var concurrencyGate = new SemaphoreSlim(MaxConcurrentSeedOperationsPerLevel);
            var levelTasks = dependencyLevel
                .Select(table => SeedTableWithWarningThrottledAsync(table, concurrencyGate, cancellationToken))
                .ToArray();

            await Task.WhenAll(levelTasks);
        }

        _logger.LogInformation(
            "Seed finished. {Completed}/{Total} table(s) completed, {Failed} failed.",
            completedTables,
            totalTables,
            failedTables);

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
            SemaphoreSlim concurrencyGate,
            CancellationToken ct)
        {
            await concurrencyGate.WaitAsync(ct);
            Interlocked.Increment(ref runningTables);
            LogOverallProgress();

            try
            {
                var succeeded = await SeedTableWithWarningAsync(table, ct);
                if (!succeeded)
                {
                    Interlocked.Increment(ref failedTables);
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


    private async Task<bool> SeedTableWithWarningAsync(SeedTablePlan table, CancellationToken cancellationToken)
    {
        const bool allowRetry = true;

        for (var attempt = 1; attempt <= MaxSeedAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                await SeedTableAsync(table, cancellationToken);
                return true;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex) when (allowRetry && attempt < MaxSeedAttempts && SqlClientTransientRetry.IsTransientTransportError(ex))
            {
                SqlConnection.ClearAllPools();
                var delay = TimeSpan.FromSeconds(Math.Pow(2, attempt - 1));
                _logger.LogWarning(
                    ex,
                    "Transient transport failure while seeding {SourceDatabase}.{Schema}.{Table} -> {TargetDatabase}.{Schema}.{Table} (attempt {Attempt}/{MaxAttempts}). Retrying in {DelaySeconds}s.",
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

    private async Task SeedTableAsync(SeedTablePlan table, CancellationToken cancellationToken)
    {
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
        await SqlClientTransientRetry.OpenWithRetryAsync(source, MaxSeedAttempts, cancellationToken);
        await SqlClientTransientRetry.OpenWithRetryAsync(target, MaxSeedAttempts, cancellationToken);

        var computedColumns = await LoadComputedColumnListAsync(target, table, cancellationToken);
        var columnList = (await LoadColumnListAsync(source, table, cancellationToken))
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
        var sourceQuery = $"SELECT {escapedColumns} FROM {destinationName};";
        var expectedSourceRowCount = await LoadSourceRowCountAsync(source, destinationName, cancellationToken);
        await using var sourceCommand = source.CreateCommand();
        sourceCommand.CommandText = sourceQuery;
        sourceCommand.CommandTimeout = SeedSourceQueryTimeoutSeconds;

        await using var reader = await sourceCommand.ExecuteReaderAsync(cancellationToken);
        var primaryKeyColumns = await LoadPrimaryKeyColumnListAsync(target, table, cancellationToken);
        var identityColumns = await LoadIdentityColumnListAsync(target, table, cancellationToken);
        var nonNullableColumns = await LoadNonNullableColumnListAsync(target, table, cancellationToken);
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
                cancellationToken);

            using (var bulkCopy = CreateBulkCopy(target, tempTableName, columnList, includesIdentityColumns, rowsCopied =>
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
                await bulkCopy.WriteToServerAsync(reader, cancellationToken);
            }
            copiedRows = Math.Max(copiedRows, expectedSourceRowCount);

            var mergeSql = BuildMergeSql(destinationName, tempTableName, columnList, primaryKeyColumns, nonNullableColumns);
            if (includesIdentityColumns)
            {
                mergeSql = WrapWithIdentityInsert(destinationName, mergeSql);
            }

            await _sql.ExecuteNonQueryAsync(target, mergeSql, cancellationToken);
            await _sql.ExecuteNonQueryAsync(target, $"DROP TABLE {tempTableName};", cancellationToken);
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

                await _sql.ExecuteNonQueryAsync(target, $"TRUNCATE TABLE {destinationName};", cancellationToken);
            }

            using var bulkCopy = CreateBulkCopy(target, destinationName, columnList, includesIdentityColumns, rowsCopied =>
            {
                copiedRows = rowsCopied;
                _logger.LogInformation(
                    "Seeding progress {SourceDatabase}.{Schema}.{Table}: {RowsCopied} row(s) copied.",
                    table.SourceDatabase,
                    table.Schema,
                    table.Table,
                    rowsCopied);
            });
            await bulkCopy.WriteToServerAsync(reader, cancellationToken);
            copiedRows = Math.Max(copiedRows, expectedSourceRowCount);
        }

        stopwatch.Stop();
        _logger.LogInformation(
            "Seeded {SourceDatabase}.{Schema}.{Table} into {TargetDatabase}.{Schema}.{Table} in {ElapsedMs} ms ({RowsCopied} row(s) copied).",
            table.SourceDatabase,
            table.Schema,
            table.Table,
            table.TargetDatabase,
            table.Schema,
            table.Table,
            stopwatch.ElapsedMilliseconds,
            copiedRows);

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

    private static async Task<long> LoadSourceRowCountAsync(SqlConnection source, string destinationName, CancellationToken cancellationToken)
    {
        await using var rowCountCommand = source.CreateCommand();
        rowCountCommand.CommandText = $"SELECT COUNT_BIG(*) FROM {destinationName};";
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

    private static SqlBulkCopy CreateBulkCopy(
        SqlConnection target,
        string destination,
        IEnumerable<string> columns,
        bool keepIdentity,
        Action<long>? onRowsCopied = null)
    {
        var options = keepIdentity ? SqlBulkCopyOptions.KeepIdentity : SqlBulkCopyOptions.Default;
        var bulkCopy = new SqlBulkCopy(target, options, null)
        {
            DestinationTableName = destination,
            BulkCopyTimeout = 600,
            NotifyAfter = BulkCopyProgressIntervalRows
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

    private static string EscapeIdentifier(string value) => value.Replace("]", "]]", StringComparison.Ordinal);
}
