using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;

namespace SqlClone.Infrastructure.SqlServer;

public sealed class SqlTableSeeder : ITableSeeder
{
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
        foreach (var table in tables)
        {
            await SeedTableAsync(table, cancellationToken);
        }
    }

    private async Task SeedTableAsync(SeedTablePlan table, CancellationToken cancellationToken)
    {
        var escapedSchema = EscapeIdentifier(table.Schema);
        var escapedTable = EscapeIdentifier(table.Table);
        var destinationName = $"[{escapedSchema}].[{escapedTable}]";

        await using var source = _connectionFactory.CreateSourceConnection(table.SourceDatabase);
        await using var target = _connectionFactory.CreateTargetConnection(table.TargetDatabase);
        await source.OpenAsync(cancellationToken);
        await target.OpenAsync(cancellationToken);

        var columnList = await LoadColumnListAsync(source, table, cancellationToken);
        if (columnList.Count == 0)
        {
            throw new InvalidOperationException($"No columns found for source table {table.SourceDatabase}.{table.Schema}.{table.Table}");
        }

        var escapedColumns = string.Join(", ", columnList.Select(c => $"[{EscapeIdentifier(c)}]"));

        if (table.TruncateTarget)
        {
            await _sql.ExecuteNonQueryAsync(target, $"TRUNCATE TABLE {destinationName};", cancellationToken);
        }

        var sourceQuery = $"SELECT {escapedColumns} FROM {destinationName};";
        await using var sourceCommand = source.CreateCommand();
        sourceCommand.CommandText = sourceQuery;
        sourceCommand.CommandTimeout = 300;

        await using var reader = await sourceCommand.ExecuteReaderAsync(cancellationToken);
        using var bulkCopy = new SqlBulkCopy(target)
        {
            DestinationTableName = destinationName,
            BulkCopyTimeout = 600
        };

        foreach (var col in columnList)
        {
            bulkCopy.ColumnMappings.Add(col, col);
        }

        await bulkCopy.WriteToServerAsync(reader, cancellationToken);

        _logger.LogInformation(
            "Seeded {Database}.{Schema}.{Table} into {TargetDatabase}.{Schema}.{Table}",
            table.SourceDatabase,
            table.Schema,
            table.Table,
            table.TargetDatabase,
            table.Schema,
            table.Table);
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

    private static string EscapeIdentifier(string value) => value.Replace("]", "]]", StringComparison.Ordinal);
}
