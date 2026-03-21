using Microsoft.Data.SqlClient;

namespace SqlClone.Infrastructure.SqlServer;

public sealed class SqlExecutionHelper
{
    private const int CommandTimeoutSeconds = 180;

    public async Task ExecuteNonQueryAsync(SqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = CommandTimeoutSeconds;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<T?> ExecuteScalarAsync<T>(SqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = CommandTimeoutSeconds;
        var result = await command.ExecuteScalarAsync(cancellationToken);
        if (result is null || result is DBNull)
        {
            return default;
        }

        return (T)Convert.ChangeType(result, typeof(T));
    }
}
