using Microsoft.Extensions.Logging;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;

namespace SqlClone.Infrastructure.SqlServer;

public sealed class CreateEmptyDatabaseMaterializer : IDatabaseMaterializer
{
    private readonly SqlConnectionFactory _factory;
    private readonly SqlExecutionHelper _helper;
    private readonly ILogger<CreateEmptyDatabaseMaterializer> _logger;

    public CreateEmptyDatabaseMaterializer(SqlConnectionFactory factory, SqlExecutionHelper helper, ILogger<CreateEmptyDatabaseMaterializer> logger)
    {
        _factory = factory;
        _helper = helper;
        _logger = logger;
    }

    public async Task MaterializeAsync(DatabaseClonePlan plan, CancellationToken cancellationToken)
    {
        await using var connection = _factory.CreateTargetConnection();
        await connection.OpenAsync(cancellationToken);

        var escaped = plan.Name.Replace("]", "]]", StringComparison.Ordinal);
        var sql = $"""
            IF DB_ID(N'{plan.Name.Replace("'", "''", StringComparison.Ordinal)}') IS NULL
            BEGIN
                CREATE DATABASE [{escaped}];
            END
            """;

        await _helper.ExecuteNonQueryAsync(connection, sql, cancellationToken);
        _logger.LogInformation("Ensured database {Database} exists", plan.Name);
    }
}
