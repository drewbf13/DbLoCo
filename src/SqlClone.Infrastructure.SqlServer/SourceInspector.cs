using Microsoft.Extensions.Logging;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;

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
}
