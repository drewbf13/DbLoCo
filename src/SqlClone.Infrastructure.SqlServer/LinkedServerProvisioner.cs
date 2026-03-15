using Microsoft.Extensions.Logging;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;

namespace SqlClone.Infrastructure.SqlServer;

public sealed class LinkedServerProvisioner : ILinkedServerProvisioner
{
    private readonly SqlConnectionFactory _factory;
    private readonly SqlExecutionHelper _helper;
    private readonly ILogger<LinkedServerProvisioner> _logger;

    public LinkedServerProvisioner(SqlConnectionFactory factory, SqlExecutionHelper helper, ILogger<LinkedServerProvisioner> logger)
    {
        _factory = factory;
        _helper = helper;
        _logger = logger;
    }

    public async Task ApplyAsync(IReadOnlyList<LinkedServerDefinition> linkedServers, CancellationToken cancellationToken)
    {
        if (linkedServers.Count == 0)
        {
            _logger.LogInformation("No linked server definitions configured");
            return;
        }

        await using var connection = _factory.CreateTargetConnection();
        await connection.OpenAsync(cancellationToken);

        foreach (var server in linkedServers)
        {
            var sql = BuildSql(server);
            await _helper.ExecuteNonQueryAsync(connection, sql, cancellationToken);
            _logger.LogInformation("Applied linked server {Name}", server.Name);
        }
    }

    private static string BuildSql(LinkedServerDefinition linkedServer)
    {
        static string B(bool value) => value ? "true" : "false";
        string esc(string value) => value.Replace("'", "''", StringComparison.Ordinal);

        return $"""
            IF NOT EXISTS (SELECT 1 FROM sys.servers WHERE name = N'{esc(linkedServer.Name)}')
            BEGIN
                EXEC master.dbo.sp_addlinkedserver
                    @server = N'{esc(linkedServer.Name)}',
                    @srvproduct = N'{esc(linkedServer.Product)}',
                    @provider = N'{esc(linkedServer.Provider)}',
                    @datasrc = N'{esc(linkedServer.DataSource)}',
                    @catalog = {(linkedServer.Catalog is null ? "NULL" : $"N'{esc(linkedServer.Catalog)}'")};
            END

            EXEC master.dbo.sp_serveroption @server=N'{esc(linkedServer.Name)}', @optname=N'rpc', @optvalue=N'{B(linkedServer.Rpc)}';
            EXEC master.dbo.sp_serveroption @server=N'{esc(linkedServer.Name)}', @optname=N'rpc out', @optvalue=N'{B(linkedServer.RpcOut)}';
            EXEC master.dbo.sp_serveroption @server=N'{esc(linkedServer.Name)}', @optname=N'data access', @optvalue=N'{B(linkedServer.DataAccess)}';
            """;
    }
}
