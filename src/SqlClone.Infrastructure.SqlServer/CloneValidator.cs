using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;

namespace SqlClone.Infrastructure.SqlServer;

public sealed class CloneValidator : ICloneValidator
{
    private readonly SqlConnectionFactory _factory;
    private readonly SqlExecutionHelper _helper;
    private readonly CloneOptions _options;
    private readonly ILogger<CloneValidator> _logger;

    public CloneValidator(SqlConnectionFactory factory, SqlExecutionHelper helper, IOptions<CloneOptions> options, ILogger<CloneValidator> logger)
    {
        _factory = factory;
        _helper = helper;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<ValidationResult> ValidateAsync(CancellationToken cancellationToken)
    {
        var result = new ValidationResult();
        try
        {
            await using var connection = _factory.CreateTargetConnection();
            await connection.OpenAsync(cancellationToken);
            result.SqlReachable = true;

            foreach (var db in _options.Restore.Databases)
            {
                var escaped = db.Replace("'", "''", StringComparison.Ordinal);
                var exists = await _helper.ExecuteScalarAsync<int>(connection, $"SELECT COUNT(1) FROM sys.databases WHERE name = N'{escaped}';", cancellationToken);
                result.Databases[db] = exists > 0;
            }

            foreach (var linkedServer in _options.LinkedServers.Definitions)
            {
                var escaped = linkedServer.Name.Replace("'", "''", StringComparison.Ordinal);
                var exists = await _helper.ExecuteScalarAsync<int>(connection, $"SELECT COUNT(1) FROM sys.servers WHERE name = N'{escaped}';", cancellationToken);
                result.LinkedServers[linkedServer.Name] = exists > 0;
            }

            _logger.LogInformation("Validation complete. Success: {Success}", result.IsSuccessful);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Validation failed while connecting or querying SQL Server");
            result.SqlReachable = false;
        }

        return result;
    }
}
