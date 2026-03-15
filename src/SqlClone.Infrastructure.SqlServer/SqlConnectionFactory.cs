using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Options;
using SqlClone.Domain.Models;

namespace SqlClone.Infrastructure.SqlServer;

public sealed class SqlConnectionFactory
{
    private readonly CloneOptions _options;

    public SqlConnectionFactory(IOptions<CloneOptions> options)
    {
        _options = options.Value;
    }

    public SqlConnection CreateTargetConnection(string? initialCatalog = null)
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = $"localhost,{_options.Docker.HostPort}",
            UserID = "sa",
            Password = _options.Docker.SaPassword,
            Encrypt = false,
            TrustServerCertificate = true,
            InitialCatalog = initialCatalog ?? "master"
        };
        return new SqlConnection(builder.ConnectionString);
    }

    public SqlConnection CreateSourceConnection()
    {
        return new SqlConnection(_options.Source.ConnectionString);
    }
}
