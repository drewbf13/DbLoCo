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

    public SqlConnection CreateSourceConnection(string? initialCatalog = null)
    {
        if (CanUseConfiguredSourceConnectionStringAsIs(initialCatalog))
        {
            return new SqlConnection(_options.Source.ConnectionString);
        }

        var builder = new SqlConnectionStringBuilder(_options.Source.ConnectionString);
        if (!string.IsNullOrWhiteSpace(initialCatalog))
        {
            builder.InitialCatalog = initialCatalog;
        }

        if (_options.Source.Encrypt.HasValue)
        {
            builder.Encrypt = _options.Source.Encrypt.Value;
        }

        if (_options.Source.TrustServerCertificate.HasValue)
        {
            builder.TrustServerCertificate = _options.Source.TrustServerCertificate.Value;
        }

        if (_options.Source.DisableConnectionPooling)
        {
            builder.Pooling = false;
        }

        if (_options.Source.EnableAlwaysEncrypted ?? false)
        {
            builder.ColumnEncryptionSetting = SqlConnectionColumnEncryptionSetting.Enabled;
        }

        return new SqlConnection(builder.ConnectionString);
    }

    private bool CanUseConfiguredSourceConnectionStringAsIs(string? initialCatalog)
    {
        return string.IsNullOrWhiteSpace(initialCatalog)
               && !_options.Source.Encrypt.HasValue
               && !_options.Source.TrustServerCertificate.HasValue
               && _options.Source is { DisableConnectionPooling: false, EnableAlwaysEncrypted: null };
    }
}
