using FluentAssertions;
using Microsoft.Extensions.Options;
using SqlClone.Domain.Models;
using SqlClone.Infrastructure.SqlServer;

namespace SqlClone.Tests;

public sealed class SqlConnectionFactoryTests
{
    [Fact]
    public void CreateSourceConnection_WithoutOverrides_RespectsConfiguredConnectionStringSettings()
    {
        const string connectionString = "Server=tcp:example.database.windows.net,1433;Initial Catalog=AppDb;User ID=test@domain.com;Password=test;Encrypt=True;TrustServerCertificate=False;Authentication=\"Active Directory Password\";Column Encryption Setting=enabled;";
        var options = Options.Create(new CloneOptions
        {
            Docker = new DockerOptions { SaPassword = "YourStrong!Passw0rd" },
            Source = new SourceOptions
            {
                ConnectionString = connectionString
            }
        });

        var factory = new SqlConnectionFactory(options);

        using var connection = factory.CreateSourceConnection();
        var builder = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(connection.ConnectionString);

        builder.Authentication.Should().Be(Microsoft.Data.SqlClient.SqlAuthenticationMethod.ActiveDirectoryPassword);
        builder.ColumnEncryptionSetting.Should().Be(Microsoft.Data.SqlClient.SqlConnectionColumnEncryptionSetting.Enabled);
        builder.Encrypt.Should().Be(true);
        builder.TrustServerCertificate.Should().BeFalse();
    }

    [Fact]
    public void CreateSourceConnection_WithoutOverrides_UsesConfiguredConnectionStringAsIs()
    {
        const string connectionString = "Server=tcp:example.database.windows.net,1433;Initial Catalog=AppDb;User ID=test@domain.com;Password=test;Encrypt=True;TrustServerCertificate=False;Authentication=\"Active Directory Password\";Column Encryption Setting=enabled;";
        var options = Options.Create(new CloneOptions
        {
            Docker = new DockerOptions { SaPassword = "YourStrong!Passw0rd" },
            Source = new SourceOptions
            {
                ConnectionString = connectionString
            }
        });

        var factory = new SqlConnectionFactory(options);

        using var connection = factory.CreateSourceConnection();
        connection.ConnectionString.Should().Be(connectionString);
    }

    [Fact]
    public void CreateSourceConnection_AppliesSourceTransportOverrides()
    {
        var options = Options.Create(new CloneOptions
        {
            Docker = new DockerOptions { SaPassword = "YourStrong!Passw0rd" },
            Source = new SourceOptions
            {
                ConnectionString = "Server=tcp:example.database.windows.net,1433;Initial Catalog=AppDb;User ID=test;Password=test;Encrypt=False;TrustServerCertificate=True;Pooling=True;",
                Encrypt = true,
                TrustServerCertificate = false,
                DisableConnectionPooling = true,
                EnableAlwaysEncrypted = true
            }
        });

        var factory = new SqlConnectionFactory(options);

        using var connection = factory.CreateSourceConnection("OtherDb");
        var builder = new Microsoft.Data.SqlClient.SqlConnectionStringBuilder(connection.ConnectionString);

        builder.InitialCatalog.Should().Be("OtherDb");
        builder.Encrypt.Should().Be(true);
        builder.TrustServerCertificate.Should().BeFalse();
        builder.Pooling.Should().BeFalse();
        builder.ColumnEncryptionSetting.Should().Be(Microsoft.Data.SqlClient.SqlConnectionColumnEncryptionSetting.Enabled);
    }
}
