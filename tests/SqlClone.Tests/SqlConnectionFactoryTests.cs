using FluentAssertions;
using Microsoft.Extensions.Options;
using SqlClone.Domain.Models;
using SqlClone.Infrastructure.SqlServer;

namespace SqlClone.Tests;

public sealed class SqlConnectionFactoryTests
{
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
        builder.Encrypt.Should().BeTrue();
        builder.TrustServerCertificate.Should().BeFalse();
        builder.Pooling.Should().BeFalse();
        builder.ColumnEncryptionSetting.Should().Be(Microsoft.Data.SqlClient.SqlConnectionColumnEncryptionSetting.Enabled);
    }
}
