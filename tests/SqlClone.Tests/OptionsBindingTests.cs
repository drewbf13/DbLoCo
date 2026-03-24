using FluentAssertions;
using Microsoft.Extensions.Configuration;
using SqlClone.Domain.Models;

namespace SqlClone.Tests;

public sealed class OptionsBindingTests
{
    [Fact]
    public void BindsCloneOptionsFromConfiguration()
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Clone:EnvironmentName"] = "Development",
                ["Clone:Docker:ContainerName"] = "sqlclone-test",
                ["Clone:Restore:Materializer"] = "NoOp",
                ["Clone:Restore:Databases:0"] = "DbOne",
                ["Clone:Restore:AzureBackup:BackupUrlTemplate"] = "https://acct.blob.core.windows.net/backups/{database}.bak",
                ["Clone:Restore:AzureBackup:SqlCredentialName"] = "CloneCred",
                ["Clone:Migration:Enabled"] = "true",
                ["Clone:Migration:GitRepository"] = "https://example.com/repo.git",
                ["Clone:Migration:LocalRepositoryPath"] = "C:\\repos\\migrations",
                ["Clone:Migration:Branch"] = "dev",
                ["Clone:Migration:BuildCommand"] = "dotnet run",
                ["Clone:Seed:Enabled"] = "true",
                ["Clone:Seed:Strategy"] = "LinkedServer",
                ["Clone:Seed:LinkedServerName"] = "REMOTEDEV",
                ["Clone:Seed:SourceDatabase"] = "DbOne",
                ["Clone:Seed:ExcludeSchemas:0"] = "audit",
                ["Clone:Seed:InheritedParentFilterPriorityTables:0"] = "dbo.Customer",
                ["Clone:Seed:InheritedParentFilterPriorityTables:1"] = "Orders",
                ["Clone:Seed:Tables:0:Table"] = "Lookup",
                ["Clone:Seed:Tables:0:Schema"] = "dbo",
                ["Clone:Seed:Tables:0:TruncateTarget"] = "true",
                ["Clone:Seed:Tables:0:LatestRows"] = "5000",
                ["Clone:Seed:Tables:0:LatestOrderBy"] = "[CreatedUtc] DESC, [Id] DESC",
                ["Clone:Seed:Tables:0:Order"] = "20",
                ["Clone:Seed:Tables:0:GroupKey"] = "2",
                ["Clone:Seed:Tables:0:Children:0:Table"] = "LookupChild",
                ["Clone:LinkedServers:Definitions:0:Name"] = "REMOTEDEV",
                ["Clone:LinkedServers:Definitions:0:DataSource"] = "remote-server.example.local",
                ["Clone:LinkedServers:Definitions:0:UserId"] = "linked_user",
                ["Clone:LinkedServers:Definitions:0:Password"] = "linked_password",
                ["Clone:Source:Encrypt"] = "true",
                ["Clone:Source:TrustServerCertificate"] = "false",
                ["Clone:Source:DisableConnectionPooling"] = "true"
            })
            .Build();

        var options = config.GetSection(CloneOptions.SectionName).Get<CloneOptions>();

        options.Should().NotBeNull();
        options!.Docker.ContainerName.Should().Be("sqlclone-test");
        options.Restore.Materializer.Should().Be("NoOp");
        options.Restore.Databases.Should().ContainSingle().Which.Should().Be("DbOne");
        options.Restore.AzureBackup.BackupUrlTemplate.Should().Contain("{database}");
        options.Restore.AzureBackup.SqlCredentialName.Should().Be("CloneCred");
        options.Migration.Enabled.Should().BeTrue();
        options.Migration.LocalRepositoryPath.Should().Be("C:\\repos\\migrations");
        options.Migration.Branch.Should().Be("dev");
        options.Seed.Enabled.Should().BeTrue();
        options.Seed.Strategy.Should().Be("LinkedServer");
        options.Seed.LinkedServerName.Should().Be("REMOTEDEV");
        options.Seed.ExcludeSchemas.Should().ContainSingle().Which.Should().Be("audit");
        options.Seed.InheritedParentFilterPriorityTables.Should().Equal("dbo.Customer", "Orders");
        options.Seed.Tables.Should().ContainSingle();
        options.Seed.Tables[0].Order.Should().Be(20);
        options.Seed.Tables[0].GroupKey.Should().Be(2);
        options.Seed.Tables[0].LatestRows.Should().Be(5000);
        options.Seed.Tables[0].LatestOrderBy.Should().Be("[CreatedUtc] DESC, [Id] DESC");
        options.Seed.Tables[0].Children.Should().ContainSingle();
        options.Seed.Tables[0].Children[0].Table.Should().Be("LookupChild");
        options.LinkedServers.Definitions.Should().ContainSingle();
        options.LinkedServers.Definitions[0].UserId.Should().Be("linked_user");
        options.LinkedServers.Definitions[0].Password.Should().Be("linked_password");
        options.Source.Encrypt.Should().BeTrue();
        options.Source.TrustServerCertificate.Should().BeFalse();
        options.Source.DisableConnectionPooling.Should().BeTrue();
    }
}
