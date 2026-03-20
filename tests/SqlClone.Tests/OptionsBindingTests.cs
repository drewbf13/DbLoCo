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
                ["Clone:Seed:SourceDatabase"] = "DbOne",
                ["Clone:Seed:Tables:0:Table"] = "Lookup",
                ["Clone:Seed:Tables:0:Schema"] = "dbo",
                ["Clone:Seed:Tables:0:TruncateTarget"] = "true",
                ["Clone:Seed:Tables:0:Order"] = "20"
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
        options.Seed.Tables.Should().ContainSingle();
        options.Seed.Tables[0].Order.Should().Be(20);
    }
}
