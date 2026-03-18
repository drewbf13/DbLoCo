using FluentAssertions;
using SqlClone.Application;
using SqlClone.Domain.Models;

namespace SqlClone.Tests;

public sealed class ClonePlanFactoryTests
{
    [Fact]
    public void CreatesPlanFromOptions()
    {
        var options = new CloneOptions
        {
            EnvironmentName = "Dev",
            Restore = new RestoreOptions
            {
                Materializer = "CreateEmpty",
                Databases = ["AppDb", "AuditDb"]
            },
            Migration = new MigrationOptions
            {
                Enabled = true,
                GitRepository = "https://example.com/db.git",
                Branch = "feature/seed",
                BuildCommand = "dotnet run"
            },
            Seed = new SeedOptions
            {
                Enabled = true,
                SourceDatabase = "AppDb",
                Tables =
                [
                    new SeedTableOptions { Table = "ZZAfter", TruncateTarget = true, Order = 20 },
                    new SeedTableOptions { Table = "AAFirst", TruncateTarget = true, Order = 10 }
                ]
            },
            LinkedServers = new LinkedServersOptions
            {
                Definitions = [new LinkedServerDefinition { Name = "REMOTE1", DataSource = "remote" }]
            }
        };

        var factory = new ClonePlanFactory();

        var plan = factory.CreatePlan(options);

        plan.EnvironmentName.Should().Be("Dev");
        plan.Databases.Should().HaveCount(2);
        plan.LinkedServers.Should().ContainSingle(ls => ls.Name == "REMOTE1");
        plan.Migration.Enabled.Should().BeTrue();
        plan.Migration.Branch.Should().Be("feature/seed");
        plan.SeedTables.Should().HaveCount(2);
        plan.SeedTables[0].Table.Should().Be("AAFirst");
        plan.SeedTables[0].Order.Should().Be(10);
        plan.SeedTables[0].SourceDatabase.Should().Be("AppDb");
    }
}
