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
    }
}
