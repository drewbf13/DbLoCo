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
                ["Clone:Restore:Databases:0"] = "DbOne"
            })
            .Build();

        var options = config.GetSection(CloneOptions.SectionName).Get<CloneOptions>();

        options.Should().NotBeNull();
        options!.Docker.ContainerName.Should().Be("sqlclone-test");
        options.Restore.Materializer.Should().Be("NoOp");
        options.Restore.Databases.Should().ContainSingle().Which.Should().Be("DbOne");
    }
}
