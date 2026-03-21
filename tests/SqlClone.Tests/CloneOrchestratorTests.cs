using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using SqlClone.Application;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;

namespace SqlClone.Tests;

public sealed class CloneOrchestratorTests
{
    [Fact]
    public async Task RunsLinkedServersBeforeMigrations()
    {
        var calls = new List<string>();
        var plan = new ClonePlan
        {
            EnvironmentName = "Dev",
            Materializer = "CreateEmpty",
            Databases = [new DatabaseClonePlan { Name = "AppDb" }],
            LinkedServers = [new LinkedServerDefinition { Name = "REMOTEDEV", DataSource = "remote" }],
            Migration = new MigrationPlan { Enabled = true, BuildCommand = "echo migrate" },
            SeedTables = [new SeedTablePlan { SourceDatabase = "AppDb", TargetDatabase = "AppDb", Schema = "dbo", Table = "RefData", Order = 10 }]
        };

        var orchestrator = new CloneOrchestrator(
            new RecordingDocker(calls),
            new RecordingMaterializer(calls),
            new RecordingMigrationRunner(calls),
            new RecordingSeeder(calls),
            new RecordingLinkedServers(calls),
            new RecordingPostClone(calls),
            new SuccessfulValidator(),
            new StaticPlanFactory(plan),
            Options.Create(new CloneOptions()),
            NullLogger<CloneOrchestrator>.Instance);

        var result = await orchestrator.CloneAsync(CancellationToken.None);

        result.Success.Should().BeTrue();
        calls.Should().ContainInOrder(
            "docker.ensure-started",
            "docker.ready",
            "materialize.AppDb",
            "linked-servers",
            "migrations",
            "seed",
            "post-clone",
            "validate");
    }

    private sealed class StaticPlanFactory(ClonePlan plan) : IClonePlanFactory
    {
        public ClonePlan CreatePlan(CloneOptions options, string? overrideEnvironment = null) => plan;
    }

    private sealed class RecordingDocker(List<string> calls) : IDockerSqlContainerManager
    {
        public Task EnsureStartedAsync(CancellationToken cancellationToken)
        {
            calls.Add("docker.ensure-started");
            return Task.CompletedTask;
        }

        public Task TeardownAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task WaitUntilReadyAsync(CancellationToken cancellationToken)
        {
            calls.Add("docker.ready");
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingMaterializer(List<string> calls) : IDatabaseMaterializer
    {
        public Task MaterializeAsync(DatabaseClonePlan plan, CancellationToken cancellationToken)
        {
            calls.Add($"materialize.{plan.Name}");
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingMigrationRunner(List<string> calls) : IDatabaseMigrationRunner
    {
        public Task RunAsync(MigrationPlan migration, CancellationToken cancellationToken)
        {
            calls.Add("migrations");
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingSeeder(List<string> calls) : ITableSeeder
    {
        public Task SeedAsync(IReadOnlyList<SeedTablePlan> tables, CancellationToken cancellationToken)
        {
            calls.Add("seed");
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingLinkedServers(List<string> calls) : ILinkedServerProvisioner
    {
        public Task ApplyAsync(IReadOnlyList<LinkedServerDefinition> linkedServers, CancellationToken cancellationToken)
        {
            calls.Add("linked-servers");
            return Task.CompletedTask;
        }
    }

    private sealed class RecordingPostClone(List<string> calls) : IPostCloneScriptRunner
    {
        public Task RunAsync(CancellationToken cancellationToken)
        {
            calls.Add("post-clone");
            return Task.CompletedTask;
        }
    }

    private sealed class SuccessfulValidator : ICloneValidator
    {
        public Task<ValidationResult> ValidateAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(new ValidationResult
            {
                SqlReachable = true,
                Databases = new Dictionary<string, bool> { ["AppDb"] = true },
                LinkedServers = new Dictionary<string, bool> { ["REMOTEDEV"] = true }
            });
        }
    }
}
