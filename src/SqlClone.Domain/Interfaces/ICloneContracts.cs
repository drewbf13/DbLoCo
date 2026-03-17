using SqlClone.Domain.Models;

namespace SqlClone.Domain.Interfaces;

public interface ISourceInspector
{
    Task<IReadOnlyList<SourceDatabaseInfo>> GetDatabasesAsync(CancellationToken cancellationToken);
}

public interface IDockerSqlContainerManager
{
    Task EnsureStartedAsync(CancellationToken cancellationToken);
    Task TeardownAsync(CancellationToken cancellationToken);
    Task WaitUntilReadyAsync(CancellationToken cancellationToken);
}

public interface IDatabaseMaterializer
{
    Task MaterializeAsync(DatabaseClonePlan plan, CancellationToken cancellationToken);
}

public interface IDatabaseMigrationRunner
{
    Task RunAsync(MigrationPlan migration, CancellationToken cancellationToken);
}

public interface ITableSeeder
{
    Task SeedAsync(IReadOnlyList<SeedTablePlan> tables, CancellationToken cancellationToken);
}

public interface ILinkedServerProvisioner
{
    Task ApplyAsync(IReadOnlyList<LinkedServerDefinition> linkedServers, CancellationToken cancellationToken);
}

public interface IPostCloneScriptRunner
{
    Task RunAsync(CancellationToken cancellationToken);
}

public interface ICloneValidator
{
    Task<ValidationResult> ValidateAsync(CancellationToken cancellationToken);
}

public interface ICloneOrchestrator
{
    Task<CloneExecutionResult> CloneAsync(CancellationToken cancellationToken);
}

public interface IClonePlanFactory
{
    ClonePlan CreatePlan(CloneOptions options, string? overrideEnvironment = null);
}
