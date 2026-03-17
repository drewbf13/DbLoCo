using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;

namespace SqlClone.Application;

public sealed class CloneOrchestrator : ICloneOrchestrator
{
    private readonly IDockerSqlContainerManager _docker;
    private readonly IDatabaseMaterializer _materializer;
    private readonly IDatabaseMigrationRunner _migrationRunner;
    private readonly ITableSeeder _tableSeeder;
    private readonly ILinkedServerProvisioner _linkedServers;
    private readonly IPostCloneScriptRunner _postClone;
    private readonly ICloneValidator _validator;
    private readonly IClonePlanFactory _planFactory;
    private readonly CloneOptions _options;
    private readonly ILogger<CloneOrchestrator> _logger;

    public CloneOrchestrator(
        IDockerSqlContainerManager docker,
        IDatabaseMaterializer materializer,
        IDatabaseMigrationRunner migrationRunner,
        ITableSeeder tableSeeder,
        ILinkedServerProvisioner linkedServers,
        IPostCloneScriptRunner postClone,
        ICloneValidator validator,
        IClonePlanFactory planFactory,
        IOptions<CloneOptions> options,
        ILogger<CloneOrchestrator> logger)
    {
        _docker = docker;
        _materializer = materializer;
        _migrationRunner = migrationRunner;
        _tableSeeder = tableSeeder;
        _linkedServers = linkedServers;
        _postClone = postClone;
        _validator = validator;
        _planFactory = planFactory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task<CloneExecutionResult> CloneAsync(CancellationToken cancellationToken)
    {
        var plan = _planFactory.CreatePlan(_options);
        _logger.LogInformation("Starting clone for environment {Environment}", plan.EnvironmentName);

        await _docker.EnsureStartedAsync(cancellationToken);
        await _docker.WaitUntilReadyAsync(cancellationToken);

        foreach (var database in plan.Databases)
        {
            _logger.LogInformation("Materializing database {Database}", database.Name);
            await _materializer.MaterializeAsync(database, cancellationToken);
        }

        await _migrationRunner.RunAsync(plan.Migration, cancellationToken);
        await _tableSeeder.SeedAsync(plan.SeedTables, cancellationToken);

        await _linkedServers.ApplyAsync(plan.LinkedServers, cancellationToken);
        await _postClone.RunAsync(cancellationToken);

        var validation = await _validator.ValidateAsync(cancellationToken);

        _logger.LogInformation("Clone completed with status {Status}", validation.IsSuccessful ? "Success" : "Failed");
        return new CloneExecutionResult
        {
            Success = validation.IsSuccessful,
            Plan = plan,
            Validation = validation,
            Messages = [validation.IsSuccessful ? "Clone succeeded" : "Clone failed validation"]
        };
    }
}
