using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using SqlClone.Application;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;
using SqlClone.Infrastructure.Docker;
using SqlClone.Infrastructure.SqlServer;

namespace SqlClone.Infrastructure;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSqlClone(this IServiceCollection services)
    {
        services.AddSingleton<IClonePlanFactory, ClonePlanFactory>();
        services.AddSingleton<ICloneOrchestrator, CloneOrchestrator>();

        services.AddSingleton<IDockerSqlContainerManager, DockerSqlContainerManager>();
        services.AddSingleton<SqlConnectionFactory>();
        services.AddSingleton<SqlExecutionHelper>();

        services.AddSingleton<ISourceInspector, SourceInspector>();
        services.AddSingleton<IDatabaseMigrationRunner, GitDatabaseMigrationRunner>();
        services.AddSingleton<ITableSeeder, SqlTableSeeder>();
        services.AddSingleton<ILinkedServerProvisioner, LinkedServerProvisioner>();
        services.AddSingleton<IPostCloneScriptRunner, PostCloneScriptRunner>();
        services.AddSingleton<ICloneValidator, CloneValidator>();

        services.AddSingleton<IDatabaseMaterializer>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<CloneOptions>>().Value;
            return options.Restore.Materializer.ToLowerInvariant() switch
            {
                "noop" => sp.GetRequiredService<NoOpDatabaseMaterializer>(),
                "azurebackup" => sp.GetRequiredService<AzureBackupDatabaseMaterializer>(),
                _ => sp.GetRequiredService<CreateEmptyDatabaseMaterializer>()
            };
        });

        services.AddSingleton<NoOpDatabaseMaterializer>();
        services.AddSingleton<CreateEmptyDatabaseMaterializer>();
        services.AddSingleton<AzureBackupDatabaseMaterializer>();

        return services;
    }
}
