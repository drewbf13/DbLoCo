using Microsoft.Extensions.Logging;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;

namespace SqlClone.Infrastructure.SqlServer;

public sealed class NoOpDatabaseMaterializer : IDatabaseMaterializer
{
    private readonly ILogger<NoOpDatabaseMaterializer> _logger;

    public NoOpDatabaseMaterializer(ILogger<NoOpDatabaseMaterializer> logger)
    {
        _logger = logger;
    }

    public Task MaterializeAsync(DatabaseClonePlan plan, CancellationToken cancellationToken)
    {
        _logger.LogInformation("NoOp materializer selected. Skipping {Database}", plan.Name);
        return Task.CompletedTask;
    }
}
