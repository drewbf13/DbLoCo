using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;

namespace SqlClone.Application;

public sealed class ClonePlanFactory : IClonePlanFactory
{
    public ClonePlan CreatePlan(CloneOptions options, string? overrideEnvironment = null)
    {
        ArgumentNullException.ThrowIfNull(options);

        return new ClonePlan
        {
            EnvironmentName = overrideEnvironment ?? options.EnvironmentName,
            Materializer = options.Restore.Materializer,
            Databases = options.Restore.Databases.Select(name => new DatabaseClonePlan { Name = name }).ToList(),
            LinkedServers = options.LinkedServers.Definitions.ToList()
        };
    }
}
