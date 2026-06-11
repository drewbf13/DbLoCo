using Microsoft.EntityFrameworkCore;
using ScoutHub;

if (args.Length == 0)
{
    Console.Error.WriteLine("Usage: ScoutHub <connection-string>");
    return 1;
}

var options = new DbContextOptionsBuilder<ScoutHubContext>()
    .UseSqlServer(args[0])
    .Options;

await using var context = new ScoutHubContext(options);
var pending = (await context.Database.GetPendingMigrationsAsync()).ToList();
Console.WriteLine(pending.Count == 0
    ? "Schema is up to date; no pending migrations."
    : $"Applying {pending.Count} migration(s): {string.Join(", ", pending)}");

await context.Database.MigrateAsync();
Console.WriteLine("Migrations applied.");
return 0;
