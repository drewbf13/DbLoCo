using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace ScoutHub;

public class ScoutHubContext(DbContextOptions<ScoutHubContext> options) : DbContext(options)
{
    public DbSet<Team> Teams => Set<Team>();
    public DbSet<Player> Players => Set<Player>();
    public DbSet<ScoutingReport> ScoutingReports => Set<ScoutingReport>();
    public DbSet<Workout> Workouts => Set<Workout>();
    public DbSet<WorkoutResult> WorkoutResults => Set<WorkoutResult>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ScoutingReport>(report =>
        {
            report.Property(r => r.Grade).HasPrecision(4, 2);
            report.HasIndex(r => r.CreatedUtc);
        });
    }
}

public class ScoutHubContextFactory : IDesignTimeDbContextFactory<ScoutHubContext>
{
    public ScoutHubContext CreateDbContext(string[] args)
    {
        var connectionString = args.FirstOrDefault()
            ?? "Server=localhost,14999;Database=ScoutHub;User Id=sa;Password=ScoutHub!Demo1;Encrypt=False;TrustServerCertificate=True";
        var options = new DbContextOptionsBuilder<ScoutHubContext>()
            .UseSqlServer(connectionString)
            .Options;
        return new ScoutHubContext(options);
    }
}
