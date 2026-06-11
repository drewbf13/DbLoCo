namespace ScoutHub;

public class Team
{
    public int Id { get; set; }
    public required string City { get; set; }
    public required string Name { get; set; }
    public required string Conference { get; set; }
    public required string Division { get; set; }
    public List<Player> Players { get; set; } = [];
}

public class Player
{
    public int Id { get; set; }
    public int TeamId { get; set; }
    public Team? Team { get; set; }
    public required string FirstName { get; set; }
    public required string LastName { get; set; }
    public required string Position { get; set; }
    public required string College { get; set; }
    public int? HeightInches { get; set; }
    public int? WeightPounds { get; set; }
    public List<ScoutingReport> ScoutingReports { get; set; } = [];
}

public class Workout
{
    public int Id { get; set; }
    public int PlayerId { get; set; }
    public Player? Player { get; set; }
    public DateTime WorkoutDate { get; set; }
    public required string Location { get; set; }
    public List<WorkoutResult> Results { get; set; } = [];
}

public class WorkoutResult
{
    public int Id { get; set; }
    public int WorkoutId { get; set; }
    public Workout? Workout { get; set; }
    public required string Drill { get; set; }
    public decimal Result { get; set; }
}

public class ScoutingReport
{
    public long Id { get; set; }
    public int PlayerId { get; set; }
    public Player? Player { get; set; }
    public required string ScoutName { get; set; }
    public decimal Grade { get; set; }
    public required string Notes { get; set; }
    public DateTime CreatedUtc { get; set; }
}
