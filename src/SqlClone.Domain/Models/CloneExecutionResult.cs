namespace SqlClone.Domain.Models;

public sealed class CloneExecutionResult
{
    public bool Success { get; init; }
    public ClonePlan? Plan { get; init; }
    public ValidationResult? Validation { get; init; }
    public List<string> Messages { get; init; } = [];

    public static CloneExecutionResult Failed(string message) => new()
    {
        Success = false,
        Messages = [message]
    };
}
