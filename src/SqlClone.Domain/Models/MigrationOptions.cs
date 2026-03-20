namespace SqlClone.Domain.Models;

public sealed class MigrationOptions
{
    public bool Enabled { get; set; }
    public string GitRepository { get; set; } = string.Empty;
    public string LocalRepositoryPath { get; set; } = string.Empty;
    public string Branch { get; set; } = "main";
    public string BuildCommand { get; set; } = string.Empty;
    public string WorkingDirectory { get; set; } = string.Empty;
}
