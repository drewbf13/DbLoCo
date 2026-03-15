namespace SqlClone.Domain.Models;

public sealed class CloneOptions
{
    public const string SectionName = "Clone";

    public string EnvironmentName { get; set; } = "Development";
    public DockerOptions Docker { get; set; } = new();
    public SourceOptions Source { get; set; } = new();
    public RestoreOptions Restore { get; set; } = new();
    public LinkedServersOptions LinkedServers { get; set; } = new();
    public PostCloneOptions PostClone { get; set; } = new();
}

public sealed class DockerOptions
{
    public string ContainerName { get; set; } = "sqlclone-local";
    public string Image { get; set; } = "mcr.microsoft.com/mssql/server:2022-latest";
    public int HostPort { get; set; } = 14333;
    public string SaPassword { get; set; } = "YourStrong!Passw0rd";
    public bool RemoveVolumeOnTeardown { get; set; }
}
