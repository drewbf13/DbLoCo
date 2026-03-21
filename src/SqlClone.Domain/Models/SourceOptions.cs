namespace SqlClone.Domain.Models;

public sealed class SourceOptions
{
    public string ConnectionString { get; set; } = string.Empty;
    public bool EnableAlwaysEncrypted { get; set; }
}
