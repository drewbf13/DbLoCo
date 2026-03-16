namespace SqlClone.Domain.Models;

public sealed class RestoreOptions
{
    public string Materializer { get; set; } = "CreateEmpty";
    public List<string> Databases { get; set; } = [];
    public AzureBackupRestoreOptions AzureBackup { get; set; } = new();
}

public sealed class AzureBackupRestoreOptions
{
    /// <summary>
    /// URL template used to resolve the backup blob URL.
    /// Supports {database} and {sourceServer} tokens.
    /// Example: https://myacct.blob.core.windows.net/sql-backups/{sourceServer}/{database}.bak
    /// </summary>
    public string BackupUrlTemplate { get; set; } = string.Empty;

    /// <summary>
    /// Optional SAS token used to build a SQL credential for RESTORE FROM URL.
    /// Can be supplied with or without a leading '?'.
    /// </summary>
    public string? SharedAccessSignature { get; set; }

    /// <summary>
    /// SQL credential name used while restoring from URL.
    /// </summary>
    public string SqlCredentialName { get; set; } = "SqlCloneAzureBackupSas";
}
