namespace SqlClone.Domain.Models;

public sealed class PostCloneOptions
{
    public List<string> ScriptFolders { get; set; } = [];

    /// <summary>
    /// Database the post-clone scripts run against. When unset, the runner resolves the
    /// first restored database, then the seed source database. Set explicitly to override.
    /// </summary>
    public string? Database { get; set; }
}
