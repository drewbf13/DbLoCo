namespace SqlClone.Domain.Models;

public sealed class RestoreOptions
{
    public string Materializer { get; set; } = "CreateEmpty";
    public List<string> Databases { get; set; } = [];
}
