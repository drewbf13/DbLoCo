namespace SqlClone.Domain.Models;

public sealed class LinkedServersOptions
{
    public List<LinkedServerDefinition> Definitions { get; set; } = [];
}

public sealed class LinkedServerDefinition
{
    public string Name { get; set; } = string.Empty;
    public string Product { get; set; } = "SQL Server";
    public string Provider { get; set; } = "MSOLEDBSQL";
    public string DataSource { get; set; } = string.Empty;
    public string? Catalog { get; set; }
    public bool Rpc { get; set; } = true;
    public bool RpcOut { get; set; } = true;
    public bool DataAccess { get; set; } = true;
}
