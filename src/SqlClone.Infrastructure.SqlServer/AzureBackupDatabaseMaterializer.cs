using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;

namespace SqlClone.Infrastructure.SqlServer;

public sealed class AzureBackupDatabaseMaterializer : IDatabaseMaterializer
{
    private readonly SqlConnectionFactory _factory;
    private readonly CloneOptions _options;
    private readonly ILogger<AzureBackupDatabaseMaterializer> _logger;

    public AzureBackupDatabaseMaterializer(
        SqlConnectionFactory factory,
        IOptions<CloneOptions> options,
        ILogger<AzureBackupDatabaseMaterializer> logger)
    {
        _factory = factory;
        _options = options.Value;
        _logger = logger;
    }

    public async Task MaterializeAsync(DatabaseClonePlan plan, CancellationToken cancellationToken)
    {
        var backupOptions = _options.Restore.AzureBackup;
        if (string.IsNullOrWhiteSpace(backupOptions.BackupUrlTemplate))
        {
            throw new InvalidOperationException("Clone:Restore:AzureBackup:BackupUrlTemplate must be configured when Materializer is AzureBackup.");
        }

        var sourceServer = ResolveSourceServerName(_options.Source.ConnectionString);
        var backupUrl = ResolveBackupUrl(backupOptions.BackupUrlTemplate, plan.Name, sourceServer);

        await using var connection = _factory.CreateTargetConnection();
        await connection.OpenAsync(cancellationToken);

        await EnsureDatabaseIsDroppedAsync(connection, plan.Name, cancellationToken);

        var credentialClause = await EnsureCredentialAsync(connection, backupUrl, backupOptions, cancellationToken);
        var files = await GetBackupFilesAsync(connection, backupUrl, credentialClause, cancellationToken);
        if (files.Count == 0)
        {
            throw new InvalidOperationException($"No files found in backup URL {backupUrl}.");
        }

        var dataPath = await GetServerPathAsync(connection, isLog: false, cancellationToken) ?? "/var/opt/mssql/data/";
        var logPath = await GetServerPathAsync(connection, isLog: true, cancellationToken) ?? dataPath;

        var moveClauses = BuildMoveClauses(plan.Name, files, dataPath, logPath);

        var escapedDbName = EscapeSqlString(plan.Name);
        var restoreSql = $"""
            RESTORE DATABASE [{EscapeSqlIdentifier(plan.Name)}]
            FROM URL = N'{EscapeSqlString(backupUrl)}'
            WITH REPLACE,
                 STATS = 5{credentialClause}{moveClauses};
            """;

        _logger.LogInformation("Restoring {Database} from {BackupUrl}", plan.Name, backupUrl);
        await using var restore = connection.CreateCommand();
        restore.CommandTimeout = 0;
        restore.CommandText = restoreSql;
        await restore.ExecuteNonQueryAsync(cancellationToken);

        _logger.LogInformation("Restored database {Database} from Azure backup", escapedDbName);
    }

    private static string ResolveSourceServerName(string connectionString)
    {
        var builder = new SqlConnectionStringBuilder(connectionString);
        var dataSource = builder.DataSource;
        if (string.IsNullOrWhiteSpace(dataSource))
        {
            throw new InvalidOperationException("Clone:Source:ConnectionString is missing Data Source/Server and cannot be used to infer backup location.");
        }

        var withoutPrefix = dataSource.StartsWith("tcp:", StringComparison.OrdinalIgnoreCase)
            ? dataSource[4..]
            : dataSource;

        var withoutPort = withoutPrefix.Split(',')[0];
        return withoutPort.Trim();
    }

    private static string ResolveBackupUrl(string template, string databaseName, string sourceServer)
    {
        return template
            .Replace("{database}", Uri.EscapeDataString(databaseName), StringComparison.OrdinalIgnoreCase)
            .Replace("{sourceServer}", Uri.EscapeDataString(sourceServer), StringComparison.OrdinalIgnoreCase);
    }

    private async Task EnsureDatabaseIsDroppedAsync(SqlConnection connection, string databaseName, CancellationToken cancellationToken)
    {
        var escaped = EscapeSqlString(databaseName);
        var sql = $"""
            IF DB_ID(N'{escaped}') IS NOT NULL
            BEGIN
                ALTER DATABASE [{EscapeSqlIdentifier(databaseName)}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{EscapeSqlIdentifier(databaseName)}];
            END
            """;

        await using var command = connection.CreateCommand();
        command.CommandTimeout = 120;
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<string?> GetServerPathAsync(SqlConnection connection, bool isLog, CancellationToken cancellationToken)
    {
        var propertyName = isLog ? "InstanceDefaultLogPath" : "InstanceDefaultDataPath";
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT CAST(SERVERPROPERTY('{propertyName}') AS nvarchar(4000));";
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is DBNull or null ? null : value.ToString();
    }

    private static async Task<IReadOnlyList<BackupFileInfo>> GetBackupFilesAsync(SqlConnection connection, string backupUrl, string credentialClause, CancellationToken cancellationToken)
    {
        var files = new List<BackupFileInfo>();

        await using var command = connection.CreateCommand();
        command.CommandText = $"RESTORE FILELISTONLY FROM URL = N'{EscapeSqlString(backupUrl)}' WITH FILE = 1{credentialClause};";
        command.CommandTimeout = 120;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            files.Add(new BackupFileInfo(
                LogicalName: reader.GetString(reader.GetOrdinal("LogicalName")),
                IsLog: string.Equals(reader.GetString(reader.GetOrdinal("Type")), "L", StringComparison.OrdinalIgnoreCase)));
        }

        return files;
    }

    private static string BuildMoveClauses(string databaseName, IReadOnlyList<BackupFileInfo> files, string dataPath, string logPath)
    {
        var dataIndex = 0;
        var logIndex = 0;
        var clauses = files.Select(file =>
        {
            if (file.IsLog)
            {
                logIndex++;
                var suffix1 = logIndex == 1 ? string.Empty : $"_{logIndex}";
                var targetFile1 = $"{databaseName}_log{suffix1}.ldf";
                return $",\n                 MOVE N'{EscapeSqlString(file.LogicalName)}' TO N'{EscapeSqlString(Path.Combine(logPath, targetFile1))}'";
            }

            dataIndex++;
            var suffix = dataIndex == 1 ? string.Empty : $"_{dataIndex}";
            var targetFile = $"{databaseName}{suffix}.mdf";
            return $",\n                 MOVE N'{EscapeSqlString(file.LogicalName)}' TO N'{EscapeSqlString(Path.Combine(dataPath, targetFile))}'";
        });

        return string.Concat(clauses);
    }

    private static async Task<string> EnsureCredentialAsync(
        SqlConnection connection,
        string backupUrl,
        AzureBackupRestoreOptions options,
        CancellationToken cancellationToken)
    {
        var sas = options.SharedAccessSignature;
        if (string.IsNullOrWhiteSpace(sas))
        {
            sas = await CreateUserDelegationSasAsync(backupUrl, cancellationToken);
        }

        if (string.IsNullOrWhiteSpace(sas))
        {
            return string.Empty;
        }

        var credentialName = string.IsNullOrWhiteSpace(options.SqlCredentialName)
            ? "SqlCloneAzureBackupSas"
            : options.SqlCredentialName;

        sas = sas.Trim().TrimStart('?');

        var escapedCredential = EscapeSqlIdentifier(credentialName);
        var escapedSecret = EscapeSqlString(sas);

        var sql = $"""
            IF EXISTS (SELECT 1 FROM sys.credentials WHERE name = N'{EscapeSqlString(credentialName)}')
            BEGIN
                DROP CREDENTIAL [{escapedCredential}];
            END;

            CREATE CREDENTIAL [{escapedCredential}]
            WITH IDENTITY = N'SHARED ACCESS SIGNATURE',
                 SECRET = N'{escapedSecret}';
            """;

        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.CommandTimeout = 60;
        await command.ExecuteNonQueryAsync(cancellationToken);

        return $",\n                 CREDENTIAL = N'{EscapeSqlString(credentialName)}'";
    }

    private static async Task<string?> CreateUserDelegationSasAsync(string backupUrl, CancellationToken cancellationToken)
    {
        var blobUri = new BlobUriBuilder(new Uri(backupUrl));
        if (string.IsNullOrWhiteSpace(blobUri.BlobContainerName) || string.IsNullOrWhiteSpace(blobUri.BlobName))
        {
            throw new InvalidOperationException($"Backup URL must point to a blob. Received: {backupUrl}");
        }

        var endpoint = new Uri($"{blobUri.Scheme}://{blobUri.Host}");
        var serviceClient = new BlobServiceClient(endpoint, new DefaultAzureCredential());

        var startsOn = DateTimeOffset.UtcNow.AddMinutes(-5);
        var expiresOn = DateTimeOffset.UtcNow.AddHours(1);
        var delegationKey = await serviceClient.GetUserDelegationKeyAsync(startsOn, expiresOn, cancellationToken);

        var sas = new BlobSasBuilder
        {
            BlobContainerName = blobUri.BlobContainerName,
            BlobName = blobUri.BlobName,
            Resource = "b",
            StartsOn = startsOn,
            ExpiresOn = expiresOn
        };

        sas.SetPermissions(BlobSasPermissions.Read);
        return sas.ToSasQueryParameters(delegationKey.Value, serviceClient.AccountName).ToString();
    }

    private static string EscapeSqlString(string value) => value.Replace("'", "''", StringComparison.Ordinal);

    private static string EscapeSqlIdentifier(string value) => value.Replace("]", "]]", StringComparison.Ordinal);

    private sealed record BackupFileInfo(string LogicalName, bool IsLog);
}
