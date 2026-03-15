using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;

namespace SqlClone.Infrastructure.SqlServer;

public sealed class PostCloneScriptRunner : IPostCloneScriptRunner
{
    private readonly SqlConnectionFactory _factory;
    private readonly SqlExecutionHelper _helper;
    private readonly CloneOptions _options;
    private readonly ILogger<PostCloneScriptRunner> _logger;

    public PostCloneScriptRunner(SqlConnectionFactory factory, SqlExecutionHelper helper, IOptions<CloneOptions> options, ILogger<PostCloneScriptRunner> logger)
    {
        _factory = factory;
        _helper = helper;
        _options = options.Value;
        _logger = logger;
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var files = _options.PostClone.ScriptFolders
            .Where(Directory.Exists)
            .SelectMany(folder => Directory.EnumerateFiles(folder, "*.sql", SearchOption.TopDirectoryOnly))
            .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (files.Count == 0)
        {
            _logger.LogInformation("No post-clone SQL scripts found");
            return;
        }

        await using var connection = _factory.CreateTargetConnection();
        await connection.OpenAsync(cancellationToken);

        foreach (var file in files)
        {
            var sql = await File.ReadAllTextAsync(file, cancellationToken);
            await _helper.ExecuteNonQueryAsync(connection, sql, cancellationToken);
            _logger.LogInformation("Executed post-clone script {Script}", file);
        }
    }
}
