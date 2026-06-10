using System.Text.RegularExpressions;
using Microsoft.Data.SqlClient;
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

        var targetDatabase = ResolveTargetDatabase();
        if (string.IsNullOrWhiteSpace(targetDatabase))
        {
            _logger.LogWarning(
                "Skipping post-clone scripts: could not resolve a target database. Set Clone:PostClone:Database, Clone:Restore:Databases, or Clone:Seed:SourceDatabase.");
            return;
        }

        _logger.LogInformation("Running post-clone scripts against database {Database}", targetDatabase);
        await using var connection = _factory.CreateTargetConnection(targetDatabase);
        await connection.OpenAsync(cancellationToken);

        foreach (var file in files)
        {
            var sql = await File.ReadAllTextAsync(file, cancellationToken);
            var batches = SplitBatches(sql);
            var failed = 0;
            foreach (var batch in batches)
            {
                try
                {
                    await _helper.ExecuteNonQueryAsync(connection, batch, cancellationToken);
                }
                catch (SqlException ex)
                {
                    failed++;
                    _logger.LogWarning("Post-clone batch failed in {Script}: {Error}", file, ex.Message);
                }
            }

            if (failed > 0)
            {
                _logger.LogWarning("Executed post-clone script {Script} with {Failed}/{Total} failed batch(es)", file, failed, batches.Count);
            }
            else
            {
                _logger.LogInformation("Executed post-clone script {Script} ({BatchCount} batch(es))", file, batches.Count);
            }
        }
    }

    // The cloned database to run scripts against. CreateTargetConnection defaults to master,
    // which would create the objects in the wrong database, so resolve the actual clone target.
    private string? ResolveTargetDatabase()
    {
        if (!string.IsNullOrWhiteSpace(_options.PostClone.Database))
        {
            return _options.PostClone.Database;
        }

        var restoredDatabase = _options.Restore.Databases.FirstOrDefault(db => !string.IsNullOrWhiteSpace(db));
        if (!string.IsNullOrWhiteSpace(restoredDatabase))
        {
            return restoredDatabase;
        }

        return string.IsNullOrWhiteSpace(_options.Seed.SourceDatabase) ? null : _options.Seed.SourceDatabase;
    }

    // Splits a script on GO batch separators (a line containing only GO, optionally
    // followed by a repeat count) so that CREATE VIEW/FUNCTION/PROCEDURE statements,
    // which must each start their own batch, execute correctly.
    private static readonly Regex BatchSeparator = new(
        @"^\s*GO\s*(?:\d+)?\s*$",
        RegexOptions.IgnoreCase | RegexOptions.Multiline | RegexOptions.Compiled);

    private static List<string> SplitBatches(string sql) =>
        BatchSeparator.Split(sql)
            .Select(batch => batch.Trim())
            .Where(batch => batch.Length > 0)
            .ToList();
}
