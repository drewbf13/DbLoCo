using System.Diagnostics;
using Microsoft.Extensions.Logging;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;

namespace SqlClone.Infrastructure.SqlServer;

public sealed class GitDatabaseMigrationRunner : IDatabaseMigrationRunner
{
    private readonly ILogger<GitDatabaseMigrationRunner> _logger;

    public GitDatabaseMigrationRunner(ILogger<GitDatabaseMigrationRunner> logger)
    {
        _logger = logger;
    }

    public async Task RunAsync(MigrationPlan migration, CancellationToken cancellationToken)
    {
        if (!migration.Enabled)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(migration.GitRepository) || string.IsNullOrWhiteSpace(migration.BuildCommand))
        {
            throw new InvalidOperationException("Migration is enabled but GitRepository/BuildCommand are not configured.");
        }

        var workRoot = Path.Combine(Path.GetTempPath(), "sqlclone-migration", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(workRoot);

        try
        {
            await RunProcessAsync("git", $"clone --depth 1 --branch {migration.Branch} {migration.GitRepository} .", workRoot, cancellationToken);

            var runDirectory = string.IsNullOrWhiteSpace(migration.WorkingDirectory)
                ? workRoot
                : Path.Combine(workRoot, migration.WorkingDirectory);

            if (!Directory.Exists(runDirectory))
            {
                throw new DirectoryNotFoundException($"Migration working directory not found: {runDirectory}");
            }

            await RunShellCommandAsync(migration.BuildCommand, runDirectory, cancellationToken);
            _logger.LogInformation("Migration build command completed from {Repository} ({Branch})", migration.GitRepository, migration.Branch);
        }
        finally
        {
            try
            {
                Directory.Delete(workRoot, recursive: true);
            }
            catch
            {
                _logger.LogDebug("Could not clean temporary migration directory {Directory}", workRoot);
            }
        }
    }

    private static async Task RunShellCommandAsync(string command, string workingDirectory, CancellationToken cancellationToken)
    {
        if (OperatingSystem.IsWindows())
        {
            await RunProcessAsync("cmd.exe", $"/c {command}", workingDirectory, cancellationToken);
            return;
        }

        var scriptPath = Path.Combine(workingDirectory, $"sqlclone-migrate-{Guid.NewGuid():N}.sh");
        await File.WriteAllTextAsync(scriptPath, command, cancellationToken);

        try
        {
            await RunProcessAsync("/bin/bash", scriptPath, workingDirectory, cancellationToken);
        }
        finally
        {
            if (File.Exists(scriptPath))
            {
                File.Delete(scriptPath);
            }
        }
    }

    private static async Task RunProcessAsync(string fileName, string arguments, string workingDirectory, CancellationToken cancellationToken)
    {
        var startInfo = new ProcessStartInfo
        {
            FileName = fileName,
            Arguments = arguments,
            WorkingDirectory = workingDirectory,
            RedirectStandardError = true,
            RedirectStandardOutput = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        using var process = Process.Start(startInfo);
        if (process is null)
        {
            throw new InvalidOperationException($"Failed to start process {fileName} {arguments}");
        }

        var stdOut = await process.StandardOutput.ReadToEndAsync(cancellationToken);
        var stdErr = await process.StandardError.ReadToEndAsync(cancellationToken);
        await process.WaitForExitAsync(cancellationToken);

        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException($"Process failed: {fileName} {arguments}\nSTDOUT:\n{stdOut}\nSTDERR:\n{stdErr}");
        }
    }
}
