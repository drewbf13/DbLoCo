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

        if (string.IsNullOrWhiteSpace(migration.BuildCommand))
        {
            throw new InvalidOperationException("Migration is enabled but BuildCommand is not configured.");
        }

        var hasGitRepository = !string.IsNullOrWhiteSpace(migration.GitRepository);
        var hasLocalRepository = !string.IsNullOrWhiteSpace(migration.LocalRepositoryPath);
        if (!hasGitRepository && !hasLocalRepository)
        {
            throw new InvalidOperationException("Migration is enabled but no repository source is configured. Set GitRepository or LocalRepositoryPath.");
        }

        if (hasGitRepository && hasLocalRepository)
        {
            throw new InvalidOperationException("Migration config is ambiguous. Configure either GitRepository or LocalRepositoryPath, not both.");
        }

        var workRoot = string.Empty;
        var repositoryRoot = string.Empty;
        var cleanupWorkRoot = false;

        try
        {
            if (hasGitRepository)
            {
                workRoot = Path.Combine(Path.GetTempPath(), "sqlclone-migration", Guid.NewGuid().ToString("N"));
                Directory.CreateDirectory(workRoot);
                cleanupWorkRoot = true;
                repositoryRoot = workRoot;
                await RunProcessAsync("git", $"clone --depth 1 --branch {migration.Branch} {migration.GitRepository} .", repositoryRoot, cancellationToken);
            }
            else
            {
                repositoryRoot = Path.GetFullPath(Environment.ExpandEnvironmentVariables(migration.LocalRepositoryPath));
                if (!Directory.Exists(repositoryRoot))
                {
                    throw new DirectoryNotFoundException($"Migration local repository path not found: {repositoryRoot}");
                }
            }

            var runDirectory = string.IsNullOrWhiteSpace(migration.WorkingDirectory)
                ? repositoryRoot
                : Path.Combine(repositoryRoot, migration.WorkingDirectory);

            if (!Directory.Exists(runDirectory))
            {
                throw new DirectoryNotFoundException($"Migration working directory not found: {runDirectory}");
            }

            await RunShellCommandAsync(migration.BuildCommand, runDirectory, cancellationToken);
            if (hasGitRepository)
            {
                _logger.LogInformation("Migration build command completed from {Repository} ({Branch})", migration.GitRepository, migration.Branch);
            }
            else
            {
                _logger.LogInformation("Migration build command completed from local repository path {RepositoryPath}", repositoryRoot);
            }
        }
        finally
        {
            if (!cleanupWorkRoot || string.IsNullOrWhiteSpace(workRoot))
            {
                return;
            }

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
