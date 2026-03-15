using System.Diagnostics;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;

namespace SqlClone.Infrastructure.Docker;

public sealed class DockerSqlContainerManager : IDockerSqlContainerManager
{
    private readonly CloneOptions _options;
    private readonly ILogger<DockerSqlContainerManager> _logger;

    public DockerSqlContainerManager(IOptions<CloneOptions> options, ILogger<DockerSqlContainerManager> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task EnsureStartedAsync(CancellationToken cancellationToken)
    {
        var inspect = await RunDockerAsync($"inspect {_options.Docker.ContainerName} --format '{{{{.State.Running}}}}'", cancellationToken, false);
        if (inspect.ExitCode != 0)
        {
            await RunDockerRequiredAsync($"run -d --name {_options.Docker.ContainerName} -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD=\"{_options.Docker.SaPassword}\" -p {_options.Docker.HostPort}:1433 {_options.Docker.Image}", cancellationToken);
            return;
        }

        var running = inspect.StdOut.Trim().Equals("true", StringComparison.OrdinalIgnoreCase);
        if (!running)
        {
            await RunDockerRequiredAsync($"start {_options.Docker.ContainerName}", cancellationToken);
        }
    }

    public async Task TeardownAsync(CancellationToken cancellationToken)
    {
        await RunDockerAsync($"stop {_options.Docker.ContainerName}", cancellationToken, false);
        var volumeArg = _options.Docker.RemoveVolumeOnTeardown ? "-v" : string.Empty;
        await RunDockerAsync($"rm {volumeArg} {_options.Docker.ContainerName}".Trim(), cancellationToken, false);
    }

    public async Task WaitUntilReadyAsync(CancellationToken cancellationToken)
    {
        var connectionString = new SqlConnectionStringBuilder
        {
            DataSource = $"localhost,{_options.Docker.HostPort}",
            UserID = "sa",
            Password = _options.Docker.SaPassword,
            Encrypt = false,
            TrustServerCertificate = true,
            InitialCatalog = "master",
            ConnectTimeout = 2
        }.ConnectionString;

        var timeout = TimeSpan.FromMinutes(2);
        var started = DateTime.UtcNow;

        while (DateTime.UtcNow - started < timeout)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                await using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken);
                _logger.LogInformation("SQL container is ready");
                return;
            }
            catch
            {
                await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
            }
        }

        throw new TimeoutException("SQL container did not become ready before timeout.");
    }

    private async Task RunDockerRequiredAsync(string args, CancellationToken cancellationToken)
    {
        var result = await RunDockerAsync(args, cancellationToken, true);
        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException($"Docker command failed: docker {args}. {result.StdErr}");
        }
    }

    private async Task<(int ExitCode, string StdOut, string StdErr)> RunDockerAsync(string args, CancellationToken cancellationToken, bool log)
    {
        if (log)
        {
            _logger.LogInformation("docker {Args}", args);
        }

        var startInfo = new ProcessStartInfo
        {
            FileName = "docker",
            Arguments = args,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        using var process = Process.Start(startInfo) ?? throw new InvalidOperationException("Failed to start docker process");
        var stdOut = await process.StandardOutput.ReadToEndAsync(cancellationToken);
        var stdErr = await process.StandardError.ReadToEndAsync(cancellationToken);
        await process.WaitForExitAsync(cancellationToken);

        return (process.ExitCode, stdOut, stdErr);
    }
}
