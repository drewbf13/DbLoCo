using System.CommandLine;
using System.Diagnostics;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Serilog;
using SqlClone.Domain.Interfaces;
using SqlClone.Domain.Models;
using SqlClone.Infrastructure;

var builder = Host.CreateApplicationBuilder(args);

builder.Configuration
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .AddJsonFile($"appsettings.{builder.Environment.EnvironmentName}.json", optional: true, reloadOnChange: true)
    .AddJsonFile("appsettings.Local.json", optional: true, reloadOnChange: true)
    .AddEnvironmentVariables();

builder.Services
    .AddOptions<CloneOptions>()
    .Bind(builder.Configuration.GetSection(CloneOptions.SectionName));

Log.Logger = new LoggerConfiguration()
    .ReadFrom.Configuration(builder.Configuration)
    .Enrich.FromLogContext()
    .CreateLogger();

builder.Services.AddSerilog();
builder.Services.AddSqlClone();

using var host = builder.Build();

var root = new RootCommand("SQL clone utility");
var envOption = new Option<string>("--environment", () => "Development", "Environment name override");
var keepVolumeOption = new Option<bool>("--keep-volume", "Keep volume during teardown");

var init = new Command("init", "Create config defaults and verify docker") { envOption };
init.SetHandler(async (string environment) => await InitAsync(environment, host, CancellationToken.None), envOption);

var inspect = new Command("inspect-source", "Inspect source SQL endpoint");
inspect.SetHandler(async () =>
{
    var inspector = host.Services.GetRequiredService<ISourceInspector>();
    var databases = await inspector.GetDatabasesAsync(CancellationToken.None);
    Console.WriteLine($"Found {databases.Count} source user databases:");
    foreach (var db in databases)
    {
        Console.WriteLine($"- {db.Name} | {db.StateDescription} | {db.RecoveryModel} | Created UTC {db.CreateDateUtc:O}");
    }
});

var clone = new Command("clone", "Provision local SQL clone") { envOption };
clone.SetHandler(async (string environment) =>
{
    host.Services.GetRequiredService<IOptions<CloneOptions>>().Value.EnvironmentName = environment;
    var orchestrator = host.Services.GetRequiredService<ICloneOrchestrator>();
    var result = await orchestrator.CloneAsync(CancellationToken.None);
    Console.WriteLine(result.Success ? "Clone succeeded." : "Clone failed.");
    foreach (var message in result.Messages)
    {
        Console.WriteLine($"  {message}");
    }
}, envOption);

var validate = new Command("validate", "Validate local SQL clone state");
validate.SetHandler(async () =>
{
    var validator = host.Services.GetRequiredService<ICloneValidator>();
    var result = await validator.ValidateAsync(CancellationToken.None);
    Console.WriteLine($"SQL reachable: {result.SqlReachable}");
    foreach (var db in result.Databases)
    {
        Console.WriteLine($"Database {db.Key}: {(db.Value ? "OK" : "Missing")}");
    }

    foreach (var linked in result.LinkedServers)
    {
        Console.WriteLine($"Linked server {linked.Key}: {(linked.Value ? "OK" : "Missing")}");
    }

    Console.WriteLine(result.IsSuccessful ? "Validation successful." : "Validation failed.");
});

var teardown = new Command("teardown", "Stop and remove SQL container") { keepVolumeOption };
teardown.SetHandler(async (bool keepVolume) =>
{
    if (keepVolume)
    {
        host.Services.GetRequiredService<IOptions<CloneOptions>>().Value.Docker.RemoveVolumeOnTeardown = false;
    }

    await host.Services.GetRequiredService<IDockerSqlContainerManager>().TeardownAsync(CancellationToken.None);
    Console.WriteLine("Teardown complete.");
}, keepVolumeOption);

root.Add(init);
root.Add(inspect);
root.Add(clone);
root.Add(validate);
root.Add(teardown);

return await root.InvokeAsync(args);

static async Task InitAsync(string environment, IHost host, CancellationToken cancellationToken)
{
    var localFile = Path.Combine(Environment.CurrentDirectory, "appsettings.Local.json");
    if (!File.Exists(localFile))
    {
        await File.WriteAllTextAsync(localFile, "{}", cancellationToken);
        Console.WriteLine("Created appsettings.Local.json");
    }

    var dockerVersion = await RunProcessAsync("docker", "--version", cancellationToken);
    Console.WriteLine(dockerVersion.exitCode == 0
        ? $"Docker detected: {dockerVersion.stdOut.Trim()}"
        : "Docker not detected. Install Docker Desktop and ensure docker CLI is in PATH.");

    var options = host.Services.GetRequiredService<IOptions<CloneOptions>>().Value;
    Console.WriteLine($"Environment: {environment}");
    Console.WriteLine("Required settings/secrets:");
    Console.WriteLine("- Clone:Source:ConnectionString");
    Console.WriteLine($"- Clone:Docker:SaPassword (current length: {options.Docker.SaPassword.Length})");
}

static async Task<(int exitCode, string stdOut, string stdErr)> RunProcessAsync(string fileName, string arguments, CancellationToken cancellationToken)
{
    var startInfo = new ProcessStartInfo
    {
        FileName = fileName,
        Arguments = arguments,
        RedirectStandardOutput = true,
        RedirectStandardError = true,
        UseShellExecute = false,
        CreateNoWindow = true
    };

    try
    {
        using var process = Process.Start(startInfo);
        if (process is null)
        {
            return (-1, string.Empty, "Could not start process.");
        }

        var stdOut = await process.StandardOutput.ReadToEndAsync(cancellationToken);
        var stdErr = await process.StandardError.ReadToEndAsync(cancellationToken);
        await process.WaitForExitAsync(cancellationToken);
        return (process.ExitCode, stdOut, stdErr);
    }
    catch (Exception ex)
    {
        return (-1, string.Empty, ex.Message);
    }
}
