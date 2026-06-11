<#
.SYNOPSIS
  Stands up the "demo-source" SQL Server container that plays the role of the shared
  dev environment (e.g. an Azure SQL Managed Instance) in the SqlClone demo.

.DESCRIPTION
  One-time (or any-time reset) setup for the demo:

    1. Start a SQL Server 2022 container named 'demo-source' on the given port
    2. Build the ScoutHub schema by running its EF migrations
    3. Load fictional scouting data (32 teams / 500 players / 50,000 reports)
    4. Plant "drift" programmable objects that exist in no migration
       (the clone's programmable-object sync reveals these during the demo)

  Re-running the script resets the data (the seed script clears and reloads).

.PARAMETER Port
  Host port for the demo-source container. Default 14999 (the demo clone uses 14333).

.PARAMETER SaPassword
  SA password for the demo-source container. Default 'ScoutHub!Demo1' — also baked into
  appsettings.Demo.json; change both together.

.PARAMETER Recreate
  Remove any existing demo-source container first.
#>
[CmdletBinding()]
param(
    [int]$Port = 14999,
    [string]$SaPassword = 'ScoutHub!Demo1',
    [switch]$Recreate
)

$ErrorActionPreference = 'Stop'
$demoRoot = $PSScriptRoot
$repoRoot = Split-Path -Parent $demoRoot
$containerName = 'demo-source'
$connectionString = "Server=localhost,$Port;Database=ScoutHub;User Id=sa;Password=$SaPassword;Encrypt=False;TrustServerCertificate=True"

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw "docker was not found on PATH." }
if (-not (Get-Command dotnet -ErrorAction SilentlyContinue)) { throw "dotnet was not found on PATH." }

$existing = (& docker ps -a --filter "name=^/$containerName$" --format '{{.Names}}')
if ($existing -eq $containerName) {
    if ($Recreate) {
        Write-Host "Removing existing $containerName container..." -ForegroundColor Yellow
        & docker rm -f $containerName | Out-Null
        $existing = $null
    }
    else {
        Write-Host "Container $containerName already exists; starting it (use -Recreate for a clean slate)." -ForegroundColor Yellow
        & docker start $containerName | Out-Null
    }
}

if ($existing -ne $containerName) {
    Write-Host "[1/4] Starting $containerName on port $Port..." -ForegroundColor Green
    & docker run -d --name $containerName `
        -e 'ACCEPT_EULA=Y' `
        -e "MSSQL_SA_PASSWORD=$SaPassword" `
        -p "${Port}:1433" `
        mcr.microsoft.com/mssql/server:2022-latest | Out-Null
}
else {
    Write-Host "[1/4] Reusing running $containerName container." -ForegroundColor Green
}

Write-Host "      Waiting for SQL Server to accept connections..." -ForegroundColor DarkGray
$deadline = (Get-Date).AddMinutes(3)
while ($true) {
    # cmd /c swallows the probe's stderr so Windows PowerShell 5.1 ($ErrorActionPreference
    # = 'Stop') doesn't turn a not-ready-yet attempt into a terminating error.
    & cmd /c "docker exec $containerName /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P $SaPassword -Q `"SELECT 1`" >nul 2>&1"
    if ($LASTEXITCODE -eq 0) { break }
    if ((Get-Date) -gt $deadline) { throw "SQL Server in $containerName did not become ready within 3 minutes." }
    Start-Sleep -Seconds 3
}

Write-Host "[2/4] Building ScoutHub schema (EF migrations)..." -ForegroundColor Green
& dotnet run --project (Join-Path $demoRoot 'ScoutHub') -- $connectionString
if ($LASTEXITCODE -ne 0) { throw "ScoutHub migration run failed (exit $LASTEXITCODE)." }

Write-Host "[3/4] Loading fictional scouting data..." -ForegroundColor Green
& docker cp (Join-Path $demoRoot 'seed-source.sql') "${containerName}:/tmp/seed-source.sql"
& docker exec $containerName /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P $SaPassword -b -i /tmp/seed-source.sql
if ($LASTEXITCODE -ne 0) { throw "Seed script failed (exit $LASTEXITCODE)." }

Write-Host "[4/4] Planting drift objects (views/procs/functions in no migration)..." -ForegroundColor Green
& docker cp (Join-Path $demoRoot 'plant-drift.sql') "${containerName}:/tmp/plant-drift.sql"
& docker exec $containerName /opt/mssql-tools18/bin/sqlcmd -C -S localhost -U sa -P $SaPassword -b -i /tmp/plant-drift.sql
if ($LASTEXITCODE -ne 0) { throw "Drift script failed (exit $LASTEXITCODE)." }

Write-Host ""
Write-Host "demo-source is ready." -ForegroundColor Cyan
Write-Host "  connection : $connectionString"
Write-Host "  next       : dotnet run --project src/SqlClone.Console -- clone --environment Demo   (from $repoRoot)"
