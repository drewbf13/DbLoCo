<#
.SYNOPSIS
  Provisions a seeded local SQL Server via the SqlClone tool, snapshots it into a
  Docker image, and pushes that image to an Azure Container Registry (ACR).

.DESCRIPTION
  The SqlClone 'clone' command builds a local SQL Server container
  (Clone:Docker:ContainerName, default 'sqlclone-local') whose data lives in the
  container's writable layer (the mssql image declares no VOLUME), so a plain
  'docker commit' captures the fully seeded databases. This script orchestrates:

    clone -> stop container -> docker commit -> tag for ACR -> az acr login -> docker push

  Teammates can then 'docker pull' a ready-to-use seeded database instead of each
  running the multi-minute clone pipeline themselves.

.PARAMETER AcrName
  ACR registry short name (NOT the full login server). Login server is derived as
  "<AcrName>.azurecr.io". Required.

.PARAMETER DbImage
  Image repository name. Default 'sqlclone-localdb'.

.PARAMETER Tag
  Image tag. Defaults to today's UTC date, e.g. 20260608.

.PARAMETER ContainerName
  Name of the container the clone tool provisions. Must match Clone:Docker:ContainerName.
  Default 'sqlclone-local'.

.PARAMETER Environment
  Environment name passed to the clone tool (selects appsettings.<env>.json). Default 'Development'.

.PARAMETER SkipClone
  Skip running the clone pipeline and snapshot the existing container as-is.

.PARAMETER SkipPush
  Build and tag the image locally but do not log in to ACR or push.

.PARAMETER NoLatest
  Do not also tag/push ':latest'.

.PARAMETER KeepRunning
  Restart the local container after the snapshot is taken (clone stops it for a clean commit).

.EXAMPLE
  ./build-and-push-image.ps1 -AcrName myregistry

.EXAMPLE
  ./build-and-push-image.ps1 -AcrName myregistry -Tag release-candidate -SkipClone
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$AcrName,

    [string]$DbImage = 'sqlclone-localdb',

    [string]$Tag = (Get-Date -Format 'yyyyMMdd'),

    [string]$ContainerName = 'sqlclone-local',

    [string]$Environment = 'Development',

    [switch]$SkipClone,
    [switch]$SkipPush,
    [switch]$NoLatest,
    [switch]$KeepRunning
)

$ErrorActionPreference = 'Stop'

function Assert-Tool($name) {
    if (-not (Get-Command $name -ErrorAction SilentlyContinue)) {
        throw "Required tool '$name' was not found on PATH."
    }
}

function Invoke-Native($file, $arguments) {
    Write-Host "> $file $arguments" -ForegroundColor DarkGray
    & $file @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed (exit $LASTEXITCODE): $file $arguments"
    }
}

$loginServer = "$AcrName.azurecr.io"
$localRef    = "${DbImage}:${Tag}"
$acrRef      = "$loginServer/${DbImage}:${Tag}"
$acrLatest   = "$loginServer/${DbImage}:latest"
$toolRoot    = Split-Path -Parent $PSScriptRoot

Write-Host "SqlClone image build" -ForegroundColor Cyan
Write-Host "  tool root      : $toolRoot"
Write-Host "  container      : $ContainerName"
Write-Host "  local image    : $localRef"
Write-Host "  ACR image      : $acrRef"
Write-Host "  also :latest   : $([bool](-not $NoLatest))"
Write-Host "  push to ACR    : $([bool](-not $SkipPush))"
Write-Host ""

Assert-Tool docker
if (-not $SkipPush) { Assert-Tool az }

# 1. Provision + seed the local container via the clone pipeline.
if (-not $SkipClone) {
    Assert-Tool dotnet
    Write-Host "[1/5] Running clone pipeline (this can take a while)..." -ForegroundColor Green
    Push-Location $toolRoot
    try {
        Invoke-Native dotnet @('run', '--project', 'src/SqlClone.Console', '--', 'clone', '--environment', $Environment)
    }
    finally {
        Pop-Location
    }
}
else {
    Write-Host "[1/5] Skipping clone (snapshotting existing container)." -ForegroundColor Yellow
}

# Confirm the container exists before snapshotting.
$exists = (& docker ps -a --filter "name=^/$ContainerName$" --format '{{.Names}}')
if ($exists -ne $ContainerName) {
    throw "Container '$ContainerName' not found. Run without -SkipClone, or check Clone:Docker:ContainerName."
}

# 2. Stop the container so SQL Server flushes and the snapshot is consistent.
Write-Host "[2/5] Stopping container for a consistent snapshot..." -ForegroundColor Green
& docker stop $ContainerName | Out-Null

# 3. Commit the seeded container to a local image.
#    - Bake ACCEPT_EULA=Y so consumers don't have to pass it.
#    - Scrub MSSQL_SA_PASSWORD from image metadata; the SA password set at clone time
#      already lives inside the seeded master DB, so consumers authenticate with that value.
Write-Host "[3/5] Committing container -> $localRef ..." -ForegroundColor Green
Invoke-Native docker @('commit', '--change', 'ENV ACCEPT_EULA=Y', '--change', 'ENV MSSQL_SA_PASSWORD=', $ContainerName, $localRef)

# 4. Tag for ACR.
Write-Host "[4/5] Tagging for ACR..." -ForegroundColor Green
Invoke-Native docker @('tag', $localRef, $acrRef)
if (-not $NoLatest) { Invoke-Native docker @('tag', $localRef, $acrLatest) }

# Restart the local container if asked (clone left it stopped).
if ($KeepRunning) {
    Write-Host "      Restarting local container..." -ForegroundColor DarkGray
    & docker start $ContainerName | Out-Null
}

# 5. Push to ACR.
if ($SkipPush) {
    Write-Host "[5/5] Skipping push (-SkipPush). Local images ready:" -ForegroundColor Yellow
    Write-Host "        $acrRef"
    if (-not $NoLatest) { Write-Host "        $acrLatest" }
}
else {
    Write-Host "[5/5] Logging in to ACR '$AcrName' and pushing..." -ForegroundColor Green
    Invoke-Native az @('acr', 'login', '--name', $AcrName)
    Invoke-Native docker @('push', $acrRef)
    if (-not $NoLatest) { Invoke-Native docker @('push', $acrLatest) }
}

Write-Host ""
Write-Host "Done." -ForegroundColor Cyan
Write-Host "Consumers can run it with:" -ForegroundColor Cyan
Write-Host "  docker run -d --name $ContainerName -p 14333:1433 $acrRef"
