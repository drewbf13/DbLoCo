<#
.SYNOPSIS
  Regenerates the local SQL Server clone from scratch using the full SqlClone pipeline.

.DESCRIPTION
  Runs the SqlClone tool end to end so the local container is rebuilt with every step
  the pipeline applies:

    teardown (remove old container) ->
    clone:
      - start SQL Server container
      - materialize the configured databases
      - apply linked servers
      - run the configured migration command to build the schema
      - seed the configured tables from the source
      - sync programmable objects: query the source for the functions / views / stored
        procedures the migrated schema is missing and create them, auto-capturing any
        migration drift
      - run any custom post-clone scripts in scripts/postclone
    validate

  Configuration (source connection, SA password, seed table list, etc.) comes from
  src/SqlClone.Console/appsettings.<Environment>.json + appsettings.Local.json, exactly
  like running the tool by hand. Requires Docker Desktop running and network access to
  the source SQL endpoint.

.PARAMETER Environment
  Environment name passed to the tool (selects appsettings.<env>.json). Default 'Development'.

.PARAMETER NoTeardown
  Skip removing the existing container first (re-seeds in place instead of a clean rebuild).

.PARAMETER NoValidate
  Skip the final validate step.

.PARAMETER AcrName
  When set, after regenerating, commit the running container to an image and push it to
  "<AcrName>.azurecr.io" via build-and-push-image.ps1 (az acr login + docker push).
  Registry short name, not the full login server. When omitted, nothing is built or pushed.

.PARAMETER DbImage
  Image repository name for the push. Default 'sqlclone-localdb'. Only used with -AcrName.

.PARAMETER Tag
  Image tag for the push. Defaults to today's UTC date. Only used with -AcrName.

.PARAMETER NoLatest
  Do not also tag/push ':latest'. Only used with -AcrName.

.EXAMPLE
  ./regenerate-local-db.ps1

.EXAMPLE
  ./regenerate-local-db.ps1 -Environment Development -NoTeardown

.EXAMPLE
  # Regenerate, then commit + push the result to ACR
  ./regenerate-local-db.ps1 -AcrName myregistry
#>
[CmdletBinding()]
param(
    [string]$Environment = 'Development',
    [switch]$NoTeardown,
    [switch]$NoValidate,

    [string]$AcrName,
    [string]$DbImage = 'sqlclone-localdb',
    [string]$Tag = (Get-Date -Format 'yyyyMMdd'),
    [switch]$NoLatest
)

$ErrorActionPreference = 'Stop'
$toolRoot = Split-Path -Parent $PSScriptRoot

function Invoke-Tool([string[]]$toolArgs) {
    Write-Host "> dotnet run --project src/SqlClone.Console -- $($toolArgs -join ' ')" -ForegroundColor DarkGray
    Push-Location $toolRoot
    try {
        & dotnet run --project src/SqlClone.Console -- @toolArgs
        if ($LASTEXITCODE -ne 0) {
            throw "SqlClone '$($toolArgs[0])' failed (exit $LASTEXITCODE)."
        }
    }
    finally {
        Pop-Location
    }
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw "docker was not found on PATH." }
if (-not (Get-Command dotnet -ErrorAction SilentlyContinue)) { throw "dotnet was not found on PATH." }

Write-Host "Regenerating local clone (environment: $Environment)" -ForegroundColor Cyan

if (-not $NoTeardown) {
    Write-Host "[1/3] Tearing down any existing container..." -ForegroundColor Green
    Invoke-Tool @('teardown')
}
else {
    Write-Host "[1/3] Skipping teardown (-NoTeardown); will re-seed in place." -ForegroundColor Yellow
}

Write-Host "[2/3] Cloning: migrate schema -> seed -> sync functions/views/procs -> post-clone scripts..." -ForegroundColor Green
Invoke-Tool @('clone', '--environment', $Environment)

if (-not $NoValidate) {
    Write-Host "[3/3] Validating..." -ForegroundColor Green
    Invoke-Tool @('validate')
}
else {
    Write-Host "[3/3] Skipping validate (-NoValidate)." -ForegroundColor Yellow
}

Write-Host "`nLocal clone regenerated." -ForegroundColor Cyan

if (-not [string]::IsNullOrWhiteSpace($AcrName)) {
    Write-Host "`nBuilding + pushing image to ACR '$AcrName'..." -ForegroundColor Cyan
    $pushScript = Join-Path $PSScriptRoot 'build-and-push-image.ps1'
    $pushArgs = @{
        AcrName     = $AcrName
        DbImage     = $DbImage
        Tag         = $Tag
        SkipClone   = $true   # the container was just regenerated above
        KeepRunning = $true   # leave the local DB usable after the snapshot
    }
    if ($NoLatest) { $pushArgs.NoLatest = $true }
    & $pushScript @pushArgs
}
