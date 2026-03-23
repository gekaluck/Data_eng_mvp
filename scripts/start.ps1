param(
    [switch]$Rebuild,
    [switch]$FollowLogs
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Assert-CommandExists {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found on PATH."
    }
}

function Invoke-Docker {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$Args
    )

    & docker @Args
    if ($LASTEXITCODE -ne 0) {
        throw "docker $($Args -join ' ') failed with exit code $LASTEXITCODE."
    }
}

function Assert-EnvFileReady {
    param([string]$EnvPath)

    if (-not (Test-Path $EnvPath)) {
        if (-not (Test-Path ".env.example")) {
            throw ".env is missing and .env.example was not found."
        }
        Copy-Item ".env.example" ".env"
        Write-Host "Created .env from .env.example."
        Write-Host "Update secrets in .env, then rerun this script."
        throw "Stopping because .env was just created from template."
    }

    $content = Get-Content $EnvPath -Raw
    $placeholderValues = @(
        "REPLACE_WITH_GENERATED_FERNET_KEY",
        "REPLACE_WITH_STRONG_PASSWORD"
    )

    foreach ($placeholder in $placeholderValues) {
        if ($content.Contains($placeholder)) {
            throw ".env contains placeholder value '$placeholder'. Update .env and rerun."
        }
    }
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir

Push-Location $repoRoot
try {
    Assert-CommandExists -Name "docker"
    Invoke-Docker compose version | Out-Null
    Invoke-Docker info | Out-Null
    Assert-EnvFileReady -EnvPath ".env"

    docker image inspect data-eng-mvp-airflow:latest *> $null
    $customImageExists = ($LASTEXITCODE -eq 0)

    if ($Rebuild -or -not $customImageExists) {
        Write-Host "Rebuilding images..."
        Invoke-Docker compose build
    } else {
        Write-Host "Using existing image data-eng-mvp-airflow:latest (pass -Rebuild to rebuild)."
    }

    Write-Host "Starting services..."
    Invoke-Docker compose up -d

    Write-Host ""
    Write-Host "Current service status:"
    Invoke-Docker compose ps
    Write-Host ""
    Write-Host "Airflow UI: http://localhost:8080"
    Write-Host "MinIO Console: http://localhost:9001"
    Write-Host ""
    Write-Host "To stop: docker compose down"

    if ($FollowLogs) {
        docker compose logs -f
    }
}
finally {
    Pop-Location
}
