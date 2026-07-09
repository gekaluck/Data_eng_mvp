param(
    [ValidateSet("up", "down", "restart", "status", "logs")]
    [string]$Command = "up",

    [switch]$Rebuild,
    [switch]$FollowLogs,
    [switch]$Volumes
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
        Write-Host "Update the placeholder values in .env, then rerun this script."
        throw "Stopping because .env was just created from template."
    }

    $content = Get-Content $EnvPath -Raw
    if ($content.Contains("REPLACE_WITH_")) {
        throw ".env still contains REPLACE_WITH_ placeholder values. Update .env and rerun."
    }
}

function Test-AirflowImageExists {
    & docker image inspect data-eng-mvp-airflow:latest *> $null
    return ($LASTEXITCODE -eq 0)
}

function Show-ServiceUrls {
    Write-Host ""
    Write-Host "Local services:"
    Write-Host "  Airflow:     http://localhost:8080"
    Write-Host "  MinIO:       http://localhost:9001"
    Write-Host "  Trino:       http://localhost:8081"
    Write-Host "  JupyterLab:  http://localhost:8888"
    Write-Host ""
    Write-Host "Useful commands:"
    Write-Host "  .\scripts\stack.ps1 status"
    Write-Host "  .\scripts\stack.ps1 logs"
    Write-Host "  .\scripts\stack.ps1 down"
}

function Initialize-StackCommand {
    Assert-CommandExists -Name "docker"
    Invoke-Docker compose version | Out-Null
    Invoke-Docker info | Out-Null
}

function Start-Stack {
    Assert-EnvFileReady -EnvPath ".env"

    if ($Rebuild -or -not (Test-AirflowImageExists)) {
        Write-Host "Building project images..."
        Invoke-Docker compose build
    } else {
        Write-Host "Using existing image data-eng-mvp-airflow:latest (pass -Rebuild to rebuild)."
    }

    Write-Host "Starting services..."
    Invoke-Docker compose up -d

    Write-Host ""
    Write-Host "Current service status:"
    Invoke-Docker compose ps
    Show-ServiceUrls

    if ($FollowLogs) {
        Invoke-Docker compose logs -f
    }
}

function Stop-Stack {
    if ($Volumes) {
        Write-Host "Stopping services and deleting Docker volumes..."
        Invoke-Docker compose down -v
        return
    }

    Write-Host "Stopping services. Docker volumes are preserved."
    Invoke-Docker compose down
}

function Restart-Stack {
    Stop-Stack
    Start-Stack
}

function Show-Status {
    Invoke-Docker compose ps
    Show-ServiceUrls
}

function Show-Logs {
    Invoke-Docker compose logs -f
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Split-Path -Parent $scriptDir

Push-Location $repoRoot
try {
    Initialize-StackCommand

    if ($Volumes -and $Command -ne "down") {
        throw "-Volumes can only be used with the down command."
    }

    switch ($Command) {
        "up" { Start-Stack }
        "down" { Stop-Stack }
        "restart" { Restart-Stack }
        "status" { Show-Status }
        "logs" { Show-Logs }
    }
}
finally {
    Pop-Location
}
