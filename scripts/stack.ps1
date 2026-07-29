param(
    [ValidateSet("up", "down", "restart", "status", "logs", "superset")]
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

function Assert-SupersetEnvReady {
    param([string]$EnvPath)

    $required = @(
        "SUPERSET_SECRET_KEY",
        "SUPERSET_ADMIN_PASSWORD"
    )
    $lines = Get-Content $EnvPath
    $missing = @()

    foreach ($name in $required) {
        $processValue = [Environment]::GetEnvironmentVariable($name)
        $envLine = $lines | Where-Object { $_ -match "^$([regex]::Escape($name))=" } | Select-Object -First 1
        $fileValue = if ($envLine) { ($envLine -split "=", 2)[1].Trim() } else { "" }
        $value = if ($processValue) { $processValue } else { $fileValue }

        if (-not $value -or $value.Contains("REPLACE_WITH_")) {
            $missing += $name
        }
    }

    if ($missing.Count -gt 0) {
        throw "Superset configuration is missing or still placeholder-valued: $($missing -join ', '). Update .env and rerun."
    }
}

function Initialize-SupersetSecret {
    param([string]$EnvPath)

    $processValue = [Environment]::GetEnvironmentVariable("SUPERSET_SECRET_KEY")
    if ($processValue -and -not $processValue.Contains("REPLACE_WITH_")) {
        return
    }

    $lines = [System.Collections.Generic.List[string]]::new()
    $lines.AddRange([string[]](Get-Content -LiteralPath $EnvPath))
    $secretIndex = -1
    $fileValue = ""

    for ($index = 0; $index -lt $lines.Count; $index++) {
        if ($lines[$index] -match '^SUPERSET_SECRET_KEY=') {
            $secretIndex = $index
            $fileValue = ($lines[$index] -split "=", 2)[1].Trim()
            break
        }
    }

    if ($fileValue -and -not $fileValue.Contains("REPLACE_WITH_")) {
        return
    }

    $bytes = New-Object byte[] 64
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $generator.GetBytes($bytes)
    } finally {
        $generator.Dispose()
    }
    $secret = [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+', '-').Replace('/', '_')
    $secretLine = "SUPERSET_SECRET_KEY=$secret"

    if ($secretIndex -ge 0) {
        $lines[$secretIndex] = $secretLine
    } else {
        if ($lines.Count -gt 0 -and $lines[$lines.Count - 1] -ne "") {
            $lines.Add("")
        }
        $lines.Add($secretLine)
    }

    $utf8WithoutBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllLines((Resolve-Path $EnvPath), $lines, $utf8WithoutBom)
    [Environment]::SetEnvironmentVariable("SUPERSET_SECRET_KEY", $secret, "Process")
    Write-Host "Generated SUPERSET_SECRET_KEY and saved it to .env."
}

function Show-ServiceUrls {
    Write-Host ""
    Write-Host "Local services:"
    Write-Host "  Airflow:     http://localhost:8080"
    Write-Host "  MinIO:       http://localhost:9001"
    Write-Host "  Trino:       http://localhost:8081"
    Write-Host "  JupyterLab:  http://localhost:8888"
    Write-Host "  Superset:    http://localhost:8088 (when the serving profile is running)"
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

function Start-Superset {
    Assert-EnvFileReady -EnvPath ".env"
    Initialize-SupersetSecret -EnvPath ".env"
    Assert-SupersetEnvReady -EnvPath ".env"

    Write-Host "Building and starting the Superset serving profile..."
    Invoke-Docker compose --profile serving up -d --build superset
    Invoke-Docker compose --profile serving ps
    Write-Host ""
    Write-Host "Superset: http://localhost:8088"
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
        "superset" { Start-Superset }
    }
}
finally {
    Pop-Location
}
