param(
    [switch]$Volumes
)

$scriptPath = Join-Path $PSScriptRoot "stack.ps1"
& $scriptPath down -Volumes:$Volumes
