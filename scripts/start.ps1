param(
    [switch]$Rebuild,
    [switch]$FollowLogs
)

$scriptPath = Join-Path $PSScriptRoot "stack.ps1"
& $scriptPath up -Rebuild:$Rebuild -FollowLogs:$FollowLogs
