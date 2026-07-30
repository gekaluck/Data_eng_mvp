Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:DefaultMaxDates = 120
$script:SparkDagId = "gold_coincap_assets"
$script:DbtDagId = "gold_dbt_coincap_assets"
$script:SparkTables = @(
    "gold.crypto.daily_snapshot",
    "gold.crypto.market_cap_rank_change",
    "gold.crypto.weekly_rolling_average"
)
$script:DbtTables = @(
    "gold.crypto_dbt.daily_snapshot",
    "gold.crypto_dbt.mc_rank_change",
    "gold.crypto_dbt.weekly_roll_avg"
)

function Show-Usage {
    @"
Usage:
  .\scripts\rebuild_gold.ps1 [--dry-run] [--engine spark|dbt|both]
      [--start-date YYYY-MM-DD --end-date YYYY-MM-DD]
      [--max-dates N] [--poll-seconds N] [--run-timeout-seconds N]

Default mode discovers Silver dates and rebuilds only dates incomplete in the
selected Gold engine. Supplying a date range explicitly reruns every actual
Silver date in that inclusive range. Calendar dates absent from Silver are
never submitted to Airflow.
"@ | Write-Host
}

function ConvertTo-IsoDate {
    param(
        [Parameter(Mandatory = $true)][string]$Value,
        [Parameter(Mandatory = $true)][string]$OptionName
    )

    $parsed = [datetime]::MinValue
    $ok = [datetime]::TryParseExact(
        $Value,
        "yyyy-MM-dd",
        [Globalization.CultureInfo]::InvariantCulture,
        [Globalization.DateTimeStyles]::None,
        [ref]$parsed
    )
    if (-not $ok) {
        throw "$OptionName must use YYYY-MM-DD (received '$Value')."
    }
    return $parsed.ToString("yyyy-MM-dd")
}

function ConvertTo-PositiveInt {
    param(
        [Parameter(Mandatory = $true)][string]$Value,
        [Parameter(Mandatory = $true)][string]$OptionName
    )

    $parsed = 0
    if (-not [int]::TryParse($Value, [ref]$parsed) -or $parsed -lt 1) {
        throw "$OptionName must be a positive integer (received '$Value')."
    }
    return $parsed
}

function Parse-RebuildGoldArguments {
    param([string[]]$Arguments)

    $options = [ordered]@{
        DryRun = $false
        Engine = "both"
        StartDate = $null
        EndDate = $null
        MaxDates = $script:DefaultMaxDates
        PollSeconds = 10
        RunTimeoutSeconds = 7200
        Help = $false
    }

    for ($index = 0; $index -lt $Arguments.Count; $index++) {
        $argument = $Arguments[$index]
        switch ($argument) {
            "--dry-run" { $options.DryRun = $true; continue }
            "--help" { $options.Help = $true; continue }
            "-h" { $options.Help = $true; continue }
            "--engine" {
                if (++$index -ge $Arguments.Count) { throw "--engine requires a value." }
                $engine = $Arguments[$index].ToLowerInvariant()
                if ($engine -notin @("spark", "dbt", "both")) {
                    throw "--engine must be spark, dbt, or both."
                }
                $options.Engine = $engine
                continue
            }
            "--start-date" {
                if (++$index -ge $Arguments.Count) { throw "--start-date requires a value." }
                $options.StartDate = ConvertTo-IsoDate $Arguments[$index] "--start-date"
                continue
            }
            "--end-date" {
                if (++$index -ge $Arguments.Count) { throw "--end-date requires a value." }
                $options.EndDate = ConvertTo-IsoDate $Arguments[$index] "--end-date"
                continue
            }
            "--max-dates" {
                if (++$index -ge $Arguments.Count) { throw "--max-dates requires a value." }
                $options.MaxDates = ConvertTo-PositiveInt $Arguments[$index] "--max-dates"
                continue
            }
            "--poll-seconds" {
                if (++$index -ge $Arguments.Count) { throw "--poll-seconds requires a value." }
                $options.PollSeconds = ConvertTo-PositiveInt $Arguments[$index] "--poll-seconds"
                continue
            }
            "--run-timeout-seconds" {
                if (++$index -ge $Arguments.Count) { throw "--run-timeout-seconds requires a value." }
                $options.RunTimeoutSeconds = ConvertTo-PositiveInt $Arguments[$index] "--run-timeout-seconds"
                continue
            }
            default { throw "Unknown argument: $argument" }
        }
    }

    if (($null -eq $options.StartDate) -xor ($null -eq $options.EndDate)) {
        throw "Provide both --start-date and --end-date, or neither."
    }
    if ($null -ne $options.StartDate -and $options.EndDate -lt $options.StartDate) {
        throw "--end-date must be on or after --start-date."
    }

    return [pscustomobject]$options
}

function Invoke-DockerCommand {
    param([Parameter(Mandatory = $true)][string[]]$Arguments)

    $stdoutPath = [IO.Path]::GetTempFileName()
    $stderrPath = [IO.Path]::GetTempFileName()
    try {
        # Windows PowerShell promotes native stderr to its error stream. Trino emits
        # a harmless JLine warning there, so capture it without Stop semantics and
        # decide success solely from the native exit code.
        $previousErrorActionPreference = $ErrorActionPreference
        try {
            $ErrorActionPreference = "Continue"
            & docker @Arguments 1> $stdoutPath 2> $stderrPath
            $exitCode = $LASTEXITCODE
        }
        finally {
            $ErrorActionPreference = $previousErrorActionPreference
        }
        $stdout = Get-Content -LiteralPath $stdoutPath -Raw
        $stderr = Get-Content -LiteralPath $stderrPath -Raw
        if ($exitCode -ne 0) {
            throw "docker $($Arguments -join ' ') failed with exit code $exitCode.`n$stderr$stdout"
        }
        if ($stderr -and $VerbosePreference -ne "SilentlyContinue") {
            Write-Verbose $stderr.Trim()
        }
        return $stdout
    }
    finally {
        Remove-Item -LiteralPath $stdoutPath, $stderrPath -Force -ErrorAction SilentlyContinue
    }
}

function New-StringSet {
    param([string[]]$Values = @())

    $set = [Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($value in $Values) { [void]$set.Add($value) }
    return ,$set
}

function ConvertFrom-JsonItems {
    param([Parameter(Mandatory = $true)][string]$Json)

    # Windows PowerShell 5 can preserve a JSON top-level array as one nested
    # pipeline object. Enumerate it explicitly so callers filter individual rows.
    $parsed = $Json | ConvertFrom-Json
    $items = @()
    foreach ($item in $parsed) { $items += $item }
    return $items
}

function Get-GoldCoverage {
    $sql = @'
WITH coverage AS (
    SELECT 'silver.crypto.price_snapshots' AS dataset, snapshot_date
    FROM silver.crypto.price_snapshots
    GROUP BY snapshot_date
    UNION ALL
    SELECT 'gold.crypto.daily_snapshot', snapshot_date
    FROM gold.crypto.daily_snapshot GROUP BY snapshot_date
    UNION ALL
    SELECT 'gold.crypto.market_cap_rank_change', snapshot_date
    FROM gold.crypto.market_cap_rank_change GROUP BY snapshot_date
    UNION ALL
    SELECT 'gold.crypto.weekly_rolling_average', snapshot_date
    FROM gold.crypto.weekly_rolling_average GROUP BY snapshot_date
    UNION ALL
    SELECT 'gold.crypto_dbt.daily_snapshot', snapshot_date
    FROM gold.crypto_dbt.daily_snapshot GROUP BY snapshot_date
    UNION ALL
    SELECT 'gold.crypto_dbt.mc_rank_change', snapshot_date
    FROM gold.crypto_dbt.mc_rank_change GROUP BY snapshot_date
    UNION ALL
    SELECT 'gold.crypto_dbt.weekly_roll_avg', snapshot_date
    FROM gold.crypto_dbt.weekly_roll_avg GROUP BY snapshot_date
)
SELECT dataset, CAST(snapshot_date AS varchar) AS snapshot_date
FROM coverage
ORDER BY dataset, snapshot_date
'@

    $output = Invoke-DockerCommand @(
        "compose", "exec", "-T", "trino", "trino",
        "--output-format", "TSV_HEADER", "--execute", $sql
    )
    $rows = @($output | ConvertFrom-Csv -Delimiter "`t")
    $tableDates = @{}
    $allDatasets = @("silver.crypto.price_snapshots") + $script:SparkTables + $script:DbtTables
    foreach ($dataset in $allDatasets) { $tableDates[$dataset] = New-StringSet }
    foreach ($row in $rows) {
        if (-not $tableDates.ContainsKey($row.dataset)) {
            throw "Unexpected dataset returned by discovery query: $($row.dataset)"
        }
        [void]$tableDates[$row.dataset].Add($row.snapshot_date)
    }

    return [pscustomobject]@{
        SilverDates = @($tableDates["silver.crypto.price_snapshots"] | Sort-Object)
        TableDates = $tableDates
    }
}

function Get-ContiguousBlocks {
    param([string[]]$Dates)

    $sortedDates = @($Dates | Sort-Object -Unique)
    if ($sortedDates.Count -eq 0) { return @() }

    $blocks = @()
    $currentDates = @($sortedDates[0])
    for ($index = 1; $index -lt $sortedDates.Count; $index++) {
        $previous = [datetime]::ParseExact($sortedDates[$index - 1], "yyyy-MM-dd", $null)
        $current = [datetime]::ParseExact($sortedDates[$index], "yyyy-MM-dd", $null)
        if (($current - $previous).Days -eq 1) {
            $currentDates += $sortedDates[$index]
            continue
        }
        $blocks += [pscustomobject]@{
            StartDate = $currentDates[0]
            EndDate = $currentDates[-1]
            Count = $currentDates.Count
            Dates = @($currentDates)
        }
        $currentDates = @($sortedDates[$index])
    }
    $blocks += [pscustomobject]@{
        StartDate = $currentDates[0]
        EndDate = $currentDates[-1]
        Count = $currentDates.Count
        Dates = @($currentDates)
    }
    return $blocks
}

function Test-EngineDateComplete {
    param(
        [Parameter(Mandatory = $true)]$Coverage,
        [Parameter(Mandatory = $true)][string]$Engine,
        [Parameter(Mandatory = $true)][string]$Date
    )

    $requiredTables = if ($Engine -eq "spark") { $script:SparkTables } else { $script:DbtTables }
    foreach ($table in $requiredTables) {
        if (-not $Coverage.TableDates[$table].Contains($Date)) { return $false }
    }
    return $true
}

function New-RebuildPlan {
    param(
        [Parameter(Mandatory = $true)]$Coverage,
        [Parameter(Mandatory = $true)][string]$Engine,
        [string]$StartDate,
        [string]$EndDate,
        [int]$MaxDates = $script:DefaultMaxDates
    )

    $explicitRange = -not [string]::IsNullOrEmpty($StartDate)
    $eligibleDates = @($Coverage.SilverDates | Where-Object {
        -not $explicitRange -or ($_ -ge $StartDate -and $_ -le $EndDate)
    })

    $sparkDates = @()
    if ($Engine -in @("spark", "both")) {
        $sparkDates = if ($explicitRange) {
            @($eligibleDates)
        } else {
            @($eligibleDates | Where-Object { -not (Test-EngineDateComplete $Coverage "spark" $_) })
        }
    }

    $dbtDates = @()
    if ($Engine -in @("dbt", "both")) {
        $dbtDates = if ($explicitRange) {
            @($eligibleDates)
        } else {
            @($eligibleDates | Where-Object { -not (Test-EngineDateComplete $Coverage "dbt" $_) })
        }
    }

    foreach ($target in @(
        [pscustomobject]@{ Name = "Spark"; Dates = @($sparkDates) },
        [pscustomobject]@{ Name = "dbt"; Dates = @($dbtDates) }
    )) {
        if ($target.Dates.Count -gt $MaxDates) {
            throw "$($target.Name) plan contains $($target.Dates.Count) dates, exceeding the --max-dates safety cap of $MaxDates."
        }
    }

    return [pscustomobject]@{
        ExplicitRange = $explicitRange
        EligibleDates = @($eligibleDates)
        SparkDates = @($sparkDates)
        DbtDates = @($dbtDates)
        SparkBlocks = @(Get-ContiguousBlocks $sparkDates)
        DbtBlocks = @(Get-ContiguousBlocks $dbtDates)
    }
}

function Write-DatePlan {
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][string[]]$Dates
    )

    $blocks = @(Get-ContiguousBlocks $Dates)
    Write-Host "${Label}: $($Dates.Count) date(s)"
    if ($Dates.Count -eq 0) {
        Write-Host "  (none)"
        return
    }
    Write-Host "  Exact dates: $($Dates -join ', ')"
    Write-Host "  Contiguous blocks:"
    foreach ($block in $blocks) {
        Write-Host "    $($block.StartDate)..$($block.EndDate) ($($block.Count))"
    }
}

function Write-RebuildPlan {
    param(
        [Parameter(Mandatory = $true)]$Coverage,
        [Parameter(Mandatory = $true)]$Plan,
        [Parameter(Mandatory = $true)]$Options
    )

    Write-Host "Gold rebuild discovery"
    Write-Host "  Silver dates: $($Coverage.SilverDates.Count)"
    Write-Host "  Silver contiguous blocks:"
    foreach ($block in @(Get-ContiguousBlocks $Coverage.SilverDates)) {
        Write-Host "    $($block.StartDate)..$($block.EndDate) ($($block.Count))"
    }
    Write-Host "  Existing table coverage:"
    foreach ($table in $script:SparkTables + $script:DbtTables) {
        Write-Host "    $table`: $($Coverage.TableDates[$table].Count) date(s)"
    }
    $mode = if ($Plan.ExplicitRange) {
        "explicit Silver-date rerun ($($Options.StartDate)..$($Options.EndDate))"
    } else {
        "missing/incomplete Gold dates only"
    }
    Write-Host "  Selection mode: $mode"
    Write-DatePlan "Spark plan" $Plan.SparkDates
    Write-DatePlan "dbt plan" $Plan.DbtDates
    Write-Host "Safety: only Gold DAG IDs are triggerable; discovery is SELECT-only."
    Write-Host "CoinCap/API, Bronze, and Silver ingestion DAGs are never invoked."
}

function Get-AirflowDagRows {
    $output = Invoke-DockerCommand @(
        "compose", "exec", "-T", "airflow-scheduler",
        "airflow", "dags", "list", "--output", "json"
    )
    return @(ConvertFrom-JsonItems $output)
}

function Get-PausedGoldDags {
    param([string[]]$DagIds)

    $rows = @(Get-AirflowDagRows)
    $pausedDagIds = @()
    foreach ($dagId in $DagIds) {
        $row = @($rows | Where-Object { $_.dag_id -eq $dagId })
        if ($row.Count -ne 1) { throw "Airflow DAG '$dagId' was not found exactly once." }
        if ($row[0].is_paused -eq $true -or $row[0].is_paused -eq "True") {
            $pausedDagIds += $dagId
        }
    }
    return $pausedDagIds
}

function Set-GoldDagPaused {
    param(
        [Parameter(Mandatory = $true)][string]$DagId,
        [Parameter(Mandatory = $true)][bool]$Paused
    )

    $operation = if ($Paused) { "pause" } else { "unpause" }
    [void](Invoke-DockerCommand @(
        "compose", "exec", "-T", "airflow-scheduler",
        "airflow", "dags", $operation, $DagId
    ))
}

function Start-GoldDagRun {
    param(
        [Parameter(Mandatory = $true)][string]$DagId,
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][hashtable]$Conf
    )

    $json = $Conf | ConvertTo-Json -Compress
    # Windows PowerShell 5 removes embedded double quotes when invoking native
    # executables. Backslash-escape them so Docker delivers valid JSON to Airflow.
    $nativeJson = $json.Replace('"', '\"')
    [void](Invoke-DockerCommand @(
        "compose", "exec", "-T", "airflow-scheduler",
        "airflow", "dags", "trigger", "--run-id", $RunId,
        "--conf", $nativeJson, "--output", "json", $DagId
    ))
}

function Wait-GoldDagRun {
    param(
        [Parameter(Mandatory = $true)][string]$DagId,
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][int]$PollSeconds,
        [Parameter(Mandatory = $true)][int]$TimeoutSeconds
    )

    $deadline = [datetime]::UtcNow.AddSeconds($TimeoutSeconds)
    $lastState = ""
    while ([datetime]::UtcNow -lt $deadline) {
        $output = Invoke-DockerCommand @(
            "compose", "exec", "-T", "airflow-scheduler",
            "airflow", "dags", "list-runs", "-d", $DagId, "--output", "json"
        )
        $runs = @(ConvertFrom-JsonItems $output)
        $run = @($runs | Where-Object { $_.run_id -eq $RunId })
        if ($run.Count -eq 1) {
            $state = [string]$run[0].state
            if ($state -ne $lastState) {
                Write-Host "  Airflow run $RunId state: $state"
                $lastState = $state
            }
            if ($state -in @("success", "failed")) { return $state }
        }
        Start-Sleep -Seconds $PollSeconds
    }
    return "timeout"
}

function Invoke-GoldRebuild {
    param(
        [Parameter(Mandatory = $true)]$Plan,
        [Parameter(Mandatory = $true)]$Options
    )

    $requiredDagIds = @()
    if ($Plan.SparkDates.Count -gt 0) { $requiredDagIds += $script:SparkDagId }
    if ($Plan.DbtDates.Count -gt 0) { $requiredDagIds += $script:DbtDagId }
    if ($requiredDagIds.Count -eq 0) {
        Write-Host "Nothing to rebuild; selected Gold tables already cover all eligible Silver dates."
        return [pscustomobject]@{ FailedRuns = @(); SuccessfulRuns = @() }
    }
    $pausedDagIds = @(Get-PausedGoldDags $requiredDagIds)
    foreach ($dagId in $pausedDagIds) {
        Write-Host "Temporarily unpausing Gold DAG: $dagId"
        Set-GoldDagPaused $dagId $false
    }

    $stamp = [datetime]::UtcNow.ToString("yyyyMMddTHHmmssfffZ")
    $failedRuns = @()
    $successfulRuns = @()

    try {
        foreach ($block in $Plan.SparkBlocks) {
            $runId = "gold-rebuild__${stamp}__spark__$($block.StartDate)__$($block.EndDate)"
            Write-Host "[spark][start] $($block.StartDate)..$($block.EndDate) ($($block.Count) date(s))"
            foreach ($date in $block.Dates) { Write-Host "[spark][queued] $date" }
            try {
                Start-GoldDagRun $script:SparkDagId $runId @{
                    start_date = $block.StartDate
                    end_date = $block.EndDate
                }
                $state = Wait-GoldDagRun $script:SparkDagId $runId $Options.PollSeconds $Options.RunTimeoutSeconds
            }
            catch {
                $state = "trigger-error"
                Write-Warning $_.Exception.Message
            }
            if ($state -eq "success") {
                $successfulRuns += $runId
                foreach ($date in $block.Dates) { Write-Host "[spark][success] $date" }
            } else {
                $failedRuns += "$runId ($state)"
                foreach ($date in $block.Dates) { Write-Host "[spark][needs-verification] $date" }
            }
        }

        foreach ($date in $Plan.DbtDates) {
            $runId = "gold-rebuild__${stamp}__dbt__$date"
            Write-Host "[dbt][start] $date"
            try {
                Start-GoldDagRun $script:DbtDagId $runId @{ target_date = $date }
                $state = Wait-GoldDagRun $script:DbtDagId $runId $Options.PollSeconds $Options.RunTimeoutSeconds
            }
            catch {
                $state = "trigger-error"
                Write-Warning $_.Exception.Message
            }
            if ($state -eq "success") {
                $successfulRuns += $runId
                Write-Host "[dbt][success] $date"
            } else {
                $failedRuns += "$runId ($state)"
                Write-Host "[dbt][failed] $date ($state)"
            }
        }
    }
    finally {
        foreach ($dagId in $pausedDagIds) {
            Write-Host "Restoring paused state for Gold DAG: $dagId"
            Set-GoldDagPaused $dagId $true
        }
    }

    return [pscustomobject]@{
        FailedRuns = @($failedRuns)
        SuccessfulRuns = @($successfulRuns)
    }
}

function Invoke-RebuildGoldMain {
    param([string[]]$Arguments)

    $options = Parse-RebuildGoldArguments $Arguments
    if ($options.Help) { Show-Usage; return 0 }

    $coverage = Get-GoldCoverage
    $plan = New-RebuildPlan $coverage $options.Engine $options.StartDate $options.EndDate $options.MaxDates
    Write-RebuildPlan $coverage $plan $options
    if ($options.DryRun) {
        Write-Host "Dry run only: no DAGs were triggered."
        return 0
    }

    $result = Invoke-GoldRebuild $plan $options
    $after = Get-GoldCoverage
    $missingAfter = @()
    foreach ($date in $plan.SparkDates) {
        if (-not (Test-EngineDateComplete $after "spark" $date)) { $missingAfter += "spark:$date" }
    }
    foreach ($date in $plan.DbtDates) {
        if (-not (Test-EngineDateComplete $after "dbt" $date)) { $missingAfter += "dbt:$date" }
    }

    Write-Host "Gold rebuild summary"
    Write-Host "  Successful Airflow runs: $($result.SuccessfulRuns.Count)"
    Write-Host "  Failed/timed-out Airflow runs: $($result.FailedRuns.Count)"
    foreach ($failure in $result.FailedRuns) { Write-Host "    $failure" }
    Write-Host "  Planned engine-dates still incomplete: $($missingAfter.Count)"
    foreach ($missing in $missingAfter) { Write-Host "    $missing" }

    if ($result.FailedRuns.Count -gt 0 -or $missingAfter.Count -gt 0) { return 1 }
    return 0
}

if ($MyInvocation.InvocationName -ne ".") {
    try {
        exit (Invoke-RebuildGoldMain $args)
    }
    catch {
        Write-Error $_.Exception.Message
        exit 1
    }
}
