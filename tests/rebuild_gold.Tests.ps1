$scriptPath = Join-Path (Split-Path $PSScriptRoot -Parent) "scripts\rebuild_gold.ps1"
. $scriptPath

function New-TestCoverage {
    param(
        [string[]]$SilverDates,
        [string[]]$SparkCompleteDates = @(),
        [string[]]$DbtCompleteDates = @()
    )

    $tableDates = @{}
    $tableDates["silver.crypto.price_snapshots"] = New-StringSet $SilverDates
    foreach ($table in $script:SparkTables) {
        $tableDates[$table] = New-StringSet $SparkCompleteDates
    }
    foreach ($table in $script:DbtTables) {
        $tableDates[$table] = New-StringSet $DbtCompleteDates
    }
    return [pscustomobject]@{
        SilverDates = @($SilverDates | Sort-Object)
        TableDates = $tableDates
    }
}

Describe "rebuild_gold argument parsing" {
    It "supports the documented GNU-style options" {
        $options = Parse-RebuildGoldArguments @(
            "--dry-run", "--engine", "dbt",
            "--start-date", "2026-07-04", "--end-date", "2026-07-11"
        )
        $options.DryRun | Should Be $true
        $options.Engine | Should Be "dbt"
        $options.StartDate | Should Be "2026-07-04"
        $options.EndDate | Should Be "2026-07-11"
    }

    It "requires both date bounds" {
        { Parse-RebuildGoldArguments @("--start-date", "2026-07-04") } |
            Should Throw "Provide both --start-date and --end-date, or neither."
    }
}

Describe "rebuild_gold contiguous blocks" {
    It "groups dates without inventing calendar gaps" {
        $blocks = @(Get-ContiguousBlocks @(
            "2026-07-04", "2026-07-05", "2026-07-07", "2026-07-11"
        ))
        $blocks.Count | Should Be 3
        $blocks[0].StartDate | Should Be "2026-07-04"
        $blocks[0].EndDate | Should Be "2026-07-05"
        $blocks[1].StartDate | Should Be "2026-07-07"
        $blocks[2].StartDate | Should Be "2026-07-11"
    }
}

Describe "rebuild_gold Airflow JSON parsing" {
    It "flattens a top-level JSON array into individual run rows" {
        $rows = @(ConvertFrom-JsonItems '[{"run_id":"one","state":"success"},{"run_id":"two","state":"failed"}]')
        $rows.Count | Should Be 2
        $rows[0].run_id | Should Be "one"
        $rows[1].state | Should Be "failed"
    }
}

Describe "rebuild_gold planning" {
    It "selects an engine date if any required Gold table is missing it" {
        $coverage = New-TestCoverage `
            -SilverDates @("2026-07-04", "2026-07-05", "2026-07-07") `
            -SparkCompleteDates @("2026-07-04") `
            -DbtCompleteDates @("2026-07-04", "2026-07-05")
        [void]$coverage.TableDates["gold.crypto_dbt.weekly_roll_avg"].Remove("2026-07-05")

        $plan = New-RebuildPlan $coverage "both" $null $null 10
        $plan.SparkDates | Should Be @("2026-07-05", "2026-07-07")
        $plan.DbtDates | Should Be @("2026-07-05", "2026-07-07")
    }

    It "reruns only actual Silver dates inside an explicit range" {
        $coverage = New-TestCoverage `
            -SilverDates @("2026-07-04", "2026-07-05", "2026-07-07") `
            -SparkCompleteDates @("2026-07-04", "2026-07-05", "2026-07-07")

        $plan = New-RebuildPlan $coverage "spark" "2026-07-05" "2026-07-07" 10
        $plan.SparkDates | Should Be @("2026-07-05", "2026-07-07")
        ($plan.SparkDates -contains "2026-07-06") | Should Be $false
        $plan.DbtDates.Count | Should Be 0
    }

    It "enforces the maximum-date safety cap" {
        $coverage = New-TestCoverage -SilverDates @(
            "2026-07-04", "2026-07-05", "2026-07-06"
        )
        { New-RebuildPlan $coverage "spark" $null $null 2 } | Should Throw
    }
}
