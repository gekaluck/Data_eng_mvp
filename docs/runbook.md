# Runbook

## Purpose

This runbook documents the normal operating paths for the local CoinCap lakehouse and the first checks to run when something looks wrong.

## Main DAGs

Regular flow:

- `coincap_regular_orchestrator`
- `bronze_coincap_assets`
- `silver_coincap_assets`
- `gold_coincap_assets`
- `gold_dbt_coincap_assets`
- `gold_dbt_coincap_tests`

Backfill flow:

- `bronze_coincap_history_backfill`
- `silver_coincap_history_backfill`

## Normal Daily Run

Use `coincap_regular_orchestrator`.

For scheduled runs:

- Airflow drives the whole chain
- Bronze, Silver, Spark Gold, dbt Gold, and dbt tests run in the expected order

For a manual one-day rerun:

1. Trigger `coincap_regular_orchestrator`
2. Set `target_date` to `YYYY-MM-DD`
3. Monitor each downstream DAG run from the orchestrator graph and logs

## Manual One-Day Replay

Use this when the regular run already happened but one layer was wrong.

### Bronze only

Trigger `bronze_coincap_assets` with:

- `target_date=YYYY-MM-DD`

### Silver only

Trigger `silver_coincap_assets` with:

- `target_date=YYYY-MM-DD`

Important:

- Silver resolves the expected Bronze partition from `target_date`
- if you do not pass `target_date` on a manual run, the sensor may wait for the wrong partition

### Spark Gold only

Trigger `gold_coincap_assets` with:

- `target_date=YYYY-MM-DD`

### dbt Gold only

Trigger `gold_dbt_coincap_assets` with:

- `target_date=YYYY-MM-DD`

Then, if needed, trigger:

- `gold_dbt_coincap_tests`

with the same `target_date`.

## Backfill Procedure

Backfills are still manual and separate from the regular orchestrator.

Recommended sequence:

1. Trigger `bronze_coincap_history_backfill`
2. Confirm the Bronze window landed in MinIO
3. Trigger or rerun `silver_coincap_history_backfill` with the same resolved window
4. Verify Silver tables contain the expected dates
5. Rebuild affected Gold dates

## Key Validation Queries

### Silver date coverage

```sql
select snapshot_date, count(*)
from silver.crypto.price_snapshots
where snapshot_date between date '2026-03-20' and date '2026-04-02'
group by 1
order by 1;
```

### Gold row presence for one date

```sql
select count(*)
from gold.crypto.daily_snapshot
where snapshot_date = date '2026-04-02';
```

```sql
select count(*)
from gold.crypto_dbt.daily_snapshot
where snapshot_date = date '2026-04-02';
```

### Compare Spark Gold vs dbt Gold

```sql
with spark_old as (
    select *
    from gold.crypto.daily_snapshot
    where snapshot_date = date '2026-04-02'
),
dbt_new as (
    select *
    from gold.crypto_dbt.daily_snapshot
    where snapshot_date = date '2026-04-02'
)
select * from spark_old
except
select * from dbt_new

union all

select * from dbt_new
except
select * from spark_old;
```

## First Checks By Symptom

### Bronze succeeded but Silver is waiting

Check:

- manual Silver run used the correct `target_date`
- the Bronze S3 partition exists for that date
- `wait_for_bronze` logs show the expected key

### Silver succeeded but Gold is empty

Check:

- `silver.crypto.price_snapshots` has rows for the requested `snapshot_date`
- `daily_snapshot` also requires the prior day
- rolling metrics require enough history in Silver
- 14d/30d comparisons require exact historical dates in Silver

### dbt Gold failed but Spark Gold passed

Check:

- dbt compiled SQL in `dbt/target/compiled`
- Trino-compatible SQL and Iceberg configs
- file encoding issues such as UTF-8 BOMs

### History backfill succeeded but expected dates are missing

Check:

- resolved `anchor_snapshot_date`
- resolved `window_start_date` / `window_end_date`
- whether Bronze history files exist for that exact window
- Silver history backfill logs for cleaned row counts and post-merge counts

## Useful Operational Commands

### Infrastructure

```powershell
make build
make up
make down
make ps
```

### Logs

```powershell
make logs-scheduler
make logs-trino
make logs-lab
```

### Tests

```powershell
make test
make test-dag
```

### dbt

```powershell
dbt debug --project-dir dbt --profiles-dir dbt
dbt docs generate --project-dir dbt --profiles-dir dbt
dbt docs serve --project-dir dbt --profiles-dir dbt --port 8082
```

## Recovery Principles

- Always verify the date being processed.
- Replay the smallest layer that can safely fix the issue.
- Prefer rerunning one date over rebuilding everything.
- Validate Silver before blaming Gold.
- Validate data presence before comparing business logic.