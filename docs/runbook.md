# Runbook

## Purpose

This runbook documents the normal operating paths for the local CoinCap lakehouse and the first checks to run when something looks wrong.

## Main DAGs

Regular flow:

- `coincap_regular_orchestrator`
- `bronze_capture_sync`
- `silver_coincap_assets`
- `gold_coincap_assets`
- `gold_dbt_coincap_assets`
- `gold_dbt_coincap_tests`

Manual only (not in the daily chain):

- `bronze_coincap_assets` — one-off local CoinCap fetch. The scheduled daily call lives
  in GitHub Actions now (D027); running this spends a CoinCap credit.

Backfill flow:

- `bronze_coincap_history_backfill`
- `silver_coincap_history_backfill`

## Normal Daily Run

Use `coincap_regular_orchestrator`.

For scheduled runs:

- The orchestrator syncs whatever the cloud capture has landed since last time, then
  runs Silver, Spark Gold, dbt Gold, and dbt tests over exactly those dates
- A run that finds nothing new **skips** rather than reprocessing identical partitions
- After the laptop has been off, the first run is the catch-up — no special procedure

For a manual one-day rerun of the whole chain:

1. Trigger `coincap_regular_orchestrator`
2. Set `start_date` and `end_date` to the same `YYYY-MM-DD`, and `overwrite=true`
   (without `overwrite` the sync finds nothing to do and the run skips)
3. Monitor each downstream DAG run from the orchestrator graph and logs

The orchestrator takes `start_date`/`end_date`, not `target_date` — it processes the
window the sync discovered rather than a single assumed day.

### Sync only

Trigger `bronze_capture_sync` to pull captured days into Bronze without running any
transforms. Optional `start_date`/`end_date` narrow the window; `overwrite=true`
re-copies dates already present locally.

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

- `target_date=YYYY-MM-DD` for a single date, **or**
- `start_date=YYYY-MM-DD` and `end_date=YYYY-MM-DD` to rebuild an inclusive range
  (each date is built in turn; per-date partition overwrite, so unrelated dates are
  untouched). Use this after a history backfill to rebuild the whole window in one run.
  Ranges over `MAX_GOLD_RANGE_DAYS` (366) are rejected as a safety guard.

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
5. Rebuild affected Gold dates in one run: trigger `gold_coincap_assets` with
   `start_date` / `end_date` covering the backfilled window (see "Spark Gold only")

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

### The orchestrator keeps skipping

The sync found nothing new, which means no fresh snapshot reached the bucket. Check, in
order:

- the GitHub Actions "Daily CoinCap capture" workflow — is it still enabled? GitHub
  disables scheduled workflows after 60 days of repo inactivity
- its recent runs for failures (an expired CoinCap key, or IAM changes)
- whether the object for the expected UTC date actually exists in the capture bucket

A skip is the correct response to "nothing new", so the fault is upstream in the cloud
capture, not in the local stack.

### The sync task fails with AccessDenied

The local `capture_s3` connection needs a **read** key with both `s3:GetObject` (on
`<bucket>/crypto/assets/*`) and `s3:ListBucket` (on `<bucket>`, no `/*`). The capture
workflow's key is write-only by design and will fail here. Confirm `.env` has the reader
credentials, not the writer's.

### Bronze succeeded but Silver is waiting

Check:

- manual Silver run used the correct `target_date`
- the Bronze S3 partition exists for that date
- `wait_for_bronze` logs show the expected key

### Silver succeeded but Gold is empty

Check:

- `silver.crypto.price_snapshots` has rows for the requested `snapshot_date`
  (with no rows for that date, `daily_snapshot` is genuinely empty and the validator fails)
- `daily_snapshot` tolerates a missing prior day — the row is kept with a null
  `price_change_pct` / `price_change_rank`, so a gap no longer empties the table
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
.\scripts\stack.ps1 up
.\scripts\stack.ps1 up -Rebuild
.\scripts\stack.ps1 down
.\scripts\stack.ps1 down -Volumes
.\scripts\stack.ps1 status
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
