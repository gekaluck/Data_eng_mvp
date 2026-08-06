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

### Every layer is exactly one day behind, and nothing failed

The orchestrator ran *before* that day's capture landed, so it found nothing and skipped;
the next day's run picks the snapshot up. Compare the capture workflow's completion time
with the orchestrator's schedule (05:30 UTC, D034):

```bash
gh run list --workflow=daily-capture.yml --limit 5
```

GitHub's scheduled runs are late, never early, and the drift grows over time — it reached
~3.5h in July 2026 and swallowed the one-hour buffer that used to be enough (I20). If
completion times are creeping toward 05:30 UTC, move the orchestrator later and update D034;
`test_orchestrator_runs_well_after_the_capture_cron` enforces a four-hour minimum gap. A
single catch-up run needs no arguments — the sync copies whatever Bronze lacks and hands the
range downstream.

### The orchestrator fails on `check_downstream_dags_ready`

A DAG it is about to trigger is paused or missing. The message names it. Unpause with:

```bash
docker compose exec airflow-scheduler airflow dags unpause gold_coincap_assets
```

This check exists because triggering a paused DAG does *not* fail — it queues a run the
scheduler never starts, and the orchestrator waits on it forever (I9). If the DAG is
reported as unknown rather than paused, look for a DAG import error instead.

### The sync fails with "the capture bucket is stale"

Files have stopped arriving in the capture bucket. The message gives the newest date it
found and how old it is. Check, in order:

- the GitHub Actions "Daily CoinCap capture" workflow — still enabled? GitHub disables
  scheduled workflows after 60 days of repo inactivity
- its recent runs, for an expired CoinCap key or an IAM change
- the bucket itself, for the expected UTC date

To re-sync an old date deliberately without tripping this, set `CAPTURE_MAX_AGE_DAYS=0`.

A stale bucket is a *failure* rather than a skip on purpose: a dead upstream and a quiet
one are otherwise indistinguishable from inside the sync (I1).

### A Gold coverage test fails

- `gold_covers_every_silver_date` — a Silver date never got built into one or both Golds.
  Rebuild that date (see "Spark Gold only" / "dbt Gold only" above).
- `gold_implementations_agree_per_date` — the two implementations disagree. Usually one of
  them was built by an older version of the model: changing a transform does not rebuild
  the partitions it already produced. Rebuild the listed dates in both engines.
- `daily_snapshot_no_stale_null_change` — a date was built before its predecessor landed in
  Silver, so its change is null and nothing recomputes it (I12). Rebuild the listed dates.
- `daily_snapshot_no_duplicate_fetch_dates` — two adjacent dates hold the same observation.
  This is a *data* defect, not a build one; see I10/I17 and the repair path below.

### A date shows 0.00% change for every coin

One API response was written under two date labels, so the day-over-day change is exactly
zero for all coins at once (I10, I11, I17). Twenty assets never all close exactly flat.

Confirm it, then repair the duplicate date from history:

```sql
select snapshot_date, count(*) filter (where price_usd = prev_price_usd) as unchanged, count(*) as coins
from gold.crypto.daily_snapshot group by 1 having count(*) = count(*) filter (where price_usd = prev_price_usd);
```

Trigger `bronze_coincap_history_backfill` with `anchor_snapshot_date` set to the day
*after* the bad date and `backfill_days=1`; the window is the N days before the anchor, so
this fetches exactly the one date. It merges over the duplicated row in place. Note the
repaired day is lower fidelity: the history endpoints return no `change_percent_24hr` or
`vwap_24hr` (D024). Then rebuild Gold for the repaired date **and the day after it**, whose
change was computed against the bad value.

For dates written after 2026-07-31, check the cause directly rather than inferring it — the
Bronze provenance audit names the mislabelled date and how late its fetch was:

```bash
docker compose exec airflow-scheduler python /opt/airflow/scripts/audit_bronze_provenance.py
```

It reports how many dates it could check at all: objects older than that date carry no
provenance columns and are counted as unauditable, not as clean (D033). Exit code is 1 when
anything is flagged. `--max-lag-hours` loosens or tightens how late a fetch may be for its
partition date (default 36h: the cloud capture runs ~0.5h in, a scheduled local run ~24h).

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
- whether `run_dbt_gold` or the downstream `publish_dbt_artifacts` task failed
- after a successful publish, both `dbt/artifacts/manifest.json` and
  `dbt/artifacts/catalog.json` exist; rerun the leaf DAG rather than copying files from
  `dbt/target/`

### AI client gets `Access Denied` from Trino

Check:

- the Trino client user is exactly `agent`
- the table is one of the fully qualified names in
  `config/ai-agent/allowed-tables.json`
- the query targets `gold.crypto_dbt`; Silver, Spark Gold, writes, and newly added dbt
  tables are denied by design
- Trino startup logs contain `Loaded resource group configuration manager file` and
  `Loaded system access control file`; after a config change, use
  `docker compose up -d --force-recreate trino`

### AI metadata adapter is stale or incomplete

Run the isolated adapter tests first, then the opt-in live smoke check. The smoke uses only
fixed-shape `DESCRIBE` and Iceberg metadata-table reads as Trino user `agent`; it does not
query CoinCap or scan Gold business rows.

```powershell
$repoRoot = (Get-Location).Path
docker run --rm --mount "type=bind,source=$repoRoot,target=/workspace" `
  -w /workspace -e AI_TRINO_HOST=host.docker.internal python:3.12-slim `
  sh -c "pip install -q -r ai_agent/requirements.txt && python -m ai_agent.smoke_metadata"
```

Expected: all five allow-listed tables appear, each has live column/stat/snapshot metadata,
and `schema_warnings` is empty. If it fails or warns:

- an allow-listed model missing from the published manifest is a hard failure; compare
  `config/ai-agent/allowed-tables.json` with dbt's physical `alias`, not its SQL filename
- regenerate `dbt/artifacts/` with the `publish_dbt_artifacts` task after changing dbt docs;
  do not substitute scratch files from `dbt/target/`
- a `Live columns missing dbt descriptions` warning means `schema.yml` is behind Iceberg;
  an `absent from the live table` warning means the artifact docs are ahead of Iceberg
- `nullable: null` is expected when Trino does not expose an Iceberg constraint; do not
  reinterpret it as nullable or non-nullable
- a retryable `ENGINE_ERROR` points to Trino health/access; a non-retryable one points to an
  incompatible metadata-table shape and should be checked against the pinned Trino version

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
