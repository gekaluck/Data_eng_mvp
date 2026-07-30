# Hardening Plan — Prompt for a Fresh Session

Paste everything below the line into a new chat in this repo. It is written to stand alone;
it does not assume the other session's context.

---

## Task

Harden the CoinCap lakehouse pipeline against five failure modes we hit in production.
Each one is documented in `docs/incidents.md` (incidents I1–I16) — read that file first,
it is the source of truth for *why* each item exists.

Follow `CLAUDE.md`: ask clarifying questions before non-trivial work, keep changes simple
and readable, work on a new branch (never `main`), and update `docs/architecture.md`,
`docs/decisions.md`, and `docs/incidents.md` before opening the PR.

Suggested branch: `feat/pipeline-hardening`.

Confirm before starting: is PR #15 (`feat/capture-sync`) merged? If not, branch from it
rather than `main` — H1 and H4 touch files it changed.

## Context you need

- Orchestration: `dags/coincap_regular_orchestrator.py` runs `sync_captured_snapshots`,
  then triggers Silver, Spark Gold, dbt Gold, and dbt Gold tests via
  `TriggerDagRunOperator(wait_for_completion=True)`, passing a `start_date`/`end_date`
  range pulled from the sync task's XCom.
- Two Gold implementations exist side by side for comparison: Spark
  (`gold.crypto.daily_snapshot`) and dbt (`gold.crypto_dbt.daily_snapshot`).
- dbt singular tests live in `dbt/tests/*.sql` and fail when they return rows. See
  `dbt/tests/daily_snapshot_no_duplicate_fetch_dates.sql` for the house style.
- Tests run inside Docker: `docker compose exec airflow-scheduler python -m pytest
  /opt/airflow/tests/ -v` (or `make test`). On Windows, invoke via PowerShell — Git Bash
  mangles container paths.
- One test fails on `main` already and is unrelated:
  `test_silver_history_backfill.py::test_transform_history_backfill_merges_into_all_target_tables`
  (column arity mismatch). Leave it alone; don't let it block you.

## The five items

### H1 — Fail fast instead of hanging (highest value)
**Fixes I9**, which cost 8 days of silent Gold outage.

`gold_coincap_assets` and `gold_dbt_coincap_assets` were paused. Triggering a paused DAG
creates a run stuck in `queued`; `TriggerDagRunOperator(wait_for_completion=True)` polls it
forever with no timeout and no alert. 30 orphaned queued runs accumulated while Bronze and
Silver kept producing fresh data, so nothing looked wrong.

Make this loud:
- Set a sensible `execution_timeout` on each of the four trigger tasks so an indefinite wait
  becomes a failed task.
- Before triggering, check the target DAG is not paused and fail with a clear message naming
  the DAG if it is. (`DagModel.get_dagmodel(dag_id).is_paused`, or equivalent.)
- Consider `reset_dag_run=True` so a re-run doesn't collide with an existing run id.

Add a DAG-integrity test asserting every trigger task has an `execution_timeout`.

### H2 — Assert layer coverage
**Fixes I9, I12, I13.** No test ever asserted that the layers agree, so an 8-day Gold gap,
a missing Silver date, and a stale null all went unnoticed.

Add dbt tests (or SQL assertions in the existing style):
- Every `silver.crypto.price_snapshots` date has rows in **both** Gold implementations.
- Both Gold implementations have the same date coverage and row counts per date.
- No Gold row has a null `price_change_pct` when the previous day *does* exist in Silver
  (this is the stale-null signature from I12 — it means Gold was built before Silver caught
  up and never recomputed).

Note: `dbt/tests/daily_snapshot_gap_dates_retained.sql` is currently **untracked** in the
working tree and overlaps this area. Read it and either finish it or fold it in — don't
duplicate it.

### H3 — Capture freshness
**Fixes the blind spot behind I1.** If the CoinCap key expires or GitHub disables the cron
(it does so after 60 days of repo inactivity), files stop arriving, the sync finds nothing,
and the run *skips* — which is also the correct behavior when there is genuinely nothing new.
A dead upstream is indistinguishable from a quiet one.

In `dags/utils/capture_sync.py`, after listing the capture bucket, fail (or raise a clearly
logged warning) when the newest captured date is older than a configurable threshold —
default around 2 days, env-overridable. Distinguish "bucket is empty" from "bucket is stale"
in the message. Keep the existing skip behavior for the genuinely-nothing-new case.

Unit-test the threshold logic; it should be a pure function like the existing `plan_sync`.

### H4 — Reconcile dbt Gold with Spark Gold
**Fixes I16.** D025 changed the Spark model to keep rows when the prior day is missing and
null the change. The dbt model still filters them out:

```sql
where prev_price_usd is not null
    and prev_snapshot_date = date_add('day', -1, snapshot_date)
```

So the two diverge on the first day after every gap — 07-22 currently has 20 rows in Spark
Gold and 19 in dbt. That invalidates the comparison at exactly the dates worth comparing.

Bring dbt in line with D025: keep the row, null `price_change_pct` and `price_change_rank`,
and exclude the coin from the change ranking rather than giving it an arbitrary rank. Then
verify the two implementations match with the comparison query in `docs/runbook.md`.

### H5 — Fetch timestamp in Bronze (do this one last, or separately)
**Fixes the diagnosability gap behind I6 and I10.** Bronze stores no record of when a
snapshot was actually fetched, so a mislabeled object is undetectable after the fact — I10
was inferred from a price coincidence, not detected.

`CoinCapAssetsResponse.timestamp` is already validated and then discarded; only
`validated.data` is written. Persist it (plus arguably the wall-clock fetch time) as a column
in both writers: `dags/bronze_coincap.py` and `scripts/capture_daily_snapshot.py`. They must
stay byte-compatible with each other — that compatibility is the premise of the whole sync
design (D026/D027).

This changes the Bronze schema, so it touches Silver's reader too. If it turns out to ripple
further than expected, stop and put it on its own branch rather than growing this PR.

## Constraints and cautions

- **Do not rebuild Silver from Bronze for 2026-07-22 → 2026-07-28.** Bronze holds duplicated
  wrong-day prices for that window and Silver holds the good values. Reprocessing would make
  the data worse. See D028 and incident I10.
- Don't spend CoinCap credits. The free tier is 500/month and history backfill costs roughly
  100 credits per day of data (D024). None of these five items should need an API call.
- The stack must stay runnable locally. Don't add services or dependencies without asking.
- Beware bind-mounted config and branch switches: `docker compose restart <svc>` reuses the
  stale container definition, `docker compose up -d --force-recreate <svc>` rebuilds it from
  the current checkout (incident I8).

## Acceptance criteria

1. A paused downstream DAG causes a **failed** orchestrator task within minutes, naming the
   paused DAG — not an indefinite hang.
2. A missing or stale date in any layer fails a test rather than showing up as a gap in a
   dashboard days later.
3. A capture bucket that has stopped receiving files produces a failure, not a skip.
4. Both Gold implementations return identical date coverage and row counts, including on the
   first day after a gap.
5. Full suite green apart from the known pre-existing `test_silver_history_backfill` failure.
6. `docs/incidents.md` updated: H1–H5 moved out of "open hardening items" as they land, and
   I9/I12/I13/I16 statuses revised.
