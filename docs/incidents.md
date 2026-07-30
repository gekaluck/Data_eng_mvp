# Pipeline Incidents

## How to Read This

A running log of things that actually went wrong in this pipeline, what caused them, and
what we did about it. Companion to the other docs:

- `decisions.md` — *why* the system is shaped the way it is
- `runbook.md` — *how* to operate it and debug by symptom
- `incidents.md` (this file) — *what broke*, and what the failure taught us

Most entries here produced a decision, a test, or a design change. The point is the
evolution story: nearly every structural choice in this project is a response to a
specific failure recorded below.

Status key: `fixed` | `mitigated` | `open` | `accepted`

---

## Timeline at a glance

| # | Date | Issue | Status |
|---|------|-------|--------|
| I1 | 2026-04 → 07 | Coverage depended on the laptop being on — a ~3-month Silver hole | fixed (I1 → D026/D027) |
| I2 | 2026-07-11 | One 5-day history backfill consumed ~498 of 500 monthly credits | accepted (D024) |
| I3 | 2026-07-11 | Gold produced zero rows whenever the prior day was missing | fixed (D025) |
| I4 | 2026-07-11 | Bronze retried quota-exhaustion 429s, burning credits faster | fixed (D025) |
| I5 | 2026-07-28 | Capture workflow `AccessDenied` on `PutObject` | fixed |
| I6 | 2026-07-28 | `--date` mistaken for time travel; today's prices filed under an old date | fixed (guard documented) |
| I7 | 2026-07-29 | Sync `AccessDenied` — writer key in `.env`, reader key never created | fixed |
| I8 | 2026-07-29 | Trino broke when branches were switched under a running stack | fixed |
| I9 | 2026-07-22 → 29 | **Gold DAGs paused → orchestrator hung silently for 8 days** | fixed, hardening open |
| I10 | 2026-07-22 → 28 | **Bronze objects overwritten with wrong-day prices** | mitigated (D028) |
| I11 | 2026-07-23 | Duplicated fetch produced a fake 0.00% market day | fixed (test added) |
| I12 | 2026-07-19 | Gold built out of order left a permanent null | fixed by rebuild, cause open |
| I13 | 2026-07-10 | Silver missing a date Bronze had | fixed by rebuild |
| I14 | 2026-07-29 | Orchestrator ran 30 min *before* the capture, adding 24h lag | fixed |
| I15 | 2026-07-29 | dbt Gold was single-date while Spark Gold took ranges | fixed |
| I16 | 2026-07-29 | dbt and Spark Gold disagree at gap boundaries | open |

---

## I1 — Capture depended on the laptop being on
**Date**: 2026-04-08 → 2026-07-08 · **Status**: fixed

**Symptom**: A ~92-day hole in Silver (plus a 9-day hole 03-22 → 03-30). Coverage only
accrued on days the machine happened to be running.

**Root cause**: The only thing fetching CoinCap was a local Airflow DAG.

**Fix**: Moved the daily call to GitHub Actions writing to S3 (D026), then made the cloud
capture the *only* scheduled call with local Airflow syncing from the bucket (D027).

**Lesson**: The one component that must be reliable was the one most coupled to the least
reliable thing in the system. Capture and processing have opposite requirements — only the
cheap, must-not-miss half needed to leave the laptop.

---

## I2 — Credit exhaustion from deep backfill
**Date**: 2026-07-11 · **Status**: accepted

**Symptom**: A single 5-day, ~20-coin history backfill consumed ~498 of the 500 free
monthly credits.

**Root cause**: CoinCap's free tier bills by data volume, not call count, and the history
endpoints are per-coin.

**Fix**: None — accepted as a constraint. Strategy changed to build forward from the daily
snapshot (D024); deep backfill is now a rare manual operation.

**Lesson**: This constraint is why I1 mattered so much. If you can't buy history back, you
cannot afford to miss days.

---

## I3 — Gold emptied by a missing prior day
**Date**: 2026-07-11 · **Status**: fixed

**Symptom**: An isolated snapshot date produced zero Gold rows and tripped the count
validator, failing the whole run.

**Root cause**: `daily_snapshot` computed day-over-day change via `LAG` and then filtered
`WHERE prev_price_usd IS NOT NULL`, dropping every row when the prior day was absent.

**Fix**: Keep the row, leave `prev_price_usd` / `price_change_pct` null (D025).

**Note**: Only the Spark implementation was changed. See I16 — the dbt model still filters.

---

## I4 — Retrying an exhausted quota
**Date**: 2026-07-11 · **Status**: fixed

**Symptom**: Bronze retried HTTP 429 five times with 60s backoffs even when the monthly
quota was gone, and rejected requests still counted against it.

**Root cause**: CoinCap returns 429 for both the transient per-minute limit and a hard
quota. The retry loop couldn't tell them apart.

**Fix**: Only retry when the body indicates the per-minute limit; fail fast otherwise (D025).

---

## I5 — Capture workflow `AccessDenied` on `PutObject`
**Date**: 2026-07-28 · **Status**: fixed

**Symptom**: `s3:PutObject ... because no identity-based policy allows` — despite a policy
that appeared to grant exactly that.

**Root cause**: The IAM policy's resource ARN didn't match the real bucket. Compounded by a
red herring: IAM access key IDs in one account share a long prefix (`AKIASKVU3JRK`), so
comparing prefixes cannot distinguish two keys. Only the last 8 characters differ.

**Fix**: Correct ARN; verified identity with `sts:GetCallerIdentity` rather than by
inspecting key strings.

**Lesson**: When debugging AWS auth, ask AWS *who you are* — don't infer it from
credentials. And note the ARN shape asymmetry: `s3:PutObject`/`GetObject` need
`arn:aws:s3:::bucket/prefix/*`, while `s3:ListBucket` needs the bare `arn:aws:s3:::bucket`.

---

## I6 — `--date` mistaken for time travel
**Date**: 2026-07-28 · **Status**: fixed

**Symptom**: A manual workflow run with an explicit date produced an S3 object whose key
said one date and whose contents were from another.

**Root cause**: `/assets` is a **live** endpoint with no date parameter. The capture's
`--date` only selects the object key. Passing an old date files *current* prices under it.

**Fix**: Deleted the object; documented the trap prominently in
`autonomous-daily-capture.md`. Real history requires `/assets/{id}/history` (see I2 for why
that's expensive).

**Lesson**: This is the same defect class as I10, found earlier and by luck. Nothing in the
system could have flagged it, because Bronze records no fetch time.

---

## I7 — Sync `AccessDenied`, twice
**Date**: 2026-07-29 · **Status**: fixed

**Symptom**: `bronze_capture_sync` failed on `ListObjectsV2`, reporting the identity as
`crypto-capture-writer` — a key that is write-only by design.

**Root cause**: `.env` still held the writer's credentials. The reader IAM user existed but
had never been issued an access key (creating a user and creating its key are separate
console steps).

**Fix**: Issued a key for the reader, updated `.env`, recreated the containers. Verified
via `sts:GetCallerIdentity` before re-running.

**Lesson**: Keeping the CI key write-only is what made this failure *safe* — a leaked
workflow secret can't read or delete history. The friction was the point.

---

## I8 — Trino broke on a branch switch
**Date**: 2026-07-29 · **Status**: fixed

**Symptom**: Every Trino query failed with `Invalid JSON file '/etc/trino/access-rules.json'`,
and the container couldn't even restart.

**Root cause**: Trino was started while a branch containing `access-control.properties` and
`access-rules.json` was checked out. Switching branches removed those files from the mounted
`./config` directory beneath the running container, which reads config only at startup.

**Fix**: `docker compose up -d --force-recreate trino`, which rebuilt the container from the
current branch's service definition rather than the stale one.

**Lesson**: Bind-mounted config plus branch switching means the running stack can silently
diverge from the checkout. `restart` reuses the old definition; `up --force-recreate`
doesn't.

---

## I9 — Paused Gold DAGs hung the orchestrator for 8 days
**Date**: 2026-07-22 → 2026-07-29 · **Status**: fixed; hardening still open

**Symptom**: Superset showed a hard gap after 07-21. Bronze and Silver were complete through
07-29; both Gold implementations stopped dead at 07-21. Orchestrator runs were marked
`failed` daily with no useful error.

**Root cause**: `gold_coincap_assets` and `gold_dbt_coincap_assets` were **paused** (most
likely during Superset dashboard work). Triggering a paused DAG creates a run in `queued`
that the scheduler never picks up, and `TriggerDagRunOperator(wait_for_completion=True)`
polls it forever with no timeout. 30 orphaned queued runs accumulated, several targeting
dates with no Silver data at all.

**Fix**: Cleared the stale queued runs, unpaused both DAGs, and rebuilt Gold over the
affected range in one pass using the range support from PR #15.

**Still open**: nothing prevents a recurrence. A paused downstream DAG remains an untimed,
unalerted, indefinite hang. This is hardening item **H1**.

**Lesson**: The worst failures aren't loud. Two layers stayed green and produced fresh data
for 8 days while the layer everyone actually looks at was frozen.

---

## I10 — Bronze objects overwritten with wrong-day prices
**Date**: 2026-07-22 → 2026-07-28 · **Status**: mitigated (D028)

**Symptom**: Bronze holds one identical Bitcoin price (63799.552250) on 07-22, 07-23, 07-24,
07-25 **and** 07-27, plus a second (63708.900000) on 07-26 and 07-28. Silver holds distinct,
plausible values for those same dates — Bronze and Silver disagree.

**Root cause**: Two properties of the local fetch DAG combined. It fetched **live** `/assets`
but named the object from the run's `logical_date`, and it uploaded with
`load_bytes(..., replace=True)`. So any late or repeated run for a past date overwrote that
date's object with current prices. When the machine came back after being off, several
catch-up runs fired within seconds — `scheduled__2026-07-22` and `scheduled__2026-07-23`
both started at `2026-07-24T02:08`, two seconds apart — each storing the same response under
a different date. Manual triggers and cleared runs do this identically; the result is
indistinguishable.

**Mitigation, not a fix**: The bad objects remain. Silver was built before the overwrites and
still holds the good values, so **Silver is more trustworthy than Bronze for this window**
and must not be rebuilt from it. Recorded as D028; the "immutable landing zone" claim was
removed from `architecture.md`.

**Why it can't recur**: The cloud capture resolves its date from the wall clock at fetch
time, so a delayed or retried run still writes *today's* object with today's data. The sync
only copies dates Bronze lacks, and `overwrite` is opt-in.

**Lesson**: "Raw landing zone" and "mutable in place" are incompatible. And because Bronze
stores no fetch timestamp, this was inferred from a price coincidence rather than detected —
see hardening item **H5**.

---

## I11 — A fabricated flat-market day
**Date**: 2026-07-23 · **Status**: fixed

**Symptom**: In the Superset "Daily Price Change" chart, every coin sat at exactly 0.00% on
one date — indistinguishable from a genuinely flat market.

**Root cause**: A consequence of I10. `price_change_pct = (price - prev_price) / prev_price`,
so two adjacent dates holding the same observation yield exactly zero for every coin at once.

**Fix**: Added `dbt/tests/daily_snapshot_no_duplicate_fetch_dates.sql`, which fails when
*every* coin on a date is unchanged from the previous day. Requiring all coins is what makes
it safe — stablecoins sit near zero daily, so a per-coin threshold would false-positive.
The test fails on 07-23 today.

**Lesson**: Twenty assets never all close exactly flat. Impossible-in-nature patterns make
excellent assertions.

---

## I12 — Out-of-order Gold left a permanent null
**Date**: 2026-07-19 · **Status**: fixed by rebuild; cause open

**Symptom**: 07-19 had a Gold row with a null `price_change_pct`, even though Silver held
both 07-18 and 07-19.

**Root cause**: Gold computes the day-over-day `LAG` at run time. 07-19's Gold ran before
07-18's Silver landed, so there was no prior day to compare against. Nothing recomputes it
when the missing day later arrives — the null is permanent until someone rebuilds.

**Fix**: Rebuilt Gold across 07-10 → 07-29.

**Still open**: Gold's correctness depends on build order and nothing detects a stale null.

---

## I13 — Silver missing a date Bronze had
**Date**: 2026-07-10 · **Status**: fixed

**Symptom**: A chart break that survived a full Gold rebuild.

**Root cause**: 07-10 was absent from **Silver** while present in Bronze — the Silver run for
that date never happened or failed. Gold cannot build what Silver lacks, and 07-11's change
was null in turn because its prior day was missing.

**Fix**: Built Silver for 07-10 from the existing Bronze object, then rebuilt Gold across the
range. No API calls needed — the data was already local.

**Lesson**: Layer coverage drifts independently. Nothing asserted that Silver covers Bronze,
or that Gold covers Silver — hardening item **H2**.

---

## I14 — Orchestrator ran before the capture
**Date**: 2026-07-29 · **Status**: fixed

**Symptom**: Every scheduled run synced the *previous* day's snapshot.

**Root cause**: The orchestrator was on `@daily` (00:00 UTC) while the capture cron runs at
00:30 UTC — it woke up 30 minutes too early, every day.

**Fix**: Moved the orchestrator to `30 1 * * *`, an hour after the capture, which also
absorbs GitHub's scheduled-run drift. The two crons are now coupled; noted in D027.

**Lesson**: 00:30 UTC is also the *safest* capture slot available. GitHub cron drift is
always late, so from just after midnight a delay moves further into the same UTC day and can
never change the partition label. A 23:45 UTC slot would be maximally fragile.

---

## I15 — dbt Gold couldn't process a range
**Date**: 2026-07-29 · **Status**: fixed

**Symptom**: Caught in review, not in production. A 7-day catch-up would have built 7 days of
Spark Gold and 1 day of dbt Gold.

**Root cause**: Range support (`start_date`/`end_date`) was added to Spark Gold in PR #13 but
not to Silver, dbt Gold, or dbt Gold tests.

**Fix**: All four now take ranges. `test_range_capable_dags_accept_start_and_end_date` fails
if any regresses.

**Lesson**: The two Gold implementations exist to be compared. A capability added to one and
not the other silently invalidates the comparison at exactly the interesting dates.

---

## I16 — dbt and Spark Gold disagree at gap boundaries
**Date**: 2026-07-29 · **Status**: open

**Symptom**: 07-22 has 20 rows in `gold.crypto.daily_snapshot` and 19 in
`gold.crypto_dbt.daily_snapshot`.

**Root cause**: D025 changed the *Spark* model to keep rows with a missing prior day and null
the change. The dbt model still has `where prev_price_usd is not null and prev_snapshot_date
= date_add('day', -1, snapshot_date)`, so it drops them. The implementations therefore
diverge on the first day after every gap.

**Fix**: Not yet applied — hardening item **H4**.

---

## Open hardening items

Tracked here so they don't get lost; see the hardening plan for implementation detail.

| ID | Item | Addresses |
|----|------|-----------|
| H1 | Fail fast on a hung/paused downstream trigger (`execution_timeout`, paused check) | I9 |
| H2 | Assert layer coverage: Silver covers Bronze, Gold covers Silver | I9, I12, I13 |
| H3 | Capture-freshness assertion in the sync, so a dead cloud capture is loud | I1 |
| H4 | Reconcile dbt Gold with Spark Gold at gap boundaries | I16, I3 |
| H5 | Record the fetch timestamp in Bronze so mislabeling is detectable | I6, I10 |
