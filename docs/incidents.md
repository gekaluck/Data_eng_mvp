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
| I16 | 2026-07-29 | dbt and Spark Gold disagree at gap boundaries | fixed (stored data rebuilt) |
| I17 | 2026-07-19 | A second duplicated-fetch pair, outside the I10 window | fixed (repaired from history) |
| I18 | 2026-07-30 | One coin without history aborts the whole backfill, mid-spend | fixed |
| I19 | 2026-04 → 07-30 | Two Gold serving models held 9 dates while `daily_snapshot` held 107 | fixed (backfilled, test added) |

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

**Hardened (H1)**: the orchestrator now checks every downstream DAG's paused flag before
triggering anything and fails naming the offender, and all four trigger tasks carry an
`execution_timeout` so no wait can be indefinite. `test_every_trigger_task_has_an_execution_timeout`
fails if a future trigger task is added without one. Verified against the real metadata DB
by pausing `gold_coincap_assets` and watching the guard fire.

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

**Update (2026-07-30)**: Running the same signature across all history turned up a second
instance on 07-19 (I17), outside the window D028 had recorded. Both 07-19 and 07-23 have
now been repaired from `/assets/{id}/history`, and this test passes across all 107 dates.

---

## I12 — Out-of-order Gold left a permanent null
**Date**: 2026-07-19 · **Status**: fixed by rebuild; cause open

**Symptom**: 07-19 had a Gold row with a null `price_change_pct`, even though Silver held
both 07-18 and 07-19.

**Root cause**: Gold computes the day-over-day `LAG` at run time. 07-19's Gold ran before
07-18's Silver landed, so there was no prior day to compare against. Nothing recomputes it
when the missing day later arrives — the null is permanent until someone rebuilds.

**Fix**: Rebuilt Gold across 07-10 → 07-29.

**Hardened (H2)**: `daily_snapshot_no_stale_null_change` now fails when any Gold row has a
null `price_change_pct` while Silver *does* hold that coin's previous day — the exact
signature of a date built before its predecessor landed. Gold's correctness still depends
on build order; what changed is that getting it wrong is now loud the same day instead of
permanent and invisible.

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
or that Gold covers Silver.

**Hardened (H2)**: `gold_covers_every_silver_date` fails when any Silver date is missing
from either Gold implementation, so a date that never got built is caught by a test rather
than by someone noticing a break in a chart.

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

**Fix**: The *model* was already brought in line by `be800bc` (the Superset PR), which
replaced the `where` with `case when` guards — so by the time H4 was picked up, only the
**stored data** still diverged: those dates had been built by the older model and nothing
rebuilds a Gold date when the model changes. Rebuilt 04-07, 07-10 and 07-22 in both
engines; `gold_implementations_agree_per_date` now asserts they stay in step.

**Lesson**: A model fix and the data it produced are two different things. Changing a
transform silently leaves every previously-built partition on the old logic, and the
incident stays open in the data long after it is closed in the code.

---

## I17 — A second duplicated-fetch pair, outside the documented window
**Date**: 2026-07-19 · **Status**: mitigated

**Symptom**: The Superset "Daily Price Change" chart showed a second all-zero day besides
the known 07-23 — every coin at exactly 0.00% on **07-19**.

**Root cause**: The same defect as I10, but earlier and outside the 07-22 → 07-28 window
D028 documents. The Bronze objects for 07-18 and 07-19 are identical *and carry the same
S3 LastModified*, `2026-07-20T20:24:23Z` — one live `/assets` response written under two
date labels by catch-up runs. `change_percent_24hr` is identical too (1.2134…), which no
two real days share. Silver was built from those objects, so Gold produced an exact 0.00%
change for all 20 coins.

**Fix**: Repaired 07-19 from `/assets/{id}/history` — a single-date backfill
(`anchor_snapshot_date=2026-07-20, backfill_days=1`), which merges over the duplicated row
in place — then rebuilt Gold for 07-19 **and 07-20**, whose change had been computed
against the bad value. BTC on 07-19 went from 65280.55 (07-18's price, a 0.00% change for
all 20 coins) to 64812.7375, a real −0.72% day.

Two properties of a repaired day, both expected:
- **Lower fidelity.** `change_percent_24hr` and `vwap_24hr` are null, because the history
  endpoints do not return them (D024).
- **A wider coin universe.** The backfill covers every coin in Silver's `coins` table, so
  07-19 has 25 rows where its neighbours have 20. The five extra coins have no 07-18 row,
  so their change is correctly null.

**07-23 too**: the duplicate pair from I10 proper was repaired the same way
(`anchor_snapshot_date=2026-07-24, backfill_days=1`), then Gold rebuilt for 07-23 and
07-24. BTC went from 65060.9 (07-22's price) to 66103.17, a real +1.60% day. No date in
the 107-date history now shows every coin unchanged from the previous day.

**Note**: Both members of a duplicated pair are suspect, not just the second. The fetch
happened on 07-20, so 07-18's row is *also* really a 07-20 observation. Only the duplicate
is detectable, so only the duplicate was repaired.

**Lesson**: D028 scoped the damage to the window we happened to look at. The detectable
signature — two adjacent dates sharing a price to the last decimal — was never run across
the whole history, so a second instance sat four days outside the boundary. When you find
a data defect, search for its signature everywhere before writing down its extent.

---

## I18 — One coin without history aborts the whole backfill, mid-spend
**Date**: 2026-07-30 · **Status**: fixed

**Symptom**: The single-date backfill for 07-19 ran for six minutes, then died on
`404 ... Asset history not found` for `gamecredits`. Nothing was written, and the credits
spent on the ~15 coins fetched before it were gone. Airflow then scheduled a retry, which
would have re-fetched all of them on the way to the same 404.

**Root cause**: Two compounding defects. The coin universe comes from Silver's `coins`
table — every coin ever seen — but CoinCap returns 404 forever for delisted or renamed
assets, and the fetch loop treated any HTTP error as fatal. Worse, the task carried
`retries: 2`, so a task that spends credits was set up to spend them again from the start.

**Fix**: A 404 on a per-coin history endpoint now raises `CoinCapHistoryNotFound`, which
the loop logs and skips, reporting the skipped coins at the end; other statuses stay fatal.
Retries on the fetch task are now `0` — the per-minute 429 backoff inside `_fetch_json`
already covers the one failure that clears on its own (I4), and anything else is a human's
call, not something to pay for twice.

**Lesson**: Retries and metered resources are a bad pair. The retry count was inherited
from a DAG default written for free operations; nobody re-derived it for the one task in
the repo where each attempt costs money.

---

## I19 — Two Gold serving models were never backfilled, and one of them could not be
**Date**: 2026-07-31 · **Status**: fixed

**Symptom**: The Superset availability dashboard showed 98 days `partial` and only 9
`available` — 46% of the calendar `missing`, the rest almost entirely `partial`. It looked
like a reporting artifact left over from the flat-day repairs.

**Root cause**: It was not an artifact. `daily_snapshot` covered all 107 Silver dates, but
`mc_rank_change` and `weekly_roll_avg` held **9** — only the dates a daily run had touched.
Both are incremental models that had never been rebuilt across history, and
`data_availability_daily` compares row counts across all three, so it correctly reported
every one of the other 98 days as short.

`mc_rank_change` could not simply be backfilled, either. Its 14- and 30-day lookback CTEs
were filtered by date *only* when the `snapshot_date` variable was set; without it they
selected every date and joined on `coin_id` alone, so a full-refresh would have crossed
every date with every other date. The model was structurally single-date, and the only way
that stayed invisible is that it was only ever run one date at a time.

**Fix**: The lookback joins now match on `coin_id` **and** the offset date, which is correct
for both a single-date run and a full history rebuild. Both models were then rebuilt with
`--full-refresh`: each went from 9 dates to 107 (2,175 rows, matching `daily_snapshot`), and
all 98 `partial` days flipped to `available`.

A new dbt test, `gold_serving_models_agree_per_date`, fails when the three dbt serving
models disagree on per-date row counts.

**Lesson**: This is I9's lesson one layer over. D029 asserted that `daily_snapshot` covers
Silver and that the two Gold *implementations* agree — but nothing asserted anything about
the other two serving models, so they drifted for months while every test stayed green. The
availability table did report it, in a colour, on a dashboard. A report nobody has to
acknowledge is not an assertion; the same fact only became actionable once a test failed on
it. Coverage assertions have to name every model that serves, not just the flagship one.

**Related**: The same investigation showed that 77 of the 107 now-`available` days carry no
`volume_usd_24hr` or `vwap_24hr` at all (28% field coverage overall) — a known consequence
of backfilled days lacking those fields, but one the dashboard never surfaced. Now reported
as `volume_coverage_pct` / `vwap_coverage_pct` rather than folded into the status (D031).

---

## Open hardening items

Tracked here so they don't get lost; see the hardening plan for implementation detail.

| ID | Item | Addresses | Status |
|----|------|-----------|--------|
| H5 | Record the fetch timestamp in Bronze so mislabeling is detectable | I6, I10, I17 | open — own branch |

### Landed

| ID | Item | Addresses | How it fails now |
|----|------|-----------|------------------|
| H1 | Paused-DAG check + `execution_timeout` on every trigger task | I9 | The orchestrator task fails within seconds, naming the paused DAG |
| H2 | Coverage and cross-implementation dbt tests | I9, I12, I13 | A missing date or a stale null fails the dbt test DAG the same day |
| H6 | Per-date row-count agreement across the three dbt serving models | I19 | A serving model that stops being built fails the dbt test DAG the same day |
| H3 | Capture-freshness assertion in the sync | I1 | A bucket that stopped receiving files fails instead of skipping |
| H4 | dbt Gold reconciled with Spark Gold (data rebuilt) | I16, I3 | `gold_implementations_agree_per_date` fails on any divergence |

H5 stays open deliberately: it changes the Bronze schema and touches Silver's reader, so
it belongs on its own branch rather than growing this one. It is the item that would have
made I10 and I17 *detectable* rather than inferred from a price coincidence.
