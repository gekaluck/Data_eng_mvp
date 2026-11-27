# Autonomous Daily Capture

## Goal

Capture the daily CoinCap `/assets` snapshot **without the local stack running**, so
date coverage stops depending on the laptop being on. The heavy transforms
(Silver/Gold/dbt) stay local and on-demand; only the single daily API call moves to
the cloud.

This directly addresses the coverage gaps seen in Silver (a ~3-month hole where the
local stack simply wasn't running). See D024 (build-forward strategy).

## Design

Decouple **capture** (must be reliable → cloud, cheap) from **processing**
(heavy → local, batch).

```
GitHub Actions (daily cron)          Local stack (on-demand)
  fetch CoinCap /assets                sync captured snapshots -> MinIO bronze
  validate (Pydantic)          --->    Silver / Gold / dbt (existing DAGs)
  write Parquet to S3 bucket           (per-date, unchanged)
```

- **Capture** = `scripts/capture_daily_snapshot.py`, run by
  `.github/workflows/daily-capture.yml` on a daily schedule (00:30 UTC) and on manual
  dispatch. It reuses the **local pipeline's** Pydantic contract
  (`schemas.coincap.CoinCapAssetsResponse`), Parquet shape, and object-key layout
  (`bronze_assets_key`) so the output is byte-compatible with what Bronze writes.
- **Storage** = an **AWS S3** bucket in `us-east-1`. The stack is already S3-native
  (MinIO), so reading these snapshots locally is the same code pointed at a different
  endpoint. The `CAPTURE_S3_*` env vars stay provider-agnostic — any S3-compatible
  store (R2, MinIO) works by changing endpoint + region only.

### Why a bucket, not a git data branch

Data-in-git is an anti-pattern (repo bloat, no lifecycle, loses partition/scan
benefits). A bucket is the real-world landing-zone pattern and, because the stack
already speaks S3, barely more work.

## One-time setup (you)

1. **Create the S3 bucket** — region `us-east-1`, Block Public Access on, versioning
   off, default SSE-S3 encryption. Bucket names are globally unique across AWS.
2. **Create a scoped IAM user** (the script uses static keys, so a user, not a role):
   - No console access. Attach an inline policy allowing only `s3:PutObject` on
     `arn:aws:s3:::<bucket>/crypto/assets/*` — write-only, into one prefix.
   - Create an access key ("Application running outside AWS"); the secret shows once.
   - The Phase 2 local sync will need its own key with `s3:GetObject` +
     `s3:ListBucket`. Keep the reader separate from this writer.
3. **Add GitHub Actions secrets** (repo → Settings → Secrets and variables → Actions):
   - `COINCAP_API_KEY`
   - `CAPTURE_S3_ENDPOINT_URL` — `https://s3.us-east-1.amazonaws.com`
   - `CAPTURE_S3_ACCESS_KEY_ID`
   - `CAPTURE_S3_SECRET_ACCESS_KEY`
   - `CAPTURE_S3_BUCKET`

   The region is *not* a secret; it's set inline in the workflow. It must be a real
   AWS region — placeholders like R2's `auto` are rejected by AWS.
4. **Verify** — the `schedule:` trigger only fires from the **default branch**, so merge
   first, then trigger manually (Actions → "Daily CoinCap capture" → Run workflow).
   Confirm one `crypto/assets/year=/month=/day=/assets.parquet` object appears in the
   bucket. Costs 1 CoinCap call.

Secrets never live in the repo (D013). `.env.example` documents the same vars for the
local sync.

### Operational notes

- The capture runs at **00:30 UTC**. The orchestrator doesn't try to guess when it lands:
  it polls **hourly at `:15`** (D047). Each poll is a bucket listing and a quiet skip until
  the day's file appears, and the next poll after that processes it. Three fixed offsets
  in a row were outrun by drift (I14, I20, I30), so no schedule here assumes a delay any more.
  `test_orchestrator_runs_well_after_the_capture_cron` checks that a capture landing at
  any minute of its UTC day waits at most an hour for the next run.
- GitHub's scheduled runs are best-effort and **late, never early**, and the lateness has
  grown steadily. Early drift on this repo was 5–30 min, about 3.5h in July 2026 (I20), a
  4.6h median in September, and a 7.5h median in the last week of November, with a worst
  case of 9.8h (the 11-24 run started at 10:15 UTC). GitHub can also skip a day
  outright (2026-10-11, I29). Drift doesn't corrupt anything, because the partition date is
  resolved inside the script from the UTC wall clock at fetch time. It would only cross a
  day boundary past ~23.5h.
- GitHub **disables scheduled workflows after 60 days of repo inactivity** (it emails
  first). A quiet stretch on the repo silently stops capture — the thing this design
  exists to prevent.
- Cost at this volume is negligible: one small Parquet PUT per day, KB-scale objects,
  plus the orchestrator's hourly LIST while the laptop is up (~720 a month at most, a
  fraction of a cent).
  S3 charges egress for the Phase 2 pull back down; at these sizes it rounds to zero.

## Phases

- **Phase 1 — Capture (done, D026):** capture script + scheduled workflow writing to
  the bucket. *Outcome: snapshots accumulate daily, laptop-independent — no new gaps.*
- **Phase 2 — Local sync + catch-up (done, D027):** `bronze_capture_sync` copies captured
  date-partitions from the bucket into MinIO `bronze/`; the orchestrator runs the same
  logic inline and feeds the caught-up range to Silver and both Gold paths.
  *Outcome: the local warehouse comes current in one ordinary run.*
- **Phase 3 — Cleanup (done, folded into Phase 2):** the cloud capture is now the only
  scheduled CoinCap call. `bronze_coincap_assets` stays for manual one-offs but is no
  longer chained into the daily flow — keeping both would have meant two writers on the
  same Bronze key and two API credits per day.

## Running the sync

The orchestrator does this automatically on its hourly poll. To pull manually:

- **`bronze_capture_sync`** — copies every captured date Bronze is missing. Optional
  `start_date`/`end_date` narrow the window; `overwrite=true` re-copies dates that
  already exist locally (the repair path for a bad Bronze partition).
- **`coincap_regular_orchestrator`** — same sync, then Silver and Gold over exactly the
  dates it pulled. Skips itself when there's nothing new rather than rewriting identical
  partitions.

To replay one day end to end: trigger the orchestrator with
`start_date=end_date=YYYY-MM-DD` and `overwrite=true`.

## Local read credentials

The sync needs its **own** IAM key — `s3:GetObject` **and** `s3:ListBucket` on the
capture bucket. Don't reuse the workflow's writer key: it's deliberately write-only, and
listing is what the sync does first.

`s3:ListBucket` is a *bucket*-level action, so unlike `PutObject` its resource is the
bucket ARN with no trailing `/*`. Getting these two confused is the usual cause of an
`AccessDenied` that looks inexplicable:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::<bucket>"
    },
    {
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::<bucket>/crypto/assets/*"
    }
  ]
}
```

Put the key in `.env` as `CAPTURE_S3_ACCESS_KEY_ID` / `CAPTURE_S3_SECRET_ACCESS_KEY`
plus `CAPTURE_S3_BUCKET`; docker-compose builds the Airflow `capture_s3` connection from
them. That connection uses the JSON form rather than a connection URI, because AWS secret
keys routinely contain `/` and `+`, which corrupt a URI unless percent-encoded.

## Acceptance criteria

- Scheduled workflow produces a validated Parquet per day in Bronze's key layout, no
  secrets committed.
- A laptop-off day no longer creates a Silver gap after a local sync + run.
- Existing Silver/Gold DAGs consume captured data with no schema changes.

## Gotcha: `--date` relabels, it does not time-travel

`/assets` is a **live** endpoint — it returns the market as of now and takes no date
parameter. The capture's `--date` flag (and the workflow's date input) only chooses the
**object key**. Passing an old date therefore files *today's* prices under that date.

Use it for relabeling around the UTC midnight boundary or re-running a failed job, never
to fill a historical gap. Real history needs `/assets/{id}/history`, which is what
`bronze_coincap_history_backfill` uses — a different endpoint, per-coin, thinner payload,
and expensive in credits (D024).

Bronze stores no fetch timestamp, so a mislabeled partition is indistinguishable from a
real one after the fact. If you produce one while testing, delete the object before the
sync carries it into Bronze.
