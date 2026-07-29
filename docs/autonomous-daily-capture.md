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

- GitHub's scheduled runs are best-effort and often drift 5–30 min past 00:30 UTC. The
  partition date is resolved inside the script, so drift is harmless.
- GitHub **disables scheduled workflows after 60 days of repo inactivity** (it emails
  first). A quiet stretch on the repo silently stops capture — the thing this design
  exists to prevent.
- Cost at this volume is negligible: one small Parquet PUT per day, KB-scale objects.
  S3 charges egress for the Phase 2 pull back down; at these sizes it rounds to zero.

## Phases

- **Phase 1 — Capture (this change):** capture script + scheduled workflow writing to
  the bucket. *Outcome: snapshots accumulate daily, laptop-independent — no new gaps.*
- **Phase 2 — Local sync + catch-up (next):** a `make sync` / small DAG task that copies
  captured date-partitions from the bucket into MinIO `bronze/`, then runs Silver/Gold
  over the caught-up range (reuses the existing per-date DAGs and the range-aware Gold
  rebuild). *Outcome: bring the local warehouse current in one step.*
- **Phase 3 — Cleanup (optional):** make the cloud capture the single source of daily
  capture; keep the local fetch DAG for manual one-offs only.

## Acceptance criteria

- Scheduled workflow produces a validated Parquet per day in Bronze's key layout, no
  secrets committed.
- A laptop-off day no longer creates a Silver gap after a local sync + run.
- Existing Silver/Gold DAGs consume captured data with no schema changes.
