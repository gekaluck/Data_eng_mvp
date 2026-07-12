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
- **Storage** = an S3-compatible bucket (Cloudflare R2 or AWS S3). The stack is already
  S3-native (MinIO), so reading these snapshots locally is the same code with a
  different endpoint. Config is provider-agnostic via `CAPTURE_S3_*` env vars.

### Why a bucket, not a git data branch

Data-in-git is an anti-pattern (repo bloat, no lifecycle, loses partition/scan
benefits). A bucket is the real-world landing-zone pattern and, because the stack
already speaks S3, barely more work.

## One-time setup (you)

1. **Create a bucket** — Cloudflare R2 (free tier, S3-compatible, no egress) or AWS S3.
   - R2: create a bucket, then an R2 API token (Object Read & Write).
   - Note the **endpoint URL** (`https://<account_id>.r2.cloudflarestorage.com`),
     **access key id**, **secret access key**, and **bucket name**.
2. **Add GitHub Actions secrets** (repo → Settings → Secrets and variables → Actions):
   - `COINCAP_API_KEY`
   - `CAPTURE_S3_ENDPOINT_URL`
   - `CAPTURE_S3_ACCESS_KEY_ID`
   - `CAPTURE_S3_SECRET_ACCESS_KEY`
   - `CAPTURE_S3_BUCKET`
3. **Verify** — trigger the workflow manually (Actions → "Daily CoinCap capture" →
   Run workflow). Confirm one `crypto/assets/year=/month=/day=/assets.parquet` object
   appears in the bucket. Costs 1 CoinCap call.

Secrets never live in the repo (D013). `.env.example` documents the same vars for the
local sync.

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
