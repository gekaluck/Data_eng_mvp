# Superset Serving Layer

## Purpose

Apache Superset is the user-facing BI and exploration layer over the curated dbt
Gold schema. Jupyter remains the engineering/debugging surface; Superset provides
reusable charts, dashboards, filters, and SQL Lab without adding another data copy.

The query path is:

```text
Gold Iceberg -> Trino -> Superset
```

Superset only connects to `gold.crypto_dbt`. Its metadata (users, datasets, charts,
and dashboard state) is stored in a dedicated Postgres service and Docker volume.

## One-time environment setup

Set `SUPERSET_ADMIN_PASSWORD` in the local `.env` file. The PowerShell launcher
generates a cryptographically random `SUPERSET_SECRET_KEY` and saves it to `.env`
when the value is absent. To generate one manually instead:

```powershell
py -c "import secrets; print(secrets.token_urlsafe(64))"
```

Do not commit `.env`. The username, first name, last name, and email use the local
defaults shown in `.env.example` unless explicitly overridden.

## Start and open

PowerShell:

```powershell
.\scripts\stack.ps1 superset
```

Or Make:

```bash
make superset
```

Open `http://localhost:8088`, sign in with `SUPERSET_ADMIN_USERNAME` and
`SUPERSET_ADMIN_PASSWORD`, and open **Crypto Lakehouse — Gold Analytics**.

The serving services use the optional Compose profile `serving`, so ordinary
`docker compose up -d` remains focused on the data platform.

## Reproducible assets

`superset/bootstrap_assets.py` is the source of truth for the Trino connection,
datasets, charts, and dashboard. Stable UUIDs make the bootstrap idempotent: it
updates managed assets in place instead of creating duplicates.

Run it again after changing asset definitions:

```bash
make superset-init
```

The bootstrap exposes these physical dbt relations:

- `daily_snapshot`
- `mc_rank_change`
- `weekly_roll_avg`
- `latest_market_snapshot`
- `data_availability_daily`

## Availability semantics

`data_availability_daily` generates a complete calendar from the first local Silver
snapshot through today. Completely absent dates therefore remain visible.

| Status | Meaning |
|---|---|
| `available` | Silver meets the expected asset count and every serving Gold model has the same row count. |
| `partial` | Silver exists, but its count is low or one of the Gold datasets is missing/mismatched. |
| `missing` | No local Silver price snapshot exists for the date. |

`availability_reason` records the first deterministic cause. The default expected
asset count is 20 and can be overridden with the dbt variable
`expected_asset_count`.

This reports **local analytical availability**. It does not prove that PR 14's
remote capture bucket contains an object; that requires the later cloud-to-MinIO
sync/manifest phase.

## Access boundary

Trino file-based access control grants the `superset` identity `SELECT` only on
`gold.crypto_dbt`. Silver access and Gold writes are denied. Superset also disables
DML, CTAS, CVAS, uploads, and asynchronous execution in its database definition.

Trino still uses unauthenticated HTTP in this local single-user stack. A client can
claim another username, so this is a blast-radius guardrail rather than a hostile-user
security boundary. Add TLS and Trino authentication before exposing either service
beyond localhost.

## Operations

```bash
make logs-superset
docker compose --profile serving ps
docker compose --profile serving stop superset superset-db
```

The `superset-metadata-data` volume preserves users and UI edits. The managed assets
can always be reconstructed by the bootstrap, but unexported manual UI edits cannot.

If the admin password is lost, reset it after supplying the normal environment:

```bash
docker compose --profile serving exec superset \
  superset fab reset-password --username admin --password NEW_PASSWORD
```

The Compose setup is for local development and portfolio demonstration, not a
production Superset deployment. Redis/Celery, alerts, scheduled reports, TLS, and
external authentication are deliberately out of scope for this MVP.
