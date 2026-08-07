# Architecture

## Overview

This is a **local-first batch data platform** built around crypto market data.
Its purpose is learning, not production deployment.

The system follows a **lakehouse-style** architecture with three layers:
- **Bronze**: raw, untransformed data as received from the source API
- **Silver**: cleaned, typed, deduplicated data in structured tables
- **Gold**: aggregated, business-level datasets ready for analysis

---

## Data Flow

Capture is deliberately split from processing: the daily API call runs in the cloud so
coverage doesn't depend on the laptop being on (D026), while every transform stays local.
The cloud capture is the **only** scheduled CoinCap call; local Airflow consumes what it
lands (D027).

```text
[CoinCap API]
      |
      v
  GitHub Actions daily cron
  (scripts/capture_daily_snapshot.py)
      |
      v
  S3 capture bucket (raw Parquet, Bronze's key layout)
      |
      v
  Airflow orchestrator: sync_captured_snapshots
  (copies only the dates Bronze lacks; emits the caught-up range)
      |
      v
  Bronze (Parquet in MinIO, date-partitioned)
      |
      v
  PySpark Silver transforms
      |
      v
  Silver (Iceberg tables in MinIO, JDBC catalog metadata in Postgres)
      |
      v
  Spark Gold path + dbt Gold path
      |                  |
      |                  +--> dbt artifacts (manifest + catalog, refreshed after each build)
      |
      v
  Gold (Iceberg tables in MinIO, JDBC catalog metadata in Postgres)
      |
      v
  Trino SQL serving boundary          dbt artifacts
      |                                    |
      +--> Superset dashboards              |
      |    + SQL Lab / Jupyter              |
      |                                    v
      +--> MCP metadata + planning server <-+
           (stdio + loopback streamable HTTP)
                    |
                    v
              local MCP clients
```

Everything from Bronze down runs over the **range the sync discovered**, not a fixed
"today", so a laptop that was off for a week catches up in one ordinary run.

Before triggering anything, the orchestrator checks that every downstream DAG is
registered and unpaused, and each trigger task carries an `execution_timeout`. Triggering
a paused DAG otherwise produces a run stuck in `queued` that is waited on forever — the
mechanism behind an 8-day silent Gold outage (I9, D029). The dbt test DAG runs after
*both* Gold branches, since its tests compare them.

`bronze_coincap_assets` (the original local fetch) still exists for manual one-off
pulls but is no longer part of the daily chain — see D027 for how the daily call
migrated from local Airflow to the cloud.

---

## Components

### Data Source - CoinCap API
- REST API currently served from `rest.coincap.io`
- Current access model requires an API key
- Provides price, market cap, volume, and historical data for cryptocurrencies
- We fetch daily snapshots for the top 10-20 coins by market cap

### Orchestration - Apache Airflow
- Runs locally via Docker Compose
- Manages scheduling, retries, backfills, DAG dependencies, and the regular orchestrator flow
- Airflow is a first-class learning target, not just glue

### Compute - PySpark
- Local PySpark (Spark 3.5.x)
- Used for bronze-to-silver and one Gold implementation
- Explicit DataFrame API

### Transformation SQL - dbt
- Runs against the local Trino service
- Builds a second Gold implementation for learning and comparison
- Runs tests as SQL assertions in the orchestrated regular flow
- Publishes `manifest.json` and `catalog.json` to `dbt/artifacts/` after every successful
  Airflow-managed Gold build; a failed build leaves the publish task unrun

### Query Layer - Trino
- Single-node Trino runs locally in Docker
- Reads and writes Iceberg tables through the shared JDBC catalog
- Serves as the SQL endpoint for dbt and ad hoc exploration
- Applies a read-only Gold access policy to the Superset identity
- Applies a separate read-only `agent` identity to `gold.crypto_dbt` only
- Routes `agent` queries through a local resource group capped at one concurrent query,
  two queued queries, 128 MB soft memory, and 1 GB of physical scans per hour; all other
  identities retain the fallback resource group

### AI-Agent Platform Boundary
- The MCP server's engine identity is `agent`; Trino is the unbypassable read-only
  backstop, while richer SQL validation remains the MCP tool layer's responsibility
- `config/ai-agent/allowed-tables.json` explicitly enumerates the five canonical dbt Gold
  relations. Adding a dbt model does not expose it automatically
- Live Iceberg metadata remains authoritative for structure. The published dbt artifacts
  supply descriptions and lineage and are treated as potentially stale if they disagree
  with the live catalog
- The first transport-agnostic guardrail slice parses the Trino AST, accepts exactly one
  root `SELECT`, resolves physical tables without mistaking CTE names for tables, requires
  `catalog.schema.table`, and checks every dependency against the explicit allow-list
- One FastMCP registry exposes the five metadata tools plus scan-free `explain_query`
  through stdio and streamable HTTP. Both transports return the same typed success payloads
  and the same `{code, message, retryable, hint}` tool errors with MCP `isError` set
- `explain_query` runs the strict AST/allow-list guardrail before asking Trino for
  `EXPLAIN (TYPE DISTRIBUTED)`. It returns at most 12,000 plan characters and a typed
  semantic verdict; it never constructs `EXPLAIN ANALYZE` or executes caller rows
- HTTP is stateless JSON on `/mcp` and binds to loopback only. Explicit allowed-host and
  allowed-origin checks protect the local endpoint from DNS rebinding. Remote or multi-user
  exposure is not an implicit configuration change; it requires an authentication decision
- AI runtime and test dependencies live under `ai_agent/`; they are installed by an
  isolated CI job and are not part of the Airflow image (D036)

### Serving Layer - Apache Superset
- Runs as an optional Docker Compose profile for end-user exploration
- Reads canonical dbt Gold relations through Trino; it does not copy lake data
- Provisions the connection, datasets, charts, filters, and dashboard idempotently from code
- Splits into a **Market** tab and a **Pipeline Health** tab: what the data says, and
  whether it can be believed (D031)
- Includes a daily availability view that exposes available, partial, and missing dates,
  plus per-date field completeness — row counts alone pass a day that arrived without
  volume or VWAP

### Storage - MinIO
- S3-compatible object storage, runs as a Docker container
- Acts as the lakehouse backing store for all layers
- One bucket per layer: `s3a://bronze/...`, `s3a://silver/...`, `s3a://gold/...`

### Cloud Capture - GitHub Actions + AWS S3
- One scheduled job per day fetches the CoinCap `/assets` snapshot and writes raw
  Parquet to an S3 bucket, independent of the local stack (D026)
- Reuses the Bronze Pydantic contract and object-key layout, so captured objects are
  byte-compatible with what the local Bronze DAG writes
- Since D027 this is the **only** scheduled CoinCap call in the system
- The only cloud dependency in the project; everything downstream stays local
- Writes with a scoped, write-only IAM key; the local sync reads with a separate one
- See `docs/autonomous-daily-capture.md`

### Capture Sync - `bronze_capture_sync` / orchestrator task
- Copies captured days the local Bronze doesn't have yet, byte-for-byte (no
  transformation — the capture already applied the Pydantic contract)
- Idempotent by construction: it syncs the set difference, so re-running copies nothing
- Publishes the caught-up date range, which drives Silver and both Gold paths
- Planning logic is pure and unit-tested in `dags/utils/capture_sync.py`; the standalone
  DAG exists for manual "pull what's new" runs
- Fails when the newest captured date is older than `CAPTURE_MAX_AGE_DAYS` (default 2), so
  a dead cloud capture is distinguishable from a genuinely quiet day (D029). Set it to `0`
  to re-sync an old date deliberately

### Table Format - Apache Iceberg
- Iceberg 1.5.x on top of Spark
- JDBC catalog metadata stored in Postgres
- Data and metadata files stored in MinIO
- Used for silver and gold layers
- Gives us schema evolution, partition management, time travel, and incremental reads

---

## Data Modeling Approach

Data modeling is a cross-cutting concern across the three layers. Each layer has a
different modeling philosophy, and understanding why is as important as the code.

### Modeling by Layer

| Layer  | Modeling style            | Key question                                   |
|--------|---------------------------|------------------------------------------------|
| Bronze | No model (raw storage)    | "What did the source give us?"                 |
| Silver | Entity-centric / 3NF      | "What are the real-world things in this data?" |
| Gold   | Analytical / dimensional  | "What questions do we want to answer?"         |

### Silver: Entity Modeling

In the silver layer we identify **entities** and give each one a clean, typed table.
For crypto data, the core entities are:

- **Coin**: static or slow-changing attributes such as name and symbol
- **Price snapshot**: time-series facts such as price, volume, and market cap

Separating these is deliberate:
- Coin attributes change rarely while price data changes daily
- Keeping them apart avoids redundant storage and makes updates cleaner
- Joins are explicit and cheap at this scale

This is effectively **third normal form (3NF)**.

### Gold: Dimensional / Analytical Modeling

In the gold layer we reshape data around **questions**, not entities.

- **Fact tables**: measurable events and observations
- **Dimension tables**: descriptive context
- **Denormalization**: intentional where it improves readability and query ergonomics
- **Pre-aggregations**: rolling averages, rankings, and period-over-period changes

The gold layer optimizes for the reader, not the writer.

---

## Layer Definitions

### Bronze
- **Format**: Parquet (Snappy compression)
- **Storage**: MinIO (`s3://bronze/crypto/assets/year=YYYY/month=MM/day=DD/assets.parquet`)
- **Schema enforcement**: Pydantic validation at ingestion time, then an explicit PyArrow
  schema declared once in `dags/utils/bronze_snapshot.py` and shared by both writers
- **Modeling**: none; bronze preserves source shape exactly
- **Provenance**: every snapshot also carries `api_timestamp_ms` (CoinCap's response
  timestamp) and `fetched_at_utc` (our wall clock at fetch time), so a mislabelled object is
  detectable instead of inferred from a price coincidence (D033). Snake_case marks the two
  columns we add; every other column is CoinCap's own camelCase name. Audit with
  `scripts/audit_bronze_provenance.py`; dates written before 2026-07-31 have neither column
- **Purpose**: landing zone and source for reprocessing
- **Caveat**: not actually immutable. The local fetch DAG uploads with `replace=True` and
  names objects from `logical_date`, so a late run overwrote a past date with live prices.
  Objects for 2026-07-22..07-28 are affected and Silver holds better values — see D028
  before reprocessing that window. The cloud capture cannot do this (D027).

### Silver
- **Format**: Iceberg tables (Parquet underneath)
- **Schema enforcement**: explicit PySpark schemas
- **Transformations**: type casting, renaming, deduplication, null handling
- **Modeling**: entity-centric with a clear grain per table
- **Purpose**: clean, queryable, structured data

### Gold
- **Format**: Iceberg tables (Parquet underneath)
- **Transformations**: aggregations, window functions, derived metrics
- **Implementations**: Spark Gold and dbt Gold side by side
- **Modeling**: analytical and dimensional
- **Purpose**: analysis-ready datasets
- **Gaps are tolerated, not hidden**: a date whose predecessor is missing keeps its rows
  with null change columns (D025), and dbt tests assert that both implementations cover
  every Silver date, agree on row counts per date, and never leave a null change where
  Silver holds the previous day (D029)
- **Every serving model is asserted, not just the flagship one**: the three dbt serving
  models must agree on per-date row counts. Two of them silently held 9 dates against
  `daily_snapshot`'s 107 until this was checked (I19)

---

## Infrastructure Stack (Local)

| Component      | Implementation      | Runs In        |
|----------------|---------------------|----------------|
| Orchestration  | Apache Airflow 2.x  | Docker         |
| Compute        | PySpark 3.5.x       | Local / Docker |
| Query engine   | Trino 477           | Docker         |
| BI / serving   | Superset 6.0        | Docker         |
| AI tool protocol | MCP Python SDK 1.x | Host / isolated runtime |
| Storage        | MinIO               | Docker         |
| Table format   | Iceberg 1.5.x       | Spark plugin   |
| Catalog        | JDBC                | Postgres       |
| OS             | Windows + Docker    | Host           |

---

## Cloud Extension (Optional, Future)

If this later moves to AWS, the rough mapping is:

| Local           | AWS Equivalent      |
|-----------------|---------------------|
| MinIO           | S3                  |
| JDBC catalog    | AWS Glue / REST     |
| PySpark (local) | EMR Serverless      |
| Airflow (local) | MWAA or self-hosted |

This is context only. The one piece already in the cloud is the daily capture (D026);
nothing else in the project requires cloud access.
