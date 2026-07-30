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
      |
      v
  Gold (Iceberg tables in MinIO, JDBC catalog metadata in Postgres)
      |
      v
  Trino SQL serving boundary
      |
      v
  Superset dashboards + SQL Lab / Jupyter debugging
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

### Query Layer - Trino
- Single-node Trino runs locally in Docker
- Reads and writes Iceberg tables through the shared JDBC catalog
- Serves as the SQL endpoint for dbt and ad hoc exploration
- Applies a read-only Gold access policy to the Superset identity

### Serving Layer - Apache Superset
- Runs as an optional Docker Compose profile for end-user exploration
- Reads canonical dbt Gold relations through Trino; it does not copy lake data
- Provisions the connection, datasets, charts, and dashboard idempotently from code
- Includes a daily availability view that exposes available, partial, and missing dates

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
- **Schema enforcement**: Pydantic validation at ingestion time
- **Modeling**: none; bronze preserves source shape exactly
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

---

## Infrastructure Stack (Local)

| Component      | Implementation      | Runs In        |
|----------------|---------------------|----------------|
| Orchestration  | Apache Airflow 2.x  | Docker         |
| Compute        | PySpark 3.5.x       | Local / Docker |
| Query engine   | Trino 477           | Docker         |
| BI / serving   | Superset 6.0        | Docker         |
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
