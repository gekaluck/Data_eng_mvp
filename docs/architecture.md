# Architecture

## Overview

This is a **local-first batch data platform** built around crypto market data.
Its purpose is learning — not production deployment.

The system follows a **lakehouse-style** architecture with three layers:
- **Bronze**: raw, untransformed data as received from the source API
- **Silver**: cleaned, typed, deduplicated data in structured tables
- **Gold**: aggregated, business-level datasets ready for analysis

---

## Data Flow

```
[CoinCap API]
      |
      v
  Airflow DAG (daily)
      |
      v
  Bronze (raw JSON/Parquet in MinIO)
      |
      v
  PySpark job (clean, type, deduplicate)
      |
      v
  Silver (Iceberg tables in MinIO, catalog in SQLite)
      |
      v
  PySpark job (aggregate)
      |
      v
  Gold (Iceberg tables in MinIO, catalog in SQLite)
```

---

## Components

### Data Source — CoinCap API
- Public REST API, no API key required
- Provides price, market cap, volume, and historical data for cryptocurrencies
- We fetch daily snapshots for the top 10–20 coins by market cap

### Orchestration — Apache Airflow
- Runs locally via the official Docker Compose setup
- Manages scheduling, retries, backfills, and DAG dependencies
- Airflow is a **first-class learning target**, not just glue

### Compute — PySpark
- Local PySpark (Spark 3.5.x)
- Used for bronze-to-silver and silver-to-gold transformations
- Explicit DataFrame API — no magic wrappers

### Storage — MinIO
- S3-compatible object storage, runs as a Docker container
- Acts as the "data lake" backing store for all layers
- One bucket per layer: `s3a://bronze/...`, `s3a://silver/...`, `s3a://gold/...`

### Table Format — Apache Iceberg
- Iceberg 1.5.x on top of Spark
- JDBC catalog backed by SQLite (simplest local option)
- Used for silver and gold layers
- Gives us: schema evolution, partition management, time travel, incremental reads

---

## Data Modeling Approach

Data modeling is a cross-cutting concern across the three layers. Each layer has a
different modeling philosophy, and understanding *why* is as important as the code.

### Modeling by Layer

| Layer  | Modeling style         | Key question                                      |
|--------|------------------------|---------------------------------------------------|
| Bronze | No model (raw storage) | "What did the source give us?"                    |
| Silver | Entity-centric / 3NF   | "What are the real-world things in this data?"    |
| Gold   | Analytical / dimensional | "What questions do we want to answer?"           |

### Silver: Entity Modeling
In the silver layer we identify **entities** (the nouns in our domain) and give each
one a clean, well-typed table. For crypto data, the core entities are:

- **Coin** — static/slow-changing attributes (name, symbol, rank)
- **Price snapshot** — time-series fact (price, volume, market cap at a point in time)

Separating these is a deliberate modeling choice:
- Coin attributes change rarely; price data changes daily
- Keeping them apart avoids redundant storage and makes updates cleaner
- Joins are cheap and explicit — you always know what you're working with

This is essentially **third normal form (3NF)**: eliminate redundancy, one fact in one place.

### Gold: Dimensional / Analytical Modeling
In the gold layer we reshape data around **questions**, not entities. This is where
concepts from dimensional modeling apply:

- **Fact tables**: measurable events (e.g., daily price facts with foreign keys)
- **Dimension tables**: descriptive context (e.g., coin dimension with attributes)
- **Denormalization**: intentional — gold trades normalization for query simplicity
- **Pre-aggregations**: rolling averages, rankings, period-over-period changes

The gold layer optimizes for the *reader*, not the *writer*.

### Why This Matters
Most data engineering courses skip modeling or treat it as an afterthought. But in practice,
**bad models cause more pain than bad code**. A poorly modeled silver layer cascades into
confusing gold tables and unreliable metrics. We'll build both layers deliberately and
explain the tradeoffs as we go.

---

## Layer Definitions

### Bronze
- **Format**: Raw JSON or Parquet (as-is from API)
- **Storage**: MinIO, partitioned by ingestion date
- **Schema enforcement**: None — store what we receive
- **Modeling**: None — this is intentional. Bronze preserves the source shape exactly,
  so we can always reprocess from scratch if our models change.
- **Purpose**: Immutable landing zone; source of truth for reprocessing

### Silver
- **Format**: Iceberg tables (Parquet underneath)
- **Schema enforcement**: Yes — explicit PySpark schemas
- **Transformations**: Type casting, renaming, deduplication, null handling
- **Modeling**: Entity-centric (3NF). Separate tables for distinct real-world concepts.
  Each table has a clear grain (one row = one coin, or one row = one coin per day).
- **Purpose**: Clean, queryable, structured data

### Gold
- **Format**: Iceberg tables (Parquet underneath)
- **Transformations**: Aggregations, window functions, derived metrics
- **Modeling**: Analytical / dimensional. Fact and dimension tables, denormalized where
  it helps readability. Designed around the questions we want to answer.
- **Purpose**: Analysis-ready datasets (e.g., daily summaries, rolling averages, rankings)

---

## Infrastructure Stack (Local)

| Component      | Implementation         | Runs In       |
|----------------|------------------------|---------------|
| Orchestration  | Apache Airflow 2.x     | Docker        |
| Compute        | PySpark 3.5.x          | Local / Docker|
| Storage        | MinIO                  | Docker        |
| Table format   | Iceberg 1.5.x          | Spark plugin  |
| Catalog        | JDBC (SQLite)          | Local file    |
| OS             | Windows + WSL2/Docker  | Host          |

---

## Cloud Extension (Optional, Future)

If we later extend to AWS, the mapping would be:

| Local           | AWS Equivalent     |
|-----------------|--------------------|
| MinIO           | S3                 |
| SQLite catalog  | AWS Glue Catalog   |
| PySpark (local) | EMR Serverless     |
| Airflow (local) | MWAA or self-hosted|

This is documented for context only. Nothing in the project requires cloud access.
