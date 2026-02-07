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
- Paths follow: `s3a://lakehouse/bronze/...`, `s3a://lakehouse/silver/...`, `s3a://lakehouse/gold/...`

### Table Format — Apache Iceberg
- Iceberg 1.5.x on top of Spark
- JDBC catalog backed by SQLite (simplest local option)
- Used for silver and gold layers
- Gives us: schema evolution, partition management, time travel, incremental reads

---

## Layer Definitions

### Bronze
- **Format**: Raw JSON or Parquet (as-is from API)
- **Storage**: MinIO, partitioned by ingestion date
- **Schema enforcement**: None — store what we receive
- **Purpose**: Immutable landing zone; source of truth for reprocessing

### Silver
- **Format**: Iceberg tables (Parquet underneath)
- **Schema enforcement**: Yes — explicit PySpark schemas
- **Transformations**: Type casting, renaming, deduplication, null handling
- **Purpose**: Clean, queryable, structured data

### Gold
- **Format**: Iceberg tables (Parquet underneath)
- **Transformations**: Aggregations, window functions, derived metrics
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
