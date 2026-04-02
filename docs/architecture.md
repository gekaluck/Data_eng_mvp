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

```text
[CoinCap API]
      |
      v
  Airflow orchestrator / manual DAG runs
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
  Trino + dbt + comparison/debugging
```

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

### Storage - MinIO
- S3-compatible object storage, runs as a Docker container
- Acts as the lakehouse backing store for all layers
- One bucket per layer: `s3a://bronze/...`, `s3a://silver/...`, `s3a://gold/...`

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
- **Purpose**: immutable landing zone and source of truth for reprocessing

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

---

## Infrastructure Stack (Local)

| Component      | Implementation      | Runs In        |
|----------------|---------------------|----------------|
| Orchestration  | Apache Airflow 2.x  | Docker         |
| Compute        | PySpark 3.5.x       | Local / Docker |
| Query engine   | Trino 477           | Docker         |
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

This is context only. Nothing in the project requires cloud access.
