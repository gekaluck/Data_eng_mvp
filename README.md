# Data Engineering MVP

Local-first crypto lakehouse project for learning Airflow, Spark, Iceberg, Trino, and dbt through a realistic batch pipeline.

## What This Repo Does

The pipeline ingests CoinCap market data into a three-layer lakehouse:

- Bronze: raw CoinCap payloads stored as date-partitioned Parquet in MinIO
- Silver: cleaned and typed Iceberg tables built with PySpark
- Gold: analytics-ready tables built in two parallel paths
  - Spark Gold in `gold.crypto.*`
  - dbt Gold in `gold.crypto_dbt.*`

Regular runs are orchestrated by Airflow through one root DAG:

- `coincap_regular_orchestrator`

That DAG triggers:

1. `bronze_coincap_assets`
2. `silver_coincap_assets`
3. `gold_coincap_assets` and `gold_dbt_coincap_assets` in parallel
4. `gold_dbt_coincap_tests` after the dbt Gold build succeeds

Backfills remain manual and separate from the regular daily flow.

## Stack

- Airflow for orchestration
- PySpark for Bronze -> Silver and Spark Gold transforms
- Apache Iceberg for Silver and Gold tables
- MinIO for S3-compatible object storage
- Postgres for Airflow metadata and Iceberg JDBC catalogs
- Trino for SQL access
- dbt for the SQL Gold path and data tests

## Repo Layout

- [dags](C:/Repos/Data_eng_mvp/dags): Airflow DAG definitions
- [spark](C:/Repos/Data_eng_mvp/spark): PySpark transforms and helper modules
- [dbt](C:/Repos/Data_eng_mvp/dbt): dbt project for the SQL Gold path
- [docs](C:/Repos/Data_eng_mvp/docs): architecture notes, decisions, and runbook material
- [tests](C:/Repos/Data_eng_mvp/tests): DAG and schema checks
- [config](C:/Repos/Data_eng_mvp/config): Trino and service configuration

## Quick Start

1. Copy [.env.example](C:/Repos/Data_eng_mvp/.env.example) to `.env` and fill in:
   - Airflow credentials
   - MinIO credentials
   - `COINCAP_API_KEY`
2. Build the image:

```powershell
make build
```

3. Start the stack:

```powershell
make up
```

4. Open the local services:
   - Airflow: `http://localhost:8080`
   - MinIO console: `http://localhost:9001`
   - Trino: `localhost:8081`
   - JupyterLab: `http://localhost:8888`

## Regular Flow

The normal entrypoint is the orchestrator DAG:

- `coincap_regular_orchestrator`

Use that DAG for:

- scheduled daily processing
- manual reruns for a single `target_date`

The leaf DAGs remain useful for:

- isolated debugging
- targeted manual reruns
- backfill support

## dbt Workflow

On the host:

```powershell
dbt debug --project-dir dbt --profiles-dir dbt
dbt run --project-dir dbt --profiles-dir dbt --select daily_snapshot mc_rank_change wkly_roll_avg --vars '{"snapshot_date": "2026-04-02"}'
dbt test --project-dir dbt --profiles-dir dbt --select daily_snapshot mc_rank_change wkly_roll_avg --vars '{"snapshot_date": "2026-04-02"}'
```

Inside Airflow, the regular orchestrator runs the dbt build and dbt tests as separate downstream DAGs.

## Useful Commands

```powershell
make ps
make logs-scheduler
make trino
make test
make test-dag
```

## Documentation Map

- [architecture.md](C:/Repos/Data_eng_mvp/docs/architecture.md): system overview and layer design
- [decisions.md](C:/Repos/Data_eng_mvp/docs/decisions.md): technical decisions log
- [runbook.md](C:/Repos/Data_eng_mvp/docs/runbook.md): operational procedures and debugging steps
- [table_browser.md](C:/Repos/Data_eng_mvp/docs/table_browser.md): Trino/Jupyter/dbt exploration
- [dbt/README.md](C:/Repos/Data_eng_mvp/dbt/README.md): dbt-specific setup and usage

## Current State

This is intentionally a learning project, but the pipeline now includes several production-like concerns:

- explicit date propagation
- separate regular vs backfill flows
- retry behavior
- manual rerun support
- dbt tests in the orchestrated path
- dual Gold implementations for comparison and learning

## Notes

- Existing local data may need replay if you change catalog configuration or DAG contracts.
- The project is local-first; cloud equivalents are documented but not required.