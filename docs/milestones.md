# Milestones

## How to Read This

Each milestone is a self-contained piece of work with a clear outcome.
We work on one milestone at a time, on a dedicated branch.

Status key: `planned` | `in progress` | `done`

---

## M0 — Documentation & Planning
**Status**: done
**Branch**: `chore/initial-docs`

**Goal**: Set up project documentation, agree on architecture, and define the roadmap.

**Deliverables**:
- `docs/architecture.md` — system design and data flow
- `docs/milestones.md` — this file
- `docs/decisions.md` — technical decision log

**Why first**: Writing things down forces clarity. We align on the plan before writing code,
which prevents wasted effort and keeps the learning path coherent.

---

## M1 — Local Infrastructure
**Status**: in progress
**Branch**: `feat/m1-local-infra`

**Goal**: Get Airflow + MinIO running locally via Docker Compose. Verify the setup works.

**Deliverables**:
- `docker-compose.yml` with Airflow (official image) + MinIO
- A trivial "hello world" Airflow DAG that confirms the scheduler is working
- MinIO accessible and buckets creatable
- Brief setup instructions in docs

**Why second**: Everything else (DAGs, Spark jobs, Iceberg tables) depends on infrastructure
being up and verified. No point writing pipelines if the platform doesn't start.

**Key learning**:
- How Airflow's Docker Compose works (scheduler, webserver, worker, metadata DB)
- What MinIO provides and how S3-compatible storage works
- Docker networking basics between services

---

## M2 — Bronze Layer (Raw Ingestion)
**Status**: planned
**Branch**: TBD

**Goal**: Build the first real Airflow DAG that fetches crypto data from CoinCap and writes
raw data to MinIO (bronze layer).

**Deliverables**:
- Airflow DAG: daily schedule, fetches top 10–20 coins
- Raw data lands in MinIO as Parquet or JSON, partitioned by date
- Basic error handling and logging

**Key learning**:
- DAG structure: tasks, dependencies, schedule intervals
- Airflow operators (PythonOperator or similar)
- Writing to S3-compatible storage from Python
- Bronze layer design: what to store, how to partition, why immutability matters
- **Data modeling**: Why bronze has no model — understanding the value of storing raw data
  as-is and how it protects you when upstream schemas or your own models change

---

## M3 — Silver Layer (Iceberg + Spark)
**Status**: planned
**Branch**: TBD

**Goal**: PySpark job reads bronze data, cleans/transforms it, and writes to Iceberg tables
in the silver layer.

**Deliverables**:
- PySpark job with explicit schema definitions
- Iceberg table creation with JDBC/SQLite catalog
- Airflow DAG orchestrating the Spark job
- Deduplication and type-casting logic

**Key learning**:
- PySpark DataFrame API: reads, transforms, writes
- Iceberg basics: table creation, partitioning, catalog config
- Spark + Iceberg integration (JARs, configuration)
- Orchestrating Spark from Airflow
- **Data modeling**: Entity identification — breaking raw API responses into distinct
  tables (coins vs. price snapshots). Understanding table grain ("what does one row mean?"),
  third normal form in practice, and why separating entities reduces redundancy

---

## M4 — Gold Layer (Aggregations)
**Status**: planned
**Branch**: TBD

**Goal**: Build aggregation pipelines that produce analysis-ready gold tables.

**Deliverables**:
- PySpark jobs for aggregations (daily summaries, rolling averages, rankings)
- Gold Iceberg tables
- Airflow DAG with full bronze → silver → gold dependency chain

**Key learning**:
- Window functions in Spark
- Multi-step DAG design
- **Data modeling**: Dimensional modeling in practice — fact vs. dimension tables,
  intentional denormalization, designing tables around questions (not entities),
  pre-computed metrics (rolling averages, rankings, period-over-period comparisons).
  Understanding why gold trades normalization for query simplicity

---

## M5 — Airflow Depth (Backfills, Retries, Incremental)
**Status**: planned
**Branch**: TBD

**Goal**: Add production-like Airflow patterns: backfills, retries, idempotency,
parameterized runs, incremental logic.

**Deliverables**:
- DAGs support backfill for historical date ranges
- Retry policies configured and tested
- Incremental writes (process only new/changed data)
- Parameterized DAG runs

**Key learning**:
- `execution_date` / logical date vs. run date
- Idempotent pipelines: why and how
- Airflow CLI for backfills
- Incremental vs. full-refresh tradeoffs

---

## M6 — Polish & Review
**Status**: planned
**Branch**: TBD

**Goal**: Wrap up with schema evolution examples, light maintenance concepts,
and documentation cleanup.

**Deliverables**:
- Schema evolution example (add/rename a column in Iceberg)
- Documentation of maintenance concepts (compaction, snapshot expiry)
- Final project retrospective

**Key learning**:
- Iceberg schema evolution in practice
- Operational concerns in a lakehouse
- How to think about what comes next

---

## Progress Log

| Date       | Milestone | Notes                                      |
|------------|-----------|---------------------------------------------|
| 2026-02-06 | M0        | Created initial docs, agreed on stack & plan|
| 2026-02-06 | M1        | Docker Compose with Airflow (LocalExecutor) + MinIO, hello_world DAG |
