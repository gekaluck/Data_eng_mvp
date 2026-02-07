# Technical Decisions

## How to Read This

Each entry records a decision we made, why we made it, and what alternatives we considered.
This is a lightweight version of an Architecture Decision Record (ADR).

Entries are numbered and dated. We don't remove old entries — if a decision is reversed,
we add a new entry referencing the old one.

---

## D001 — Data Source: CoinCap API
**Date**: 2026-02-06
**Status**: accepted

**Decision**: Use the CoinCap REST API as the primary data source.

**Why**:
- No API key required — zero friction to get started
- Simple REST endpoints for asset listings and historical data
- Sufficient data (price, volume, market cap) for meaningful transformations
- Rate limits are generous enough for daily batch of 10–20 coins

**Alternatives considered**:
- **CoinGecko**: More popular, richer data, but the free tier now requires a demo API key
  and has tighter rate limits. Good fallback if CoinCap proves insufficient.
- **CoinMarketCap**: Requires API key signup. Richer data but more setup friction.

**Revisit if**: CoinCap data quality or availability becomes a problem.

---

## D002 — Local Storage: MinIO
**Date**: 2026-02-06
**Status**: accepted

**Decision**: Use MinIO as the local S3-compatible object store.

**Why**:
- Drop-in S3 replacement — same APIs, same SDKs
- Lightweight Docker container, easy to run
- Industry-standard choice for local lakehouse setups
- Smooth migration path to real S3 if we ever go to cloud

**Alternatives considered**:
- **Local filesystem**: Simpler, but doesn't teach S3 patterns. Would require rewriting
  paths and configs if we ever add cloud support.
- **LocalStack**: Full AWS emulator, overkill for our needs.

---

## D003 — Iceberg Catalog: JDBC with SQLite
**Date**: 2026-02-06
**Status**: accepted

**Decision**: Use the Iceberg JDBC catalog backed by a local SQLite database.

**Why**:
- Simplest possible catalog — no extra services to run
- Good enough for local development and learning
- Lets us focus on Iceberg table operations without catalog complexity

**Alternatives considered**:
- **REST catalog (Nessie, Iceberg REST)**: More realistic for multi-user setups,
  but adds another Docker container and configuration surface. Not worth it for solo learning.
- **Hive Metastore**: Traditional choice, but heavy (requires its own DB, Thrift service).
  Overkill for local use.
- **AWS Glue Catalog**: Cloud-only. Would break the local-first constraint.

**Revisit if**: We need concurrent access or want to simulate a more production-like setup.

---

## D004 — Orchestration: Airflow Official Docker Compose
**Date**: 2026-02-06
**Status**: accepted

**Decision**: Use the official Apache Airflow Docker Compose setup.

**Why**:
- Realistic — mirrors what teams actually run
- Well-documented by the Airflow project
- Includes scheduler, webserver, worker, metadata DB, and supporting services
- 32GB RAM is more than enough to run it comfortably

**Alternatives considered**:
- **Standalone Airflow (pip install)**: Lighter, but loses the multi-container architecture
  that's important to understand.
- **Slimmed-down custom Compose**: Possible, but we'd lose the "this is what real Airflow
  looks like" benefit. We can trim later if needed.

---

## D005 — Compute: Local PySpark
**Date**: 2026-02-06
**Status**: accepted

**Decision**: Run PySpark locally (Spark 3.5.x) for all transformations.

**Why**:
- Direct control over Spark configuration and behavior
- Explicit DataFrame API usage — nothing hidden
- Spark 3.5.x is current stable and has good Iceberg 1.5.x support
- Local mode is sufficient for our data volumes (10–20 coins daily)

**Alternatives considered**:
- **Spark in Docker**: Adds complexity without much benefit at our scale.
  Can revisit if we need Spark to talk to Airflow containers directly.
- **Pandas**: Simpler, but defeats the purpose of learning Spark.

---

## D006 — Python Tooling: Plain pip + requirements.txt
**Date**: 2026-02-06
**Status**: accepted

**Decision**: Use pip and requirements.txt for dependency management. No linting tools.

**Why**:
- Minimal friction
- Universally understood
- Learning focus is on data engineering, not tooling

**Alternatives considered**:
- **Poetry / uv**: Better dependency resolution, but adds a tool to learn.
- **Ruff / Black**: Good practice, but not the focus of this project.

**Revisit if**: Dependency conflicts become painful, or we want to add CI.

---

## D007 — Coin Count: Top 10–20 by Market Cap
**Date**: 2026-02-06
**Status**: accepted

**Decision**: Start with top 10 coins. Expand to 20 if data volume stays manageable.

**Why**:
- 10 is enough for meaningful aggregations, rankings, and window functions
- Keeps API calls, storage, and processing fast during development
- Easy to scale up later — it's just a parameter

---

## D008 — Scheduling Cadence: Daily Batch
**Date**: 2026-02-06
**Status**: accepted

**Decision**: Start with daily batch processing.

**Why**:
- Simplest Airflow scheduling pattern
- Clean backfill behavior (one run per day)
- Matches the granularity we need for learning
- Avoids complexity of sub-daily scheduling (overlapping runs, catchup logic)

**Revisit if**: We want to explore near-real-time patterns or streaming concepts.
