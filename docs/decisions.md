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

---

## D009 — Data Modeling: Entity-Centric Silver, Dimensional Gold
**Date**: 2026-02-06
**Status**: accepted

**Decision**: Use entity-centric (3NF-leaning) modeling in silver and dimensional
(fact/dimension) modeling in gold. Treat data modeling as a learning thread across
milestones, not a separate task.

**Why**:
- Silver and gold layers serve different audiences and purposes — the model should reflect that
- Entity modeling in silver teaches normalization, grain, and why separating concerns matters
- Dimensional modeling in gold teaches how analytical systems differ from transactional ones
- Doing both in one project shows the full spectrum of modeling choices
- Weaving modeling into each milestone makes it practical, not academic

**Alternatives considered**:
- **One Big Table everywhere**: Simple, but teaches bad habits. Redundancy, update anomalies,
  and unclear grain make OBT painful at scale.
- **Full Kimball star schema**: More rigorous, but overkill for a 10-coin dataset.
  We borrow the useful concepts (fact/dim separation, grain clarity) without going full
  conformance or bus matrix.
- **Data Vault**: Interesting for auditability and historical tracking, but too abstract
  for a first project. Better explored after the basics are solid.

**Key concepts we'll practice**:
- Table grain: "what does one row represent?"
- Entity identification: breaking a flat API response into distinct concepts
- Normalization vs. denormalization: when each is appropriate and why
- Fact vs. dimension: what's measurable vs. what's descriptive
- Pre-aggregation: trading compute for query speed in gold

---

## D010 — Airflow Executor: LocalExecutor
**Date**: 2026-02-06
**Status**: accepted

**Decision**: Use LocalExecutor instead of CeleryExecutor for Airflow.

**Why**:
- No need for Redis or a Celery worker — fewer containers, simpler setup
- Sufficient for a single-machine learning project
- Reduces Docker Compose from ~8 services to 6
- Tasks run as subprocesses of the scheduler, which is fine for our volume

**Alternatives considered**:
- **CeleryExecutor**: The official Docker Compose default. Adds Redis + worker containers.
  More realistic for production, but unnecessary overhead for learning.
- **SequentialExecutor**: Even simpler, but can only run one task at a time.
  Too limiting once we have multi-task DAGs.

**Revisit if**: We need parallel task execution across multiple workers.

---

## D011 — MinIO Bucket Layout: Three Separate Buckets
**Date**: 2026-02-06
**Status**: accepted

**Decision**: Create one MinIO bucket per data layer: `bronze`, `silver`, `gold`.

**Why**:
- Clear separation of concerns — each layer is independently visible and manageable
- Mirrors how teams often use separate S3 buckets/prefixes in production
- Simple to set up with `mc mb` commands
- Paths become clean: `s3a://bronze/...`, `s3a://silver/...`, `s3a://gold/...`

**Alternatives considered**:
- **Single bucket with prefixes** (`s3a://lakehouse/bronze/...`): Simpler initial setup,
  but less realistic and harder to apply per-bucket policies later.

---

## D012 — Airflow S3 Connection: Environment Variable
**Date**: 2026-02-06
**Status**: accepted

**Decision**: Configure the MinIO/S3 connection via the `AIRFLOW_CONN_MINIO_S3` environment
variable rather than creating it through the Airflow UI.

**Why**:
- Reproducible — connection exists automatically on every `docker compose up`
- No manual UI steps to forget or misconfigure
- Version-controlled alongside the rest of the infrastructure
- Standard Airflow pattern for connection management in code

**Alternatives considered**:
- **Airflow UI**: Clickable but not reproducible. Easy to forget during fresh setup.
- **Airflow CLI** (`airflow connections add`): Reproducible but requires a running scheduler.
  Env var is simpler and works at container startup.

---

## D013 — Security: Environment Variables for Credentials
**Date**: 2026-02-07
**Status**: accepted

**Decision**: All sensitive credentials (Fernet key, admin passwords, database passwords) 
are managed via environment variables in a `.env` file that is excluded from version control.

**Why**:
- **Security**: Prevents hardcoded credentials from being committed to the repository
- **Fernet key**: Required for encrypting sensitive data in the Airflow metadata database; 
  an empty or weak key exposes stored credentials and connection strings
- **Flexibility**: Allows different credentials for dev, staging, and production environments
- **Standard practice**: Industry-standard approach for secrets management in containerized apps
- **Compliance**: Helps meet security compliance requirements by separating secrets from code

**Implementation**:
- `.env` file contains actual secrets (gitignored)
- `.env.example` provides a template that can be safely committed
- `docker-compose.yml` references environment variables using `${VAR_NAME}` syntax
- All default passwords have been removed from the codebase

**Security Requirements**:
- Fernet key must be a cryptographically secure 32-byte base64-encoded value
- All passwords must be changed from defaults before production use
- `.env` file must never be committed to version control
- Each environment should have unique credentials

**Alternatives considered**:
- **Hardcoded values**: Simple but insecure; credentials in version control is a critical vulnerability
- **Docker secrets**: More secure for production but adds complexity for local development
- **External secrets manager** (HashiCorp Vault, AWS Secrets Manager): Overkill for local dev; 
  good for production but requires additional infrastructure

**Revisit if**: Moving to production deployment where a dedicated secrets management solution 
would be more appropriate.

