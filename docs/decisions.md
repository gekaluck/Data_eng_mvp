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

**Superseded in part by D021**: this original decision assumed CoinCap's public,
unauthenticated API host.

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

---

## D014 — Bronze Format: Parquet (not JSON)
**Date**: 2026-02-12
**Status**: accepted

**Decision**: Store Bronze layer data as Snappy-compressed Parquet files, not raw JSON.

**Why**:
- Columnar format — efficient for the analytical reads that downstream layers perform
- Snappy compression gives ~5x size reduction with minimal CPU cost
- Self-describing schema embedded in the file (no sidecar schema files needed)
- PyArrow makes conversion trivial (3 lines of code)
- Parquet is the standard interchange format in data lakehouse architectures

**Alternatives considered**:
- **Raw JSON**: Simpler, but larger files, slower reads, no embedded schema.
  Would need separate schema tracking for downstream consumers.
- **CSV**: Even worse than JSON for nested/nullable data. No type info.
- **Avro**: Good for streaming, but Parquet is a better fit for batch analytics.

---

## D015 — Pydantic V2 Data Contracts at Ingestion
**Date**: 2026-02-12
**Status**: accepted

**Decision**: Validate all API responses with Pydantic V2 models before writing to Bronze.

**Why**:
- Catches API schema changes immediately at ingestion, not days later in Silver
- Acts as executable documentation of the expected API shape
- Pydantic V2 is fast (Rust-based core) — negligible overhead for our data volume
- Keeps numeric fields as strings (Bronze = raw), explicit type conversions in Silver

**Alternatives considered**:
- **No validation**: Risk silent data corruption if CoinCap changes their API
- **JSON Schema**: More verbose, harder to maintain, not as natural in Python
- **Pandera / Great Expectations**: Designed for DataFrames, overkill at this stage

---

## D016 — Single-Task Bronze DAG
**Date**: 2026-02-12
**Status**: accepted

**Decision**: Implement the Bronze DAG as a single task (fetch + validate + upload in one function).

**Why**:
- Total payload is <100KB — splitting adds XCom serialization overhead with zero benefit
- Fewer tasks = simpler debugging, fewer failure modes
- If any step fails, the whole task retries cleanly (no partial state)
- Can split later if data volume grows (YAGNI)

**Alternatives considered**:
- **Three separate tasks** (fetch → validate → upload): More "correct" DAG design,
  but the XCom overhead and debugging complexity aren't justified for this data size.

**Revisit if**: Data volume exceeds what XCom handles comfortably (~50MB).

---

## D017 — Pip Packages Inline in docker-compose
**Date**: 2026-02-12
**Status**: accepted

**Superseded by D022**.

**Decision**: Install additional Python packages via `_PIP_ADDITIONAL_REQUIREMENTS` in
docker-compose, not via a custom Dockerfile.

**Why**:
- Simplest approach — no Dockerfile to maintain
- Standard pattern from the official Airflow Docker guide
- `requirements.txt` exists as documentation but is not mounted as a volume
- Keeps the infrastructure configuration in one place

**Alternatives considered**:
- **Custom Dockerfile**: More correct for production, but adds build step complexity
  that isn't needed for local learning
- **Mount requirements.txt + pip install on startup**: Fragile and adds startup latency

**Revisit if**: Package list grows large or we need system-level dependencies.

---

## D018 — Tests Run Inside Docker
**Date**: 2026-02-12
**Status**: accepted

**Decision**: Run pytest inside the Airflow scheduler container, not on the host.

**Why**:
- All dependencies (Airflow, Pydantic, PyArrow) are already installed in the container
- No need for a local virtual environment — reduces "works on my machine" issues
- `./tests` volume is mounted into the container for live editing
- `make test` wraps the command for convenience

**Alternatives considered**:
- **Local venv**: Would require installing Airflow locally (heavy, version mismatch risk)
- **Separate test container**: More isolated, but overkill for a learning project

---

## D019 — Custom Dockerfile for Java (PySpark)
**Date**: 2026-02-17
**Status**: accepted

**Decision**: Add a `Dockerfile` that extends `apache/airflow:2.10.4` with OpenJDK 17 JRE.
The `docker-compose.yml` switches from `image:` to `build: .`.

**Why**:
- PySpark requires a JVM at runtime. The official Airflow image does not include Java.
- Java is a system-level dependency — it must come from the OS package manager, not pip.
- A minimal Dockerfile (5 lines) keeps the change contained and easy to understand.
- We install the JRE (not the full JDK) to keep the image size smaller.

**Alternatives considered**:
- **Pre-built image with Java** (e.g., `bitnami/airflow`): Deviates from the official image
  we've used since M1. Introduces an unknown base with different paths and defaults.
- **Host-level Spark** (run Spark on Windows): Requires Java + Spark installed on the host,
  breaks the "runnable in Docker" contract, and complicates the cross-OS story.
- **Spark in a separate container** (SparkSubmitOperator): Realistic for production, but adds
  another service to configure. Not justified at this project's scale.

**Impact on D017**: D017 said "no custom Dockerfile". This decision supersedes it for
system-level dependencies. D022 later superseded D017 for Python dependency installation too.

---

## D020 — Iceberg Catalog: Hadoop (supersedes D003)
**Date**: 2026-02-17
**Status**: accepted

**Decision**: Use the Iceberg Hadoop catalog instead of JDBC/SQLite (D003).
The Hadoop catalog stores table metadata as JSON files directly in MinIO under
`s3a://silver/iceberg/<namespace>/<table>/metadata/`.

**Why**:
- D003 chose JDBC/SQLite as the "simplest" catalog, but in a Docker environment the
  JDBC catalog adds real friction: a `sqlite-jdbc` JAR must be downloaded and placed on
  the classpath, and the SQLite file needs a stable path across container restarts.
- The Hadoop catalog needs only the `iceberg-spark-runtime` JAR (already required for
  table operations). No extra JARs, no extra config, no extra volume.
- Both catalogs teach the same Iceberg concepts: table creation, partitioning, schema
  evolution, time travel. The catalog choice is an implementation detail, not a
  learning objective.
- The Hadoop catalog stores metadata in the same MinIO bucket as the data, making it
  easy to inspect: browse `s3a://silver/iceberg/` to see exactly what Iceberg writes.

**Tradeoffs vs JDBC**:
- The Hadoop catalog does not support concurrent writes from multiple Spark sessions.
  For a single-machine learning project with sequential daily runs, this doesn't matter.
- If we later add multi-user or concurrent write scenarios, revisit with a REST catalog
  (Nessie, Iceberg REST) or Hive Metastore. Those are better production choices anyway.

**Alternatives considered**:
- **JDBC/SQLite** (D003): Technically correct but harder to wire up. Revisit for M5+.
- **REST catalog (Nessie)**: Best for production concurrent writes, but adds another
  Docker container and configuration surface. Not worth it here.

---

## D021 — CoinCap API Host and Auth Refresh (supersedes part of D001)
**Date**: 2026-03-15
**Status**: accepted

**Decision**: Update the Bronze ingestion configuration to use CoinCap's current API host
(`rest.coincap.io`) and require an API key via environment variable.

**Why**:
- The original public host (`api.coincap.io`) no longer resolves in our environment.
- CoinCap's current official docs and signup flow now point to an authenticated API model.
- Keeping the host and path in environment variables makes future provider changes lower-risk.
- An explicit `COINCAP_API_KEY` requirement fails fast and avoids confusing DNS-style errors.

**Implementation**:
- `COINCAP_API_BASE_URL` defaults to `https://rest.coincap.io/v3`
- `COINCAP_ASSETS_PATH` defaults to `/assets`
- `COINCAP_API_KEY` is passed into the Airflow containers and sent as a bearer token
- Bronze retries transient HTTP failures and raises a clearer infrastructure/config error

**Alternatives considered**:
- **Keep `api.coincap.io` hardcoded**: No longer viable; the host is retired or inaccessible.
- **Switch providers immediately**: Reasonable fallback, but unnecessary while CoinCap still
  offers the required asset endpoint.
- **Hardcode the new host in Python only**: Works short-term, but makes future provider changes
  harder than an env-driven configuration.

---

## D022 — Install Python Dependencies at Image Build Time (supersedes D017)
**Date**: 2026-03-15
**Status**: accepted

**Decision**: Install Python dependencies from `requirements.txt` during the Docker image
build, not via `_PIP_ADDITIONAL_REQUIREMENTS` at container startup.

**Why**:
- We already maintain a custom Dockerfile for Java, so build-time Python deps fit the current model.
- Startup-time installs make container boot slower and less predictable.
- Build-time installation makes test and runtime environments match more closely.
- The current repo already uses `requirements.txt` as the authoritative dependency list.

**Alternatives considered**:
- **Keep `_PIP_ADDITIONAL_REQUIREMENTS`**: Simpler initially, but less reproducible and slower at startup.
- **Install deps manually inside running containers**: Fragile and not reproducible.
- **Separate test image**: More isolation, but unnecessary for a local learning project.

---

## D023 — Iceberg Catalog: JDBC with Postgres (supersedes D020)
**Date**: 2026-04-01
**Status**: accepted

**Decision**: Use Iceberg **JDBC catalogs backed by Postgres**, shared by Spark, Trino,
and dbt (one catalog DB per layer: `iceberg_silver`, `iceberg_gold`). This replaces the
Hadoop catalog chosen in D020.

**Why**:
- Trino cannot read the Hadoop (filesystem) catalog, so once Gold needed to be queried
  from Trino/dbt, D020 stopped working for the multi-engine setup this project now runs.
- A single JDBC catalog in Postgres gives Spark, Trino, and dbt one shared, consistent
  view of table metadata — the whole point of running all three against the same tables.
- Postgres is already in the stack (Airflow metadata DB), so the JDBC catalog adds a
  database and connection config, not a new service. This also sidesteps the earlier
  D003 friction (SQLite JAR + stable file path) that originally motivated D020.

**Tradeoffs**:
- More configuration surface than the Hadoop catalog (catalog DBs, connection settings in
  the Trino catalog `.properties` files and Spark/dbt configs).
- Existing Hadoop-catalog metadata is not reused automatically; Silver/Gold tables must be
  recreated in the JDBC catalog (or their locations registered in Trino). See the
  migration note in [table_browser.md](table_browser.md).

**Alternatives considered**:
- **Stay on the Hadoop catalog (D020)**: Rejected — not queryable from Trino.
- **REST catalog (Nessie / Iceberg REST)**: A stronger production choice for concurrent
  multi-engine writes, but adds another container and config surface not justified here.

---

## D024 — CoinCap Free Tier is Credit-Metered: Build Forward, Don't Backfill Deep (refines D001/D021)
**Date**: 2026-07-11
**Status**: accepted

**Decision**: Treat the daily snapshot as the primary way we accumulate history. Deep
historical backfill on CoinCap is a rare, deliberate, small (~≤5 day) manual operation,
not a routine part of the pipeline.

**Why**:
- CoinCap's free tier is capped at **500 credits/month** and is billed by **data volume**,
  not call count. Empirically, a single 5-day history backfill of our ~20–25 coin universe
  consumed ~498 of 500 credits — effectively the whole month in one run.
- The history endpoints also return a **thinner payload** than the daily `/assets` snapshot
  (only price/marketcap/time; no `vwap24Hr`, no intraday `volumeUsd24Hr`, no rank/symbol/
  supply). Fields we can derive (rank, day-over-day change) we compute; fields we can't are
  left null. So backfilled days are inherently lower-fidelity than forward-collected days.
- The daily DAG makes a single `/assets` call, is full-fidelity, and costs ~1 call/day.
  Left running, it accrues a real, high-quality series for free. In a month you simply *have*
  a month of good data — no backfill economics to fight.

**Consequences**:
- The Gold layer must tolerate coverage gaps (see D025) so a sparse forward-built series
  still produces tables instead of failing.
- The Bronze backfill DAG stays in the repo as a manual tool, but is not auto-scheduled and
  should be run with a small window (≤5 days) and a human deciding it's worth the credits.

**Alternatives considered**:
- **Keep doing deep backfills on CoinCap**: Rejected — unaffordable on the free tier and
  low-fidelity for the historical fields.
- **Switch to CoinGecko for a one-time historical seed**: Viable (more generous free tier,
  has market-cap history) but requires a parallel ingestion path + schema. Deferred until the
  project demonstrably *needs* historical depth; build-forward covers the near term.

**Revisit if**: We need meaningful historical depth soon (then plan a one-time CoinGecko seed),
or we move to a paid CoinCap tier with a larger credit budget.

---

## D025 — Gold Tolerates Coverage Gaps; Bronze Distinguishes Rate-Limit vs Quota 429s
**Date**: 2026-07-11
**Status**: accepted

**Decision**: Make the Gold `daily_snapshot` transform resilient to a missing prior day, and
make the Bronze backfill's 429 handling distinguish a transient per-minute limit from an
exhausted daily/monthly quota.

**Why**:
- **Gold gap-tolerance**: `build_daily_snapshot` computes day-over-day price change via a
  `LAG` over the prior day. It previously dropped every row when the prior day was absent
  (`WHERE prev_price_usd IS NOT NULL`), so an isolated snapshot date produced **zero rows**
  and tripped the count validator (`ValueError`) — failing the whole Gold run. Under the
  build-forward strategy (D024) sparse dates are normal, so this hard requirement is wrong.
  We now keep the day's rows and leave `prev_price_usd` / `price_change_pct` null when the
  prior day is missing. Normal days are unaffected (prev is non-null for every coin). A coin
  with a null change is also excluded from `price_change_rank` (left null via a `WHEN` guard,
  ordering by `desc_nulls_last`) so it never receives an arbitrary rank.
- **Bronze 429 split**: CoinCap returns HTTP 429 for both the transient per-minute burst
  limit (clears on its own) and a hard daily/monthly quota (does not). The retry loop
  previously retried *both* up to 5× with 60s backoffs. Against an exhausted quota that is
  pure waste and — because rejected requests can still count against the quota — actively
  burned credits faster. We now only retry when the body indicates the per-minute limit and
  fail fast otherwise.

**Consequences**:
- Rebuilding Gold for a backfilled date range is currently a manual, idempotent operation
  (per-date, `overwritePartitions` only touches that date's partition). Automating it as a
  range-aware Gold DAG is planned as a separate change.
- Per-call pacing (`COINCAP_MAX_CALLS_PER_MINUTE`, default 5) and retry limits are
  env-configurable for higher tiers.

**Alternatives considered**:
- **Backfill the missing prior day instead of tolerating the gap**: Rejected as the default —
  it spends scarce credits (D024) to satisfy a transform constraint that shouldn't exist.
- **Drop the Gold count validator**: Rejected — zero rows for a date that genuinely has
  Silver data is still a real error worth catching.

---

## D026 — Daily Capture Runs in the Cloud and Lands in S3 (implements D024)
**Date**: 2026-07-28
**Status**: accepted

**Decision**: Split **capture** from **processing**. The single daily CoinCap `/assets`
call runs in GitHub Actions on a cron and writes a raw Parquet snapshot to an **AWS S3**
bucket (`us-east-1`). All heavy transforms (Silver/Gold/dbt) stay local and on-demand.
The capture script reuses the local pipeline's Pydantic contract, Parquet shape, and
object-key layout, so its output is byte-compatible with what the Bronze DAG writes.

**Why**:
- D024 commits us to building history forward from the daily snapshot, which only works
  if the snapshot actually happens every day. Tying that to the laptop being on produced
  a ~3-month hole in Silver — the exact failure the strategy can't absorb.
- Capture and processing have opposite requirements: capture must be **reliable and is
  cheap** (one API call, KBs of output); processing is **heavy but can be batched and
  late**. Only the reliable-and-cheap half needs to leave the laptop.
- GitHub Actions is already present, free at this cadence, and needs no new
  infrastructure to babysit. S3 was chosen over Cloudflare R2 because there's an existing
  AWS account — no new service or vendor relationship (agent rule 5) — and because the
  local→cloud story in `architecture.md` already maps MinIO onto S3.

**Consequences**:
- Bronze now has **two writers**: the local DAG and the cloud capture. They agree by
  construction (shared `schemas.coincap` / `bronze_assets_key`), and both are idempotent
  per date, so a doubly-captured day overwrites rather than duplicates.
- Credentials become a real concern: the workflow's IAM user is write-only into
  `crypto/assets/*`, so a leaked key can't read or delete history.
- Phase 2 (local sync) is now required for the captured data to be *usable* — until it
  lands, snapshots accumulate in S3 but never reach Silver/Gold.
- Capture reliability now depends on GitHub keeping the schedule enabled; it disables
  cron workflows after 60 days of repo inactivity.

**Alternatives considered**:
- **Commit snapshots to a git data branch**: Rejected — data-in-git bloats the repo,
  has no lifecycle policy, and throws away the partition/scan properties the whole
  lakehouse design is about.
- **Cloudflare R2**: Genuinely attractive (free tier, zero egress for the Phase 2 pull),
  but at KB/day the egress saving is theoretical and it means one more account. Reversible
  — swapping back is an endpoint + region change.
- **Run the whole pipeline in the cloud**: Rejected — this is a local-first learning
  project. Moving Spark/Trino/Airflow off the laptop changes what the project *is* and
  costs real money.

**Revisit if**: Capture volume grows enough that egress matters (→ R2), or we want the
snapshot to land somewhere the local stack can read without a sync step.
---

## D030 — Superset serves canonical dbt Gold through read-only Trino

**Date**: 2026-07-18
**Status**: Accepted
**Note**: Filed as a second `D027` by mistake — two PRs in flight picked the same next
number — and renumbered on 2026-07-31. Nothing referenced it under the old number; every
`D027` elsewhere in the repo means the cloud-capture decision below, which is why that one
kept the number. Left here in date order so the log still reads chronologically.

**Decision**:
- Add Apache Superset as an optional local `serving` Compose profile.
- Connect it only to physical relations in `gold.crypto_dbt` through Trino.
- Manage the database, datasets, charts, and dashboard in an idempotent Python bootstrap
  with stable UUIDs.
- Add `latest_market_snapshot` as a dashboard-friendly current-state relation and
  `data_availability_daily` as a complete calendar of available, partial, and missing days.
- Make the rank-change and weekly-average relations incremental so historical dates remain
  queryable, and apply the missing-prior-day tolerance from D025 consistently.
- Restrict the Trino `superset` identity to `SELECT` on the dbt Gold schema.

**Why**:
- Trino remains the single query boundary over Iceberg, and dbt remains the canonical
  place for business logic.
- A complete date spine makes absent data visible instead of silently omitting dates.
- Code-managed assets make the local dashboard reproducible without relying on an
  unversioned Superset metadata volume.

**Security boundary**:
- The file-based Trino policy is a local blast-radius guardrail. Because the MVP still uses
  unauthenticated HTTP, it is not an adversarial identity boundary; TLS and authentication
  are required before exposing the services beyond localhost.

**Alternatives considered**:
- **Streamlit**: flexible, but would require building navigation, filtering, SQL exploration,
  and dashboard state that Superset already provides.
- **Metabase**: simpler onboarding, but less aligned with the SQL-first, code-provisioned
  learning goals for this repository.
- **Superset virtual datasets containing all business SQL**: rejected because it would move
  transformation logic out of dbt and make testing and reuse weaker.


---

## D027 — Cloud Capture Becomes the Only Daily Fetch; Local Airflow Syncs and Processes (completes D026)
**Date**: 2026-07-28
**Status**: accepted

**Decision**: The regular orchestrator no longer calls CoinCap. It starts with a
`sync_captured_snapshots` task that copies captured days from the S3 bucket into Bronze,
then hands the **exact range it just synced** to Silver and both Gold implementations.
`bronze_coincap_assets` remains in the repo as a manual one-off fetch, deliberately
unchained from the daily flow.

**How we got here** (the local → cloud shift, in four steps):

| Step | Decision | Where the daily call lived | What forced the next step |
|------|----------|---------------------------|---------------------------|
| 1 | D001/D016 | Local Airflow, single-task Bronze DAG | Coverage only accrued when the laptop was on |
| 2 | D024 | Same, but now the *primary* history mechanism | Free tier is credit-metered; deep backfill is unaffordable, so forward collection must not miss days |
| 3 | D026 | Added a cloud capture writing to S3 | Two daily writers now existed — a race and a doubled credit spend |
| 4 | **D027** | Cloud only; local Airflow consumes | — |

The through-line: D024 made *never missing a day* the whole strategy, and a laptop
cannot deliver that. Each step after it moves the one part that must be reliable further
from the laptop, while keeping everything expensive and exploratory local.

**Why**:
- **One writer per key.** After D026 both the local DAG and the cloud job wrote
  `crypto/assets/year=.../assets.parquet` daily. Last-writer-wins on identical data is
  harmless right up until it isn't, and nothing recorded which one produced a file
  (Bronze stores no fetch timestamp).
- **Credits.** Two calls a day for one day's data is 60 of 500 monthly credits spent on
  duplication (D024).
- **The sync discovers its own range.** The orchestrator reads `start_date`/`end_date`
  off the sync task's XCom rather than assuming "today", so a laptop that was off for a
  week processes exactly the seven days that arrived — the catch-up is a normal run, not
  a special procedure.
- **Skip when there's nothing new.** An empty sync raises `AirflowSkipException` instead
  of running Spark to rewrite identical partitions.

**Consequences**:
- Silver, dbt Gold, and dbt Gold tests gained `start_date`/`end_date` to match Spark
  Gold (which got it earlier). All four had to move together: if one ignored the range
  it would process a single day of a multi-day catch-up and leave the layers out of
  step. `test_range_capable_dags_accept_start_and_end_date` guards this.
- The local stack now needs read credentials for the capture bucket — a second Airflow
  connection (`capture_s3`) and a **separate read-only IAM key**, not the writer's.
- The daily flow depends on GitHub Actions being healthy. A silently disabled schedule
  now stops the pipeline, not just one snapshot — worth checking when Bronze stops
  advancing.
- Bronze's own idempotency is unchanged: the sync only copies dates Bronze lacks, so
  re-running it is free.
- The orchestrator moved off `@daily` to `30 1 * * *`. The two schedules are now coupled:
  the capture writes at 00:30 UTC, so a midnight orchestrator run would consistently
  process the *previous* day and add a ~24h lag for no reason. If the capture cron ever
  moves, this must move with it.

**Alternatives considered**:
- **Keep both writers for a comparison period**: Rejected. The comparison it would buy
  is weak (both run the same validation on the same endpoint), and the cost is a daily
  race plus doubled credits.
- **Dynamic task mapping over dates in the orchestrator** instead of range params on each
  DAG: Genuinely appealing — per-date tasks visible in the grid, parallel execution, and
  a good Airflow learning target. Rejected for now because Spark Gold already had range
  params, so mapping would have meant two different multi-date idioms in one pipeline.
  Worth revisiting as a deliberate exercise.
- **Have the sync DAG trigger Silver/Gold itself**: Rejected — inverts the orchestrator
  pattern and scatters the pipeline's shape across two files.

**Revisit if**: We want per-date parallelism (→ dynamic task mapping), or the catch-up
window regularly grows large enough that sequential per-date Spark runs get slow.

---

## D028 — Bronze Is Mutable in Practice; Silver May Hold Better Data Than Bronze
**Date**: 2026-07-29
**Status**: accepted

**Decision**: Treat the Bronze `crypto/assets/` objects for 2026-07-22..2026-07-28 as
**less trustworthy than Silver**, and do not rebuild Silver from Bronze for that window.
Record that `architecture.md`'s claim that Bronze is an "immutable landing zone and source
of truth for reprocessing" was not true of the local fetch DAG.

**What we found**: For 07-22..07-28, Bronze and Silver disagree. Bronze holds one
identical Bitcoin price (63799.552250) on 07-22, 07-23, 07-24, 07-25 **and** 07-27, and a
second identical value (63708.900000) on 07-26 and 07-28. Silver holds distinct, plausible
per-day values for the same dates. No history backfill covers this window (anchors are
03-15, 04-01, 07-09), so Silver was not repaired from the history endpoint — it simply
still holds what Bronze contained *at the time Silver ran*, and Bronze was overwritten
afterwards.

**Why it happened**: two properties of the old local fetch DAG combined.
1. It fetched **live** `/assets` but named the object from the run's `logical_date`.
2. It uploaded with `load_bytes(..., replace=True)`.

So any late or repeated run for a past logical date overwrote that date's Bronze object
with whatever the market looked like *at run time*. When the machine came back after being
off, several catch-up runs fired within seconds of each other — `scheduled__2026-07-22`
and `scheduled__2026-07-23` both started at 2026-07-24T02:08, two seconds apart — and each
stored the same response under a different date. Manual triggers and cleared runs do the
same thing; the resulting data is indistinguishable.

**Consequences**:
- **Reprocessing is not safe for that window.** Re-running Silver from Bronze for
  07-22..07-28 would replace good values with the duplicated snapshot and reintroduce
  false zero-change days. Any future "rebuild everything from Bronze" needs to exclude it.
- Bronze is only trustworthy for dates whose object was written by a run that executed on
  the day it was labelling. We have no way to verify that per-object after the fact —
  Bronze stores no fetch timestamp (see the gap noted in D026's consequences).
- One duplicate pair did reach Silver and Gold: 07-22 and 07-23 share 65060.9, producing a
  0.00% change for all 20 coins on 07-23. `daily_snapshot_no_duplicate_fetch_dates` now
  fails on exactly this.

**Why the new design closes it**: the cloud capture resolves its date from the wall clock
at fetch time, never from a logical date, so it can only ever write *today's* object with
today's data — a late or retried run is still correct. The sync copies only dates Bronze
lacks, and `overwrite` is opt-in. Neither can stamp an old date with new prices.

**Alternatives considered**:
- **Rebuild Silver from Bronze for consistency**: Rejected — it would consistently make the
  data worse. Silver is the better record here.
- **Backfill 07-22..07-28 from `/assets/{id}/history` to settle it authoritatively**:
  ~700 credits against a 500/month cap (D024). Not worth it; Silver's values are already
  plausible and distinct.

**Update (2026-07-30)**: the *duplicated* dates are a different case from the merely
suspect ones, and are worth repairing individually. A single-date backfill costs ~50 calls
and merges over the bad row in place (`WHEN MATCHED THEN UPDATE SET *`), so the choice is
per-date rather than all-or-nothing. Both duplicated dates — 07-19 (I17) and 07-23 — have
been repaired this way. Measured cost: ~50 calls and ~20 minutes each, and two in one day
did not exhaust the monthly quota, so "~100 credits per day of data" reads as an upper
bound. The unit of spend is the date.

The rest of the 07-22..07-28 window stays exactly as D028 describes — those dates are
*suspect* but not provably wrong, and there is no signature to repair against. Do not
rebuild Silver from Bronze there.

**Revisit if**: We add a fetch timestamp to Bronze (which would make this diagnosable
rather than inferred), or we need a provable-provenance rebuild.

---

## D029 — Silence Is a Failure Mode: Assert Coverage, Time Out Waits, Fail on a Quiet Upstream
**Date**: 2026-07-30
**Status**: accepted

**Decision**: Treat "nothing happened" as a condition the pipeline must actively rule out,
not a state it may rest in. Concretely, four rules:

1. **No unbounded wait.** Every `TriggerDagRunOperator` with `wait_for_completion=True`
   carries an `execution_timeout`, and the orchestrator refuses to trigger a DAG that is
   paused or unknown to Airflow.
2. **Coverage is asserted, not assumed.** dbt tests fail when a Silver date is missing from
   either Gold implementation, when the two Gold implementations disagree on row counts per
   date, or when a null day-over-day change coexists with a Silver row for the previous day.
3. **A quiet upstream must be distinguishable from a dead one.** The sync fails when the
   newest captured date is older than `CAPTURE_MAX_AGE_DAYS` (default 2), and says whether
   the bucket is empty or merely stale.
4. **Metered operations do not retry.** The credit-spending backfill task runs with
   `retries: 0`, and a coin CoinCap has no history for is skipped rather than fatal.

**Why**: Every incident that cost real time in this project was quiet rather than loud. I9
hid an 8-day Gold outage behind two green layers and 30 queued runs. I12 left a permanent
null that no test looked for. I13's missing Silver date survived a full Gold rebuild. I1's
three-month hole accrued because a skip and a death look identical from inside the sync.
None of these were logic bugs — every individual component did what it was told. What was
missing was any statement of what *should* be true across components.

**Consequences**:
- The dbt test DAG now depends on the Spark Gold branch as well as the dbt one, because the
  cross-implementation test reads both. Left parallel, it would fail on dates Spark simply
  had not built yet — a test that races its own subject.
- Whole-history assertions get slower as coverage grows. At ~107 dates this is under a
  second; if it ever matters, they can take the date range the run is processing.
- `CAPTURE_MAX_AGE_DAYS=0` disables the freshness check, the escape hatch for deliberately
  re-syncing an old date long after the fact.

**Alternatives considered**:
- **Alert instead of fail**: Rejected. There is no alerting channel in a local-first stack,
  and a warning in a log nobody reads is what these incidents already looked like.
- **Assert coverage in Python rather than dbt tests**: Rejected — the assertions are about
  data, the house style for data assertions is a singular dbt test that returns offending
  rows, and returning the rows makes a failure self-diagnosing.

---

## D031 — The Serving Dashboard Separates "What the Data Says" from "Whether to Believe It"
**Date**: 2026-07-31
**Status**: accepted

**Decision**: The Superset dashboard becomes two tabs on one dashboard — **Market** and
**Pipeline Health** — with three supporting rules:

1. **Observability KPIs are recent and denominated.** The all-time "missing days" count is
   retired in favour of days since the last snapshot, coverage over the last 30 completed
   days, and the current unbroken streak. All three are scoped to *completed* days, because
   the calendar spine includes today, which is legitimately empty until the 01:30 UTC
   capture lands.
2. **Coverage is shown on a time axis, not as a proportion.** A per-day status strip and a
   100%-stacked monthly mix replace the all-time pie, which could only say how many days
   were missing, never which ones or whether the trend was improving.
3. **Availability reports field completeness alongside row counts**, as information rather
   than as a status input. `volume_coverage_pct` and `vwap_coverage_pct` are new columns on
   `data_availability_daily`; `availability_status` is deliberately unchanged.

**Why**:
- The single page mixed two readers. Market charts had no filters at all, and the
  observability charts reported all-time totals that could never improve no matter how well
  the pipeline ran — a metric that cannot move is not an instrument.
- Row-count availability overstates trust. 77 of 107 `available` days carry no volume or
  VWAP at all (28% field coverage overall), because backfilled days structurally lack those
  fields. `weekly_roll_avg_volume` is null on most of the history, and nothing said so.
- Field completeness was left out of `availability_status` on purpose: folding it in would
  turn ~72% of history `partial` and destroy the meaning of the status that was just
  repaired in I19. Row-count availability answers "did the pipeline run"; field coverage
  answers "how rich is the day". They are different questions and get different columns.

**Consequences**:
- `data_availability_daily` reads `volume_usd_24hr` and `vwap_24hr` from Silver, so it now
  depends on those columns existing.
- Native filters (date range, symbol, market-cap rank) are scoped: the KPI tiles are
  excluded from the time filter, since "days since last snapshot" is a statement about now
  and windowing it would make it lie.

**Alternatives considered**:
- **Two separate dashboards**: cleaner separation, but two URLs and two filter sets to keep
  in sync for one reader.
- **A calendar heatmap** for coverage: the natural visual, but it is a legacy plugin whose
  availability varies by Superset build. A daily stacked bar on a time axis reads almost the
  same and uses a viz type this stack certainly supports.
- **Folding field coverage into `availability_status`**: rejected above.

---

## D032 — The Coverage Gap Stays; We Do Not Buy History Back
**Date**: 2026-07-31
**Status**: accepted

**Decision**: Leave the two coverage gaps — 87 days (2026-04-08 → 07-03) and 4 days
(2026-03-15 → 03-18) — permanently unfilled. Do not spend CoinCap credits, and do not add a
second price source, to make the series contiguous. Treat sparse coverage as a documented
property of the dataset rather than a defect awaiting repair.

**Why**:

- **Cost.** At D024's measured rate of ~100 credits per day of data against a 500/month free
  tier, 87 days is ~8,700 credits — roughly 17 months of quota. This is not a free-tier
  operation, and it is not worth a paid tier for a learning project.
- **The filled data would be mostly empty.** Measured on the two days actually repaired this
  way (I17): a history-backfilled date carries `price_usd` and `market_cap_usd` and nothing
  else. `volume_usd_24hr`, `vwap_24hr` and `change_percent_24hr` are all **entirely null** —
  0 of 25 rows, not sparse. Backfilling would place an 87-day block in the middle of the
  series where 3 of 5 measure columns are absent, so every volume or VWAP analysis would
  acquire a large hole positioned exactly over the newest, most expensive data.
- **The sparseness is the test bed.** The AI-agent layer (`ai-agent-architecture.md`, R3) is
  built around a confidence gate that refuses rather than guesses. Sparse coverage with
  honest nulls — 32% of 14-day deltas and 55% of 30-day deltas are null — is precisely the
  condition that gate exists to handle. A dense series would remove the most valuable thing
  to evaluate it against.
- **The gap is also the better story.** It records something true: a side project's coverage
  is a record of the author's attention. The architecture had no opinion about that until it
  was too late, which is the point of I1 and of D026/D027.

**Consequences**:
- Any question spanning 2026-04 → 07 returns a partial answer, and consumers must handle
  that. `data_availability_daily` is the table that makes it visible; the Gold `schema.yml`
  descriptions state it for every affected column.
- Rolling and lookback metrics stay null across the gap boundary by design (D025). This is
  not a bug to be reported again.
- If a future need genuinely requires dense history, revisit with a *specific* requirement
  (an eval set, a named analysis) rather than filling it speculatively.

**Alternatives considered**:
- **Backfill the 4-day March gap only** (~400 credits): rejected. Tidiness, not value —
  nothing queries that far back, and the filled days would still be missing 3 of 5 columns.
- **Backfill from a free exchange API** (Binance/Kraken public OHLCV): technically viable and
  unmetered, but the prices are one exchange's rather than CoinCap's cross-exchange
  aggregate, and the coin universe differs. That is a provenance change requiring its own
  column and its own decision, not a quiet fill.
- **Pay for a CoinCap tier**: rejected for a learning project whose stated goal is the
  infrastructure, not research-grade data.

**Revisit if**: The agent eval demonstrably needs a contiguous multi-month window, or the
project acquires a purpose that depends on continuous history.

---

## D033 — Bronze Records Its Own Provenance (closes H5, hardens D028)
**Date**: 2026-07-31
**Status**: accepted

**Decision**: Every Bronze `/assets` snapshot now carries two columns describing where it
came from — `api_timestamp_ms` (CoinCap's own response timestamp, validated all along and
previously discarded) and `fetched_at_utc` (our wall clock at fetch time) — written by a
single shared builder, `dags/utils/bronze_snapshot.build_snapshot_parquet`, that both
writers call. Nothing downstream consumes them: Silver selects its columns by name and is
unchanged. Detection lives in an operator script, `scripts/audit_bronze_provenance.py`.

**Why**:

- **The defect was real but undetectable.** I10 and I17 are the same failure — a live
  `/assets` response stored under a past date's label — and both were found by noticing that
  two dates shared a price to the last decimal, not by any check. Bronze recorded nothing
  about *when* an object was fetched, so the evidence had to be reconstructed from S3
  `LastModified` metadata, which any copy or re-upload destroys. A fetch timestamp inside
  the object survives the copy that the capture sync performs (D027).
- **A per-row column, not file metadata.** Both values are constant across a snapshot's rows,
  which argues for Parquet key-value metadata. A repeated column costs almost nothing after
  compression, is visible to every reader including Trino and Spark without special handling,
  and survives a rewrite that a metadata key would not.
- **One builder, not two.** The premise of D026/D027 is that the cloud capture and the local
  DAG produce interchangeable objects, and until now that was maintained by two code paths
  being kept similar by hand. Extracting the builder makes the compatibility structural: the
  schema is declared once, explicitly, and a change to it cannot land in only one writer.
- **Detection stays out of the daily path.** 37 of Bronze's dates predate this change and
  carry no provenance at all, so a test asserting "every date is auditable" would fail
  permanently on history that cannot be fixed. The audit script reports coverage honestly —
  how many dates it *could* check — and only fails on what it can prove.

**Consequences**:
- Bronze objects written from now on are one column-pair wider. Silver ignores them; the
  Silver reader has a test (`test_transform_reads_a_bronze_object_with_provenance_columns`)
  that builds its input through the real writer, so the two cannot drift apart unnoticed.
- The audit can only ever cover dates written after this change. The 37 existing ones stay
  unauditable, and I10/I17 remain historically inferred rather than confirmed.
- `scripts/` is now bind-mounted read-only into the Airflow containers so the audit can run
  where MinIO is reachable.
- A future Silver column carrying `fetched_at_utc` forward would let a dbt test assert
  freshness in SQL. Deliberately not done now: it is an Iceberg schema change on a table
  Gold reads, and it would be null for every existing row.

**Alternatives considered**:
- **Parquet file-level key-value metadata**: cheaper, but invisible to SQL readers and easy
  to lose on any rewrite.
- **Trusting S3 `LastModified`**: already available and already proved insufficient — the
  sync copies objects, which resets it. It was usable for I17 only because those objects had
  never been copied.
- **A dbt/Airflow test failing on stale provenance every day**: rejected for now. With 37
  unauditable dates it would either fail permanently or need a hardcoded cutoff date, and
  the signature it looks for (I10) is already prevented structurally by D027.

**Revisit if**: The provenance columns reach Silver, or the audit finds a real instance —
either would justify promoting it from a script into the daily test path.

---

## D034 — The Orchestrator Runs at 05:30 UTC (refines D027, supersedes its schedule)
**Date**: 2026-07-31
**Status**: accepted

**Decision**: Move `coincap_regular_orchestrator` from `30 1 * * *` to `30 5 * * *`, five
hours after the capture cron rather than one. State both crons together in the DAG file and
assert the gap in a test rather than leaving the coupling implicit in two comments.

**Why**:

- **The old buffer was sized to a number that changed.** D027 chose an hour against a
  documented drift of "5–30 minutes". Measured on 2026-07-30 and 07-31, the 00:30 UTC
  capture completed at 03:40 and 03:59 UTC. The orchestrator was running before the capture
  it was meant to follow, so every layer sat a day behind with nothing failing (I20).
- **Five hours, not more.** The constraint at the other end is the UTC day boundary: the
  capture resolves its partition date from the wall clock at fetch time (D027), so a sync
  after 23:59 UTC would be reaching for a label that no longer means "today". Five hours
  covers observed drift with margin and leaves most of the day spare.
- **Lateness is one-directional.** GitHub's scheduler runs late, never early, so headroom
  only ever needs to grow in one direction — which is exactly why an hour felt safe and
  wasn't.
- **The coupling belongs in code.** Both crons now sit in the orchestrator DAG as named
  constants, and `test_orchestrator_runs_well_after_the_capture_cron` fails below a
  four-hour gap. The pairing has now broken twice (I14, I20) while each schedule was
  defensible in isolation; a comment in two files was not enough.

**Consequences**:
- Fresh data lands in Gold ~4 hours later in the day than before. Nothing consumes it on a
  tighter clock than "sometime today".
- A drift beyond five hours reintroduces the one-day lag. It stays *safe* — H3's freshness
  check tolerates two days, and the next run catches up — but the test's four-hour floor is
  a tripwire on the assumption, not a guarantee about GitHub.
- If the capture cron moves, the orchestrator must move with it; the test now enforces that
  rather than trusting the reader.

**Alternatives considered**:
- **Have the capture workflow trigger the sync directly** (dispatch a webhook, or write a
  sentinel object the sync waits on): removes the guessed offset entirely and is the
  structurally correct answer. Rejected for now because it needs an inbound path to a
  laptop-hosted Airflow, which the whole D026/D027 split exists to avoid. Revisit if the
  local stack ever gains a stable public endpoint.
- **Run the orchestrator twice a day**: would mask drift rather than fix it, and doubles
  Spark work on a laptop for no new data.
- **Poll for the day's object with a sensor before syncing**: the sync already skips
  harmlessly when there is nothing new; a sensor would convert a cheap skip into a task
  holding a worker slot for hours.

**Revisit if**: Observed drift approaches five hours, or the capture gains a way to signal
completion.
