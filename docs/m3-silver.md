# M3 — Silver Layer Setup & Reference

## What Was Built

A PySpark transformation pipeline that reads daily bronze Parquet data and writes to
two Iceberg tables in the silver layer:

**`silver.crypto.coins`** — coin metadata (slowly-changing)
- Grain: one row per coin
- Updated with MERGE INTO (upsert): rank changes are reflected, new coins are inserted
- Fields: id, symbol, name, rank, supply, max_supply

**`silver.crypto.price_snapshots`** — daily price facts (time-series)
- Grain: one row per (coin_id, snapshot_date)
- Partitioned by `snapshot_date` for efficient date-range queries
- Written with `overwritePartitions()` — reruns for the same date are safe and idempotent
- Fields: coin_id, snapshot_date, price_usd, market_cap_usd, volume_usd_24hr,
  change_percent_24hr, vwap_24hr

The Airflow DAG (`silver_coincap_assets`) waits for the expected Bronze file for the
target date to appear in MinIO before starting the transform.

---

## Files Added/Modified

| File | Purpose |
|------|---------|
| `Dockerfile` | Extends apache/airflow with OpenJDK 17 JRE (PySpark needs a JVM) |
| `docker-compose.yml` | Switched to `build: .`, added CoinCap env passthrough, spark/ volume, JAR cache volume |
| `spark/silver_transform.py` | PySpark job: reads bronze → writes two Iceberg silver tables |
| `dags/silver_coincap.py` | Airflow DAG: Bronze-file sensor + subprocess PySpark call |
| `tests/test_dag_integrity.py` | Added silver DAG integrity tests |
| `requirements.txt` | Added `pyspark==3.5.3` |
| `docs/decisions.md` | Added D019 (custom Dockerfile), D020 (Hadoop catalog) |

---

## How to Verify

### 1. Build the custom image (first time only)
```bash
make build
# or: docker compose build
# Takes 2-3 minutes — installs Java + pyspark
```

### 2. Start services
```bash
make restart
# Use `docker compose up -d --force-recreate` after only changing .env values
```

### 3. Trigger the bronze DAG first
- Open http://localhost:8080
- Make sure `.env` contains a valid `COINCAP_API_KEY`
- Trigger `bronze_coincap_assets` manually
- Optional: set `target_date` (`YYYY-MM-DD`) in the trigger form for a manual backfill-style run
- Wait for it to go green (check MinIO at http://localhost:9001 to confirm Parquet landed)

### 4. Trigger the silver DAG
- Trigger `silver_coincap_assets` manually for the same target date
- Optional: set the same `target_date` (`YYYY-MM-DD`) in the trigger form
- `wait_for_bronze` will turn green when the expected Bronze file exists for that date
- `run_silver_transform` will start Spark — first run downloads ~300MB of JARs, takes a few minutes
- Subsequent runs use the cached JARs (much faster)

### 5. Inspect the output in MinIO
- Open http://localhost:9001
- Navigate to `silver` bucket → `iceberg/` prefix
- You'll see: `crypto/coins/metadata/`, `crypto/price_snapshots/metadata/`, and `data/` folders
- The metadata JSON files are Iceberg's catalog entries (Hadoop catalog style)

### 6. Run tests
```bash
make test       # all tests
make test-dag   # DAG integrity tests only
```

---

## Why Two Tables?

The CoinCap API returns a flat list where each row mixes metadata with time-series facts:

```
id=bitcoin, name=Bitcoin, rank=1, priceUsd=65000, volumeUsd24Hr=30B, ...
```

Two different things are entangled here:
- **What the coin is** (id, name, symbol) — changes rarely or never
- **What the price is right now** (priceUsd, volume, marketCap) — changes every day

Keeping them in one table means repeating "Bitcoin", "BTC", etc. in every daily row.
Separating them means:
- `coins` has one row per coin — clean, minimal, updateable
- `price_snapshots` has one row per (coin, date) — pure facts, no redundant metadata

This is the core idea behind **entity-centric (3NF) modeling**: each table has one job,
one grain, and one clear answer to "what does one row represent?"

---

## Key Design Decisions

- **Hadoop catalog over JDBC/SQLite** (D020): Simpler setup in Docker, same learning value
- **Custom Dockerfile** (D019): Java is a system dep, needs apt-get not pip
- **MERGE INTO for coins**: Shows the upsert pattern — important for slowly-changing dimensions
- **overwritePartitions() for snapshots**: Idempotent daily writes without touching other partitions
- **Date-based manual runs**: `target_date` makes manual Bronze/Silver runs line up with the same partition
- **subprocess for Spark**: Isolates JVM lifecycle; mirrors how SparkSubmitOperator works

---

## What to Try Next (M4)

- PySpark aggregation job over `price_snapshots`: rolling averages, rankings, period comparisons
- Gold Iceberg tables designed around analytical questions ("what were the top 5 coins last week?")
- Full Airflow DAG chain: bronze → silver → gold with proper dependencies
- Window functions: `RANK()`, `LAG()`, rolling 7-day average
