# M2 — Bronze Layer Setup & Reference

## What Was Built

A daily Airflow DAG (`bronze_coincap_assets`) that:
1. Fetches top 20 crypto assets from the CoinCap API
2. Validates the response with Pydantic V2 models
3. Converts to Snappy-compressed Parquet
4. Uploads to MinIO at `bronze/crypto/assets/year=YYYY/month=MM/day=DD/assets.parquet`

Also added: Makefile, requirements.txt, pytest test suite.

---

## Files Added/Modified

| File | Purpose |
|------|---------|
| `requirements.txt` | Central dependency list |
| `docker-compose.yml` | Added pip packages + tests volume mount |
| `dags/schemas/coincap.py` | Pydantic V2 models for CoinCap API |
| `dags/bronze_coincap.py` | Bronze ingestion DAG |
| `tests/test_schemas.py` | Pydantic model unit tests |
| `tests/test_dag_integrity.py` | DAG import + structure tests |
| `tests/conftest.py` | Adds dags/ to sys.path |
| `Makefile` | Convenience commands |

---

## How to Verify

### 1. Restart with new dependencies
```bash
make restart
# Wait ~60s for pip install to finish
```

### 2. Check for DAG import errors
```bash
make logs-scheduler
# Look for "bronze_coincap_assets" in the parsed DAGs list
```

### 3. Verify in Airflow UI
- Open http://localhost:8080
- `bronze_coincap_assets` should appear in the DAG list
- Trigger it manually (play button)
- Task should turn green

### 4. Verify in MinIO
- Open http://localhost:9001
- Navigate to `bronze` bucket
- Check path: `crypto/assets/year=.../month=.../day=.../assets.parquet`

### 5. Run tests
```bash
make test           # all tests
make test-schemas   # schema tests only (faster)
```

---

## Key Design Decisions

- **Parquet over JSON** (D014): Columnar, compressed, self-describing
- **Pydantic validation** (D015): Catches API changes at ingestion, not downstream
- **Single-task DAG** (D016): Payload <100KB, splitting adds overhead without benefit
- **Tests in Docker** (D018): No local venv needed, all deps already available

See `docs/decisions.md` for full rationale.

---

## What to Try Next (M3)

- PySpark job reading Bronze Parquet and writing to Silver Iceberg tables
- Entity modeling: separate coins (slow-changing) from price snapshots (time-series)
- Airflow DAG orchestrating Spark job with bronze → silver dependency
