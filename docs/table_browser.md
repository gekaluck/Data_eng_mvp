# Table Browser

## Purpose

This project stores Silver and Gold as Iceberg tables in MinIO. You now have two
local ways to explore them:

- **JupyterLab + Spark** for notebook-style inspection using the same Spark runtime
  as the DAG transforms
- **Trino** for SQL-first querying, dbt development, and BI-friendly access

Both use the same JDBC-backed Iceberg catalog metadata in Postgres and the same
object storage in MinIO.

## What is available

- `jupyter-lab` service in `docker-compose.yml`
- `trino` service in `docker-compose.yml`
- `spark/lakehouse_browser.py` helper module
- `notebooks/lakehouse_browser.ipynb` starter notebook
- `dbt/` project initialized for Trino

The runtime exposes:

- `silver` catalog at `s3a://silver/iceberg`
- `gold` catalog at `s3a://gold/iceberg`

## JupyterLab workflow

1. Rebuild the Airflow image if dependencies changed:

   ```bash
   make build
   ```

2. Start the notebook service:

   ```bash
   make lab
   ```

3. Open JupyterLab:

   - URL: `http://localhost:8888`
   - Token: value of `JUPYTER_TOKEN` from `.env`

4. Open `notebooks/lakehouse_browser.ipynb`

## Trino workflow

1. Start Trino:

   ```bash
   make trino
   ```

2. Connect with a SQL client to:

   - Host: `localhost`
   - Port: `8081`
   - Catalog: `silver` or `gold`
   - Schema: `crypto`

3. Example queries:

   ```sql
   SHOW TABLES FROM silver.crypto;
   SELECT * FROM silver.crypto.coins LIMIT 20;
   SELECT * FROM gold.crypto.daily_snapshot LIMIT 20;
   ```

## dbt workflow

1. Install `dbt-trino`
2. Copy `dbt/profiles.yml.example` to `dbt/profiles.yml`
3. Validate the connection:

   ```bash
   dbt debug --project-dir dbt --profiles-dir dbt
   ```

4. Inspect the declared Silver sources:

   ```bash
   dbt ls --project-dir dbt --profiles-dir dbt --resource-type source
   ```

## Important migration note

This repo previously used the Iceberg Hadoop catalog. Trino cannot query that
catalog directly, so the runtime now uses Iceberg JDBC catalogs backed by Postgres.

Implication:

- Existing Silver and Gold table data in MinIO is still there
- Existing Hadoop-catalog metadata is not reused automatically
- Rerun the Spark transforms to recreate catalog entries in JDBC, or register the
  existing table locations in Trino before using dbt against old local data

## References

- [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg)
- [Trino metastore/catalog options](https://trino.io/docs/current/object-storage/metastores.html)
- [Iceberg Spark configuration](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [Iceberg JDBC catalog](https://iceberg.apache.org/docs/latest/jdbc/)
