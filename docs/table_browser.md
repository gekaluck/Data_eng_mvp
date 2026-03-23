# Table Browser

## Purpose

This project stores Silver and Gold as Iceberg tables in MinIO. The easiest way to
browse them locally, without changing the existing Hadoop catalog setup, is JupyterLab
backed by the same Spark/Iceberg configuration used by the DAG transforms.

## What was added

- `jupyter-lab` service in `docker-compose.yml`
- `spark/lakehouse_browser.py` helper module
- `notebooks/lakehouse_browser.ipynb` starter notebook

The notebook opens:

- `silver` catalog at `s3a://silver/iceberg`
- `gold` catalog at `s3a://gold/iceberg`

Both use the local MinIO endpoint and credentials from `.env`.

## How to use it

1. Rebuild the image because JupyterLab and pandas were added:

   ```bash
   make build
   ```

2. Start the notebook service:

   ```bash
   make lab
   ```

3. Set the token in the real `.env` file, not `.env.example`:

   ```bash
   JUPYTER_TOKEN=your_local_token_here
   ```

   If `JUPYTER_TOKEN` is missing from `.env`, Docker Compose falls back to the
   default token `data-eng-mvp`.

4. Recreate the notebook service after changing `.env`:

   ```bash
   docker compose up -d --force-recreate jupyter-lab
   ```

5. Open JupyterLab:

   - URL: `http://localhost:8888`
   - Token: value of `JUPYTER_TOKEN` from `.env`

6. Open `notebooks/lakehouse_browser.ipynb`

7. Run the cells to:

   - list namespaces
   - list tables in `silver.crypto`
   - preview rows from a table
   - inspect a table schema

## Typical examples

```python
list_tables(spark, "silver", "crypto")
preview_table(spark, "silver", "crypto", "coins", limit=20)
describe_table(spark, "silver", "crypto", "price_snapshots")
```

Later, when Gold exists:

```python
list_tables(spark, "gold", "crypto")
preview_table(spark, "gold", "crypto", "your_gold_table", limit=20)
```

## Why not Trino right now?

Trino is a good table browser/query layer for Iceberg, but the current repo writes
Iceberg tables with the Hadoop catalog. Trino's Iceberg connector supports Hive,
Glue, JDBC, REST, Nessie, and Snowflake catalogs, but not Hadoop catalogs.

Official references:

- [Trino Iceberg connector](https://trino.io/docs/current/connector/iceberg.html)
- [Trino metastore/catalog options](https://trino.io/docs/current/object-storage/metastores.html)

If we later want DBeaver or BI tools over these tables, the right next step is to move
the Iceberg catalog from Hadoop to a Trino-compatible catalog such as JDBC, REST, or
Nessie.
