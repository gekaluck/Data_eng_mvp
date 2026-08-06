# dbt + Trino

This project is initialized for dbt against the local Trino service. On the host,
Trino is exposed at `localhost:8081`. Inside the Airflow containers, dbt connects
to the `trino` service on port `8080`.

## Quick start

1. Make sure Trino is running.
2. Run the connection check:

   ```bash
   dbt debug --project-dir dbt --profiles-dir dbt
   ```

3. Inspect models and sources:

   ```bash
   dbt ls --project-dir dbt --profiles-dir dbt
   ```

4. Run the Gold models for one logical date:

   ```bash
   dbt run --project-dir dbt --profiles-dir dbt --select daily_snapshot mc_rank_change wkly_roll_avg latest_market_snapshot data_availability_daily --vars '{"snapshot_date": "2026-04-01"}'
   ```

The default target writes models to the `gold.crypto_dbt` schema in Trino. Silver
tables are declared as sources under the `silver` catalog.

`latest_market_snapshot` is a small serving model for dashboard overview charts.
`data_availability_daily` generates a complete date spine and classifies each date
as `available`, `partial`, or `missing`; see `docs/superset.md`.

## Rebuilding the incremental models across all history

Omit `snapshot_date` and the incremental models read every Silver date, so a
full-refresh rebuilds them from scratch:

```bash
dbt run --project-dir dbt --profiles-dir dbt --select daily_snapshot mc_rank_change wkly_roll_avg --full-refresh
```

Then rebuild the two table models that read them:

```bash
dbt run --project-dir dbt --profiles-dir dbt --select data_availability_daily latest_market_snapshot
```

This is safe to repeat — everything is derived from Silver and no source data is
touched. It is also how I19 was repaired: `mc_rank_change` and `wkly_roll_avg` had
never been rebuilt and held 9 dates against `daily_snapshot`'s 107.

## dbt docs

Generate docs metadata and the catalog:

```bash
dbt docs generate --project-dir dbt --profiles-dir dbt
```

The Airflow-managed Gold build generates the same metadata into `dbt/artifacts/` only
after every requested date builds successfully. That ignored runtime directory is the
stable input path for the AI metadata adapter; `dbt/target/` remains local dbt scratch
output.

Serve the docs site locally:

```bash
dbt docs serve --project-dir dbt --profiles-dir dbt --port 8082
```

The lineage graph is most useful after the Gold models and tests are in place.

## Airflow integration

The regular Airflow entrypoint is now:

- `coincap_regular_orchestrator`

That orchestrator triggers:

1. Bronze
2. Silver
3. `gold_coincap_assets` and `gold_dbt_coincap_assets` in parallel; the dbt DAG publishes
   fresh artifacts after its model task succeeds
4. `gold_dbt_coincap_tests` after the dbt build succeeds

The dbt Gold build DAG and dbt test DAG also support manual runs through the same
`target_date` parameter used by the other regular DAGs.

## Important note about existing local data

This repo now uses Iceberg JDBC catalogs for Spark and Trino. If you already
created Silver or Gold tables with the previous Hadoop catalog setup, rerun the
Spark jobs to recreate them in the JDBC catalog, or register the existing table
locations into Trino before using dbt.

