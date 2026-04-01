# dbt + Trino

This project is initialized for dbt against the local Trino service exposed by
Docker Compose on `localhost:8081`.

## Quick start

1. Install `dbt-trino` in the environment where you plan to run dbt.
2. Copy `profiles.yml.example` to `profiles.yml`.
3. Run `dbt debug --project-dir . --profiles-dir .`
4. Run `dbt ls --project-dir . --profiles-dir .`

The default target writes models to the `gold.crypto` schema in Trino. Silver
tables are declared as sources under the `silver` catalog.

## Important note about existing local data

This repo now uses Iceberg JDBC catalogs for Spark and Trino. If you already
created Silver or Gold tables with the previous Hadoop catalog setup, rerun the
Spark jobs to recreate them in the JDBC catalog, or register the existing table
locations into Trino before using dbt.
