"""Idempotently provision the Crypto Lakehouse Superset assets.

The objects use stable UUIDs and are updated in place. This keeps a clean checkout
reproducible without committing Superset's mutable metadata database.
"""

from __future__ import annotations

import json
import os
from uuid import UUID

from superset.app import create_app


DATABASE_UUID = UUID("11111111-1111-4111-8111-111111111111")
DASHBOARD_UUID = UUID("44444444-4444-4444-8444-444444444444")

DATASET_UUIDS = {
    "daily_snapshot": UUID("22222222-2222-4222-8222-222222222221"),
    "mc_rank_change": UUID("22222222-2222-4222-8222-222222222222"),
    "weekly_roll_avg": UUID("22222222-2222-4222-8222-222222222223"),
    "data_availability_daily": UUID("22222222-2222-4222-8222-222222222224"),
    "latest_market_snapshot": UUID("22222222-2222-4222-8222-222222222225"),
}

CHART_UUIDS = {
    "market": UUID("33333333-3333-4333-8333-333333333331"),
    "movers": UUID("33333333-3333-4333-8333-333333333332"),
    "price": UUID("33333333-3333-4333-8333-333333333333"),
    "rank": UUID("33333333-3333-4333-8333-333333333334"),
    "availability": UUID("33333333-3333-4333-8333-333333333335"),
    "availability_mix": UUID("33333333-3333-4333-8333-333333333336"),
    "missing_days": UUID("33333333-3333-4333-8333-333333333337"),
}


def sql_metric(label: str, expression: str) -> dict[str, object]:
    """Return the portable Superset adhoc-SQL metric shape."""
    return {
        "aggregate": None,
        "column": None,
        "expressionType": "SQL",
        "hasCustomLabel": True,
        "label": label,
        "optionName": f"metric_{label.lower().replace(' ', '_')}",
        "sqlExpression": expression,
    }


def ensure_database(session, Database, admin_user):
    database = session.query(Database).filter(Database.uuid == DATABASE_UUID).one_or_none()
    if database is None:
        database = Database(uuid=DATABASE_UUID)
    database.database_name = "Crypto Gold (Trino)"
    database.verbose_name = "Crypto Gold (Trino)"
    database.sqlalchemy_uri = "trino://superset@trino:8080/gold"
    database.expose_in_sqllab = True
    database.allow_run_async = False
    database.allow_file_upload = False
    database.allow_ctas = False
    database.allow_cvas = False
    database.allow_dml = False
    database.created_by = database.created_by or admin_user
    database.changed_by = admin_user
    database.extra = json.dumps(
        {
            "allows_virtual_table_explore": True,
            "metadata_cache_timeout": {},
            "schemas_allowed_for_file_upload": [],
        }
    )
    session.add(database)
    session.commit()
    return database


DATASET_DESCRIPTIONS = {
    "daily_snapshot": "Daily coin prices, market metrics, ranks, and gap-tolerant day-over-day change.",
    "mc_rank_change": "Market-cap ranks and 14/30-day rank and price movements.",
    "weekly_roll_avg": "Daily prices and volumes with seven-observation rolling averages.",
    "data_availability_daily": "Calendar-complete local serving availability, including entirely missing dates.",
    "latest_market_snapshot": "Latest available daily market snapshot for dashboard overview charts.",
}

COLUMN_DESCRIPTIONS = {
    "snapshot_date": "Logical UTC snapshot date.",
    "availability_status": "Overall status: available, partial, or missing.",
    "availability_reason": "First deterministic cause of a partial or missing date.",
    "silver_row_count": "Silver price snapshot rows available for the date.",
    "daily_snapshot_row_count": "dbt daily snapshot rows available for the date.",
    "rank_change_row_count": "dbt rank-change rows available for the date.",
    "weekly_average_row_count": "dbt weekly-average rows available for the date.",
}


def ensure_dataset(session, SqlaTable, database, table_name: str):
    dataset_uuid = DATASET_UUIDS[table_name]
    dataset = session.query(SqlaTable).filter(SqlaTable.uuid == dataset_uuid).one_or_none()
    if dataset is None:
        dataset = SqlaTable(uuid=dataset_uuid)
    dataset.table_name = table_name
    dataset.schema = "crypto_dbt"
    dataset.catalog = "gold"
    dataset.database = database
    dataset.description = DATASET_DESCRIPTIONS[table_name]
    dataset.is_sqllab_view = False
    dataset.main_dttm_col = "snapshot_date"
    session.add(dataset)
    session.commit()

    result = dataset.fetch_metadata()
    for column in dataset.columns:
        if column.column_name in COLUMN_DESCRIPTIONS:
            column.description = COLUMN_DESCRIPTIONS[column.column_name]
    session.add(dataset)
    session.commit()
    print(
        f"Dataset {table_name}: added={result.added}, "
        f"modified={result.modified}, removed={result.removed}"
    )
    return dataset


def ensure_chart(session, Slice, admin_user, *, key: str, name: str, dataset, viz_type: str, params: dict):
    chart_uuid = CHART_UUIDS[key]
    chart = session.query(Slice).filter(Slice.uuid == chart_uuid).one_or_none()
    if chart is None:
        chart = Slice(uuid=chart_uuid)
    params = {
        "adhoc_filters": [],
        "datasource": f"{dataset.id}__table",
        "time_range": "No filter",
        "viz_type": viz_type,
        **params,
    }
    chart.slice_name = name
    chart.datasource_id = dataset.id
    chart.datasource_type = "table"
    chart.viz_type = viz_type
    chart.params = json.dumps(params, sort_keys=True)
    chart.description = f"Managed serving-layer chart: {name}."
    chart.created_by = chart.created_by or admin_user
    chart.changed_by = admin_user
    session.add(chart)
    session.commit()
    return chart


def chart_definitions(datasets: dict[str, object]) -> list[dict[str, object]]:
    daily = datasets["daily_snapshot"]
    latest = datasets["latest_market_snapshot"]
    rank = datasets["mc_rank_change"]
    availability = datasets["data_availability_daily"]
    return [
        {
            "key": "market",
            "name": "Latest Market Snapshot",
            "dataset": latest,
            "viz_type": "table",
            "params": {
                "all_columns": [
                    "snapshot_date", "coin_rank", "symbol", "name", "price_usd",
                    "price_change_pct", "market_cap_usd", "volume_usd_24hr",
                ],
                "include_search": True,
                "order_by_cols": ['["coin_rank", true]'],
                "page_length": 25,
                "row_limit": 100,
                "table_filter": True,
            },
        },
        {
            "key": "movers",
            "name": "Latest Daily Movers",
            "dataset": latest,
            "viz_type": "echarts_timeseries_bar",
            "params": {
                "groupby": [],
                "metrics": [sql_metric("Price change %", "max(price_change_pct)")],
                "orientation": "vertical",
                "row_limit": 25,
                "show_legend": False,
                "sort_series_type": "sum",
                "x_axis": "symbol",
                "y_axis_format": ".2f",
            },
        },
        {
            "key": "price",
            "name": "Daily Price Change - Last 30 Days (%)",
            "dataset": daily,
            "viz_type": "echarts_timeseries_line",
            "params": {
                "granularity_sqla": "snapshot_date",
                "groupby": ["symbol"],
                "metrics": [sql_metric("Daily price change (%)", "avg(price_change_pct)")],
                "row_limit": 10_000,
                "series_limit": 12,
                "series_limit_metric": sql_metric(
                    "Average market cap (USD)",
                    "avg(market_cap_usd)",
                ),
                "show_legend": True,
                "time_range": "Last 30 days",
                "time_grain_sqla": "P1D",
                "x_axis_time_format": "smart_date",
                "y_axis_format": ".2f",
            },
        },
        {
            "key": "rank",
            "name": "Market-Cap Rank Changes",
            "dataset": rank,
            "viz_type": "table",
            "params": {
                "all_columns": [
                    "snapshot_date", "mc_rank", "symbol", "name",
                    "mc_rank_diff_14d", "mc_rank_diff_30d",
                    "price_diff_14d_pct", "price_diff_30d_pct",
                ],
                "include_search": True,
                "order_by_cols": ['["snapshot_date", false]', '["mc_rank", true]'],
                "page_length": 25,
                "row_limit": 1_000,
                "table_filter": True,
            },
        },
        {
            "key": "availability",
            "name": "Daily Data Availability",
            "dataset": availability,
            "viz_type": "table",
            "params": {
                "all_columns": [
                    "snapshot_date", "availability_status", "availability_reason",
                    "silver_row_count", "daily_snapshot_row_count",
                    "rank_change_row_count", "weekly_average_row_count",
                    "source_coverage_pct", "daily_snapshot_coverage_pct",
                ],
                "include_search": True,
                "order_by_cols": ['["snapshot_date", false]'],
                "page_length": 50,
                "row_limit": 1_000,
                "table_filter": True,
            },
        },
        {
            "key": "availability_mix",
            "name": "Availability Status Mix",
            "dataset": availability,
            "viz_type": "pie",
            "params": {
                "donut": True,
                "groupby": ["availability_status"],
                "innerRadius": 45,
                "label_type": "key_percent",
                "metric": sql_metric("Days", "count(*)"),
                "row_limit": 10,
                "show_labels": True,
                "show_legend": True,
            },
        },
        {
            "key": "missing_days",
            "name": "Missing Days",
            "dataset": availability,
            "viz_type": "big_number_total",
            "params": {
                "metric": sql_metric(
                    "Missing days",
                    "sum(case when availability_status = 'missing' then 1 else 0 end)",
                ),
                "subheader": "Calendar days with no local Silver snapshot",
                "y_axis_format": ",d",
            },
        },
    ]


def dashboard_positions(charts: list[object]) -> dict[str, object]:
    positions: dict[str, object] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {
            "id": "GRID_ID",
            "type": "GRID",
            "children": [],
            "parents": ["ROOT_ID"],
        },
        "HEADER_ID": {
            "id": "HEADER_ID",
            "type": "HEADER",
            "meta": {"text": "Crypto Lakehouse — Gold Analytics"},
        },
    }
    row_groups = [charts[0:2], charts[2:4], charts[4:7]]
    for row_index, row_charts in enumerate(row_groups, start=1):
        row_id = f"ROW-{row_index}"
        positions["GRID_ID"]["children"].append(row_id)
        positions[row_id] = {
            "id": row_id,
            "type": "ROW",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        width = 12 // len(row_charts)
        for chart in row_charts:
            chart_id = f"CHART-{chart.id}"
            positions[row_id]["children"].append(chart_id)
            positions[chart_id] = {
                "id": chart_id,
                "type": "CHART",
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {
                    "chartId": chart.id,
                    "height": 50,
                    "sliceName": chart.slice_name,
                    "uuid": str(chart.uuid),
                    "width": width,
                },
            }
    return positions


def main() -> None:
    app = create_app()
    with app.app_context():
        from superset import security_manager
        from superset.connectors.sqla.models import SqlaTable
        from superset.extensions import db
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        admin_user = security_manager.find_user(
            username=os.environ["SUPERSET_ADMIN_USERNAME"]
        )
        if admin_user is None:
            raise RuntimeError("Superset admin user was not found after initialization")

        database = ensure_database(db.session, Database, admin_user)
        datasets = {
            name: ensure_dataset(db.session, SqlaTable, database, name)
            for name in DATASET_UUIDS
        }
        charts = [
            ensure_chart(db.session, Slice, admin_user, **definition)
            for definition in chart_definitions(datasets)
        ]

        dashboard = (
            db.session.query(Dashboard)
            .filter(Dashboard.uuid == DASHBOARD_UUID)
            .one_or_none()
        )
        if dashboard is None:
            dashboard = Dashboard(uuid=DASHBOARD_UUID)
        dashboard.dashboard_title = "Crypto Lakehouse — Gold Analytics"
        dashboard.slug = "crypto-gold-analytics"
        dashboard.description = (
            "Curated Gold-layer market analytics and daily local-data availability."
        )
        dashboard.position_json = json.dumps(dashboard_positions(charts))
        dashboard.json_metadata = json.dumps(
            {
                "color_scheme": "supersetColors",
                "expanded_slices": {},
                "native_filter_configuration": [],
                "refresh_frequency": 0,
                "show_native_filters": True,
            }
        )
        dashboard.published = True
        dashboard.owners = [admin_user]
        dashboard.slices = charts
        dashboard.created_by = dashboard.created_by or admin_user
        dashboard.changed_by = admin_user
        db.session.add(dashboard)
        db.session.commit()

        print(
            f"Provisioned {len(datasets)} datasets, {len(charts)} charts, "
            f"and dashboard /superset/dashboard/{dashboard.slug}/"
        )


if __name__ == "__main__":
    main()
