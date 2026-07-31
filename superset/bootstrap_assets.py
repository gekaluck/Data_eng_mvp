"""Idempotently provision the Crypto Lakehouse Superset assets.

The objects use stable UUIDs and are updated in place. This keeps a clean checkout
reproducible without committing Superset's mutable metadata database.

The dashboard has two tabs because it serves two different readers:

* **Market** — what the data says. Filterable by date, symbol, and rank.
* **Pipeline Health** — whether the data can be believed at all. Freshness,
  calendar coverage, and field-level completeness.

Mixing the two on one page made both worse: the market charts had no filters and
the observability charts reported all-time totals that could never improve.
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
    # Market tab
    "market": UUID("33333333-3333-4333-8333-333333333331"),
    "movers": UUID("33333333-3333-4333-8333-333333333332"),
    "price": UUID("33333333-3333-4333-8333-333333333333"),
    "rank": UUID("33333333-3333-4333-8333-333333333334"),
    "rolling": UUID("33333333-3333-4333-8333-33333333333e"),
    # Pipeline Health tab
    "availability": UUID("33333333-3333-4333-8333-333333333335"),
    "freshness_days": UUID("33333333-3333-4333-8333-333333333338"),
    "coverage_30d": UUID("33333333-3333-4333-8333-333333333339"),
    "available_streak": UUID("33333333-3333-4333-8333-33333333333a"),
    "coverage_timeline": UUID("33333333-3333-4333-8333-33333333333b"),
    "coverage_monthly": UUID("33333333-3333-4333-8333-33333333333c"),
    "field_completeness": UUID("33333333-3333-4333-8333-33333333333d"),
}

# Charts this bootstrap used to manage. They are deleted on every run so a
# re-bootstrap leaves no orphans behind in the chart list.
#   - availability_mix: an all-time status pie, dominated by pre-collection history.
#   - missing_days: a big number that counted days before collection started, so it
#     could never improve and was never actionable.
RETIRED_CHART_UUIDS = [
    UUID("33333333-3333-4333-8333-333333333336"),
    UUID("33333333-3333-4333-8333-333333333337"),
]

# Native filter ids are stable so a re-bootstrap does not orphan saved filter state.
FILTER_TIME_ID = "NATIVE_FILTER-crypto-time"
FILTER_SYMBOL_ID = "NATIVE_FILTER-crypto-symbol"
FILTER_RANK_ID = "NATIVE_FILTER-crypto-rank"

USD_PRICE_FORMAT = "$,.4f"
USD_LARGE_FORMAT = "$.3s"
PCT_FORMAT = ".2f"
COUNT_FORMAT = ",d"


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


def number_columns(**formats: str) -> dict[str, dict[str, str]]:
    """Return table `column_config` entries applying a d3 format per column."""
    return {column: {"d3NumberFormat": fmt} for column, fmt in formats.items()}


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
    "volume_coverage_pct": "Share of the date's rows that carry a 24h volume. Backfilled days lack it entirely.",
    "vwap_coverage_pct": "Share of the date's rows that carry a 24h VWAP. Backfilled days lack it entirely.",
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


def delete_retired_charts(session, Slice) -> int:
    removed = 0
    for chart_uuid in RETIRED_CHART_UUIDS:
        chart = session.query(Slice).filter(Slice.uuid == chart_uuid).one_or_none()
        if chart is not None:
            session.delete(chart)
            removed += 1
    session.commit()
    return removed


def market_charts(datasets: dict[str, object]) -> list[dict[str, object]]:
    daily = datasets["daily_snapshot"]
    latest = datasets["latest_market_snapshot"]
    rank = datasets["mc_rank_change"]
    weekly = datasets["weekly_roll_avg"]
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
                "column_config": number_columns(
                    price_usd=USD_PRICE_FORMAT,
                    price_change_pct=PCT_FORMAT,
                    market_cap_usd=USD_LARGE_FORMAT,
                    volume_usd_24hr=USD_LARGE_FORMAT,
                    coin_rank=COUNT_FORMAT,
                ),
                "include_search": True,
                "order_by_cols": ['["coin_rank", true]'],
                "page_length": 25,
                "row_limit": 100,
                "table_filter": True,
            },
        },
        {
            "key": "movers",
            "name": "Latest Daily Movers (%)",
            "dataset": latest,
            "viz_type": "echarts_timeseries_bar",
            # Sorted by the metric, so the biggest gainers and losers sit at the ends
            # instead of being scattered across an alphabetical axis.
            "params": {
                "groupby": [],
                "metrics": [sql_metric("Price change %", "max(price_change_pct)")],
                "orientation": "vertical",
                "row_limit": 25,
                "show_legend": False,
                "sort_series_type": "sum",
                "x_axis": "symbol",
                "x_axis_sort": "Price change %",
                "x_axis_sort_asc": False,
                "y_axis_format": PCT_FORMAT,
            },
        },
        {
            "key": "price",
            "name": "Cumulative Price Change - Last 30 Days (%)",
            "dataset": daily,
            "viz_type": "echarts_timeseries_line",
            # Cumulative rather than daily: the raw daily series was 12 overlapping
            # zero-centred lines. Cumulative separates the series and makes a gap
            # read as a flat segment instead of a spike.
            "params": {
                "granularity_sqla": "snapshot_date",
                "groupby": ["symbol"],
                "metrics": [sql_metric("Daily price change (%)", "avg(price_change_pct)")],
                "rolling_type": "cumsum",
                "row_limit": 10_000,
                "series_limit": 8,
                "series_limit_metric": sql_metric(
                    "Average market cap (USD)",
                    "avg(market_cap_usd)",
                ),
                "show_legend": True,
                "time_range": "Last 30 days",
                "time_grain_sqla": "P1D",
                "x_axis_time_format": "smart_date",
                "y_axis_format": PCT_FORMAT,
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
                "column_config": number_columns(
                    mc_rank=COUNT_FORMAT,
                    mc_rank_diff_14d="+,d",
                    mc_rank_diff_30d="+,d",
                    price_diff_14d_pct=PCT_FORMAT,
                    price_diff_30d_pct=PCT_FORMAT,
                ),
                "include_search": True,
                "order_by_cols": ['["snapshot_date", false]', '["mc_rank", true]'],
                "page_length": 25,
                # Newest date first, then 25 rows: the current standings, not all
                # 2,175 rows of history. Use the date filter to pin an older day.
                "row_limit": 25,
                "table_filter": True,
            },
        },
        {
            "key": "rolling",
            "name": "Price vs 7-Day Rolling Average",
            "dataset": weekly,
            "viz_type": "echarts_timeseries_line",
            # weekly_roll_avg was the one provisioned dataset no chart read. Price
            # against its own trailing average is the question that model exists to
            # answer, and it is not asked anywhere else on the dashboard.
            #
            # One coin at a time by design: absolute USD prices span BTC at ~64,000
            # and USDT at ~1, so overlaying coins on a shared axis would be
            # unreadable. series_limit 1 defaults to the largest by market cap; the
            # Symbol filter swaps which coin is shown.
            #
            # Price only, not volume: wkly_roll_avg_volume is null on most of the
            # history (28% field coverage, see Pipeline Health), so a volume line
            # would be mostly absent and would imply data we do not have.
            #
            # Defaults to 30 days because the full range is mostly the Apr 8 - Jul 9
            # coverage hole (I1), which a line chart draws as a long straight
            # segment between two real observations. Markers are on so the actual
            # observations are visible: across a gap the line is interpolation, and
            # the absence of markers is what says so. Widen with the date filter.
            "params": {
                "granularity_sqla": "snapshot_date",
                "groupby": ["symbol"],
                "markerEnabled": True,
                "markerSize": 5,
                "metrics": [
                    sql_metric("Price (USD)", "avg(price_usd)"),
                    sql_metric("7-day rolling average (USD)", "avg(wkly_roll_avg_price)"),
                ],
                "row_limit": 10_000,
                "series_limit": 1,
                "series_limit_metric": sql_metric(
                    "Average market cap (USD)",
                    "avg(market_cap_usd)",
                ),
                "show_legend": True,
                "time_grain_sqla": "P1D",
                "time_range": "Last 30 days",
                # Scale to the data instead of anchoring at zero. A price series
                # oscillating around $64,000 was drawn as two flat lines in the top
                # tenth of a chart whose axis started at $0 — the gap between price
                # and its own average, the whole point here, was invisible. Bounds
                # are left empty on purpose so this follows whichever coin and date
                # range the filters select.
                "truncateYAxis": True,
                "y_axis_bounds": [None, None],
                "x_axis": "snapshot_date",
                "x_axis_time_format": "smart_date",
                # `~` trims trailing zeros: BTC reads $64,139.58 and USDT still
                # reads $0.9989 rather than collapsing to $1.
                "y_axis_format": "$,.4~f",
            },
        },
    ]


def health_charts(datasets: dict[str, object]) -> list[dict[str, object]]:
    availability = datasets["data_availability_daily"]
    return [
        {
            "key": "freshness_days",
            "name": "Days Since Last Snapshot",
            "dataset": availability,
            "viz_type": "big_number_total",
            # The one number that should page someone: how long since data landed.
            "params": {
                "metric": sql_metric(
                    "Days since last snapshot",
                    "date_diff('day', max(case when availability_status <> 'missing' "
                    "then snapshot_date end), current_date)",
                ),
                "subheader": "1 is normal before the daily capture lands; 3+ is a stall",
                "y_axis_format": COUNT_FORMAT,
            },
        },
        {
            "key": "coverage_30d",
            "name": "Coverage - Last 30 Days",
            "dataset": availability,
            "viz_type": "big_number_total",
            # A denominated, recent number: unlike an all-time missing-day count,
            # this one can actually improve as the daily capture accrues data.
            # Counts completed days only — today is legitimately empty until the
            # 01:30 UTC capture lands, and counting it would understate coverage
            # every morning.
            "params": {
                "metric": sql_metric(
                    "Coverage last 30 days (%)",
                    "round(100.0 * count_if(availability_status = 'available' "
                    "and snapshot_date between date_add('day', -30, current_date) "
                    "and date_add('day', -1, current_date)) / 30.0, 1)",
                ),
                "subheader": "% of the last 30 completed days fully served",
                "y_axis_format": ".1f",
            },
        },
        {
            "key": "available_streak",
            "name": "Current Available Streak",
            "dataset": availability,
            "viz_type": "big_number_total",
            "params": {
                # Also completed days only, for the same reason as the 30-day tile:
                # otherwise the streak resets to zero every night at midnight.
                "metric": sql_metric(
                    "Consecutive available days",
                    "date_diff('day', coalesce(max(case when availability_status <> 'available' "
                    "and snapshot_date < current_date then snapshot_date end), "
                    "date_add('day', -1, min(snapshot_date))), "
                    "max(case when snapshot_date < current_date then snapshot_date end))",
                ),
                "subheader": "Completed days with no gap, up to yesterday",
                "y_axis_format": COUNT_FORMAT,
            },
        },
        {
            "key": "coverage_timeline",
            "name": "Coverage Timeline - Daily Status",
            "dataset": availability,
            "viz_type": "echarts_timeseries_bar",
            # One bar per calendar day, coloured by status: a coverage strip that
            # shows *where* the gaps sit. A pie could only ever show how many.
            "params": {
                "granularity_sqla": "snapshot_date",
                "groupby": ["availability_status"],
                "metrics": [sql_metric("Days", "count(*)")],
                "row_limit": 10_000,
                "show_legend": True,
                "stack": "Stack",
                "time_grain_sqla": "P1D",
                "x_axis": "snapshot_date",
                "x_axis_time_format": "smart_date",
                "y_axis_format": COUNT_FORMAT,
            },
        },
        {
            "key": "coverage_monthly",
            "name": "Monthly Coverage Mix",
            "dataset": availability,
            "viz_type": "echarts_timeseries_bar",
            # 100% stacked by month: the trend line for "is build-forward working".
            "params": {
                "granularity_sqla": "snapshot_date",
                "groupby": ["availability_status"],
                "metrics": [sql_metric("Days", "count(*)")],
                "row_limit": 10_000,
                "show_legend": True,
                "stack": "Expand",
                "time_grain_sqla": "P1M",
                "x_axis": "snapshot_date",
                "x_axis_time_format": "smart_date",
                "y_axis_format": ".0%",
            },
        },
        {
            "key": "field_completeness",
            "name": "Field Completeness - Volume and VWAP (%)",
            "dataset": availability,
            "viz_type": "echarts_timeseries_line",
            # Row-count availability passes a day that arrived without volume or
            # vwap. This is the chart that stops `available` from overstating trust.
            "params": {
                "granularity_sqla": "snapshot_date",
                "groupby": [],
                "metrics": [
                    sql_metric("Volume coverage (%)", "avg(volume_coverage_pct)"),
                    sql_metric("VWAP coverage (%)", "avg(vwap_coverage_pct)"),
                ],
                "row_limit": 10_000,
                "show_legend": True,
                "time_grain_sqla": "P1D",
                "x_axis": "snapshot_date",
                "x_axis_time_format": "smart_date",
                "y_axis_format": ".1f",
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
                    "source_coverage_pct", "volume_coverage_pct", "vwap_coverage_pct",
                ],
                "column_config": number_columns(
                    silver_row_count=COUNT_FORMAT,
                    daily_snapshot_row_count=COUNT_FORMAT,
                    rank_change_row_count=COUNT_FORMAT,
                    weekly_average_row_count=COUNT_FORMAT,
                    source_coverage_pct=".1f",
                    volume_coverage_pct=".1f",
                    vwap_coverage_pct=".1f",
                ),
                "include_search": True,
                "order_by_cols": ['["snapshot_date", false]'],
                "page_length": 50,
                "row_limit": 1_000,
                "table_filter": True,
            },
        },
    ]


HEALTH_LEGEND = (
    "### How to read this tab\n\n"
    "**available** — Silver holds the expected asset count and all three dbt serving "
    "models have the same row count for the date.\n\n"
    "**partial** — the date exists but a model is short or missing.\n\n"
    "**missing** — no local Silver snapshot for that calendar date. Dates before "
    "collection started are structurally missing, not a failure.\n\n"
    "This measures **local analytical availability**. It does not prove the cloud "
    "capture bucket holds an object for the date. Field completeness is reported "
    "separately below: a day can be `available` on row counts and still carry no "
    "volume or VWAP."
)


def dashboard_positions(market: list[object], health: list[object]) -> dict[str, object]:
    """Build the v2 position tree: ROOT -> TABS -> TAB -> ROW -> CHART."""
    tabs_id = "TABS-crypto"
    positions: dict[str, object] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": [tabs_id]},
        # Superset still expects a GRID node even when the top level is tabs.
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
        tabs_id: {
            "id": tabs_id,
            "type": "TABS",
            "children": [],
            "parents": ["ROOT_ID"],
            "meta": {},
        },
    }

    def add_tab(tab_id: str, title: str, rows: list[list[object]], heights: list[int]) -> None:
        positions[tabs_id]["children"].append(tab_id)
        positions[tab_id] = {
            "id": tab_id,
            "type": "TAB",
            "children": [],
            "parents": ["ROOT_ID", tabs_id],
            "meta": {"text": title, "defaultText": title, "placeholder": title},
        }
        for row_index, (row_items, height) in enumerate(zip(rows, heights), start=1):
            row_id = f"ROW-{tab_id}-{row_index}"
            positions[tab_id]["children"].append(row_id)
            positions[row_id] = {
                "id": row_id,
                "type": "ROW",
                "children": [],
                "parents": ["ROOT_ID", tabs_id, tab_id],
                "meta": {"background": "BACKGROUND_TRANSPARENT"},
            }
            width = 12 // len(row_items)
            for item in row_items:
                if isinstance(item, str):
                    # A markdown component; the string is its body.
                    component_id = f"MARKDOWN-{tab_id}-{row_index}"
                    positions[component_id] = {
                        "id": component_id,
                        "type": "MARKDOWN",
                        "children": [],
                        "parents": ["ROOT_ID", tabs_id, tab_id, row_id],
                        "meta": {"code": item, "height": height, "width": width},
                    }
                else:
                    component_id = f"CHART-{item.id}"
                    positions[component_id] = {
                        "id": component_id,
                        "type": "CHART",
                        "children": [],
                        "parents": ["ROOT_ID", tabs_id, tab_id, row_id],
                        "meta": {
                            "chartId": item.id,
                            "height": height,
                            "sliceName": item.slice_name,
                            "uuid": str(item.uuid),
                            "width": width,
                        },
                    }
                positions[row_id]["children"].append(component_id)

    add_tab(
        "TAB-market",
        "Market",
        rows=[market[0:2], market[2:4], market[4:5]],
        heights=[50, 50, 50],
    )
    add_tab(
        "TAB-health",
        "Pipeline Health",
        rows=[
            health[0:3],            # KPI tiles
            [HEALTH_LEGEND],        # status legend
            health[3:5],            # daily timeline + monthly mix
            health[5:6],            # field completeness
            health[6:7],            # detail table
        ],
        heights=[22, 32, 50, 40, 60],
    )
    return positions


def native_filters(datasets: dict[str, object], health: list[object]) -> list[dict[str, object]]:
    """Date, symbol, and rank filters — the dashboard had none before.

    KPI tiles are excluded from the time filter: "days since last snapshot" and
    "current streak" are statements about now, and windowing them makes them lie.
    """
    daily_id = datasets["daily_snapshot"].id
    kpi_chart_ids = [chart.id for chart in health[0:3]]
    health_chart_ids = [chart.id for chart in health]

    def base(filter_id: str, name: str, filter_type: str, excluded: list[int]) -> dict[str, object]:
        return {
            "id": filter_id,
            "name": name,
            "filterType": filter_type,
            "type": "NATIVE_FILTER",
            "description": "",
            "cascadeParentIds": [],
            "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
            "scope": {"rootPath": ["ROOT_ID"], "excluded": excluded},
        }

    time_filter = base(FILTER_TIME_ID, "Date range", "filter_time", kpi_chart_ids)
    time_filter["targets"] = [{}]
    time_filter["controlValues"] = {}

    symbol_filter = base(FILTER_SYMBOL_ID, "Symbol", "filter_select", health_chart_ids)
    symbol_filter["targets"] = [{"datasetId": daily_id, "column": {"name": "symbol"}}]
    symbol_filter["controlValues"] = {
        "multiSelect": True,
        "enableEmptyFilter": False,
        "defaultToFirstItem": False,
        "inverseSelection": False,
        "searchAllOptions": False,
    }

    rank_filter = base(FILTER_RANK_ID, "Market-cap rank", "filter_range", health_chart_ids)
    rank_filter["targets"] = [{"datasetId": daily_id, "column": {"name": "coin_rank"}}]
    rank_filter["controlValues"] = {"enableSingleValue": None}

    return [time_filter, symbol_filter, rank_filter]


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
        market = [
            ensure_chart(db.session, Slice, admin_user, **definition)
            for definition in market_charts(datasets)
        ]
        health = [
            ensure_chart(db.session, Slice, admin_user, **definition)
            for definition in health_charts(datasets)
        ]
        retired = delete_retired_charts(db.session, Slice)

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
        dashboard.position_json = json.dumps(dashboard_positions(market, health))
        dashboard.json_metadata = json.dumps(
            {
                "color_scheme": "supersetColors",
                "expanded_slices": {},
                "native_filter_configuration": native_filters(datasets, health),
                "refresh_frequency": 0,
                "show_native_filters": True,
                "label_colors": {
                    "available": "#1FA97F",
                    "partial": "#FCC700",
                    "missing": "#E04355",
                },
            }
        )
        dashboard.published = True
        dashboard.owners = [admin_user]
        dashboard.slices = market + health
        dashboard.created_by = dashboard.created_by or admin_user
        dashboard.changed_by = admin_user
        db.session.add(dashboard)
        db.session.commit()

        print(
            f"Provisioned {len(datasets)} datasets, {len(market) + len(health)} charts "
            f"({retired} retired), and dashboard /superset/dashboard/{dashboard.slug}/"
        )


if __name__ == "__main__":
    main()
