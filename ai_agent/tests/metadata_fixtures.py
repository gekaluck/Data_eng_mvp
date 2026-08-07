"""Small dbt artifacts shared by metadata adapter tests."""

from ai_agent.mcp_server.allow_list import TableAllowList
from ai_agent.mcp_server.dbt_artifacts import DbtArtifactAdapter

DAILY_ID = "model.crypto_lakehouse.daily_snapshot"
LATEST_ID = "model.crypto_lakehouse.latest_market_snapshot"
HIDDEN_ID = "model.crypto_lakehouse.hidden_model"
SOURCE_ID = "source.crypto_lakehouse.silver.price_snapshots"
TEST_ID = "test.crypto_lakehouse.not_null_daily_snapshot_coin_id.123"


def make_allow_list() -> TableAllowList:
    return TableAllowList(
        tables=frozenset(
            {
                "gold.crypto_dbt.daily_snapshot",
                "gold.crypto_dbt.latest_market_snapshot",
            }
        )
    )


def make_dbt_adapter() -> DbtArtifactAdapter:
    manifest = {
        "metadata": {"invocation_id": "fixture-invocation"},
        "nodes": {
            DAILY_ID: {
                "resource_type": "model",
                "database": "gold",
                "schema": "crypto_dbt",
                "name": "daily_snapshot",
                "alias": "daily_snapshot",
                "description": "Primary daily market snapshot.",
                "tags": ["market"],
                "columns": {
                    "snapshot_date": {
                        "name": "snapshot_date",
                        "description": "UTC snapshot date.",
                        "data_type": "date",
                    },
                    "coin_id": {
                        "name": "coin_id",
                        "description": "Stable asset identifier.",
                    },
                },
            },
            LATEST_ID: {
                "resource_type": "model",
                "database": "gold",
                "schema": "crypto_dbt",
                "name": "latest_market_snapshot",
                "alias": "latest_market_snapshot",
                "description": "Latest available snapshot.",
                "tags": [],
                "columns": {},
            },
            HIDDEN_ID: {
                "resource_type": "model",
                "database": "gold",
                "schema": "crypto_dbt",
                "name": "hidden_model",
                "alias": "hidden_model",
                "description": "Not allow-listed.",
                "tags": [],
                "columns": {},
            },
            TEST_ID: {
                "resource_type": "test",
                "name": "not_null_daily_snapshot_coin_id",
                "column_name": "coin_id",
                "depends_on": {"nodes": [DAILY_ID]},
                "config": {"severity": "ERROR"},
            },
        },
        "sources": {
            SOURCE_ID: {
                "resource_type": "source",
                "database": "silver",
                "schema": "crypto",
                "name": "price_snapshots",
                "alias": "price_snapshots",
            }
        },
        "parent_map": {
            DAILY_ID: [SOURCE_ID],
            LATEST_ID: [DAILY_ID],
            HIDDEN_ID: [],
            SOURCE_ID: [],
            TEST_ID: [DAILY_ID],
        },
        "child_map": {
            DAILY_ID: [LATEST_ID, TEST_ID],
            LATEST_ID: [],
            HIDDEN_ID: [],
            SOURCE_ID: [DAILY_ID],
            TEST_ID: [],
        },
    }
    catalog = {
        "metadata": {"invocation_id": "fixture-invocation"},
        "nodes": {
            DAILY_ID: {"stats": {"num_rows": {"value": 123}}},
            LATEST_ID: {"stats": {"has_stats": {"value": False}}},
        }
    }
    return DbtArtifactAdapter(manifest, catalog, make_allow_list())
