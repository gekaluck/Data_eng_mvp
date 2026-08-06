"""Static checks for the platform controls required by the AI-agent layer."""

import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_json(relative_path: str) -> dict:
    return json.loads((REPO_ROOT / relative_path).read_text(encoding="utf-8"))


def test_agent_can_select_only_from_canonical_dbt_gold():
    rules = _load_json("config/trino/access-rules.json")

    agent_catalogs = [rule for rule in rules["catalogs"] if rule.get("user") == "agent"]
    assert agent_catalogs == [
        {"user": "agent", "catalog": "gold", "allow": "read-only"},
        {"user": "agent", "catalog": ".*", "allow": "none"},
    ]

    agent_tables = [rule for rule in rules["tables"] if rule.get("user") == "agent"]
    assert agent_tables == [
        {
            "user": "agent",
            "catalog": "gold",
            "schema": "crypto_dbt",
            "table": ".*",
            "privileges": ["SELECT"],
        },
        {"user": "agent", "privileges": []},
    ]
    assert [rule for rule in rules["schemas"] if rule.get("user") == "agent"] == [
        {"user": "agent", "owner": False}
    ]
    assert [rule for rule in rules["queries"] if rule.get("user") == "agent"] == [
        {"user": "agent", "allow": ["execute"]}
    ]


def test_mcp_allow_list_enumerates_only_current_dbt_gold_models():
    allow_list = _load_json("config/ai-agent/allowed-tables.json")
    expected_tables = {
        f"gold.crypto_dbt.{model.stem}"
        for model in (REPO_ROOT / "dbt/models/gold").glob("*.sql")
    }
    assert set(allow_list["tables"]) == expected_tables
    assert len(allow_list["tables"]) == len(expected_tables)


def test_agent_has_a_bounded_resource_group_with_a_fallback_for_other_users():
    groups = _load_json("config/trino/resource-groups.json")
    global_group = groups["rootGroups"][0]
    agent_group, default_group = global_group["subGroups"]

    assert agent_group == {
        "name": "agent",
        "softMemoryLimit": "128MB",
        "hardConcurrencyLimit": 1,
        "maxQueued": 2,
        "hardPhysicalDataScanLimit": "1GB",
    }
    assert default_group["name"] == "default"
    assert groups["selectors"] == [
        {"user": "agent", "group": "global.agent"},
        {"group": "global.default"},
    ]
    assert groups["physicalDataScanQuotaPeriod"] == "1h"
