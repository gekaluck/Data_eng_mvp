"""Tests for strict, fail-closed allow-list loading."""

import json

import pytest

from ai_agent.mcp_server.allow_list import AllowListConfigError, TableAllowList


def _write_config(tmp_path, payload):
    path = tmp_path / "allowed-tables.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_loads_and_normalizes_fully_qualified_tables(tmp_path):
    path = _write_config(
        tmp_path,
        {"tables": ["gold.crypto_dbt.DAILY_SNAPSHOT"]},
    )

    allow_list = TableAllowList.from_file(path)

    assert allow_list.tables == frozenset({"gold.crypto_dbt.daily_snapshot"})
    assert allow_list.allows("GOLD.CRYPTO_DBT.DAILY_SNAPSHOT")


def test_direct_construction_is_also_fail_closed():
    with pytest.raises(AllowListConfigError):
        TableAllowList(tables=frozenset({"daily_snapshot"}))


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"tables": []},
        {"tables": [1]},
        {"tables": ["daily_snapshot"]},
        {"tables": ["crypto_dbt.daily_snapshot"]},
        {"tables": ["gold.crypto-dbt.daily_snapshot"]},
        {"tables": ["gold.crypto_dbt.daily_snapshot"], "typo": []},
        {
            "tables": [
                "gold.crypto_dbt.daily_snapshot",
                "GOLD.CRYPTO_DBT.DAILY_SNAPSHOT",
            ]
        },
    ],
)
def test_rejects_ambiguous_or_unsafe_configuration(tmp_path, payload):
    with pytest.raises(AllowListConfigError):
        TableAllowList.from_file(_write_config(tmp_path, payload))


def test_wraps_missing_or_invalid_json_with_the_config_path(tmp_path):
    path = tmp_path / "missing.json"

    with pytest.raises(AllowListConfigError, match="missing.json"):
        TableAllowList.from_file(path)
