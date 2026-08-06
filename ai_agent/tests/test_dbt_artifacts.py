"""Tests for allow-list-filtered dbt docs and lineage."""

import json

import pytest

from ai_agent.mcp_server.allow_list import TableAllowList
from ai_agent.mcp_server.dbt_artifacts import DbtArtifactAdapter, DbtArtifactError
from ai_agent.mcp_server.errors import ErrorCode, GuardrailError
from ai_agent.tests.metadata_fixtures import (
    DAILY_ID,
    LATEST_ID,
    SOURCE_ID,
    make_allow_list,
    make_dbt_adapter,
)


def test_list_tables_exposes_only_allowed_models_and_catalog_row_count():
    adapter = make_dbt_adapter()

    tables = adapter.list_tables()

    assert [item.table for item in tables] == [
        "gold.crypto_dbt.daily_snapshot",
        "gold.crypto_dbt.latest_market_snapshot",
    ]
    assert tables[0].approx_rows == 123
    assert tables[1].approx_rows is None
    assert adapter.list_tables(tag="MARKET") == (tables[0],)
    assert adapter.list_tables(schema="gold.crypto_dbt") == tables
    assert adapter.list_tables(schema="other") == ()


@pytest.mark.parametrize(("field", "value"), [("schema", ""), ("tag", 3)])
def test_list_tables_rejects_invalid_optional_filters(field, value):
    with pytest.raises(GuardrailError) as error:
        make_dbt_adapter().list_tables(**{field: value})
    assert error.value.code is ErrorCode.PARSE_ERROR


def test_model_docs_accepts_model_or_table_and_includes_declared_tests():
    adapter = make_dbt_adapter()

    docs = adapter.get_model_docs("GOLD.CRYPTO_DBT.DAILY_SNAPSHOT")

    assert docs.model == "daily_snapshot"
    assert docs.columns[1].description == "Stable asset identifier."
    assert len(docs.tests) == 1
    assert docs.tests[0].column_name == "coin_id"
    assert docs.tests[0].severity == "ERROR"
    assert adapter.get_model_docs(DAILY_ID) == docs


def test_model_docs_rejects_non_allowed_or_blank_models():
    adapter = make_dbt_adapter()

    with pytest.raises(GuardrailError) as hidden:
        adapter.get_model_docs("hidden_model")
    assert hidden.value.code is ErrorCode.TABLE_NOT_ALLOWED

    with pytest.raises(GuardrailError) as blank:
        adapter.get_model_docs("  ")
    assert blank.value.code is ErrorCode.PARSE_ERROR


def test_lineage_traverses_only_data_nodes_and_marks_queryability():
    adapter = make_dbt_adapter()

    upstream = adapter.get_lineage("latest_market_snapshot", direction="upstream", depth=2)
    downstream = adapter.get_lineage("daily_snapshot", direction="downstream", depth=2)

    assert [(node.unique_id, node.distance) for node in upstream.nodes] == [
        (DAILY_ID, 1),
        (SOURCE_ID, 2),
    ]
    assert upstream.nodes[0].queryable is True
    assert upstream.nodes[1].queryable is False
    assert [node.unique_id for node in downstream.nodes] == [LATEST_ID]


@pytest.mark.parametrize(
    ("direction", "depth"),
    [("sideways", 1), ("upstream", 0), ("upstream", 6), ("upstream", True)],
)
def test_lineage_rejects_unbounded_or_invalid_requests(direction, depth):
    with pytest.raises(GuardrailError) as error:
        make_dbt_adapter().get_lineage(
            "daily_snapshot", direction=direction, depth=depth
        )
    assert error.value.code is ErrorCode.PARSE_ERROR


def test_adapter_fails_when_allow_list_and_manifest_disagree():
    manifest = {
        "metadata": {"invocation_id": "manifest-run"},
        "nodes": {},
        "sources": {},
        "parent_map": {},
        "child_map": {},
    }
    catalog = {"metadata": {"invocation_id": "manifest-run"}, "nodes": {}}
    allow_list = TableAllowList(
        tables=frozenset({"gold.crypto_dbt.missing_model"})
    )

    with pytest.raises(DbtArtifactError, match="missing_model"):
        DbtArtifactAdapter(manifest, catalog, allow_list)


def test_from_files_wraps_missing_and_invalid_artifacts(tmp_path):
    manifest = tmp_path / "manifest.json"
    catalog = tmp_path / "catalog.json"
    manifest.write_text(json.dumps({"nodes": {}}), encoding="utf-8")
    catalog.write_text("not-json", encoding="utf-8")

    with pytest.raises(DbtArtifactError, match="catalog.json"):
        DbtArtifactAdapter.from_files(
            make_allow_list(),
            manifest_path=manifest,
            catalog_path=catalog,
        )


def test_adapter_rejects_artifacts_from_different_dbt_invocations():
    adapter = make_dbt_adapter()
    manifest = dict(adapter._manifest)
    catalog = dict(adapter._catalog)
    catalog["metadata"] = {"invocation_id": "different-run"}

    with pytest.raises(DbtArtifactError, match="same invocation"):
        DbtArtifactAdapter(manifest, catalog, make_allow_list())
