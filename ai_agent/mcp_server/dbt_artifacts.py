"""Read semantic documentation and lineage from published dbt artifacts."""

import json
from collections import deque
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

from ai_agent.mcp_server.allow_list import TableAllowList
from ai_agent.mcp_server.errors import ErrorCode, GuardrailError
from ai_agent.mcp_server.metadata_models import (
    ColumnDocs,
    LineageNode,
    LineageResult,
    ModelDocs,
    TableSummary,
    TestDefinition,
)

DEFAULT_MANIFEST_PATH = Path("dbt/artifacts/manifest.json")
DEFAULT_CATALOG_PATH = Path("dbt/artifacts/catalog.json")


class DbtArtifactError(ValueError):
    """Raised when published dbt artifacts are missing or cannot be trusted."""


def _load_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DbtArtifactError(f"Cannot load dbt artifact {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise DbtArtifactError(f"dbt artifact {path} must contain a JSON object.")
    return payload


def _mapping(payload: Mapping[str, Any], key: str, *, path: Path) -> Mapping[str, Any]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        raise DbtArtifactError(f"dbt artifact {path} has no valid '{key}' mapping.")
    return value


def _physical_table(node: Mapping[str, Any]) -> str | None:
    parts = (node.get("database"), node.get("schema"), node.get("alias"))
    if not all(isinstance(part, str) and part for part in parts):
        return None
    return ".".join(part.casefold() for part in parts)


class DbtArtifactAdapter:
    """In-memory, allow-list-filtered view over dbt manifest and catalog JSON."""

    def __init__(
        self,
        manifest: Mapping[str, Any],
        catalog: Mapping[str, Any],
        allow_list: TableAllowList,
    ) -> None:
        self._manifest = manifest
        self._catalog = catalog
        self._allow_list = allow_list
        self._validate_artifact_pair(manifest, catalog)
        self._nodes = _mapping(manifest, "nodes", path=DEFAULT_MANIFEST_PATH)
        self._sources = _mapping(manifest, "sources", path=DEFAULT_MANIFEST_PATH)
        self._parent_map = _mapping(manifest, "parent_map", path=DEFAULT_MANIFEST_PATH)
        self._child_map = _mapping(manifest, "child_map", path=DEFAULT_MANIFEST_PATH)
        self._catalog_nodes = _mapping(catalog, "nodes", path=DEFAULT_CATALOG_PATH)
        self._models = self._index_allowed_models()

        missing = allow_list.tables - {
            table for table, _node in self._models.values()
        }
        if missing:
            raise DbtArtifactError(
                "Published dbt manifest does not contain allow-listed models: "
                + ", ".join(sorted(missing))
            )

    @classmethod
    def from_files(
        cls,
        allow_list: TableAllowList,
        manifest_path: str | Path = DEFAULT_MANIFEST_PATH,
        catalog_path: str | Path = DEFAULT_CATALOG_PATH,
    ) -> "DbtArtifactAdapter":
        """Load one internally consistent published artifact pair from disk."""
        manifest_file = Path(manifest_path)
        catalog_file = Path(catalog_path)
        return cls(
            _load_object(manifest_file),
            _load_object(catalog_file),
            allow_list,
        )

    def _index_allowed_models(
        self,
    ) -> dict[str, tuple[str, Mapping[str, Any]]]:
        indexed: dict[str, tuple[str, Mapping[str, Any]]] = {}
        for unique_id, raw_node in self._nodes.items():
            if not isinstance(unique_id, str) or not isinstance(raw_node, Mapping):
                continue
            if raw_node.get("resource_type") != "model":
                continue
            table = _physical_table(raw_node)
            if table is None or not self._allow_list.allows(table):
                continue
            indexed[unique_id] = (table, raw_node)
        return indexed

    def list_tables(
        self,
        *,
        schema: str | None = None,
        tag: str | None = None,
    ) -> tuple[TableSummary, ...]:
        """List only allow-listed dbt models, optionally filtered by schema or tag."""
        normalized_schema = self._optional_filter("schema", schema)
        normalized_tag = self._optional_filter("tag", tag)
        tables: list[TableSummary] = []
        for unique_id, (table, node) in self._models.items():
            table_schema = ".".join(table.split(".")[:2])
            if normalized_schema not in (None, table.split(".")[1], table_schema):
                continue
            tags = tuple(
                item for item in node.get("tags", []) if isinstance(item, str)
            )
            if normalized_tag and normalized_tag not in {
                item.casefold() for item in tags
            }:
                continue
            tables.append(
                TableSummary(
                    table=table,
                    description=str(node.get("description") or ""),
                    tags=tags,
                    approx_rows=self._catalog_row_count(unique_id),
                )
            )
        return tuple(sorted(tables, key=lambda item: item.table))

    @staticmethod
    def _validate_artifact_pair(
        manifest: Mapping[str, Any], catalog: Mapping[str, Any]
    ) -> None:
        manifest_metadata = manifest.get("metadata")
        catalog_metadata = catalog.get("metadata")
        if not isinstance(manifest_metadata, Mapping) or not isinstance(
            catalog_metadata, Mapping
        ):
            raise DbtArtifactError("dbt manifest and catalog must include metadata.")
        manifest_invocation = manifest_metadata.get("invocation_id")
        catalog_invocation = catalog_metadata.get("invocation_id")
        if (
            not isinstance(manifest_invocation, str)
            or not manifest_invocation
            or manifest_invocation != catalog_invocation
        ):
            raise DbtArtifactError(
                "dbt manifest and catalog are not from the same invocation."
            )

    @staticmethod
    def _optional_filter(name: str, value: str | None) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise GuardrailError(
                ErrorCode.PARSE_ERROR,
                f"Table {name} filter must be a non-empty string when provided.",
            )
        return value.strip().casefold()

    def _catalog_row_count(self, unique_id: str) -> int | None:
        raw_node = self._catalog_nodes.get(unique_id)
        if not isinstance(raw_node, Mapping):
            return None
        stats = raw_node.get("stats")
        if not isinstance(stats, Mapping):
            return None
        for key in ("num_rows", "row_count"):
            raw_stat = stats.get(key)
            if isinstance(raw_stat, Mapping):
                value = raw_stat.get("value")
                if isinstance(value, int) and not isinstance(value, bool):
                    return value
        return None

    def get_model_docs(self, model: str) -> ModelDocs:
        """Return dbt descriptions and tests for one allow-listed model."""
        unique_id, table, node = self._resolve_model(model)
        raw_columns = node.get("columns")
        columns: list[ColumnDocs] = []
        if isinstance(raw_columns, Mapping):
            for key, raw_column in raw_columns.items():
                if not isinstance(raw_column, Mapping):
                    continue
                name = raw_column.get("name", key)
                if not isinstance(name, str):
                    continue
                data_type = raw_column.get("data_type")
                columns.append(
                    ColumnDocs(
                        name=name,
                        description=str(raw_column.get("description") or ""),
                        data_type=data_type if isinstance(data_type, str) else None,
                    )
                )

        tests: list[TestDefinition] = []
        for test_id, raw_test in self._nodes.items():
            if not isinstance(test_id, str) or not isinstance(raw_test, Mapping):
                continue
            if raw_test.get("resource_type") != "test":
                continue
            dependencies = raw_test.get("depends_on")
            dependency_nodes = (
                dependencies.get("nodes") if isinstance(dependencies, Mapping) else None
            )
            if not isinstance(dependency_nodes, list) or unique_id not in dependency_nodes:
                continue
            config = raw_test.get("config")
            severity = config.get("severity") if isinstance(config, Mapping) else None
            tests.append(
                TestDefinition(
                    unique_id=test_id,
                    name=str(raw_test.get("name") or test_id),
                    column_name=(
                        raw_test.get("column_name")
                        if isinstance(raw_test.get("column_name"), str)
                        else None
                    ),
                    severity=severity if isinstance(severity, str) else None,
                )
            )

        return ModelDocs(
            model=str(node.get("name") or unique_id),
            table=table,
            description=str(node.get("description") or ""),
            columns=tuple(columns),
            tests=tuple(sorted(tests, key=lambda item: item.unique_id)),
        )

    def get_lineage(
        self,
        model: str,
        *,
        direction: Literal["upstream", "downstream"],
        depth: int,
    ) -> LineageResult:
        """Traverse bounded dbt lineage, omitting tests and other non-data nodes."""
        if direction not in ("upstream", "downstream"):
            raise GuardrailError(
                ErrorCode.PARSE_ERROR,
                "Lineage direction must be 'upstream' or 'downstream'.",
                hint="Choose one of the two documented direction values.",
            )
        if not isinstance(depth, int) or isinstance(depth, bool) or not 1 <= depth <= 5:
            raise GuardrailError(
                ErrorCode.PARSE_ERROR,
                "Lineage depth must be an integer from 1 through 5.",
                hint="Use a small bounded traversal depth.",
            )

        unique_id, _table, node = self._resolve_model(model)
        graph = self._parent_map if direction == "upstream" else self._child_map
        queue = deque((child, 1) for child in self._graph_edges(graph, unique_id))
        seen = {unique_id}
        lineage: list[LineageNode] = []

        while queue:
            related_id, distance = queue.popleft()
            if related_id in seen or distance > depth:
                continue
            seen.add(related_id)
            related = self._all_data_node(related_id)
            if related is None:
                continue
            resource_type, related_node = related
            related_table = _physical_table(related_node)
            lineage.append(
                LineageNode(
                    unique_id=related_id,
                    name=str(related_node.get("name") or related_id),
                    resource_type=resource_type,
                    distance=distance,
                    table=related_table,
                    queryable=(
                        related_table is not None
                        and self._allow_list.allows(related_table)
                    ),
                )
            )
            if distance < depth:
                queue.extend(
                    (child, distance + 1)
                    for child in self._graph_edges(graph, related_id)
                )

        return LineageResult(
            model=str(node.get("name") or unique_id),
            direction=direction,
            depth=depth,
            nodes=tuple(
                sorted(lineage, key=lambda item: (item.distance, item.unique_id))
            ),
        )

    def column_descriptions(self, table: str) -> dict[str, str]:
        """Return dbt column annotations keyed by normalized column name."""
        _unique_id, _table, node = self._resolve_model(table)
        raw_columns = node.get("columns")
        if not isinstance(raw_columns, Mapping):
            return {}
        descriptions: dict[str, str] = {}
        for key, raw_column in raw_columns.items():
            if not isinstance(key, str) or not isinstance(raw_column, Mapping):
                continue
            description = raw_column.get("description")
            if isinstance(description, str) and description:
                descriptions[key.casefold()] = description
        return descriptions

    def _resolve_model(
        self, model: str
    ) -> tuple[str, str, Mapping[str, Any]]:
        if not isinstance(model, str) or not model.strip():
            raise GuardrailError(
                ErrorCode.PARSE_ERROR,
                "Model name must be a non-empty string.",
            )
        target = model.strip().casefold()
        matches: list[tuple[str, str, Mapping[str, Any]]] = []
        for unique_id, (table, node) in self._models.items():
            names = {
                table,
                str(node.get("name") or "").casefold(),
                str(node.get("alias") or "").casefold(),
                unique_id.casefold(),
            }
            if target in names:
                matches.append((unique_id, table, node))
        if len(matches) == 1:
            return matches[0]
        raise GuardrailError(
            ErrorCode.TABLE_NOT_ALLOWED,
            f"Model '{model}' is not an allow-listed dbt Gold model.",
            hint="Call list_tables and choose one of the returned models.",
        )

    @staticmethod
    def _graph_edges(graph: Mapping[str, Any], node_id: str) -> tuple[str, ...]:
        raw_edges = graph.get(node_id, [])
        if not isinstance(raw_edges, list):
            return ()
        return tuple(edge for edge in raw_edges if isinstance(edge, str))

    def _all_data_node(
        self, unique_id: str
    ) -> tuple[Literal["model", "source"], Mapping[str, Any]] | None:
        raw_node = self._nodes.get(unique_id)
        if isinstance(raw_node, Mapping) and raw_node.get("resource_type") == "model":
            return "model", raw_node
        raw_source = self._sources.get(unique_id)
        if isinstance(raw_source, Mapping):
            return "source", raw_source
        return None
