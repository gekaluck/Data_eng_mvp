"""Transport-neutral implementations of the five catalog metadata tools."""

from typing import Literal

from ai_agent.mcp_server.dbt_artifacts import DbtArtifactAdapter
from ai_agent.mcp_server.metadata_models import (
    LineageResult,
    ModelDocs,
    TableSchema,
    TableSnapshots,
    TableSummary,
)
from ai_agent.mcp_server.trino_metadata import IcebergMetadataAdapter


class MetadataTools:
    """Compose dbt semantics with authoritative live Iceberg metadata."""

    def __init__(
        self,
        dbt: DbtArtifactAdapter,
        iceberg: IcebergMetadataAdapter,
    ) -> None:
        self._dbt = dbt
        self._iceberg = iceberg

    def list_tables(
        self,
        *,
        schema: str | None = None,
        tag: str | None = None,
    ) -> tuple[TableSummary, ...]:
        return self._dbt.list_tables(schema=schema, tag=tag)

    def get_model_docs(self, model: str) -> ModelDocs:
        return self._dbt.get_model_docs(model)

    def get_lineage(
        self,
        model: str,
        *,
        direction: Literal["upstream", "downstream"],
        depth: int = 1,
    ) -> LineageResult:
        return self._dbt.get_lineage(model, direction=direction, depth=depth)

    def get_table_snapshots(
        self, table: str, *, limit: int = 10
    ) -> TableSnapshots:
        return self._iceberg.get_table_snapshots(table, limit=limit)

    def get_table_schema(self, table: str) -> TableSchema:
        """Annotate live columns with dbt docs and surface any disagreement."""
        live = self._iceberg.get_table_schema(table)
        descriptions = self._dbt.column_descriptions(live.table)
        live_names = {column.name.casefold() for column in live.columns}
        documented_names = set(descriptions)
        warnings = list(live.warnings)
        undocumented = sorted(live_names - documented_names)
        stale_docs = sorted(documented_names - live_names)
        if undocumented:
            warnings.append(
                "Live columns missing dbt descriptions: " + ", ".join(undocumented)
            )
        if stale_docs:
            warnings.append(
                "dbt documents columns absent from the live table: "
                + ", ".join(stale_docs)
            )

        columns = tuple(
            column.model_copy(
                update={
                    "comment": column.comment
                    or descriptions.get(column.name.casefold())
                    or None
                }
            )
            for column in live.columns
        )
        return live.model_copy(update={"columns": columns, "warnings": tuple(warnings)})
