"""Typed contracts for transport-neutral catalog metadata tools."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class MetadataModel(BaseModel):
    """Immutable base model shared by all metadata-tool results."""

    model_config = ConfigDict(frozen=True)


class TableSummary(MetadataModel):
    """One allow-listed dbt model advertised to an agent."""

    table: str
    description: str
    tags: tuple[str, ...] = ()
    approx_rows: int | None = None


class ColumnSchema(MetadataModel):
    """A live Trino column, optionally annotated from dbt docs."""

    name: str
    type: str
    comment: str | None = None
    nullable: bool | None = None


class TableStats(MetadataModel):
    """Statistics derived from Iceberg metadata rather than a data scan."""

    row_count: int
    size_bytes: int
    last_updated: datetime | None = None


class TableSchema(MetadataModel):
    """Live Iceberg schema and physical-layout metadata."""

    table: str
    columns: tuple[ColumnSchema, ...]
    partition_spec: tuple[str, ...] = ()
    sort_order: tuple[str, ...] = ()
    stats: TableStats
    warnings: tuple[str, ...] = ()


class TableSnapshot(MetadataModel):
    """One Iceberg snapshot."""

    snapshot_id: int
    committed_at: datetime
    operation: str
    summary: dict[str, str] = Field(default_factory=dict)


class TableSnapshots(MetadataModel):
    """Newest-first Iceberg snapshot history for an allow-listed table."""

    table: str
    snapshots: tuple[TableSnapshot, ...]


class ColumnDocs(MetadataModel):
    """dbt documentation attached to one model column."""

    name: str
    description: str
    data_type: str | None = None


class TestDefinition(MetadataModel):
    """A dbt test whose dependency graph includes the model."""

    unique_id: str
    name: str
    column_name: str | None = None
    severity: str | None = None


class ModelDocs(MetadataModel):
    """Semantic documentation and declared tests for one dbt model."""

    model: str
    table: str
    description: str
    columns: tuple[ColumnDocs, ...]
    tests: tuple[TestDefinition, ...]


class LineageNode(MetadataModel):
    """A dbt model or source discovered while traversing lineage."""

    unique_id: str
    name: str
    resource_type: Literal["model", "source"]
    distance: int
    table: str | None = None
    queryable: bool = False


class LineageResult(MetadataModel):
    """Bounded upstream or downstream lineage for one allow-listed model."""

    model: str
    direction: Literal["upstream", "downstream"]
    depth: int
    nodes: tuple[LineageNode, ...]
