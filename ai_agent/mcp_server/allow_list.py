"""Load and normalize the explicit MCP table allow-list."""

import json
import re
from dataclasses import dataclass
from pathlib import Path


DEFAULT_ALLOW_LIST_PATH = Path("config/ai-agent/allowed-tables.json")
_TABLE_NAME = re.compile(
    r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$"
)


class AllowListConfigError(ValueError):
    """Raised when the checked-in allow-list cannot be trusted."""


@dataclass(frozen=True, slots=True)
class TableAllowList:
    """Normalized, immutable set of fully qualified Trino table names."""

    tables: frozenset[str]

    def __post_init__(self) -> None:
        if not isinstance(self.tables, frozenset) or not self.tables:
            raise AllowListConfigError("Table allow-list must be a non-empty frozenset.")
        if not all(isinstance(table, str) for table in self.tables):
            raise AllowListConfigError("Every allow-list entry must be a string.")

        normalized = frozenset(table.casefold() for table in self.tables)
        invalid = [table for table in normalized if not _TABLE_NAME.fullmatch(table)]
        if invalid:
            raise AllowListConfigError(
                "Allow-list entries must use catalog.schema.table identifiers: "
                + ", ".join(sorted(invalid))
            )
        if len(normalized) != len(self.tables):
            raise AllowListConfigError("Table allow-list contains duplicate entries.")

        object.__setattr__(self, "tables", normalized)

    @classmethod
    def from_file(cls, path: str | Path = DEFAULT_ALLOW_LIST_PATH) -> "TableAllowList":
        """Load a strict `{\"tables\": [...]}` JSON document from disk."""
        config_path = Path(path)
        try:
            payload = json.loads(config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise AllowListConfigError(
                f"Cannot load table allow-list from {config_path}: {exc}"
            ) from exc

        if not isinstance(payload, dict) or set(payload) != {"tables"}:
            raise AllowListConfigError(
                "Table allow-list must be a JSON object containing only 'tables'."
            )

        configured_tables = payload["tables"]
        if not isinstance(configured_tables, list) or not configured_tables:
            raise AllowListConfigError("Table allow-list 'tables' must be a non-empty list.")
        if not all(isinstance(table, str) for table in configured_tables):
            raise AllowListConfigError("Every allow-list entry must be a string.")

        normalized = [table.casefold() for table in configured_tables]
        if len(normalized) != len(set(normalized)):
            raise AllowListConfigError("Table allow-list contains duplicate entries.")

        return cls(tables=frozenset(configured_tables))

    def allows(self, table: str) -> bool:
        """Return whether a fully qualified table name is explicitly allowed."""
        return table.casefold() in self.tables
