"""Read-only local smoke check for all transport-neutral metadata tools."""

import json

from ai_agent.mcp_server.allow_list import TableAllowList
from ai_agent.mcp_server.dbt_artifacts import DbtArtifactAdapter
from ai_agent.mcp_server.metadata_tools import MetadataTools
from ai_agent.mcp_server.trino_metadata import IcebergMetadataAdapter, TrinoDbApiRunner


def main() -> None:
    """Exercise every metadata adapter against published artifacts and live Trino."""
    allow_list = TableAllowList.from_file()
    dbt = DbtArtifactAdapter.from_files(allow_list)
    tools = MetadataTools(
        dbt,
        IcebergMetadataAdapter(TrinoDbApiRunner.from_env(), allow_list),
    )

    report = []
    for table in tools.list_tables():
        schema = tools.get_table_schema(table.table)
        snapshots = tools.get_table_snapshots(table.table, limit=2)
        docs = tools.get_model_docs(table.table)
        lineage = tools.get_lineage(table.table, direction="upstream", depth=2)
        report.append(
            {
                "table": table.table,
                "columns": len(schema.columns),
                "rows": schema.stats.row_count,
                "size_bytes": schema.stats.size_bytes,
                "last_updated": (
                    schema.stats.last_updated.isoformat()
                    if schema.stats.last_updated
                    else None
                ),
                "partition_spec": schema.partition_spec,
                "sort_order": schema.sort_order,
                "schema_warnings": schema.warnings,
                "recent_snapshots": len(snapshots.snapshots),
                "documented_columns": len(docs.columns),
                "declared_tests": len(docs.tests),
                "upstream_nodes": len(lineage.nodes),
            }
        )
    print(json.dumps({"tables": report}, indent=2))


if __name__ == "__main__":
    main()
