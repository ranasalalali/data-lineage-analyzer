from __future__ import annotations

from pathlib import Path
from typing import Iterable

from lineage.lineage_graph import LineageGraph


def create_dbt_sources_and_models(
    initial_sources: Iterable[str],
    lineage_graph: LineageGraph,
    models_dir: str | Path,
    impacted_tables: Iterable[str],
) -> None:
    models_dir = Path(models_dir)
    models_dir.mkdir(parents=True, exist_ok=True)
    sources_yml_path = models_dir / "sources.yml"

    initial_sources_lower = {source.lower() for source in initial_sources}
    impacted_tables_lower = {table.lower() for table in impacted_tables}

    database = "raw"
    schema_name = "silver"

    with sources_yml_path.open("w", encoding="utf-8") as file_obj:
        file_obj.write("version: 2\n")
        file_obj.write("sources:\n")
        file_obj.write("  - name: silver\n")
        file_obj.write(f"    database: {database}\n")
        file_obj.write(f"    schema: {schema_name}\n")
        file_obj.write("    tables:\n")
        for source in sorted(initial_sources_lower):
            file_obj.write(f"      - name: {source}\n")
    print(f"Generated sources file: {sources_yml_path}")

    for node, connections in sorted(lineage_graph.graph.items()):
        node_parts = node.split(".")
        node_name = node_parts[1] if len(node_parts) > 1 else node_parts[0]

        if node_name.lower() in initial_sources_lower:
            continue

        model_filename = models_dir / f"{node_name}.sql"
        parent_refs = []
        color = ", docs={'node_color': 'red'}" if node_name.lower() in impacted_tables_lower else ""
        for parent in sorted(connections["parents"]):
            parent_parts = parent.split(".")
            parent_name = parent_parts[1] if len(parent_parts) > 1 else parent_parts[0]

            if parent_name.lower() in initial_sources_lower:
                parent_refs.append(f"{{{{ source('silver', '{parent_name.lower()}') }}}}")
            else:
                parent_refs.append(f"{{{{ ref('{parent_name.lower()}') }}}}")

        with model_filename.open("w", encoding="utf-8") as file_obj:
            if len(parent_refs) > 1:
                file_obj.write(f"{{{{ config(materialized='view'{color}) }}}}\n\n")
                file_obj.write("select *\nfrom (\n")
                for index, parent_ref in enumerate(parent_refs):
                    if index > 0:
                        file_obj.write("join\n")
                    file_obj.write(f"    {parent_ref} as t{index}\n")
                file_obj.write(")\n")
            elif len(parent_refs) == 1:
                file_obj.write(f"{{{{ config(materialized='view'{color}) }}}}\n\n")
                file_obj.write("select *\n")
                file_obj.write(f"from {parent_refs[0]}\n")
        print(f"Generated model file: {model_filename}")
