from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from lineage.data_processor import get_missing_columns_impact, load_csv
from lineage.dbt_generator import create_dbt_sources_and_models
from lineage.impact_analyzer import trace_lineage_with_impact
from lineage.lineage_graph import LineageGraph
from lineage.utils import save_json


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Process lineage data.")
    parser.add_argument(
        "--mapping_csv_path",
        type=Path,
        required=True,
        help="Path to the Source to Target Mapping CSV file.",
    )
    parser.add_argument(
        "--source_entities_csv_path",
        type=Path,
        required=True,
        help="Path to the Source Entities CSV file.",
    )
    parser.add_argument(
        "--invalid_mappings_path",
        type=Path,
        required=True,
        help="Path to the Invalid Mappings JSON file.",
    )
    parser.add_argument(
        "--output_dir",
        type=Path,
        default=Path("output"),
        help="Directory to store the output JSON files.",
    )
    parser.add_argument(
        "--analyze_missing",
        action="store_true",
        help="Also analyze columns referenced in mappings but missing from source_entities.csv.",
    )
    return parser


def output_impacted_lineage(
    mapping_csv_path: Path,
    source_entities_csv_path: Path,
    invalid_mappings_path: Path,
    output_dir: Path,
    analyze_missing: bool,
) -> dict[str, Any]:
    mapping_df = load_csv(mapping_csv_path)
    source_entities_df = load_csv(source_entities_csv_path)

    with invalid_mappings_path.open("r", encoding="utf-8") as file_obj:
        invalid_mappings = json.load(file_obj)

    initial_sources = source_entities_df["TableName"].dropna().unique()
    type_changes = {
        entry["attribute_name"].lower(): {
            "original_type": entry["original_type"],
            "incoming_type": entry["incoming_type"],
        }
        for entry in invalid_mappings
    }

    collective_data: list[dict[str, Any]] = []
    combined_lineage_graph = LineageGraph()
    impacted_tables: set[str] = set()

    for initial_source_table in initial_sources:
        impacted_columns = [
            entry["attribute_name"].lower()
            for entry in invalid_mappings
            if entry["entity"].lower() == initial_source_table.lower()
        ]

        impacts, table_impacts = trace_lineage_with_impact(
            mapping_df,
            initial_source_table,
            impacted_columns,
            combined_lineage_graph,
            type_changes,
        )
        impacted_tables.update(table_impacts)

        collective_data.append(
            {
                "initial_source_table": initial_source_table,
                "impacted_columns": impacted_columns,
                "lineage": {
                    node: {
                        "children": combined_lineage_graph.get_children(node),
                        "parents": combined_lineage_graph.get_parents(node),
                    }
                    for node in combined_lineage_graph.graph
                },
                "impacts": impacts,
            }
        )

    all_impacted_tables = set(impacted_tables)
    missing_columns: dict[str, Any] = {}
    if analyze_missing:
        missing_columns = get_missing_columns_impact(source_entities_df, mapping_df)
        all_impacted_tables.update(missing_columns.keys())

    output_dir.mkdir(parents=True, exist_ok=True)
    impact_analysis_path = output_dir / "impact_analysis.json"
    impacted_tables_path = output_dir / "impacted_tables.json"

    save_json(impact_analysis_path, collective_data)
    save_json(impacted_tables_path, {"impacted_tables": sorted(impacted_tables)})

    models_dir = output_dir / "dbt_sample_project" / "models"
    create_dbt_sources_and_models(
        initial_sources=initial_sources,
        lineage_graph=combined_lineage_graph,
        models_dir=models_dir,
        impacted_tables=sorted(table.lower() for table in all_impacted_tables),
    )

    print(f"Output written to {impact_analysis_path}")
    print(f"Impacted tables written to {impacted_tables_path}")

    return {
        "impact_analysis_path": str(impact_analysis_path),
        "impacted_tables_path": str(impacted_tables_path),
        "impacted_tables": sorted(impacted_tables),
        "missing_columns": missing_columns,
        "dbt_models_dir": str(models_dir),
    }


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    output_impacted_lineage(
        mapping_csv_path=args.mapping_csv_path,
        source_entities_csv_path=args.source_entities_csv_path,
        invalid_mappings_path=args.invalid_mappings_path,
        output_dir=args.output_dir,
        analyze_missing=args.analyze_missing,
    )


if __name__ == "__main__":
    main()
