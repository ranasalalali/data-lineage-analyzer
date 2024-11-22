import argparse
import json
import os
from lineage.lineage_graph import LineageGraph
from lineage.data_processor import load_csv, get_missing_columns_impact
from lineage.dbt_generator import create_dbt_sources_and_models
from lineage.impact_analyzer import trace_lineage_with_impact
from lineage.utils import save_json

def output_impacted_lineage(mapping_csv_path, source_entities_csv_path, invalid_mappings_path, output_dir, analyze_missing):
    df = load_csv(mapping_csv_path)
    df1 = load_csv(source_entities_csv_path)

    with open(invalid_mappings_path, 'r') as f:
        invalid_mappings = json.load(f)

    initial_sources = df1['TableName'].dropna().unique()
    type_changes = {entry['attribute_name'].lower(): {
        "original_type": entry['original_type'],
        "incoming_type": entry['incoming_type']
    } for entry in invalid_mappings}

    collective_data = []
    combined_lineage_graph = LineageGraph()
    impacted_tables = set()

    for initial_source_table in initial_sources:
        impacted_columns = [
            entry['attribute_name'].lower()
            for entry in invalid_mappings
            if entry['entity'].lower() == initial_source_table.lower()
        ]

        impacts, table_impacts = trace_lineage_with_impact(
            df, initial_source_table, impacted_columns, combined_lineage_graph, type_changes
        )

        impacted_tables.update(table_impacts)

        data = {
            "initial_source_table": initial_source_table,
            "impacted_columns": impacted_columns,
            "lineage": {node: {"children": combined_lineage_graph.get_children(node), "parents": combined_lineage_graph.get_parents(node)} for node in combined_lineage_graph.graph},
            "impacts": impacts
        }

        collective_data.append(data)

    if analyze_missing:
        missing_columns = get_missing_columns_impact(df1, df)
        all_table_impacted = impacted_tables.union(set(missing_columns.keys()))
    else:
        all_table_impacted = impacted_tables

    all_table_impacted = [x.lower() for x in all_table_impacted]

    output_path = os.path.join(output_dir, "impact_analysis.json")
    save_json(output_path, collective_data)

    impacted_tables_path = os.path.join(output_dir, "impacted_tables.json")
    save_json(impacted_tables_path, {
        "impacted_tables": list(impacted_tables)  # Convert set to list for JSON serialization
    })

    print(f"Output written to {output_path}")
    print(f"Impacted tables written to {impacted_tables_path}")

    # Use the existing models directory in your DBT project
    models_dir = os.path.join(output_dir, 'dbt_sample_project', 'models')
    create_dbt_sources_and_models(initial_sources, combined_lineage_graph, models_dir, all_table_impacted)
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process lineage data.")
    parser.add_argument('--mapping_csv_path', type=str, required=True, help='Path to the Source to Target Mapping CSV file.')
    parser.add_argument('--source_entities_csv_path', type=str, required=True, help='Path to the Source Entities CSV file.')
    parser.add_argument('--invalid_mappings_path', type=str, required=True, help='Path to the Invalid Mappings JSON file.')
    parser.add_argument('--output_dir', type=str, default='output', help='Directory to store the output JSON files.')
    parser.add_argument('--analyze_missing', action='store_true', help='Flag to analyze the impact of missing columns.')

    args = parser.parse_args()

    output_impacted_lineage(args.mapping_csv_path, args.source_entities_csv_path, args.invalid_mappings_path, args.output_dir, args.analyze_missing)