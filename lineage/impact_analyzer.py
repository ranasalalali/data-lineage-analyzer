def trace_lineage_with_impact(df, initial_source_table, impacted_columns, lineage_graph, type_changes):
    distinct_targets = set()
    queue = [(initial_source_table.lower(), 0)]
    visited = set()
    impacts = []
    impacted_tables = set()

    impacted_columns_lower = [col.lower() for col in impacted_columns]

    while queue:
        current_source_table, depth = queue.pop(0)

        if current_source_table in visited:
            continue
        visited.add(current_source_table)

        if current_source_table not in distinct_targets:
            distinct_targets.add(current_source_table)

        current_source_table_name = current_source_table.split('.')[1] if len(current_source_table.split('.')) == 2 else current_source_table

        next_steps = df[df['Source Table'].str.lower() == current_source_table_name]

        for _, row in next_steps.iterrows():
            source_column = row['Source Column'].lower()
            target_schema = row['Target Schema']
            target_table = row['Target Table']
            target_column = row['Target Column'].lower()
            target_column_type = row['Column Type'].lower() if isinstance(row['Column Type'], str) else row['Column Type']
            transformation = row['Transformation']

            next_source_table = f"{target_schema}.{target_table}".lower()
            lineage_graph.add_edge(current_source_table, next_source_table)

            if source_column in impacted_columns_lower:
                impacts.append({
                    "source_table": current_source_table,
                    "source_column": source_column,
                    "target_table": target_table,
                    "target_column": target_column,
                    "target_column_type": target_column_type,
                    "transformation": transformation,
                    "depth": depth + 1
                })

                impacted_tables.add(target_table.lower())

                downstream_impacts, downstream_tables = trace_downstream_impacts(
                    df, target_schema, target_table, source_column, depth + 1, lineage_graph, visited, impacted_columns_lower, type_changes
                )
                impacts.extend(downstream_impacts)
                impacted_tables.update(downstream_tables)

            if next_source_table not in visited:
                queue.append((next_source_table, depth + 1))

    return impacts, impacted_tables

def trace_downstream_impacts(df, schema, table, column, current_depth, lineage_graph, visited, impacted_columns_lower, type_changes):
    impacts = []
    impacted_tables = set()
    next_source_table = f"{schema}.{table}".lower()

    downstream_steps = df[df['Source Table'].str.lower() == table]

    for _, row in downstream_steps.iterrows():
        source_column = row['Source Column'].lower()
        target_schema = row['Target Schema']
        target_table = row['Target Table']
        target_column = row['Target Column'].lower()
        target_column_type = row['Column Type'].lower() if isinstance(row['Column Type'], str) else row['Column Type']
        transformation = row['Transformation']

        next_target_table = f"{target_schema}.{target_table}".lower()

        # Only follow columns that are considered problematic (in impacted_columns_lower)
        if source_column in impacted_columns_lower:
            lineage_graph.add_edge(next_source_table, next_target_table)

            impacts.append({
                "source_table": next_source_table,
                "source_column": source_column,
                "target_table": target_table,
                "target_column": target_column,
                "target_column_type": target_column_type,
                "transformation": transformation,
                "depth": current_depth + 1
            })

            impacted_tables.add(target_table.lower())

            further_downstream_impacts, further_tables = trace_downstream_impacts(
                df, target_schema, target_table, target_column, current_depth + 1, lineage_graph, visited, impacted_columns_lower, type_changes
            )
            impacts.extend(further_downstream_impacts)
            impacted_tables.update(further_tables)

    return impacts, impacted_tables