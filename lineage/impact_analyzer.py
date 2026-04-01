from __future__ import annotations

from collections import deque
from typing import Any

import pandas as pd

from lineage.lineage_graph import LineageGraph


def trace_lineage_with_impact(
    df: pd.DataFrame,
    initial_source_table: str,
    impacted_columns: list[str],
    lineage_graph: LineageGraph,
    type_changes: dict[str, dict[str, str]],
) -> tuple[list[dict[str, Any]], set[str]]:
    distinct_targets = set()
    queue = deque([(initial_source_table.lower(), 0)])
    visited = set()
    impacts: list[dict[str, Any]] = []
    impacted_tables: set[str] = set()

    impacted_columns_lower = [col.lower() for col in impacted_columns]

    while queue:
        current_source_table, depth = queue.popleft()

        if current_source_table in visited:
            continue
        visited.add(current_source_table)

        if current_source_table not in distinct_targets:
            distinct_targets.add(current_source_table)

        current_source_table_name = current_source_table.split(".")[-1]
        next_steps = df[df["Source Table"].str.lower() == current_source_table_name]

        for _, row in next_steps.iterrows():
            source_column = row["Source Column"].lower()
            target_schema = row["Target Schema"]
            target_table = row["Target Table"]
            target_column = row["Target Column"].lower()
            target_column_type = (
                row["Column Type"].lower()
                if isinstance(row["Column Type"], str)
                else row["Column Type"]
            )
            transformation = row["Transformation"]

            next_source_table = f"{target_schema}.{target_table}".lower()
            lineage_graph.add_edge(current_source_table, next_source_table)

            if source_column in impacted_columns_lower:
                impact_record: dict[str, Any] = {
                    "source_table": current_source_table,
                    "source_column": source_column,
                    "target_table": target_table,
                    "target_column": target_column,
                    "target_column_type": target_column_type,
                    "transformation": transformation,
                    "depth": depth + 1,
                }
                if source_column in type_changes:
                    impact_record["type_change"] = type_changes[source_column]
                impacts.append(impact_record)

                impacted_tables.add(target_table.lower())

                downstream_impacts, downstream_tables = trace_downstream_impacts(
                    df=df,
                    schema=target_schema,
                    table=target_table,
                    column=source_column,
                    current_depth=depth + 1,
                    lineage_graph=lineage_graph,
                    visited=visited,
                    impacted_columns_lower=impacted_columns_lower,
                    type_changes=type_changes,
                )
                impacts.extend(downstream_impacts)
                impacted_tables.update(downstream_tables)

            if next_source_table not in visited:
                queue.append((next_source_table, depth + 1))

    return impacts, impacted_tables


def trace_downstream_impacts(
    df: pd.DataFrame,
    schema: str,
    table: str,
    column: str,
    current_depth: int,
    lineage_graph: LineageGraph,
    visited: set[str],
    impacted_columns_lower: list[str],
    type_changes: dict[str, Any],
) -> tuple[list[dict[str, Any]], set[str]]:
    del visited, type_changes  # Kept in signature for backward-compatible extension.

    impacts: list[dict[str, Any]] = []
    impacted_tables: set[str] = set()
    next_source_table = f"{schema}.{table}".lower()

    downstream_steps = df[df["Source Table"].str.lower() == table]

    for _, row in downstream_steps.iterrows():
        source_column = row["Source Column"].lower()
        target_schema = row["Target Schema"]
        target_table = row["Target Table"]
        target_column = row["Target Column"].lower()
        target_column_type = (
            row["Column Type"].lower()
            if isinstance(row["Column Type"], str)
            else row["Column Type"]
        )
        transformation = row["Transformation"]

        next_target_table = f"{target_schema}.{target_table}".lower()

        if source_column in impacted_columns_lower:
            lineage_graph.add_edge(next_source_table, next_target_table)
            impacts.append(
                {
                    "source_table": next_source_table,
                    "source_column": source_column,
                    "target_table": target_table,
                    "target_column": target_column,
                    "target_column_type": target_column_type,
                    "transformation": transformation,
                    "depth": current_depth + 1,
                }
            )
            impacted_tables.add(target_table.lower())

            further_downstream_impacts, further_tables = trace_downstream_impacts(
                df=df,
                schema=target_schema,
                table=target_table,
                column=target_column,
                current_depth=current_depth + 1,
                lineage_graph=lineage_graph,
                visited=set(),
                impacted_columns_lower=impacted_columns_lower,
                type_changes={},
            )
            impacts.extend(further_downstream_impacts)
            impacted_tables.update(further_tables)

    return impacts, impacted_tables
