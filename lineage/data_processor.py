from __future__ import annotations

from pathlib import Path

import pandas as pd


def load_csv(file_path: str | Path) -> pd.DataFrame:
    return pd.read_csv(file_path)


def get_missing_columns_impact(source_entities_df: pd.DataFrame, mapping_df: pd.DataFrame) -> dict:
    distinct_tables = source_entities_df["TableName"].dropna().unique()
    working_mappings = mapping_df.copy()
    working_mappings["Source Table"] = working_mappings["Source Table"].str.lower()
    missing_columns: dict[str, list[dict[str, str]]] = {}

    for table in distinct_tables:
        source_info = working_mappings[
            working_mappings["Source Table"].str.endswith(table.lower(), na=False)
        ][["Source Column", "Target Table", "Transformation"]].dropna()
        existing_columns = source_entities_df[source_entities_df["TableName"] == table][
            "ColumnName"
        ].dropna().unique()
        existing_columns_lower = {col.lower() for col in existing_columns}

        for _, row in source_info.iterrows():
            source_column = row["Source Column"].strip().lower()
            if " " in source_column or "(" in source_column or "." in source_column:
                continue

            target_table = row["Target Table"]
            transformation = row["Transformation"]
            if (
                not transformation.startswith("Can be ignored")
                and source_column not in existing_columns_lower
            ):
                missing_columns.setdefault(target_table, []).append(
                    {
                        "Missing Column": source_column,
                        "Source Table": table,
                        "Transformation": transformation,
                    }
                )

    return missing_columns
