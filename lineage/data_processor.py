import pandas as pd

def load_csv(file_path):
    return pd.read_csv(file_path)

def get_missing_columns_impact(df1, df2):
    distinct_tables = df1['TableName'].dropna().unique()
    df2['Source Table'] = df2['Source Table'].str.lower()
    missing_columns = {}

    for table in distinct_tables:
        source_info = df2[df2['Source Table'].str.endswith(table.lower(), na=False)][['Source Column', 'Target Table', 'Transformation']].dropna()
        existing_columns = df1[df1['TableName'] == table]['ColumnName'].dropna().unique()
        existing_columns = [col.lower() for col in existing_columns]

        for _, row in source_info.iterrows():
            source_column = row['Source Column'].strip().lower()
            if ' ' not in source_column and '(' not in source_column and '.' not in source_column:
                target_table = row['Target Table']
                transformation = row['Transformation']
                if not transformation.startswith('Can be ignored') and source_column not in existing_columns:
                    if target_table not in missing_columns:
                        missing_columns[target_table] = []
                    missing_columns[target_table].append({"Missing Column": source_column, "Source Table": table, "Transformation": transformation})

    return missing_columns