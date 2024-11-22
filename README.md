# DBT Lineage Impact

## Overview

This project provides a framework for analyzing data lineage and the impact of changes in source columns using DBT and DuckDB. It allows users to trace data flows through various stages and visualize the effects of column changes on downstream tables.

## Features

- Trace data lineage from source to target tables.
- Analyze the impact of column changes as specified in `invalid_mappings.json`.
- Generate DBT models and documentation.
- Serve DBT documentation locally.

## Prerequisites

- Python 3.x
- DuckDB
- DBT

## Installation

1. **Clone the Repository**:
    ```bash
    git clone <your-repo-url>
    cd dbt-lineage-impact
    ```

2. **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

3. **Install DuckDB and DBT**:
    ```bash
    pip install duckdb dbt-core dbt-duckdb
    ```

## Usage

1. **Initialize DBT Project**:
    Navigate to your DBT project directory and run:
    ```bash
    dbt init
    ```

2. **Configure DuckDB**:
    Set up your `profiles.yml` file in `~/.dbt/`:
    ```yaml
    duckdb:
      outputs:
        dev:
          type: duckdb
          path: ':memory:'
      target: dev
    ```

3. **Run the Analysis**:
    Execute the main script to generate lineage and models:
    ```bash
    python main.py --mapping_csv_path ./data/Source_to_Target_Mapping.csv --source_entities_csv_path ./data/source_entities.csv --invalid_mappings_path ./data/invalid_mappings.json --output_dir ./output --analyze_missing
    ```

4. **Generate and Serve DBT Docs**:
    ```bash
    dbt docs generate
    dbt docs serve
    ```

## Directory Structure

- `models/`: Contains generated DBT models.
- `data/`: Includes sample data files.
- `output/`: Stores generated outputs and DBT project structure.
- `lineage/`: Contains Python modules for lineage and impact analysis.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request for any improvements or bug fixes.