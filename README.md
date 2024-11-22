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

1. **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

### Command-Line Arguments

The main script accepts several arguments to customize its behavior:

- `--mapping_csv_path`: Path to the Source to Target Mapping CSV file.
- `--source_entities_csv_path`: Path to the Source Entities CSV file.
- `--invalid_mappings_path`: Path to the Invalid Mappings JSON file.
- `--output_dir`: Directory to store the output JSON files and DBT models.
- `--analyze_missing`: Optional flag to analyze the impact of missing columns.

### Example Command

```bash
python main.py --mapping_csv_path ./data/Source_to_Target_Mapping.csv --source_entities_csv_path ./data/source_entities.csv --invalid_mappings_path ./data/invalid_mappings.json --output_dir ./output --analyze_missing