# data-lineage-analyzer

Small Python utility for analyzing source-to-target lineage mappings, tracing downstream impact from invalid source columns, and generating lightweight dbt scaffolding from the discovered graph.

## What it does

- loads mapping and source-entity metadata from CSV files
- loads invalid/type-changed column definitions from JSON
- traces downstream impacts for affected columns
- writes JSON reports with lineage and impacted tables
- generates a simple `sources.yml` plus dbt model SQL files for discovered downstream nodes

## Repository layout

- `lineage/` — package code
- `data/` — sample input files used by the repo today
- `output/` — sample generated artifacts already committed in the repo
- `tests/` — smoke/integration-style validation against the sample inputs
- `main.py` — compatibility wrapper that calls the packaged CLI

## Requirements

- Python 3.9+
- [`uv`](https://docs.astral.sh/uv/) recommended for environment and dependency management

## Quick start with uv

```bash
uv sync --extra dev
uv run data-lineage-analyzer \
  --mapping_csv_path ./data/Source_to_Target_Mapping.csv \
  --source_entities_csv_path ./data/source_entities.csv \
  --invalid_mappings_path ./data/invalid_mappings.json \
  --output_dir ./output/generated \
  --analyze_missing
```

## Alternative: plain pip

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
python main.py \
  --mapping_csv_path ./data/Source_to_Target_Mapping.csv \
  --source_entities_csv_path ./data/source_entities.csv \
  --invalid_mappings_path ./data/invalid_mappings.json \
  --output_dir ./output/generated \
  --analyze_missing
```

## CLI options

Run `uv run data-lineage-analyzer --help` to see the full generated help text.

```text
--mapping_csv_path         Path to the source-to-target mapping CSV
--source_entities_csv_path Path to the source entities CSV
--invalid_mappings_path    Path to the invalid mappings JSON
--output_dir               Directory to write JSON reports and generated dbt models
--analyze_missing          Also report missing source columns referenced by mappings
```

## Outputs

Running the tool writes:

- `impact_analysis.json` — per-source lineage summary, `type_changes` summary for the affected source columns, and direct/downstream impact records
- `impacted_tables.json` — impacted table names
- `dbt_sample_project/models/sources.yml` — dbt source definitions for source tables
- `dbt_sample_project/models/*.sql` — generated model stubs for downstream nodes

## Development

Install dev dependencies and run the checks:

```bash
uv sync --extra dev
uv run pytest
uv run ruff check .
```

## Notes and limitations

- The current dbt SQL generation is intentionally lightweight and emits simple `select *` model stubs.
- Impact propagation is still based primarily on matching impacted column names across lineage steps; it is useful for coarse analysis, but not yet a full semantic lineage engine.
- `type_changes` metadata is now surfaced in `impact_analysis.json` for each affected source table and on direct impact records, but propagation logic still remains intentionally simple.
