from __future__ import annotations

import json
from pathlib import Path

from lineage.cli import output_impacted_lineage

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"


def test_output_impacted_lineage_generates_expected_artifacts(tmp_path: Path) -> None:
    result = output_impacted_lineage(
        mapping_csv_path=DATA_DIR / "Source_to_Target_Mapping.csv",
        source_entities_csv_path=DATA_DIR / "source_entities.csv",
        invalid_mappings_path=DATA_DIR / "invalid_mappings.json",
        output_dir=tmp_path,
        analyze_missing=True,
    )

    impact_analysis_path = Path(result["impact_analysis_path"])
    impacted_tables_path = Path(result["impacted_tables_path"])
    models_dir = Path(result["dbt_models_dir"])

    assert impact_analysis_path.exists()
    assert impacted_tables_path.exists()
    assert (models_dir / "sources.yml").exists()

    impact_analysis = json.loads(impact_analysis_path.read_text())
    impacted_tables = json.loads(impacted_tables_path.read_text())

    assert impact_analysis, "Expected sample data to produce lineage analysis output"
    assert "impacted_tables" in impacted_tables
    assert isinstance(impacted_tables["impacted_tables"], list)
    assert any(models_dir.glob("*.sql")), "Expected dbt model files to be generated"
