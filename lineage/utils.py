from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def save_json(file_path: str | Path, data: Any) -> None:
    path = Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as json_file:
        json.dump(data, json_file, indent=4)
        json_file.write("\n")
