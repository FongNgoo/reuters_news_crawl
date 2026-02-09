# src/common/config.py

from pathlib import Path
from typing import Dict, Any

import yaml


def load_yaml(path: str | Path) -> Dict[str, Any]:
    """
    Load YAML config file safely.
    """
    path = Path(path)

    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)
