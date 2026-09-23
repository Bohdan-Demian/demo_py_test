from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


def load_validation_config(
    config_path: str | Path,
    *,
    entity: str | None = None,
    environment: str | None = None,
) -> dict[str, Any]:
    with Path(config_path).open("r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file) or {}

    if entity is not None:
        config["entity"] = entity

    if environment is not None:
        config["environment"] = environment

    return config
