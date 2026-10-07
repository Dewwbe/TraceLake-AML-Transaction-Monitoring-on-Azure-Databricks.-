"""Runtime settings and YAML configuration loading."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class Settings:
    """Environment-driven settings. Defaults give a fully local run."""

    store: str = "parquet"  # "parquet" (local) | "delta" (Databricks, Unity Catalog)
    catalog: str = "aml"
    base_path: str = "./data/lake"
    landing_path: str = "./data/landing"
    checkpoint_path: str = "./data/checkpoints"
    config_dir: str = "./config"
    reference_dir: str = "./reference"
    source_profile: str = "saml_d"
    use_autoloader: bool = False

    def config_file(self, *parts: str) -> Path:
        return Path(self.config_dir).joinpath(*parts)


def load_settings() -> Settings:
    env = os.environ.get
    return Settings(
        store=env("AML_STORE", "parquet"),
        catalog=env("AML_CATALOG", "aml"),
        base_path=env("AML_BASE_PATH", "./data/lake"),
        landing_path=env("AML_LANDING_PATH", "./data/landing"),
        checkpoint_path=env("AML_CHECKPOINT_PATH", "./data/checkpoints"),
        config_dir=env("AML_CONFIG_DIR", "./config"),
        reference_dir=env("AML_REFERENCE_DIR", "./reference"),
        source_profile=env("AML_SOURCE_PROFILE", "saml_d"),
        use_autoloader=env("AML_USE_AUTOLOADER", "false").lower() == "true",
    )


def load_yaml(path: str | Path) -> dict[str, Any]:
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data or {}
