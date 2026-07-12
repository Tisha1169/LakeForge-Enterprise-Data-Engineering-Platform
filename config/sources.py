"""Typed loader for config/sources/*.yaml.

Ingestion code is generic over `SourceConfig` — adding a new data source
means adding a YAML file here, not writing new Python.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel

SOURCES_DIR = Path(__file__).parent / "sources"


class RetryConfig(BaseModel):
    max_attempts: int = 3
    initial_backoff_seconds: float = 1.0
    max_backoff_seconds: float = 10.0


class SourceConfig(BaseModel):
    name: str
    source_type: Literal["api", "db", "file"]
    description: str = ""
    schedule: str = "@daily"
    retry: RetryConfig = RetryConfig()

    # api sources
    endpoint: str | None = None
    page_size: int = 100

    # db sources
    schema_name: str | None = None
    table: str | None = None

    # file sources
    file_pattern: str | None = None
    directory: str | None = None


def load_source_config(name: str) -> SourceConfig:
    path = SOURCES_DIR / f"{name}.yaml"
    with path.open() as f:
        raw = yaml.safe_load(f)
    return SourceConfig(**raw)


def list_source_configs() -> list[SourceConfig]:
    return [load_source_config(p.stem) for p in sorted(SOURCES_DIR.glob("*.yaml"))]
