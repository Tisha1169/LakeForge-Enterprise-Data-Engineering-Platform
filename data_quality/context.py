"""Single GX context builder — a file-based context so Data Docs (HTML
reports) actually get written to disk, unlike an ephemeral context."""

from __future__ import annotations

import os
from pathlib import Path

import great_expectations as gx
from great_expectations.data_context import AbstractDataContext

DEFAULT_PROJECT_ROOT = Path(__file__).parent


def get_context() -> AbstractDataContext:
    # Overridable so tests write GX's generated project state to a scratch
    # directory instead of this package's own directory.
    project_root = os.environ.get("DATA_QUALITY_GX_ROOT", str(DEFAULT_PROJECT_ROOT))
    return gx.get_context(mode="file", project_root_dir=project_root)
