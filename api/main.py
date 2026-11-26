"""Uvicorn/FastAPI entrypoint (`uvicorn api.main:app`) with automatic warehouse bootstrap."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from pipeline.runner import run_full_pipeline
from semantic_layer.api import SemanticAPIHandler, create_app
from shared_models import PlatformConfig


def ensure_warehouse_bootstrapped(config: PlatformConfig | None = None) -> PlatformConfig:
    """Populate the local SQLite warehouse on cold start if mart_revenue_analytics is missing."""
    cfg = config or PlatformConfig.from_env()
    db_file = Path(cfg.warehouse_sqlite_path)
    needs_seed = not db_file.exists()
    if not needs_seed:
        try:
            with sqlite3.connect(cfg.warehouse_sqlite_path) as conn:
                row = conn.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='mart_revenue_analytics'"
                ).fetchone()
                needs_seed = row is None
        except sqlite3.Error:
            needs_seed = True

    if needs_seed:
        run_full_pipeline(config=cfg, test_corrupted_contract=False)
    return cfg


_cfg = ensure_warehouse_bootstrapped()
app = create_app(_cfg)
handler = SemanticAPIHandler(config=_cfg)
