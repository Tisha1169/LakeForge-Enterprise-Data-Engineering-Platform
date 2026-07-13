"""Metadata tracking client. Every function takes an explicit SQLAlchemy
`engine` (defaulting to `default_engine()`, built from
`settings.warehouse_db_url`) rather than opening its own connection per
call, so tests can pass an in-memory SQLite engine and exercise real SQL
execution instead of mocks.
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from functools import lru_cache

from config.settings import settings
from monitoring.logging_config import get_logger
from sqlalchemy import Engine, create_engine, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from metadata.schema import (
    lineage,
    pipeline_runs,
    schema_versions,
    table_freshness,
    table_ownership,
)
from metadata.schema import to_connectable as _connectable

logger = get_logger(__name__)


@lru_cache
def default_engine() -> Engine:
    return create_engine(settings.warehouse_db_url)


def start_run(
    engine: Engine,
    pipeline_name: str,
    layer: str,
    source_name: str,
    table_name: str,
    batch_date: date,
) -> str:
    run_id = str(uuid.uuid4())
    with _connectable(engine).begin() as conn:
        conn.execute(
            pipeline_runs.insert().values(
                run_id=run_id,
                pipeline_name=pipeline_name,
                layer=layer,
                source_name=source_name,
                table_name=table_name,
                batch_date=batch_date,
                status="running",
                started_at=datetime.now(UTC),
            )
        )
    return run_id


def complete_run(
    engine: Engine,
    run_id: str,
    status: str,
    row_count: int | None = None,
    error_message: str | None = None,
) -> None:
    with _connectable(engine).begin() as conn:
        conn.execute(
            pipeline_runs.update()
            .where(pipeline_runs.c.run_id == run_id)
            .values(
                status=status,
                finished_at=datetime.now(UTC),
                row_count=row_count,
                error_message=error_message,
            )
        )


def upsert_freshness(
    engine: Engine, layer: str, table_name: str, batch_date: date, row_count: int
) -> None:
    connectable = _connectable(engine)
    now = datetime.now(UTC)
    if engine.dialect.name == "postgresql":
        stmt = pg_insert(table_freshness).values(
            layer=layer,
            table_name=table_name,
            last_successful_batch_date=batch_date,
            last_updated_at=now,
            last_row_count=row_count,
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["layer", "table_name"],
            set_={
                "last_successful_batch_date": batch_date,
                "last_updated_at": now,
                "last_row_count": row_count,
            },
        )
        with connectable.begin() as conn:
            conn.execute(stmt)
        return

    # SQLite (tests): no portable upsert across both dialects worth the
    # complexity here, so delete-then-insert inside one transaction instead.
    with connectable.begin() as conn:
        conn.execute(
            table_freshness.delete().where(
                (table_freshness.c.layer == layer) & (table_freshness.c.table_name == table_name)
            )
        )
        conn.execute(
            table_freshness.insert().values(
                layer=layer,
                table_name=table_name,
                last_successful_batch_date=batch_date,
                last_updated_at=now,
                last_row_count=row_count,
            )
        )


def get_freshness(engine: Engine, layer: str, table_name: str) -> dict | None:
    with _connectable(engine).connect() as conn:
        row = (
            conn.execute(
                select(table_freshness).where(
                    (table_freshness.c.layer == layer)
                    & (table_freshness.c.table_name == table_name)
                )
            )
            .mappings()
            .first()
        )
    return dict(row) if row else None


def record_schema_version(engine: Engine, layer: str, table_name: str, columns: list[str]) -> bool:
    """Records a new schema version if `columns` (sorted, for a stable
    fingerprint) differs from the most recently recorded version — this IS
    schema drift detection: a Silver job whose Bronze source suddenly gains
    or loses a column shows up here as a new version, without anyone having
    to eyeball a diff. Returns True iff a new version was recorded."""
    sorted_columns = sorted(columns)
    connectable = _connectable(engine)
    with connectable.connect() as conn:
        latest = (
            conn.execute(
                select(schema_versions.c.columns)
                .where(
                    (schema_versions.c.layer == layer)
                    & (schema_versions.c.table_name == table_name)
                )
                .order_by(schema_versions.c.detected_at.desc())
                .limit(1)
            )
            .scalars()
            .first()
        )
    if latest is not None and sorted(latest) == sorted_columns:
        return False

    with connectable.begin() as conn:
        conn.execute(
            schema_versions.insert().values(
                layer=layer,
                table_name=table_name,
                columns=sorted_columns,
                detected_at=datetime.now(UTC),
            )
        )
    logger.info(
        "metadata.schema_change_detected",
        extra={"context": {"layer": layer, "table_name": table_name, "columns": sorted_columns}},
    )
    return True


def get_lineage(engine: Engine, layer: str, table_name: str) -> list[dict]:
    with _connectable(engine).connect() as conn:
        rows = (
            conn.execute(
                select(lineage).where(
                    (lineage.c.layer == layer) & (lineage.c.table_name == table_name)
                )
            )
            .mappings()
            .all()
        )
    return [dict(r) for r in rows]


def get_ownership(engine: Engine, layer: str, table_name: str) -> dict | None:
    with _connectable(engine).connect() as conn:
        row = (
            conn.execute(
                select(table_ownership).where(
                    (table_ownership.c.layer == layer)
                    & (table_ownership.c.table_name == table_name)
                )
            )
            .mappings()
            .first()
        )
    return dict(row) if row else None


def list_recent_failed_runs(
    engine: Engine, layer: str, table_name: str, since: datetime
) -> list[dict]:
    """Used by monitoring/health.py — a table can look "fresh" (a successful
    run happened recently) while also having had failures in between; both
    facts matter for health status, so this is a separate query rather than
    folded into get_freshness."""
    with _connectable(engine).connect() as conn:
        rows = (
            conn.execute(
                select(pipeline_runs)
                .where(
                    (pipeline_runs.c.layer == layer)
                    & (pipeline_runs.c.table_name == table_name)
                    & (pipeline_runs.c.status == "failed")
                    & (pipeline_runs.c.started_at >= since)
                )
                .order_by(pipeline_runs.c.started_at.desc())
            )
            .mappings()
            .all()
        )
    return [dict(r) for r in rows]


@dataclass
class RunHandle:
    run_id: str
    row_count: int | None = None
    columns: list[str] | None = field(default=None)


@contextmanager
def track_run(
    engine: Engine,
    pipeline_name: str,
    layer: str,
    source_name: str,
    table_name: str,
    batch_date: date,
):
    """Records a pipeline run's full lifecycle. The caller sets
    `run.row_count` (and optionally `run.columns`, for schema-drift
    tracking) on the yielded handle before the `with` block ends:

        with track_run(engine, "customers_silver", "silver", "customers", "customers", batch_date) as run:
            result = do_the_actual_work()
            run.row_count = result.row_count

    On success: records status=success, row_count, updates freshness, and
    (if columns were set) checks for schema drift. On any exception: records
    status=failed with the error message, then re-raises — metadata
    recording never swallows a real pipeline failure.
    """
    run_id = start_run(engine, pipeline_name, layer, source_name, table_name, batch_date)
    handle = RunHandle(run_id=run_id)
    try:
        yield handle
    except Exception as exc:
        complete_run(engine, run_id, status="failed", error_message=str(exc))
        raise
    else:
        complete_run(engine, run_id, status="success", row_count=handle.row_count)
        if handle.row_count is not None:
            upsert_freshness(engine, layer, table_name, batch_date, handle.row_count)
        if handle.columns is not None:
            record_schema_version(engine, layer, table_name, handle.columns)
