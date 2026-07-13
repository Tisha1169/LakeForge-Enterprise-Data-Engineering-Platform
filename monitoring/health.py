"""Pipeline health checks, built on top of metadata/client.py — "is any
pipeline stale or failing right now," the actual question a platform team
gets paged for, answered by reading the metadata tables Phase 15 built
rather than eyeballing individual pipeline logs.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from metadata.client import get_freshness, list_recent_failed_runs
from sqlalchemy import Engine

from monitoring.logging_config import get_logger

logger = get_logger(__name__)

DEFAULT_MAX_STALENESS_HOURS = 30  # a daily pipeline that's >30h stale missed a run
DEFAULT_FAILURE_LOOKBACK_HOURS = 24


@dataclass
class TableHealth:
    layer: str
    table_name: str
    status: str  # "healthy" | "stale" | "failing" | "never_run"
    last_successful_batch_date: str | None = None
    hours_since_update: float | None = None
    recent_failure_count: int = 0
    message: str = ""

    @property
    def is_healthy(self) -> bool:
        return self.status == "healthy"


def check_table_health(
    engine: Engine,
    layer: str,
    table_name: str,
    max_staleness_hours: int = DEFAULT_MAX_STALENESS_HOURS,
    failure_lookback_hours: int = DEFAULT_FAILURE_LOOKBACK_HOURS,
    now: datetime | None = None,
) -> TableHealth:
    now = now or datetime.now(UTC)
    since = now - timedelta(hours=failure_lookback_hours)
    recent_failures = list_recent_failed_runs(engine, layer, table_name, since)

    freshness = get_freshness(engine, layer, table_name)
    if freshness is None:
        # Recent failures with no freshness record at all means every
        # attempt has failed outright, not just "hasn't run yet."
        status = "failing" if recent_failures else "never_run"
        return TableHealth(
            layer=layer,
            table_name=table_name,
            status=status,
            recent_failure_count=len(recent_failures),
            message="No successful run has ever been recorded for this table.",
        )

    last_updated_at = freshness["last_updated_at"]
    if last_updated_at.tzinfo is None:
        last_updated_at = last_updated_at.replace(tzinfo=UTC)
    hours_since_update = (now - last_updated_at).total_seconds() / 3600

    if recent_failures:
        status = "failing"
        message = f"{len(recent_failures)} failed run(s) in the last {failure_lookback_hours}h."
    elif hours_since_update > max_staleness_hours:
        status = "stale"
        message = f"Last updated {hours_since_update:.1f}h ago (threshold: {max_staleness_hours}h)."
    else:
        status = "healthy"
        message = f"Last updated {hours_since_update:.1f}h ago."

    health = TableHealth(
        layer=layer,
        table_name=table_name,
        status=status,
        last_successful_batch_date=str(freshness["last_successful_batch_date"]),
        hours_since_update=round(hours_since_update, 1),
        recent_failure_count=len(recent_failures),
        message=message,
    )
    if not health.is_healthy:
        logger.warning(
            "health.unhealthy_table",
            extra={
                "context": {
                    "layer": layer,
                    "table_name": table_name,
                    "status": status,
                    "message": message,
                }
            },
        )
    return health


def check_platform_health(
    engine: Engine,
    expected_tables: list[tuple[str, str]],
    max_staleness_hours: int = DEFAULT_MAX_STALENESS_HOURS,
    failure_lookback_hours: int = DEFAULT_FAILURE_LOOKBACK_HOURS,
    now: datetime | None = None,
) -> list[TableHealth]:
    """`expected_tables` is a list of (layer, table_name) pairs — the set of
    tables the platform is expected to be maintaining. Deliberately explicit
    rather than "every table metadata has ever seen," so a table that was
    decommissioned doesn't show up as perpetually stale."""
    now = now or datetime.now(UTC)
    results = [
        check_table_health(
            engine, layer, table_name, max_staleness_hours, failure_lookback_hours, now=now
        )
        for layer, table_name in expected_tables
    ]
    unhealthy = [r for r in results if not r.is_healthy]
    logger.info(
        "health.platform_check_complete",
        extra={"context": {"tables_checked": len(results), "unhealthy_count": len(unhealthy)}},
    )
    return results
