"""SQLAlchemy Core table definitions mirroring
docker/postgres/init-warehouse/02_schema_metadata.sql — kept manually in
sync (no Alembic yet; a natural next step once this schema needs to evolve
under active use, rather than a raw SQL init script).

Deliberately dialect-portable (no Postgres-only types like native UUID with
a server-side default) so `create_all()` works against both real Postgres
and an in-memory SQLite engine — the latter is what lets metadata/client.py
be tested with real SQL execution, not just mocks, without a live Postgres.
run_id is generated in Python (str(uuid.uuid4())) rather than a DB-side
default for the same reason.
"""

from __future__ import annotations

from sqlalchemy import JSON, Column, Date, DateTime, Integer, MetaData, String, Table, Text

metadata_obj = MetaData(schema="metadata")

pipeline_runs = Table(
    "pipeline_runs",
    metadata_obj,
    Column("run_id", String(36), primary_key=True),
    Column("pipeline_name", String, nullable=False),
    Column("layer", String, nullable=False),
    Column("source_name", String, nullable=False),
    Column("table_name", String, nullable=False),
    Column("batch_date", Date, nullable=False),
    Column("status", String, nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("row_count", Integer),
    Column("error_message", Text),
)

table_freshness = Table(
    "table_freshness",
    metadata_obj,
    Column("layer", String, primary_key=True),
    Column("table_name", String, primary_key=True),
    Column("last_successful_batch_date", Date, nullable=False),
    Column("last_updated_at", DateTime(timezone=True), nullable=False),
    Column("last_row_count", Integer),
)

schema_versions = Table(
    "schema_versions",
    metadata_obj,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("layer", String, nullable=False),
    Column("table_name", String, nullable=False),
    Column("columns", JSON, nullable=False),
    Column("detected_at", DateTime(timezone=True), nullable=False),
)

table_ownership = Table(
    "table_ownership",
    metadata_obj,
    Column("layer", String, primary_key=True),
    Column("table_name", String, primary_key=True),
    Column("owner_team", String, nullable=False),
    Column("owner_contact", String),
    Column("description", Text),
)

lineage = Table(
    "lineage",
    metadata_obj,
    Column("layer", String, primary_key=True),
    Column("table_name", String, primary_key=True),
    Column("upstream_layer", String, primary_key=True),
    Column("upstream_table_name", String, primary_key=True),
)


def to_connectable(engine):
    """Returns an engine safe to run any of this module's Table objects
    against, regardless of dialect.

    SQLite has no concept of a Postgres-style schema (`metadata.pipeline_runs`
    fails outright: `unknown database metadata` — hit this directly before
    settling on the fix below). `schema_translate_map` is SQLAlchemy's own
    supported mechanism for this: it remaps the "metadata" schema to "no
    schema" for any engine that isn't Postgres, at the connection level,
    without touching the Table definitions above at all. Idempotent to call
    on an already-translated engine.

    Every caller that touches these tables — create_all below,
    metadata/client.py, and tests that query tables directly — goes through
    this one function rather than each reimplementing the same check.
    """
    if engine.dialect.name != "postgresql":
        return engine.execution_options(schema_translate_map={"metadata": None})
    return engine


def create_all(engine) -> None:
    """Provisions the metadata tables against `engine`. Idempotent
    (create_all skips tables that already exist). Used by tests (SQLite) —
    production Postgres gets these tables from
    docker/postgres/init-warehouse/02_schema_metadata.sql at container
    startup instead, so this is not called in the normal pipeline path.
    """
    metadata_obj.create_all(to_connectable(engine))
