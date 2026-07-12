# metadata/

Lightweight custom metadata layer (Phase 15) — not a full data catalog, but
enough to answer the questions a data platform team actually gets asked:

- Pipeline run history (start/end time, status, rows processed)
- Table freshness (when was this table last successfully updated)
- Schema version history (when did a table's schema last change, and how)
- Ownership (which team/pipeline owns which table)
- Basic lineage (which upstream tables/sources fed a given table)

Implemented as a small Postgres schema plus a Python client library that
pipeline code calls at the start/end of each run to record execution facts.
Deliberately kept lightweight rather than adopting a heavyweight catalog
(Hive Metastore / OpenMetadata / Unity Catalog) — that's called out in the
docs as a natural future upgrade once the platform outgrows this.
