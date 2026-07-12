# monitoring/

Structured logging configuration and pipeline health checks (Phase 16).

- Centralized Python `logging` configuration (JSON-structured, level-aware:
  DEBUG/INFO/WARNING/ERROR/CRITICAL) imported by every pipeline module rather
  than each module configuring its own logger.
- Health-check utilities that query the `metadata/` tables to answer "is any
  pipeline stale or failing right now" — the basis for alerting.
