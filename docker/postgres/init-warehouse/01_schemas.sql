-- Reserves the warehouse schemas. Tables are added in later phases:
--   gold      -> Phase 10 (star schema fact/dim tables) / Phase 13 (dbt models)
--   metadata  -> Phase 15 (pipeline run tracking, lineage, freshness)
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS metadata;
