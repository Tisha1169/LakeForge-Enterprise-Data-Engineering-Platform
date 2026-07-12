-- Runs automatically on first container start (mounted into /docker-entrypoint-initdb.d).
-- Warehouse (gold-layer serving) and metadata schemas are added in Phase 5 / Phase 15.
CREATE EXTENSION IF NOT EXISTS pgcrypto;
