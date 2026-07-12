-- Runs automatically on first container start (mounted into /docker-entrypoint-initdb.d).
-- Source system schemas (sales, inventory) and seed data are added in Phase 5.
CREATE EXTENSION IF NOT EXISTS pgcrypto;
