#!/bin/sh
# Creates the landing/bronze/silver/gold buckets on the local MinIO instance.
# Idempotent: safe to run every startup.
set -eu

mc alias set local "http://minio:9000" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}"

for bucket in "${MINIO_BUCKET_LANDING}" "${MINIO_BUCKET_BRONZE}" "${MINIO_BUCKET_SILVER}" "${MINIO_BUCKET_GOLD}"; do
  mc mb --ignore-existing "local/${bucket}"
done

# Bronze is the permanent immutable record: enable versioning so an
# accidental overwrite creates a new version instead of losing data.
mc version enable "local/${MINIO_BUCKET_BRONZE}"

# Landing is transient staging (files live here only until ingested into
# Bronze) — auto-expire after 7 days so it never grows unbounded.
mc ilm rule add --expire-days 7 "local/${MINIO_BUCKET_LANDING}"

echo "MinIO buckets ready: ${MINIO_BUCKET_LANDING}, ${MINIO_BUCKET_BRONZE}, ${MINIO_BUCKET_SILVER}, ${MINIO_BUCKET_GOLD}"
