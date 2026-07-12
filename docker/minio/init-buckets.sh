#!/bin/sh
# Creates the landing/bronze/silver/gold buckets on the local MinIO instance.
# Idempotent: safe to run every startup.
set -eu

mc alias set local "http://minio:9000" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}"

for bucket in "${MINIO_BUCKET_LANDING}" "${MINIO_BUCKET_BRONZE}" "${MINIO_BUCKET_SILVER}" "${MINIO_BUCKET_GOLD}"; do
  mc mb --ignore-existing "local/${bucket}"
done

echo "MinIO buckets ready: ${MINIO_BUCKET_LANDING}, ${MINIO_BUCKET_BRONZE}, ${MINIO_BUCKET_SILVER}, ${MINIO_BUCKET_GOLD}"
