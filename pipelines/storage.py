"""Shared MinIO/S3 client used by every pipeline layer.

No other module should construct its own boto3 client or hardcode a bucket
name — go through `get_client()` and the `LakeLayer` enum here instead, so
switching MinIO for real S3/GCS/Azure Blob later is a config change, not a
code change across every pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import StrEnum

import boto3
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from config.settings import settings


class LakeLayer(StrEnum):
    LANDING = "landing"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


_LAYER_BUCKETS = {
    LakeLayer.LANDING: lambda: settings.minio_bucket_landing,
    LakeLayer.BRONZE: lambda: settings.minio_bucket_bronze,
    LakeLayer.SILVER: lambda: settings.minio_bucket_silver,
    LakeLayer.GOLD: lambda: settings.minio_bucket_gold,
}


def bucket_for(layer: LakeLayer) -> str:
    return _LAYER_BUCKETS[layer]()


def get_client() -> BaseClient:
    endpoint = settings.minio_endpoint
    scheme = "https" if settings.minio_secure else "http"
    return boto3.client(
        "s3",
        endpoint_url=f"{scheme}://{endpoint}",
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
    )


@dataclass(frozen=True)
class ObjectKey:
    """Builds the standard `{source}/{table}/{partition}/{filename}` object key.

    Partitioning by date is the default (matches how Bronze/Silver/Gold are
    partitioned on disk); pass `partition` explicitly for other schemes.
    """

    source: str
    table: str
    filename: str
    partition: str | None = None
    batch_date: date | None = None

    def __str__(self) -> str:
        partition = self.partition or (
            f"batch_date={self.batch_date.isoformat()}" if self.batch_date else None
        )
        parts = [self.source, self.table]
        if partition:
            parts.append(partition)
        parts.append(self.filename)
        return "/".join(parts)


def ensure_bucket(layer: LakeLayer) -> None:
    client = get_client()
    bucket = bucket_for(layer)
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        client.create_bucket(Bucket=bucket)


def put_bytes(
    layer: LakeLayer, key: ObjectKey, data: bytes, content_type: str = "application/octet-stream"
) -> str:
    client = get_client()
    bucket = bucket_for(layer)
    object_key = str(key)
    client.put_object(Bucket=bucket, Key=object_key, Body=data, ContentType=content_type)
    return f"s3://{bucket}/{object_key}"


def get_bytes(layer: LakeLayer, key: ObjectKey) -> bytes:
    client = get_client()
    response = client.get_object(Bucket=bucket_for(layer), Key=str(key))
    return response["Body"].read()


def list_objects(layer: LakeLayer, prefix: str = "") -> list[str]:
    client = get_client()
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket_for(layer), Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys


def object_exists(layer: LakeLayer, key: ObjectKey) -> bool:
    client = get_client()
    try:
        client.head_object(Bucket=bucket_for(layer), Key=str(key))
        return True
    except ClientError as exc:
        if exc.response["Error"]["Code"] in ("404", "NoSuchKey"):
            return False
        raise
