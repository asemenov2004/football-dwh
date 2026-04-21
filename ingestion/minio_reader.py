"""Чтение JSON из MinIO.

Парный модуль к minio_writer.py. Stage-loader читает daily snapshot
за конкретную dt-партицию: `source=.../dt=YYYY-MM-DD/...`.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Iterator

from minio import Minio
from minio.error import S3Error

from ingestion import config

log = logging.getLogger(__name__)


def _client() -> Minio:
    return Minio(
        config.MINIO_ENDPOINT,
        access_key=config.MINIO_ACCESS_KEY,
        secret_key=config.MINIO_SECRET_KEY,
        secure=config.MINIO_SECURE,
    )


def list_objects(bucket: str, prefix: str) -> Iterator[str]:
    """Возвращает object_key'и всех объектов под prefix (рекурсивно)."""
    client = _client()
    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        yield obj.object_name


def get_json(bucket: str, object_key: str) -> Any:
    """Читает объект и парсит его как JSON. None, если объекта нет."""
    client = _client()
    try:
        resp = client.get_object(bucket, object_key)
    except S3Error as e:
        if e.code in ("NoSuchKey", "NoSuchBucket"):
            log.warning("minio: нет объекта %s/%s", bucket, object_key)
            return None
        raise
    try:
        return json.loads(resp.read().decode("utf-8"))
    finally:
        resp.close()
        resp.release_conn()
