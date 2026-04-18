"""Запись JSON в MinIO с Hive-style партиционированием.

Ключ строится как:
    source=<src>/endpoint=<ep>/league_id=<lid>/season=<s>/dt=<YYYY-MM-DD>/<filename>.json

Идемпотентность: один и тот же (endpoint, dt, filename) перезапишет объект —
это ок для ежедневного snapshot'а: последний прогон побеждает.
"""
from __future__ import annotations

import io
import json
import logging
from datetime import datetime, timezone
from typing import Any

from minio import Minio

from ingestion import config

log = logging.getLogger(__name__)


def _client() -> Minio:
    return Minio(
        config.MINIO_ENDPOINT,
        access_key=config.MINIO_ACCESS_KEY,
        secret_key=config.MINIO_SECRET_KEY,
        secure=config.MINIO_SECURE,
    )


def build_object_key(
    *,
    source: str,
    endpoint: str,
    league_id: int | str | None,
    season: int | str | None,
    dt: str,
    filename: str,
) -> str:
    parts = [f"source={source}", f"endpoint={endpoint}"]
    if league_id is not None:
        parts.append(f"league_id={league_id}")
    if season is not None:
        parts.append(f"season={season}")
    parts.append(f"dt={dt}")
    parts.append(filename)
    return "/".join(parts)


def put_json(
    *,
    bucket: str,
    object_key: str,
    payload: Any,
) -> str:
    """Пишет payload как JSON в MinIO, возвращает полный путь s3a://bucket/key."""
    client = _client()
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    client.put_object(
        bucket_name=bucket,
        object_name=object_key,
        data=io.BytesIO(body),
        length=len(body),
        content_type="application/json",
    )
    full = f"s3a://{bucket}/{object_key}"
    log.info("wrote %s (%d bytes)", full, len(body))
    return full


def today_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")