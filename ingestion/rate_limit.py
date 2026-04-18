"""Локальный счётчик запросов к API-Football.

Зачем: free tier — 100 req/день. API возвращает заголовок x-ratelimit-requests-remaining,
но он приходит только после запроса. Нам нужно уметь заранее понять, что лимит
закончится — чтобы DAG упал до запроса, а не после.

Счётчик живёт в JSON-файле на volume Airflow-логов (общий для webserver/scheduler).
При смене даты (UTC) счётчик сбрасывается.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

from ingestion import config

log = logging.getLogger(__name__)


class RateLimitExceeded(RuntimeError):
    pass


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _load() -> dict[str, Any]:
    path = config.API_FOOTBALL_USAGE_FILE
    if not os.path.exists(path):
        return {"date": _today(), "used": 0}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.warning("usage-file повреждён (%s), стартую с нуля", e)
        return {"date": _today(), "used": 0}
    if data.get("date") != _today():
        return {"date": _today(), "used": 0}
    return data


def _save(data: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(config.API_FOOTBALL_USAGE_FILE), exist_ok=True)
    with open(config.API_FOOTBALL_USAGE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f)


def check_and_reserve(n: int = 1) -> None:
    """Проверяет, что лимит позволяет сделать ещё n запросов, и резервирует их.

    Кидает RateLimitExceeded, если превышение.
    """
    data = _load()
    used = int(data.get("used", 0))
    if used + n > config.API_FOOTBALL_DAILY_LIMIT:
        raise RateLimitExceeded(
            f"API-Football daily limit: used={used}, "
            f"requested={n}, limit={config.API_FOOTBALL_DAILY_LIMIT}"
        )
    data["used"] = used + n
    _save(data)
    log.info("API-Football usage: %d/%d", data["used"], config.API_FOOTBALL_DAILY_LIMIT)


def sync_remaining(remaining: int) -> None:
    """Подправить счётчик по факту — если заголовок x-ratelimit-requests-remaining
    показывает меньше, чем мы думаем, — доверяем серверу."""
    data = _load()
    server_used = config.API_FOOTBALL_DAILY_LIMIT - remaining
    if server_used > int(data.get("used", 0)):
        data["used"] = server_used
        _save(data)
        log.info("usage-counter синхронизирован с сервером: %d", server_used)