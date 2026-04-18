"""Тонкий HTTP-клиент API-Football.

Авторизация — через заголовок x-apisports-key (direct-домен api-sports.io).
После каждого запроса читаем заголовок x-ratelimit-requests-remaining и
синхронизируем локальный счётчик — серверу доверяем больше, чем себе.
"""
from __future__ import annotations

import logging
from typing import Any

import requests

from ingestion import config, rate_limit

log = logging.getLogger(__name__)


class ApiFootballClient:
    def __init__(self, api_key: str | None = None, timeout: int = 60):
        self.api_key = api_key or config.API_FOOTBALL_KEY
        if not self.api_key:
            raise ValueError("API_FOOTBALL_KEY пустой — задай в .env")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"x-apisports-key": self.api_key})

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        rate_limit.check_and_reserve(1)
        url = f"{config.API_FOOTBALL_BASE_URL}{path}"
        log.info("GET %s params=%s", url, params)
        resp = self.session.get(url, params=params, timeout=self.timeout)

        remaining = resp.headers.get("x-ratelimit-requests-remaining")
        if remaining is not None:
            try:
                rate_limit.sync_remaining(int(remaining))
            except ValueError:
                pass

        resp.raise_for_status()
        data = resp.json()
        errors = data.get("errors")
        # API возвращает errors и как dict, и как list — нормализуем
        if errors and (isinstance(errors, list) and len(errors) > 0
                       or isinstance(errors, dict) and len(errors) > 0):
            raise RuntimeError(f"API-Football вернул ошибки: {errors}")
        return data

    # ---------- эндпоинты Этапа 1 (MVP) ----------

    def get_leagues(self, league_id: int, season: int) -> dict[str, Any]:
        return self._get("/leagues", {"id": league_id, "season": season})

    def get_teams(self, league_id: int, season: int) -> dict[str, Any]:
        return self._get("/teams", {"league": league_id, "season": season})

    def get_fixtures(self, league_id: int, season: int) -> dict[str, Any]:
        return self._get("/fixtures", {"league": league_id, "season": season})

    def get_standings(self, league_id: int, season: int) -> dict[str, Any]:
        return self._get("/standings", {"league": league_id, "season": season})

    def get_topscorers(self, league_id: int, season: int) -> dict[str, Any]:
        return self._get("/players/topscorers", {"league": league_id, "season": season})