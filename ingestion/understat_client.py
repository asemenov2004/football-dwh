"""Синхронная обёртка над async understat (aiohttp + Understat.com).

Understat даёт xG/xA/npxG/PPDA для топ-5 лиг без ключей и без Selenium.
Сезон передаётся как год начала: 2025 = сезон 2025-26.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import aiohttp
import understat as us

log = logging.getLogger(__name__)


async def _fetch(coro_fn, *args) -> Any:
    async with aiohttp.ClientSession() as session:
        client = us.Understat(session)
        return await coro_fn(client, *args)


def get_league_players(league: str, season: int) -> list[dict[str, Any]]:
    """Сезонная статистика всех игроков лиги (xG, xA, npxG, xGChain, ...)."""
    async def _call(client, league, season):
        return await client.get_league_players(league, season)
    result = asyncio.run(_fetch(_call, league, season))
    log.info("understat/%s/%d: %d игроков", league, season, len(result))
    return result


def get_league_table(league: str, season: int) -> list[dict[str, Any]]:
    """Сезонная таблица команд с xG, NPxG, xGA, PPDA, OPPDA, xPTS.

    Understat возвращает list-of-lists где первая строка — заголовки.
    Конвертируем в list[dict].
    """
    async def _call(client, league, season):
        return await client.get_league_table(league, season)
    raw = asyncio.run(_fetch(_call, league, season))
    if not raw:
        return []
    headers = raw[0]
    rows = [dict(zip(headers, row)) for row in raw[1:]]
    log.info("understat/%s/%d: %d команд", league, season, len(rows))
    return rows