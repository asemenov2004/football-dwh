"""Загрузка API-Football JSON из MinIO в stage.af_*.

Одна функция на эндпоинт. Каждая функция:
  1. Проходит по 6 лигам за указанный сезон и dt.
  2. Читает соответствующий JSON из MinIO.
  3. Парсит payload["response"] в плоские строки + raw_payload (JSONB).
  4. Атомарно делает TRUNCATE + INSERT внутри одной транзакции.

Стратегия загрузки — full refresh: stage всегда отражает последний snapshot.
История накапливается на уровне Data Vault (Этап 3).
"""
from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any, Callable

from sqlalchemy import text

from ingestion import config
from ingestion.minio_reader import get_json
from ingestion.minio_writer import build_object_key
from stage.postgres import get_engine

log = logging.getLogger(__name__)


# ---------- helpers ----------

def _dt_str(dt: date) -> str:
    return dt.strftime("%Y-%m-%d")


def _object_key(endpoint: str, league_slug: str, season: int, dt: date) -> str:
    return build_object_key(
        source="api-football",
        endpoint=endpoint,
        league_id=league_slug,
        season=season,
        dt=_dt_str(dt),
        filename=f"{endpoint}.json",
    )


def _fetch_response(
    endpoint: str, league_slug: str, season: int, dt: date
) -> tuple[str, list[Any]]:
    """Возвращает (object_key, response_array). Если файла нет или response пуст,
    возвращает (object_key, []) и логирует WARNING — это валидный сценарий
    (например, StatsBomb не опубликовал данные для лиги)."""
    key = _object_key(endpoint, league_slug, season, dt)
    payload = get_json(config.RAW_API_FOOTBALL_BUCKET, key)
    if payload is None:
        log.warning("api-football/%s: файл %s отсутствует — skip", endpoint, key)
        return key, []
    response = payload.get("response") or []
    if not response:
        log.warning("api-football/%s: response пуст для %s — skip", endpoint, league_slug)
    return key, response


def _truncate_and_insert(table: str, insert_sql: str, rows: list[dict[str, Any]]) -> int:
    """TRUNCATE+INSERT в одной транзакции. Пустой rows тоже валиден — значит
    stage на эту дату должен быть очищен (нет свежих данных по всем лигам)."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table}"))
        if rows:
            conn.execute(text(insert_sql), rows)
    log.info("%s: загружено %d строк", table, len(rows))
    return len(rows)


def _load_endpoint(
    *,
    endpoint: str,
    dt: date,
    season: int,
    table: str,
    insert_sql: str,
    row_builder: Callable[[str, str, int, date, Any], dict[str, Any]],
    explode: Callable[[list[Any]], list[Any]] = lambda r: r,
) -> int:
    """Общий каркас: собираем строки со всех лиг, затем TRUNCATE+INSERT.

    row_builder(object_key, league_slug, season, dt, response_item) -> dict
    explode нужен там, где один объект response генерит несколько строк
    (например, standings — вложенные группы)."""
    rows: list[dict[str, Any]] = []
    for league in config.LEAGUES:
        slug = league["name"]
        key, response = _fetch_response(endpoint, slug, season, dt)
        for item in explode(response):
            rows.append(row_builder(key, slug, season, dt, item))
    return _truncate_and_insert(table, insert_sql, rows)


# ---------- leagues ----------

_LEAGUES_INSERT = """
INSERT INTO stage.af_leagues (league_id, season, dt, source_file, raw_payload)
VALUES (:league_id, :season, :dt, :source_file, CAST(:raw_payload AS JSONB))
"""


def _row_leagues(
    source_file: str, slug: str, season: int, dt: date, item: Any
) -> dict[str, Any]:
    return {
        "league_id": slug,
        "season": season,
        "dt": dt,
        "source_file": source_file,
        "raw_payload": json.dumps(item, ensure_ascii=False),
    }


def load_leagues(dt: date, season: int) -> int:
    return _load_endpoint(
        endpoint="leagues",
        dt=dt,
        season=season,
        table="stage.af_leagues",
        insert_sql=_LEAGUES_INSERT,
        row_builder=_row_leagues,
    )


# ---------- teams ----------

_TEAMS_INSERT = """
INSERT INTO stage.af_teams (team_id, league_id, season, dt, source_file, raw_payload)
VALUES (:team_id, :league_id, :season, :dt, :source_file, CAST(:raw_payload AS JSONB))
"""


def _row_teams(
    source_file: str, slug: str, season: int, dt: date, item: Any
) -> dict[str, Any]:
    team = item.get("team") or {}
    return {
        "team_id": team.get("id"),
        "league_id": slug,
        "season": season,
        "dt": dt,
        "source_file": source_file,
        "raw_payload": json.dumps(item, ensure_ascii=False),
    }


def load_teams(dt: date, season: int) -> int:
    return _load_endpoint(
        endpoint="teams",
        dt=dt,
        season=season,
        table="stage.af_teams",
        insert_sql=_TEAMS_INSERT,
        row_builder=_row_teams,
    )


# ---------- fixtures ----------

_FIXTURES_INSERT = """
INSERT INTO stage.af_fixtures
    (fixture_id, league_id, season, dt, event_date, home_id, away_id, source_file, raw_payload)
VALUES
    (:fixture_id, :league_id, :season, :dt, :event_date, :home_id, :away_id,
     :source_file, CAST(:raw_payload AS JSONB))
"""


def _row_fixtures(
    source_file: str, slug: str, season: int, dt: date, item: Any
) -> dict[str, Any]:
    fx = item.get("fixture") or {}
    teams = item.get("teams") or {}
    return {
        "fixture_id": fx.get("id"),
        "league_id": slug,
        "season": season,
        "dt": dt,
        # API возвращает ISO-8601 с timezone — Postgres сам распарсит в TIMESTAMPTZ
        "event_date": fx.get("date"),
        "home_id": (teams.get("home") or {}).get("id"),
        "away_id": (teams.get("away") or {}).get("id"),
        "source_file": source_file,
        "raw_payload": json.dumps(item, ensure_ascii=False),
    }


def load_fixtures(dt: date, season: int) -> int:
    return _load_endpoint(
        endpoint="fixtures",
        dt=dt,
        season=season,
        table="stage.af_fixtures",
        insert_sql=_FIXTURES_INSERT,
        row_builder=_row_fixtures,
    )


# ---------- standings ----------

_STANDINGS_INSERT = """
INSERT INTO stage.af_standings
    (league_id, season, team_id, dt, rank, points, source_file, raw_payload)
VALUES
    (:league_id, :season, :team_id, :dt, :rank, :points, :source_file,
     CAST(:raw_payload AS JSONB))
"""


def _explode_standings(response: list[Any]) -> list[Any]:
    """Структура: response[0].league.standings = [[row, row, ...]]. Обычно одна
    группа, но у некоторых турниров их несколько (например, UCL group stage),
    поэтому обходим все вложенные списки."""
    out: list[Any] = []
    for entry in response:
        league = entry.get("league") or {}
        for group in league.get("standings") or []:
            for row in group:
                out.append(row)
    return out


def _row_standings(
    source_file: str, slug: str, season: int, dt: date, item: Any
) -> dict[str, Any]:
    team = item.get("team") or {}
    return {
        "league_id": slug,
        "season": season,
        "team_id": team.get("id"),
        "dt": dt,
        "rank": item.get("rank"),
        "points": item.get("points"),
        "source_file": source_file,
        "raw_payload": json.dumps(item, ensure_ascii=False),
    }


def load_standings(dt: date, season: int) -> int:
    return _load_endpoint(
        endpoint="standings",
        dt=dt,
        season=season,
        table="stage.af_standings",
        insert_sql=_STANDINGS_INSERT,
        row_builder=_row_standings,
        explode=_explode_standings,
    )


# ---------- topscorers ----------

_TOPSCORERS_INSERT = """
INSERT INTO stage.af_topscorers
    (player_id, league_id, season, dt, team_id, goals, source_file, raw_payload)
VALUES
    (:player_id, :league_id, :season, :dt, :team_id, :goals, :source_file,
     CAST(:raw_payload AS JSONB))
"""


def _row_topscorers(
    source_file: str, slug: str, season: int, dt: date, item: Any
) -> dict[str, Any]:
    player = item.get("player") or {}
    # statistics — список (часто один элемент), берём первый для колонок,
    # но сохраняем item целиком в raw_payload
    stats = (item.get("statistics") or [{}])[0]
    goals_obj = stats.get("goals") or {}
    team = stats.get("team") or {}
    return {
        "player_id": player.get("id"),
        "league_id": slug,
        "season": season,
        "dt": dt,
        "team_id": team.get("id"),
        "goals": goals_obj.get("total"),
        "source_file": source_file,
        "raw_payload": json.dumps(item, ensure_ascii=False),
    }


def load_topscorers(dt: date, season: int) -> int:
    return _load_endpoint(
        endpoint="topscorers",
        dt=dt,
        season=season,
        table="stage.af_topscorers",
        insert_sql=_TOPSCORERS_INSERT,
        row_builder=_row_topscorers,
    )
