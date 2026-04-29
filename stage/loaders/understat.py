"""Загрузка Understat JSON из MinIO в stage.understat_* Postgres.

Стратегия: UPSERT по (player_id/team_title, league_id, season).
Разные сезоны накапливаются, повторный запуск обновляет данные.
"""
from __future__ import annotations

import json
import logging
from datetime import date
from typing import Any

from sqlalchemy import text

from ingestion import config
from ingestion.minio_reader import get_json
from ingestion.minio_writer import build_object_key
from stage.postgres import get_engine

log = logging.getLogger(__name__)


def _dt_str(dt: date) -> str:
    return dt.strftime("%Y-%m-%d")


_PLAYERS_UPSERT = """
INSERT INTO stage.understat_players
    (player_id, player_name, league_id, season, dt, raw_payload)
VALUES
    (:player_id, :player_name, :league_id, :season, :dt,
     CAST(:raw_payload AS JSONB))
ON CONFLICT (player_id, league_id, season) DO UPDATE
    SET dt          = EXCLUDED.dt,
        player_name = EXCLUDED.player_name,
        raw_payload = EXCLUDED.raw_payload,
        loaded_at   = now()
"""

_TEAMS_UPSERT = """
INSERT INTO stage.understat_teams
    (team_title, league_id, season, dt, raw_payload)
VALUES
    (:team_title, :league_id, :season, :dt,
     CAST(:raw_payload AS JSONB))
ON CONFLICT (team_title, league_id, season) DO UPDATE
    SET dt          = EXCLUDED.dt,
        raw_payload = EXCLUDED.raw_payload,
        loaded_at   = now()
"""

_MATCHES_UPSERT = """
INSERT INTO stage.understat_matches
    (match_id, league_id, season, dt, raw_payload)
VALUES
    (:match_id, :league_id, :season, :dt,
     CAST(:raw_payload AS JSONB))
ON CONFLICT (match_id) DO UPDATE
    SET dt          = EXCLUDED.dt,
        league_id   = EXCLUDED.league_id,
        season      = EXCLUDED.season,
        raw_payload = EXCLUDED.raw_payload,
        loaded_at   = now()
"""


def load_players(dt: date, season: int) -> int:
    rows: list[dict[str, Any]] = []
    for league in config.UNDERSTAT_LEAGUES:
        slug = league["name"]
        key = build_object_key(
            source="understat",
            endpoint="players",
            league_id=slug,
            season=season,
            dt=_dt_str(dt),
            filename="players.json",
        )
        payload = get_json(config.RAW_UNDERSTAT_BUCKET, key)
        if not payload:
            log.warning("understat/players: файл %s отсутствует — skip", key)
            continue
        for item in payload:
            rows.append({
                "player_id":   item["id"],
                "player_name": item["player_name"],
                "league_id":   slug,
                "season":      season,
                "dt":          dt,
                "raw_payload": json.dumps(item, ensure_ascii=False),
            })

    engine = get_engine()
    with engine.begin() as conn:
        if rows:
            conn.execute(text(_PLAYERS_UPSERT), rows)
    log.info("stage.understat_players: upserted %d строк", len(rows))
    return len(rows)


def load_teams(dt: date, season: int) -> int:
    rows: list[dict[str, Any]] = []
    for league in config.UNDERSTAT_LEAGUES:
        slug = league["name"]
        key = build_object_key(
            source="understat",
            endpoint="teams",
            league_id=slug,
            season=season,
            dt=_dt_str(dt),
            filename="teams.json",
        )
        payload = get_json(config.RAW_UNDERSTAT_BUCKET, key)
        if not payload:
            log.warning("understat/teams: файл %s отсутствует — skip", key)
            continue
        for item in payload:
            rows.append({
                "team_title":  item["Team"],
                "league_id":   slug,
                "season":      season,
                "dt":          dt,
                "raw_payload": json.dumps(item, ensure_ascii=False),
            })

    engine = get_engine()
    with engine.begin() as conn:
        if rows:
            conn.execute(text(_TEAMS_UPSERT), rows)
    log.info("stage.understat_teams: upserted %d строк", len(rows))
    return len(rows)


def load_matches(dt: date, season: int) -> int:
    rows: list[dict[str, Any]] = []
    for league in config.UNDERSTAT_LEAGUES:
        slug = league["name"]
        key = build_object_key(
            source="understat",
            endpoint="matches",
            league_id=slug,
            season=season,
            dt=_dt_str(dt),
            filename="matches.json",
        )
        payload = get_json(config.RAW_UNDERSTAT_BUCKET, key)
        if not payload:
            log.warning("understat/matches: файл %s отсутствует — skip", key)
            continue
        for item in payload:
            rows.append({
                "match_id":    item["id"],
                "league_id":   slug,
                "season":      season,
                "dt":          dt,
                "raw_payload": json.dumps(item, ensure_ascii=False),
            })

    engine = get_engine()
    with engine.begin() as conn:
        if rows:
            conn.execute(text(_MATCHES_UPSERT), rows)
    log.info("stage.understat_matches: upserted %d строк", len(rows))
    return len(rows)