"""Загрузка StatsBomb Open Data JSON из MinIO в stage.sb_*.

competitions — один файл на dt, плоский массив турниров (все турниры Open
Data, не только наши лиги). Для stage фильтруем по парам
(country_name, competition_name) из config.LEAGUES — не раздуваем Vault
турнирами, которые нам не нужны.

matches — для каждой лиги много сезонов. Поскольку набор сезонов меняется
между релизами Open Data, сканируем MinIO-префикс и берём всё за
указанную dt.
"""
from __future__ import annotations

import json
import logging
import math
from datetime import date
from typing import Any

from sqlalchemy import text

from ingestion import config
from ingestion.minio_reader import get_json, list_objects
from ingestion.minio_writer import build_object_key
from stage.postgres import get_engine

log = logging.getLogger(__name__)


def _nan_to_null(obj: Any) -> Any:
    """StatsBomb иногда кладёт Python float('nan') в поля вроде 'referee'.
    json.dumps сериализует NaN как токен NaN — невалидный JSON, Postgres
    JSONB его отклоняет. Рекурсивно заменяем NaN/Inf на None."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _nan_to_null(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_nan_to_null(v) for v in obj]
    return obj


def _dt_str(dt: date) -> str:
    return dt.strftime("%Y-%m-%d")


# ---------- competitions ----------

_COMPETITIONS_INSERT = """
INSERT INTO stage.sb_competitions
    (competition_id, season_id, dt, country_name, competition_name,
     source_file, raw_payload)
VALUES
    (:competition_id, :season_id, :dt, :country_name, :competition_name,
     :source_file, CAST(:raw_payload AS JSONB))
"""


def load_competitions(dt: date) -> int:
    key = build_object_key(
        source="statsbomb",
        endpoint="competitions",
        league_id=None,
        season=None,
        dt=_dt_str(dt),
        filename="competitions.json",
    )
    payload = get_json(config.RAW_STATSBOMB_BUCKET, key)

    rows: list[dict[str, Any]] = []
    if payload is None:
        log.warning("statsbomb/competitions: файл %s отсутствует — skip", key)
    else:
        # Фильтруем только турниры, которые реально интересуют нас. Open Data
        # содержит десятки соревнований (WWC, NWSL и т.п.), в Vault не нужны.
        wanted = {tuple(lg["statsbomb"]) for lg in config.LEAGUES}
        for item in payload:
            pair = (item.get("country_name"), item.get("competition_name"))
            if pair not in wanted:
                continue
            rows.append({
                "competition_id": item.get("competition_id"),
                "season_id": item.get("season_id"),
                "dt": dt,
                "country_name": item.get("country_name"),
                "competition_name": item.get("competition_name"),
                "source_file": key,
                "raw_payload": json.dumps(item, ensure_ascii=False),
            })

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE stage.sb_competitions"))
        if rows:
            conn.execute(text(_COMPETITIONS_INSERT), rows)
    log.info("stage.sb_competitions: загружено %d строк", len(rows))
    return len(rows)


# ---------- matches ----------

_MATCHES_INSERT = """
INSERT INTO stage.sb_matches
    (match_id, dt, league_id, competition_id, season_id, match_date,
     home_team_id, away_team_id, source_file, raw_payload)
VALUES
    (:match_id, :dt, :league_id, :competition_id, :season_id, :match_date,
     :home_team_id, :away_team_id, :source_file, CAST(:raw_payload AS JSONB))
"""


def _iter_matches_files(slug: str, target_dt: str) -> list[tuple[str, int]]:
    """Возвращает [(object_key, season_id), ...] для всех сезонов, у которых
    есть snapshot за target_dt."""
    prefix = f"source=statsbomb/endpoint=matches/league_id={slug}/"
    found: list[tuple[str, int]] = []
    for key in list_objects(config.RAW_STATSBOMB_BUCKET, prefix):
        season_id: int | None = None
        dt_part: str | None = None
        for part in key.split("/"):
            if part.startswith("season="):
                try:
                    season_id = int(part[len("season="):])
                except ValueError:
                    continue
            elif part.startswith("dt="):
                dt_part = part[len("dt="):]
        if dt_part == target_dt and season_id is not None:
            found.append((key, season_id))
    return found


def _parse_match_date(raw: Any) -> Any:
    """StatsBomb отдаёт match_date строкой 'YYYY-MM-DD' (иногда с временем).
    Postgres сам кастует в DATE при INSERT'е, возвращаем как есть."""
    return raw


def _build_competition_id_index(
    dt_str: str,
) -> dict[tuple[str, str, int], int]:
    """Читает тот же snapshot competitions.json и строит mapping
    (country, competition, season_id) → competition_id. Нужен, потому что
    statsbombpy в matches.json не сохраняет competition_id — только строку
    названия. Без этого резолва в stage.sb_matches.competition_id всегда NULL.
    """
    key = build_object_key(
        source="statsbomb", endpoint="competitions",
        league_id=None, season=None, dt=dt_str,
        filename="competitions.json",
    )
    payload = get_json(config.RAW_STATSBOMB_BUCKET, key) or []
    out: dict[tuple[str, str, int], int] = {}
    for c in payload:
        triple = (
            c.get("country_name"),
            c.get("competition_name"),
            c.get("season_id"),
        )
        if all(v is not None for v in triple):
            out[triple] = c.get("competition_id")
    return out


def load_matches(dt: date) -> int:
    target_dt_str = _dt_str(dt)
    rows: list[dict[str, Any]] = []

    comp_id_by_triple = _build_competition_id_index(target_dt_str)

    for league in config.LEAGUES:
        slug = league["name"]
        country, competition = league["statsbomb"]
        files = _iter_matches_files(slug, target_dt_str)
        if not files:
            log.warning(
                "statsbomb/matches: нет snapshot за %s для лиги %s — skip",
                target_dt_str, slug,
            )
            continue

        for key, season_id in files:
            comp_id = comp_id_by_triple.get((country, competition, season_id))
            if comp_id is None:
                log.warning(
                    "statsbomb/matches: не резолвится competition_id для "
                    "(%s, %s, season_id=%s) — ставим NULL",
                    country, competition, season_id,
                )
            payload = get_json(config.RAW_STATSBOMB_BUCKET, key)
            if not payload:
                log.warning("statsbomb/matches: пустой payload %s — skip", key)
                continue
            for item in payload:
                # statsbombpy в matches отдаёт плоский формат, но без id
                # команд и без competition_id. Первый резолвим через
                # competitions.json, вторые оставляем NULL (есть только
                # в lineups endpoint).
                rows.append({
                    "match_id": item.get("match_id"),
                    "dt": dt,
                    "league_id": slug,
                    "competition_id": comp_id,
                    "season_id": season_id,
                    "match_date": _parse_match_date(item.get("match_date")),
                    "home_team_id": None,
                    "away_team_id": None,
                    "source_file": key,
                    "raw_payload": json.dumps(
                        _nan_to_null(item), ensure_ascii=False,
                    ),
                })

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE stage.sb_matches"))
        if rows:
            conn.execute(text(_MATCHES_INSERT), rows)
    log.info("stage.sb_matches: загружено %d строк", len(rows))
    return len(rows)
