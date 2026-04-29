"""Общие task-функции для Understat-ingest DAG'ов (daily + historical).

Underscore-префикс — Airflow не парсит файл как DAG.
"""
from __future__ import annotations

import logging

from minio import Minio

from ingestion import config, understat_client
from ingestion.minio_writer import build_object_key, put_json, today_utc

log = logging.getLogger(__name__)


def ensure_bucket(**context) -> None:
    client = Minio(
        config.MINIO_ENDPOINT,
        access_key=config.MINIO_ACCESS_KEY,
        secret_key=config.MINIO_SECRET_KEY,
        secure=config.MINIO_SECURE,
    )
    if not client.bucket_exists(config.RAW_UNDERSTAT_BUCKET):
        client.make_bucket(config.RAW_UNDERSTAT_BUCKET)
        log.info("создан bucket %s", config.RAW_UNDERSTAT_BUCKET)


def _ingest_endpoint(
    fetcher,
    endpoint: str,
    filename: str,
    league_name: str,
    understat_id: str,
    **context,
) -> str:
    season = int(context["params"]["season"])
    dt = today_utc()
    data = fetcher(understat_id, season)
    key = build_object_key(
        source="understat",
        endpoint=endpoint,
        league_id=league_name,
        season=season,
        dt=dt,
        filename=filename,
    )
    return put_json(
        bucket=config.RAW_UNDERSTAT_BUCKET,
        object_key=key,
        payload=data,
    )


def ingest_players(league_name: str, understat_id: str, **context) -> str:
    return _ingest_endpoint(
        understat_client.get_league_players,
        "players", "players.json",
        league_name, understat_id, **context,
    )


def ingest_teams(league_name: str, understat_id: str, **context) -> str:
    return _ingest_endpoint(
        understat_client.get_league_table,
        "teams", "teams.json",
        league_name, understat_id, **context,
    )


def ingest_matches(league_name: str, understat_id: str, **context) -> str:
    return _ingest_endpoint(
        understat_client.get_league_results,
        "matches", "matches.json",
        league_name, understat_id, **context,
    )
