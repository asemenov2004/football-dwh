"""DAG: загрузка Understat (xG-статистика) → MinIO.

schedule=None — запускается вручную. Принимает параметр season
(год начала сезона: 2025 = 2025-26, 2024 = 2024-25, ...).

5 лиг × 2 эндпоинта (players + teams) = 10 задач.
По завершении триггерит stage_load_understat с тем же season.

Без rate-limit — Understat публичный сайт, нет API-ключей.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from minio import Minio

from ingestion import config, understat_client
from ingestion.minio_writer import build_object_key, put_json, today_utc

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "football_dwh",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def _ensure_bucket(**context) -> None:
    client = Minio(
        config.MINIO_ENDPOINT,
        access_key=config.MINIO_ACCESS_KEY,
        secret_key=config.MINIO_SECRET_KEY,
        secure=config.MINIO_SECURE,
    )
    if not client.bucket_exists(config.RAW_UNDERSTAT_BUCKET):
        client.make_bucket(config.RAW_UNDERSTAT_BUCKET)
        log.info("создан bucket %s", config.RAW_UNDERSTAT_BUCKET)


def _ingest_players(league_name: str, understat_id: str, **context) -> str:
    season = int(context["params"]["season"])
    dt = today_utc()
    data = understat_client.get_league_players(understat_id, season)
    key = build_object_key(
        source="understat",
        endpoint="players",
        league_id=league_name,
        season=season,
        dt=dt,
        filename="players.json",
    )
    return put_json(
        bucket=config.RAW_UNDERSTAT_BUCKET,
        object_key=key,
        payload=data,
    )


def _ingest_teams(league_name: str, understat_id: str, **context) -> str:
    season = int(context["params"]["season"])
    dt = today_utc()
    data = understat_client.get_league_table(understat_id, season)
    key = build_object_key(
        source="understat",
        endpoint="teams",
        league_id=league_name,
        season=season,
        dt=dt,
        filename="teams.json",
    )
    return put_json(
        bucket=config.RAW_UNDERSTAT_BUCKET,
        object_key=key,
        payload=data,
    )


with DAG(
    dag_id="ingest_understat",
    description="Manual ingest: Understat xG-stats → MinIO. Param: season.",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "season": Param(
            default=2025,
            type="integer",
            description="Год начала сезона (2025=2025-26, 2024=2024-25, ...)",
        )
    },
    max_active_runs=1,
    tags=["ingestion", "understat"],
) as dag:

    ensure_bucket = PythonOperator(
        task_id="ensure_bucket",
        python_callable=_ensure_bucket,
    )

    last_tasks = []
    for league in config.UNDERSTAT_LEAGUES:
        t_players = PythonOperator(
            task_id=f"{league['name']}__players",
            python_callable=_ingest_players,
            op_kwargs={
                "league_name":   league["name"],
                "understat_id":  league["understat_id"],
            },
        )
        t_teams = PythonOperator(
            task_id=f"{league['name']}__teams",
            python_callable=_ingest_teams,
            op_kwargs={
                "league_name":   league["name"],
                "understat_id":  league["understat_id"],
            },
        )
        ensure_bucket >> t_players
        ensure_bucket >> t_teams
        last_tasks.extend([t_players, t_teams])

    trigger_stage = TriggerDagRunOperator(
        task_id="trigger_stage_load_understat",
        trigger_dag_id="stage_load_understat",
        conf={"season": "{{ params.season }}"},
        wait_for_completion=False,
        reset_dag_run=True,
    )
    last_tasks >> trigger_stage