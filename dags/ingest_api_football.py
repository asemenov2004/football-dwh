"""DAG: snapshot пяти эндпоинтов API-Football для топ-5 лиг + UCL.

schedule=None — запускается вручную. Принимает параметр season.
Запускай для каждого нужного сезона: 2022, 2023, 2024.

Итого тасков: 6 лиг × 5 эндпоинтов = 30. Бюджет AF free tier — 100 req/день.
По завершении автоматически триггерит stage_load_api_football с тем же season.

Rate-limit: max_active_tasks=1, time.sleep(7) в клиенте (~8 req/min).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from ingestion import config
from ingestion.api_football_client import ApiFootballClient
from ingestion.minio_writer import build_object_key, put_json, today_utc

DEFAULT_ARGS = {
    "owner": "football_dwh",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

ENDPOINTS = [
    ("leagues", "get_leagues"),
    ("teams", "get_teams"),
    ("fixtures", "get_fixtures"),
    ("standings", "get_standings"),
    ("topscorers", "get_topscorers"),
]


def _ingest(
    endpoint: str,
    method_name: str,
    league_name: str,
    league_api_id: int,
    **context,
) -> str:
    season = int(context["params"]["season"])
    client = ApiFootballClient()
    payload = getattr(client, method_name)(league_id=league_api_id, season=season)
    key = build_object_key(
        source="api-football",
        endpoint=endpoint,
        league_id=league_name,
        season=season,
        dt=today_utc(),
        filename=f"{endpoint}.json",
    )
    return put_json(bucket=config.RAW_API_FOOTBALL_BUCKET, object_key=key, payload=payload)


with DAG(
    dag_id="ingest_api_football",
    description="Manual ingest: API-Football → MinIO (top-5 + UCL). Param: season.",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={"season": Param(default=2024, type="integer", description="Сезон AF (2022/2023/2024)")},
    max_active_tasks=1,
    max_active_runs=1,
    tags=["ingestion", "api-football"],
) as dag:
    last_tasks = []
    for league in config.LEAGUES:
        prev = None
        for endpoint, method in ENDPOINTS:
            task = PythonOperator(
                task_id=f"{league['name']}__{endpoint}",
                python_callable=_ingest,
                op_kwargs={
                    "endpoint": endpoint,
                    "method_name": method,
                    "league_name": league["name"],
                    "league_api_id": league["api_football_id"],
                },
            )
            if prev is not None:
                prev >> task
            prev = task
        last_tasks.append(prev)

    trigger_stage = TriggerDagRunOperator(
        task_id="trigger_stage_load_api_football",
        trigger_dag_id="stage_load_api_football",
        conf={"season": "{{ params.season }}"},
        wait_for_completion=False,
        reset_dag_run=True,
    )
    last_tasks >> trigger_stage