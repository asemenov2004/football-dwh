"""DAG: daily snapshot пяти эндпоинтов API-Football для топ-5 лиг + UCL.

Параллелизм: каждая лига — своя независимая цепочка задач внутри DAG-а.
Внутри лиги эндпоинты идут последовательно (leagues → teams → fixtures →
standings → topscorers), между лигами — параллельно. Если одна лига
ретраится, другие идут дальше.

Итого задач: 6 лиг × 5 эндпоинтов = 30. Бюджет API-Football free tier —
100 req/день, запас 70.

Идемпотентность: повторный запуск в тот же день перезапишет snapshot —
это ок. Rate-limit защищён счётчиком ingestion.rate_limit.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

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
    client = ApiFootballClient()
    payload = getattr(client, method_name)(
        league_id=league_api_id,
        season=config.DEFAULT_API_FOOTBALL_SEASON,
    )
    key = build_object_key(
        source="api-football",
        endpoint=endpoint,
        league_id=league_name,
        season=config.DEFAULT_API_FOOTBALL_SEASON,
        dt=today_utc(),
        filename=f"{endpoint}.json",
    )
    return put_json(
        bucket=config.RAW_API_FOOTBALL_BUCKET,
        object_key=key,
        payload=payload,
    )


with DAG(
    dag_id="ingest_api_football",
    description="Daily raw ingest: API-Football → MinIO (top-5 + UCL)",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    # free tier бьёт 429 при ~10 req/min. Сериализуем таски — 1 req каждые
    # несколько секунд, далеко от лимита.
    max_active_tasks=1,
    # два параллельных run'а (scheduled + manual) обходят max_active_tasks
    # и пробивают лимит. Запрещаем параллельные run'ы.
    max_active_runs=1,
    tags=["ingestion", "api-football"],
) as dag:
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
