"""DAG: загрузка ростеров игроков API-Football → MinIO.

Запускается вручную (schedule=None). Принимает параметр season.
Для каждой лиги пагинирует /players до последней страницы.
Каждая страница — отдельный файл: players/league_id=<slug>/season=<year>/
dt=<dt>/page-<NNN>.json.

Rate limit: max_active_tasks=1. Один прогон ≈ 6 лиг × ~15 стр = ~90 req.
Запускать по одному разу на сезон: 2022, 2023, 2024.
"""
from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from ingestion import config
from ingestion.api_football_client import ApiFootballClient
from ingestion.minio_writer import build_object_key, put_json, today_utc

DEFAULT_ARGS = {
    "owner": "football_dwh",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def _ingest_players(league_name: str, league_api_id: int, **context) -> int:
    season = context["params"]["season"]
    dt = today_utc()
    client = ApiFootballClient()

    page = 1
    total_players = 0
    while True:
        data = client.get_players(
            league_id=league_api_id, season=season, page=page
        )
        response = data.get("response") or []
        paging = data.get("paging") or {}
        total_pages = paging.get("total", 1)

        key = build_object_key(
            source="api-football",
            endpoint="players",
            league_id=league_name,
            season=season,
            dt=dt,
            filename=f"page-{page:03d}.json",
        )
        put_json(bucket=config.RAW_API_FOOTBALL_BUCKET, object_key=key, payload=data)
        total_players += len(response)

        if not response or page >= total_pages or page >= 3:
            break
        page += 1

    return total_players


with DAG(
    dag_id="ingest_af_players",
    description="Manual ingest: API-Football /players → MinIO (all pages)",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={"season": Param(default=2024, type="integer", description="Сезон")},
    max_active_tasks=1,
    max_active_runs=1,
    tags=["ingestion", "api-football", "players"],
) as dag:
    for league in config.LEAGUES:
        PythonOperator(
            task_id=f"{league['name']}__players",
            python_callable=_ingest_players,
            op_kwargs={
                "league_name": league["name"],
                "league_api_id": league["api_football_id"],
            },
        )
