"""DAG: backfill исторических сезонов API-Football → MinIO.

Загружает standings и topscorers для произвольного сезона.
Fixtures не берём — слишком дорого по квоте (100 req/день).

Запускается вручную с season=2022 и season=2023 по одному разу.
После прогона: запустить stage_load_af_historical, потом dbt_raw_vault.
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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

ENDPOINTS = [
    ("standings", "get_standings"),
    ("topscorers", "get_topscorers"),
]


def _ingest(endpoint: str, method_name: str, league_name: str,
            league_api_id: int, **context) -> str:
    season = context["params"]["season"]
    client = ApiFootballClient()
    payload = getattr(client, method_name)(
        league_id=league_api_id, season=season
    )
    key = build_object_key(
        source="api-football",
        endpoint=endpoint,
        league_id=league_name,
        season=season,
        dt=today_utc(),
        filename=f"{endpoint}.json",
    )
    return put_json(
        bucket=config.RAW_API_FOOTBALL_BUCKET,
        object_key=key,
        payload=payload,
    )


with DAG(
    dag_id="ingest_af_historical",
    description="Manual backfill: AF standings+topscorers → MinIO (old seasons)",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={"season": Param(default=2022, type="integer", description="Сезон")},
    max_active_tasks=1,
    max_active_runs=1,
    tags=["ingestion", "api-football", "historical"],
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
