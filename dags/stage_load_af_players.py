"""DAG: загрузка AF players из MinIO → stage.af_players.

Запускается вручную после ingest_af_players. Без ExternalTaskSensor —
players исторические, не daily. Принимает season через Param.

dt берётся из data_interval_end (= момент ручного запуска).
stage.af_players использует UPSERT, поэтому повторный запуск безопасен.
"""
from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from stage.loaders import api_football as af_loader

DEFAULT_ARGS = {
    "owner": "football_dwh",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _load(**context) -> int:
    dt = context["data_interval_end"].date()
    season = context["params"]["season"]
    return af_loader.load_players(dt=dt, season=season)


with DAG(
    dag_id="stage_load_af_players",
    description="Stage load: MinIO (af/players) → Postgres stage.af_players",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={"season": Param(default=2024, type="integer", description="Сезон")},
    max_active_runs=1,
    tags=["stage", "api-football", "players"],
) as dag:
    PythonOperator(task_id="load_players", python_callable=_load)
