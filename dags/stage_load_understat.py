"""DAG: загрузка Understat JSON из MinIO в stage.understat_* Postgres.

schedule=None — триггерится из ingest_understat.
Не триггерит dbt_raw_vault — dbt запускается только из AF-пайплайна.
Запускай dbt вручную после understat-загрузки если нужно обновить RV.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from stage.loaders import understat as us_loader

DEFAULT_ARGS = {
    "owner": "football_dwh",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _wrap(loader_fn):
    def _run(**context) -> int:
        dt = context["data_interval_end"].date()
        season = int(context["params"]["season"])
        return loader_fn(dt=dt, season=season)
    return _run


with DAG(
    dag_id="stage_load_understat",
    description="Stage load: MinIO (understat) → Postgres stage.understat_*",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={"season": Param(default=2025, type="integer")},
    max_active_runs=1,
    tags=["stage", "understat"],
) as dag:
    t_players = PythonOperator(
        task_id="load_players",
        python_callable=_wrap(us_loader.load_players),
    )
    t_teams = PythonOperator(
        task_id="load_teams",
        python_callable=_wrap(us_loader.load_teams),
    )