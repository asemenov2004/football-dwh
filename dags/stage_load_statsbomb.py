"""DAG: загрузка StatsBomb Open Data JSON из MinIO в stage.sb_* Postgres.

schedule=None — триггерится автоматически из ingest_statsbomb.
Не триггерит dbt_raw_vault — dbt запускается только из understat-пайплайна.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from stage.loaders import statsbomb as sb_loader

DEFAULT_ARGS = {
    "owner": "football_dwh",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _run_competitions(**context) -> int:
    dt = context["data_interval_end"].date()
    return sb_loader.load_competitions(dt=dt)


def _run_matches(**context) -> int:
    dt = context["data_interval_end"].date()
    return sb_loader.load_matches(dt=dt)


with DAG(
    dag_id="stage_load_statsbomb",
    description="Stage load: MinIO (statsbomb) → Postgres stage.sb_*",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["stage", "statsbomb"],
) as dag:
    t_competitions = PythonOperator(
        task_id="load_competitions",
        python_callable=_run_competitions,
    )
    t_matches = PythonOperator(
        task_id="load_matches",
        python_callable=_run_matches,
    )
    t_competitions >> t_matches
