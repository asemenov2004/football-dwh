"""Stage load: MinIO (understat) → Postgres stage.understat_*.

Триггерится по Dataset ds_understat_raw (см. dags/_datasets.py).
По завершении публикует ds_understat_stage → запускает dbt_raw_vault.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from _datasets import ds_understat_raw, ds_understat_stage
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
    schedule=[ds_understat_raw],
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={"season": Param(default=2025, type=["integer", "string"])},
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
    t_matches = PythonOperator(
        task_id="load_matches",
        python_callable=_wrap(us_loader.load_matches),
    )
    publish = EmptyOperator(
        task_id="publish_dataset",
        outlets=[ds_understat_stage],
    )
    [t_players, t_teams, t_matches] >> publish
