"""DAG: загрузка исторических AF данных из MinIO → stage.

Загружает standings и topscorers за указанный season.
Без ExternalTaskSensor — запускается вручную после ingest_af_historical.

ВАЖНО: stage.af_standings и stage.af_topscorers — full refresh (TRUNCATE).
После прогона этого DAG'а сразу запускать dbt_raw_vault, пока stage
содержит исторические данные. Потом daily DAG перезапишет stage текущим.
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


def _wrap(loader_fn):
    def _run(**context) -> int:
        dt = context["data_interval_end"].date()
        season = context["params"]["season"]
        return loader_fn(dt=dt, season=season)
    return _run


with DAG(
    dag_id="stage_load_af_historical",
    description="Stage load: MinIO (af historical) → Postgres stage.af_*",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={"season": Param(default=2022, type="integer", description="Сезон")},
    max_active_runs=1,
    tags=["stage", "api-football", "historical"],
) as dag:
    for name, fn in [
        ("load_standings", af_loader.load_standings),
        ("load_topscorers", af_loader.load_topscorers),
    ]:
        PythonOperator(task_id=name, python_callable=_wrap(fn))
