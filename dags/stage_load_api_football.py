"""DAG: загрузка API-Football JSON из MinIO в stage.af_* Postgres.

schedule=None — триггерится автоматически из ingest_api_football.
Принимает параметр season (прокидывается через conf из ingest).

Стратегия: full refresh. Каждый запуск TRUNCATE + INSERT за dt=сегодня.
Идемпотентно — ручной перезапуск в тот же день безопасен.

По завершении триггерит dbt_raw_vault.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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
        season = int(context["params"]["season"])
        return loader_fn(dt=dt, season=season)
    return _run


with DAG(
    dag_id="stage_load_api_football",
    description="Stage load: MinIO (api-football) → Postgres stage.af_*. Param: season.",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={"season": Param(default=2024, type="integer", description="Сезон AF")},
    max_active_runs=1,
    tags=["stage", "api-football"],
) as dag:
    loader_tasks = []
    for name, fn in [
        ("load_leagues", af_loader.load_leagues),
        ("load_teams", af_loader.load_teams),
        ("load_fixtures", af_loader.load_fixtures),
        ("load_standings", af_loader.load_standings),
        ("load_topscorers", af_loader.load_topscorers),
    ]:
        t = PythonOperator(task_id=name, python_callable=_wrap(fn))
        loader_tasks.append(t)

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_raw_vault",
        trigger_dag_id="dbt_raw_vault",
        wait_for_completion=False,
        reset_dag_run=True,
    )
    loader_tasks >> trigger_dbt