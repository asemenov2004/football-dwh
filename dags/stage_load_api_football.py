"""DAG: загрузка API-Football JSON из MinIO в stage.af_* Postgres.

Ждёт завершения `ingest_api_football` за тот же execution_date через
ExternalTaskSensor. start_date и schedule синхронизированы с upstream DAG —
без этого execution_date не совпадут и sensor повиснет в вечном poke.

Стратегия: full refresh. Каждый запуск TRUNCATE'ит stage и наливает
снова за dt=today_utc(). Идемпотентно — ручной перезапуск в тот же день
безопасен.

5 параллельных тасков — они упираются только в Postgres, rate-limit нет.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from ingestion import config
from stage.loaders import api_football as af_loader


DEFAULT_ARGS = {
    "owner": "football_dwh",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _wrap(loader_fn):
    """Адаптер: dt берём из data_interval_end — это момент фактического
    запуска scheduled-run'а, совпадает с today_utc() в ingest'е. Через
    logical_date делать нельзя: для @weekly оно = начало интервала
    (прошлое воскресенье), а ingest клал snapshot в dt запуска."""
    def _run(**context) -> int:
        dt = context["data_interval_end"].date()
        return loader_fn(dt=dt, season=config.DEFAULT_API_FOOTBALL_SEASON)
    return _run


with DAG(
    dag_id="stage_load_api_football",
    description="Stage load: MinIO (api-football) → Postgres stage.af_*",
    # Синхронно с ingest_api_football: тот же start_date и schedule,
    # чтобы execution_date совпадал у ExternalTaskSensor.
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["stage", "api-football"],
) as dag:
    wait_ingest = ExternalTaskSensor(
        task_id="wait_for_ingest",
        external_dag_id="ingest_api_football",
        # external_task_id=None ⇒ ждём DagRun целиком. В этом режиме
        # allowed/failed_states принимают только DagRunState (success/failed/
        # queued/running), TaskInstanceState сюда не подходит.
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60 * 3,  # 3 часа — запас на retry upstream
    )

    loader_tasks = []
    for name, fn in [
        ("load_leagues", af_loader.load_leagues),
        ("load_teams", af_loader.load_teams),
        ("load_fixtures", af_loader.load_fixtures),
        ("load_standings", af_loader.load_standings),
        ("load_topscorers", af_loader.load_topscorers),
    ]:
        t = PythonOperator(task_id=name, python_callable=_wrap(fn))
        wait_ingest >> t
        loader_tasks.append(t)
