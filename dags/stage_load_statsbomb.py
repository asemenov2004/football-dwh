"""DAG: загрузка StatsBomb Open Data JSON из MinIO в stage.sb_* Postgres.

Ждёт `ingest_statsbomb` (@weekly) через ExternalTaskSensor. start_date
синхронизирован с upstream, schedule тоже @weekly — execution_date совпадёт.

Две таски: competitions → matches. matches идёт вторым не из-за FK
(в stage их нет), а чтобы ошибка в competitions не оставила несвежий
matches в stage.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

from stage.loaders import statsbomb as sb_loader


DEFAULT_ARGS = {
    "owner": "football_dwh",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _run_competitions(**context) -> int:
    # dt берём из data_interval_end, а не today_utc(): так manual retry
    # спустя дни читает исходный snapshot, а не пустоту за "сегодня".
    # logical_date не годится — для @weekly это начало интервала, а
    # ingest в MinIO кладёт под dt запуска (≈ data_interval_end).
    dt = context["data_interval_end"].date()
    return sb_loader.load_competitions(dt=dt)


def _run_matches(**context) -> int:
    dt = context["data_interval_end"].date()
    return sb_loader.load_matches(dt=dt)


with DAG(
    dag_id="stage_load_statsbomb",
    description="Stage load: MinIO (statsbomb) → Postgres stage.sb_*",
    start_date=datetime(2026, 4, 1),
    schedule="@weekly",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["stage", "statsbomb"],
) as dag:
    wait_ingest = ExternalTaskSensor(
        task_id="wait_for_ingest",
        external_dag_id="ingest_statsbomb",
        # external_task_id=None ⇒ ждём DagRun; валидны только DagRunState.
        external_task_id=None,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=300,  # @weekly upstream, poke'ать чаще минуты нет смысла
        timeout=60 * 60 * 6,
    )

    t_competitions = PythonOperator(
        task_id="load_competitions",
        python_callable=_run_competitions,
    )
    t_matches = PythonOperator(
        task_id="load_matches",
        python_callable=_run_matches,
    )

    wait_ingest >> t_competitions >> t_matches
