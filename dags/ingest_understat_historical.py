"""DAG: backfill исторических сезонов Understat → MinIO (manual trigger).

Запускается руками с обязательным параметром season (2022/2023/2024).
По завершении триггерит stage_load_understat с тем же season.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from _understat_tasks import (
    ensure_bucket,
    ingest_matches,
    ingest_players,
    ingest_teams,
)
from ingestion import config

DEFAULT_ARGS = {
    "owner": "football_dwh",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="ingest_understat_historical",
    description="Backfill исторических сезонов Understat (manual)",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "season": Param(
            type=["integer", "string"],
            description="Исторический сезон (2022/2023/2024)",
        )
    },
    max_active_runs=1,
    tags=["ingestion", "understat", "historical"],
) as dag:

    t_ensure = PythonOperator(task_id="ensure_bucket", python_callable=ensure_bucket)

    last_tasks = []
    for league in config.UNDERSTAT_LEAGUES:
        for endpoint, fn in [
            ("players", ingest_players),
            ("teams", ingest_teams),
            ("matches", ingest_matches),
        ]:
            t = PythonOperator(
                task_id=f"{league['name']}__{endpoint}",
                python_callable=fn,
                op_kwargs={
                    "league_name": league["name"],
                    "understat_id": league["understat_id"],
                },
            )
            t_ensure >> t
            last_tasks.append(t)

    trigger_stage = TriggerDagRunOperator(
        task_id="trigger_stage_load_understat",
        trigger_dag_id="stage_load_understat",
        conf={"season": "{{ params.season }}"},
        wait_for_completion=False,
        reset_dag_run=True,
    )
    last_tasks >> trigger_stage
