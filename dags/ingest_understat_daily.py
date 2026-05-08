"""Ежедневный snapshot текущего сезона Understat → MinIO. Cron 04:00 UTC.

Барьер-таск publish_dataset публикует Dataset ds_understat_raw только после
всех ingestion-тасков — иначе stage-DAG триггернётся раньше времени.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from _datasets import ds_understat_raw
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
    dag_id="ingest_understat_daily",
    description="Ежедневный snapshot текущего сезона Understat",
    start_date=datetime(2026, 4, 1),
    schedule="0 4 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "season": Param(
            default=2025,
            type=["integer", "string"],
            description="Текущий сезон (по умолчанию 2025-26)",
        )
    },
    max_active_runs=1,
    tags=["ingestion", "understat", "daily"],
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

    publish = EmptyOperator(
        task_id="publish_dataset",
        outlets=[ds_understat_raw],
    )
    last_tasks >> publish
