"""Raw Vault: dbt build (stage_dv + raw_vault) → Postgres.

Триггерится по Dataset ds_understat_stage. По завершении публикует ds_raw_vault
(пока никто не подписан — build_marts остаётся manual из-за Spark-зависимости).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from _datasets import ds_raw_vault, ds_understat_stage

DEFAULT_ARGS = {
    "owner": "football_dwh",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

DBT_CMD = (
    "dbt {subcmd}"
    " --project-dir /opt/airflow/dbt"
    " --profiles-dir /opt/airflow/dbt"
    " --target prod"
    " {extra}"
)

with DAG(
    dag_id="dbt_raw_vault",
    description="Raw Vault: dbt build (stage_dv + raw_vault) → Postgres",
    start_date=datetime(2026, 4, 1),
    schedule=[ds_understat_stage],
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["dwh", "dbt", "raw_vault"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=DBT_CMD.format(subcmd="deps", extra=""),
    )

    dbt_run_stage = BashOperator(
        task_id="dbt_run_stage",
        bash_command=DBT_CMD.format(subcmd="run", extra="--select stage"),
    )

    dbt_run_raw_vault = BashOperator(
        task_id="dbt_run_raw_vault",
        bash_command=DBT_CMD.format(subcmd="run", extra="--select raw_vault"),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT_CMD.format(
            subcmd="test",
            extra="--select raw_vault --store-failures",
        ),
    )

    publish = EmptyOperator(
        task_id="publish_dataset",
        outlets=[ds_raw_vault],
    )

    dbt_deps >> dbt_run_stage >> dbt_run_raw_vault >> dbt_test >> publish
