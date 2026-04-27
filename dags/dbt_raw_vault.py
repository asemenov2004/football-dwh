"""DAG: Raw Vault — dbt build поверх stage-таблиц Postgres.

schedule=None — триггерится автоматически из stage_load_api_football.
Последовательность: dbt_deps → dbt_run_stage → dbt_run_raw_vault → dbt_test.

dbt запускается через BashOperator внутри airflow-контейнера.
Проект смонтирован как /opt/airflow/dbt (volume в docker-compose).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

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
    schedule=None,
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

    dbt_deps >> dbt_run_stage >> dbt_run_raw_vault >> dbt_test
