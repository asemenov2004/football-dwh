"""DAG: Raw Vault — dbt build поверх stage-таблиц Postgres.

Ждёт успешного завершения обоих stage-DAGов за тот же execution_date через
ExternalTaskSensor (external_task_id=None — ждём всего DagRun целиком).

Последовательность тасков:
  dbt_deps → dbt_run_stage → dbt_run_raw_vault → dbt_test

dbt запускается через BashOperator внутри airflow-контейнера,
в котором dbt-core + dbt-postgres установлены (Этап 3 requirements.txt).
Проект смонтирован как /opt/airflow/dbt (volume в docker-compose).
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState

DEFAULT_ARGS = {
    "owner": "football_dwh",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# Базовая dbt-команда: проект и профили — в /opt/airflow/dbt (из volume).
DBT_CMD = (
    "dbt {subcmd}"
    " --project-dir /opt/airflow/dbt"
    " --profiles-dir /opt/airflow/dbt"
    " --target prod"
    " {extra}"
)


with DAG(
    dag_id="dbt_raw_vault",
    description="Raw Vault: dbt build (stage_dv + raw_vault) поверх stage.* в Postgres",
    # Синхронно с stage-DAGами: тот же start_date и @daily schedule,
    # чтобы execution_date совпадал у ExternalTaskSensor.
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["dwh", "dbt", "raw_vault"],
) as dag:

    # ── 1. Ждём stage AF ────────────────────────────────────────────────────
    wait_stage_af = ExternalTaskSensor(
        task_id="wait_stage_api_football",
        external_dag_id="stage_load_api_football",
        external_task_id=None,       # ждём весь DagRun
        allowed_states=[DagRunState.SUCCESS],
        execution_delta=timedelta(0),
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    # ── 2. Ждём stage SB ────────────────────────────────────────────────────
    wait_stage_sb = ExternalTaskSensor(
        task_id="wait_stage_statsbomb",
        external_dag_id="stage_load_statsbomb",
        external_task_id=None,
        allowed_states=[DagRunState.SUCCESS],
        execution_delta=timedelta(0),
        timeout=7200,
        poke_interval=120,
        mode="reschedule",
    )

    # ── 3. dbt deps: скачать пакеты (datavault4dbt, dbt_utils) ─────────────
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=DBT_CMD.format(subcmd="deps", extra=""),
    )

    # ── 4. dbt run stage: создать/обновить views stage_dv.* ─────────────────
    dbt_run_stage = BashOperator(
        task_id="dbt_run_stage",
        bash_command=DBT_CMD.format(subcmd="run", extra="--select stage"),
    )

    # ── 5. dbt run raw_vault: инкрементально дозаписать hubs/links/sats ─────
    dbt_run_raw_vault = BashOperator(
        task_id="dbt_run_raw_vault",
        bash_command=DBT_CMD.format(subcmd="run", extra="--select raw_vault"),
    )

    # ── 6. dbt test: проверить not_null, unique, relationships ───────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT_CMD.format(
            subcmd="test",
            extra="--select raw_vault --store-failures",
        ),
    )

    # ── Граф зависимостей ────────────────────────────────────────────────────
    [wait_stage_af, wait_stage_sb] >> dbt_deps >> dbt_run_stage >> dbt_run_raw_vault >> dbt_test
