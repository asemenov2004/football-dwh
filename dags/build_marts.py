"""DAG: BV + Marts (Postgres) → ClickHouse (через MinIO).

Этап 5б. Триггер manual: run после ingest_understat_*.
Последовательность:
  1. dbt run tag:bv          (Postgres business_vault.*)
  2. dbt run tag:mart        (Postgres marts.*)
  3. dbt test tag:bv tag:mart
  4. clickhouse_load         (TRUNCATE + INSERT FROM s3() для каждой март-таблицы)

ВАЖНО: Spark-перелив (PG → MinIO Parquet) запускается отдельно скриптом
`scripts/run_spark_marts.sh`, который делает `docker exec football_spark_master
spark-submit ...`. Перед запуском DAG нужно:
  bash scripts/run_spark_marts.sh
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {
    "owner": "football_dwh",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DBT_CMD = (
    "dbt {subcmd}"
    " --project-dir /opt/airflow/dbt"
    " --profiles-dir /opt/airflow/dbt"
    " --target prod"
    " {extra}"
)

MARTS = [
    ("mart_league_table",
     "s3('http://minio:9000/marts/mart_league_table/*.parquet', '{ak}', '{sk}', 'Parquet')"),
    ("mart_top_scorers",
     "s3('http://minio:9000/marts/mart_top_scorers/*.parquet', '{ak}', '{sk}', 'Parquet')"),
    ("mart_match_facts",
     "s3('http://minio:9000/marts/mart_match_facts/*.parquet', '{ak}', '{sk}', 'Parquet')"),
    ("mart_player_overperformers",
     "s3('http://minio:9000/marts/mart_player_overperformers/*.parquet', '{ak}', '{sk}', 'Parquet')"),
    ("mart_team_xg_trend",
     "s3('http://minio:9000/marts/mart_team_xg_trend/*.parquet', '{ak}', '{sk}', 'Parquet')"),
    ("mart_team_elo_current",
     "s3('http://minio:9000/marts/mart_team_elo_current/*.parquet', '{ak}', '{sk}', 'Parquet')"),
    ("mart_team_elo_history",
     "s3('http://minio:9000/marts/mart_team_elo_history/*.parquet', '{ak}', '{sk}', 'Parquet')"),
    ("mart_sb_la_liga_history",
     "s3('http://minio:9000/marts/mart_sb_la_liga_history/*.parquet', '{ak}', '{sk}', 'Parquet')"),
]


def clickhouse_load(**_):
    """TRUNCATE + INSERT FROM s3() для каждой март-таблицы."""
    import clickhouse_connect

    ak = os.environ["MINIO_ROOT_USER"]
    sk = os.environ["MINIO_ROOT_PASSWORD"]
    ch_user = os.environ.get("CLICKHOUSE_USER", "football")
    ch_pass = os.environ.get("CLICKHOUSE_PASSWORD", "football")

    client = clickhouse_connect.get_client(
        host="clickhouse", port=8123,
        username=ch_user, password=ch_pass,
    )

    for name, s3_tmpl in MARTS:
        s3_expr = s3_tmpl.format(ak=ak, sk=sk)
        print(f"[clickhouse_load] truncate+insert {name}")
        client.command(f"TRUNCATE TABLE marts.{name}")
        client.command(f"INSERT INTO marts.{name} SELECT * FROM {s3_expr}")
        cnt = client.command(f"SELECT count() FROM marts.{name}")
        print(f"[clickhouse_load] marts.{name} rows={cnt}")


with DAG(
    dag_id="build_marts",
    description="BV + Marts (dbt) → ClickHouse load из MinIO",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=["dwh", "dbt", "bv", "mart", "clickhouse"],
) as dag:

    dbt_run_bv = BashOperator(
        task_id="dbt_run_bv",
        bash_command=DBT_CMD.format(subcmd="run", extra="--select tag:bv"),
    )

    dbt_run_mart = BashOperator(
        task_id="dbt_run_mart",
        bash_command=DBT_CMD.format(subcmd="run", extra="--select tag:mart"),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT_CMD.format(subcmd="test", extra="--select tag:bv tag:mart"),
    )

    ch_load = PythonOperator(
        task_id="clickhouse_load",
        python_callable=clickhouse_load,
    )

    dbt_run_bv >> dbt_run_mart >> dbt_test >> ch_load
