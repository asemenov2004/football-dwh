"""DAG: BV + Marts (Postgres) → ClickHouse (через MinIO).

Этап 5б. Event-driven: schedule=[ds_raw_vault] — запускается автоматически
после публикации Dataset из dbt_raw_vault. Финальная таска clickhouse_load
публикует ds_marts_clickhouse (крайняя точка пайплайна).

Spark-джобы submit'ятся через SparkSubmitOperator (client mode):
driver запускается в Airflow-контейнере, executor — на spark-worker.

Последовательность DAG-а:
  1. dbt run tag:bv             (Postgres business_vault.*)
  2. dbt run tag:mart           (Postgres marts.*: всё, кроме mart_team_elo_*)
  3. dbt test tag:bv tag:mart
  4. spark_calculate_elo        (Spark JDBC: PG marts → PG mart_team_elo_*)
  5. spark_marts_to_parquet     (Spark JDBC: PG marts → ./spark/output/*.parquet)
  6. upload_marts_to_minio      (./spark/output/ → MinIO bucket marts/)
  7. clickhouse_load            (TRUNCATE + INSERT FROM s3() для каждой март-таблицы)
"""
from __future__ import annotations

import glob
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from _datasets import ds_marts_clickhouse, ds_raw_vault


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


def upload_marts_to_minio(**_):
    """Заливает parquet из ./spark/output/ в MinIO bucket marts/.

    Spark пишет parquet локально (/opt/spark/jobs/output/ внутри spark-master,
    ./spark/output/ на хосте). Прямая запись в s3a требует aws-java-sdk-bundle
    ~273MB через Maven, который из РФ часто недоступен — поэтому двухступенчатый
    подход. Airflow видит ту же директорию через volume mount /opt/airflow/spark_output.
    """
    from minio import Minio

    src_root = "/opt/airflow/spark_output"
    bucket = "marts"

    client = Minio(
        "minio:9000",
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=False,
    )

    # Чистим bucket — марты не append-only, каждый запуск перезаливает всё.
    existing = list(client.list_objects(bucket, recursive=True))
    if existing:
        from minio.deleteobjects import DeleteObject
        delete_iter = (DeleteObject(o.object_name) for o in existing)
        errors = list(client.remove_objects(bucket, delete_iter))
        for e in errors:
            print(f"[upload_marts] delete error: {e}", flush=True)
        print(f"[upload_marts] cleared {len(existing)} old objects", flush=True)

    total_files = 0
    for mart_name, _ in MARTS:
        mart_dir = os.path.join(src_root, mart_name)
        if not os.path.isdir(mart_dir):
            raise FileNotFoundError(f"Spark output missing: {mart_dir}")

        parquet_files = glob.glob(os.path.join(mart_dir, "*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No parquet files in {mart_dir}")

        for fpath in parquet_files:
            fname = os.path.basename(fpath)
            object_name = f"{mart_name}/{fname}"
            client.fput_object(bucket, object_name, fpath)
            total_files += 1
            print(f"[upload_marts] {object_name}  ({os.path.getsize(fpath)} bytes)", flush=True)

    print(f"[upload_marts] total files uploaded: {total_files}", flush=True)


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
    # Dataset-trigger: запускается после публикации ds_raw_vault в dbt_raw_vault.
    schedule=[ds_raw_vault],
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

    # Общие настройки Spark client mode: driver=Airflow, executor=worker.
    # Память подобрана под worker (--memory 1500m): driver 512m + executor 768m.
    _SPARK_COMMON = dict(
        conn_id="spark_default",
        deploy_mode="client",
        packages="org.postgresql:postgresql:42.7.4",
        env_vars={
            "POSTGRES_HOST": "postgres",
            "POSTGRES_INTERNAL_PORT": "5432",
            "POSTGRES_DWH_DB": "dwh",
            "POSTGRES_USER": "football",
            "POSTGRES_PASSWORD": "football",
        },
        conf={
            "spark.jars.ivy": "/tmp/.ivy2",
            "spark.driver.memory": "512m",
            "spark.executor.memory": "768m",
            "spark.executor.cores": "1",
            # PySpark требует одной minor-версии Python на driver и executor.
            # Driver = Airflow-контейнер: Python 3.11 по /home/airflow/.local/bin/python3.11.
            # Executor = spark-worker: Python 3.11 доставлен в docker/spark/Dockerfile
            # и лежит по /usr/bin/python3.11. Пути разные — передаём раздельно.
            "spark.pyspark.driver.python": "/home/airflow/.local/bin/python3.11",
            "spark.pyspark.python": "/usr/bin/python3.11",
        },
    )

    spark_elo = SparkSubmitOperator(
        task_id="spark_calculate_elo",
        application="/opt/spark/jobs/jobs/calculate_elo.py",
        name="calculate_elo",
        **_SPARK_COMMON,
    )

    spark_marts = SparkSubmitOperator(
        task_id="spark_marts_to_parquet",
        application="/opt/spark/jobs/jobs/marts_pg_to_minio.py",
        name="marts_pg_to_minio",
        **_SPARK_COMMON,
    )

    upload_marts = PythonOperator(
        task_id="upload_marts_to_minio",
        python_callable=upload_marts_to_minio,
    )

    ch_load = PythonOperator(
        task_id="clickhouse_load",
        python_callable=clickhouse_load,
        # Финальная таска DAG-а публикует Dataset — крайняя точка цепочки.
        outlets=[ds_marts_clickhouse],
    )

    dbt_run_bv >> dbt_run_mart >> dbt_test >> spark_elo >> spark_marts >> upload_marts >> ch_load
