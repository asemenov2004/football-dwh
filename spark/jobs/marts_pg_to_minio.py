"""Spark-джоба: PG (public_marts.*) → Parquet в локальной папке /opt/spark/jobs/output/.
Заливка в MinIO делается отдельным шагом через `mc cp` (см. scripts/run_spark_marts.sh).

Почему не s3a:
    Прямая запись в MinIO требует hadoop-aws + aws-java-sdk-bundle (~273MB).
    Скачивание через Maven Central часто упирается в connection refused.
    Pragmatic choice: Spark делает heavy lifting (JDBC + parquet), MinIO upload
    отдельной командой через `mc` (уже есть в minio-init контейнере).

Запуск (driver-mode local в spark-master контейнере, см. scripts/run_spark_marts.sh):
    spark-submit --master local[*] \\
      --packages org.postgresql:postgresql:42.7.4 \\
      /opt/spark/jobs/jobs/marts_pg_to_minio.py
"""
from __future__ import annotations

import os
import shutil
import sys

from pyspark.sql import SparkSession


PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_INTERNAL_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DWH_DB", "dwh")
PG_USER = os.getenv("POSTGRES_USER", "football")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "football")

OUTPUT_DIR = "/opt/spark/jobs/output"

MARTS = [
    ("public_marts.mart_league_table",          f"{OUTPUT_DIR}/mart_league_table"),
    ("public_marts.mart_top_scorers",           f"{OUTPUT_DIR}/mart_top_scorers"),
    ("public_marts.mart_match_facts",           f"{OUTPUT_DIR}/mart_match_facts"),
    ("public_marts.mart_player_overperformers", f"{OUTPUT_DIR}/mart_player_overperformers"),
    ("public_marts.mart_team_xg_trend",         f"{OUTPUT_DIR}/mart_team_xg_trend"),
    ("public_marts.mart_team_elo_current",      f"{OUTPUT_DIR}/mart_team_elo_current"),
    ("public_marts.mart_team_elo_history",      f"{OUTPUT_DIR}/mart_team_elo_history"),
]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("marts_pg_to_local_parquet")
        .getOrCreate()
    )


def transfer(spark: SparkSession, dbtable: str, out_path: str) -> int:
    df = (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")
        .option("dbtable", dbtable)
        .option("user", PG_USER)
        .option("password", PG_PASS)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    rows = df.count()
    # Чистим перед записью — overwrite (марты не append-only).
    if os.path.isdir(out_path):
        shutil.rmtree(out_path)
    df.coalesce(1).write.mode("overwrite").parquet(out_path)
    print(f"[marts_pg_to_minio] {dbtable} -> {out_path}  rows={rows}", flush=True)
    return rows


def main() -> int:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        total = 0
        for dbtable, out_path in MARTS:
            total += transfer(spark, dbtable, out_path)
        print(f"[marts_pg_to_minio] total rows transferred: {total}", flush=True)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
