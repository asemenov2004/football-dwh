"""Spark: PG public_marts.* → Parquet в /opt/spark/jobs/output/.

Запись в MinIO отдельным шагом через `mc cp` (scripts/run_spark_marts.sh) —
прямой s3a требует aws-java-sdk-bundle ~273MB через Maven, который из РФ
часто отдаётся с connection refused.
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
    ("public_marts.mart_sb_la_liga_history",    f"{OUTPUT_DIR}/mart_sb_la_liga_history"),
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
