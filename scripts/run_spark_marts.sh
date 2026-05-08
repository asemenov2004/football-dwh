#!/usr/bin/env bash
# Spark-перелив: Postgres public_marts.* → Parquet → MinIO bucket marts/.
# Запускается ВРУЧНУЮ перед триггером DAG build_marts.
#
# Pipeline:
#   1. Spark JDBC read PG → write Parquet в /opt/spark/jobs/output/ (внутри
#      spark-master контейнера; на хосте — ./spark/output/).
#   2. mc (одноразовый minio/mc контейнер) копирует Parquet из ./spark/output
#      в MinIO bucket marts/.
#
# Почему так: прямая запись Spark → s3a требует jar aws-java-sdk-bundle ~273MB
# через Maven Central, который из РФ часто отдаётся с connection refused.
# Postgres-jdbc (~1.5MB) докачивается моментально. Заливка через mc легче и
# использует уже имеющийся образ minio/mc.

set -euo pipefail

# Git Bash на Windows конвертирует Unix-пути в Windows-пути.
# Отключаем для команд с путями /opt/spark/... внутри контейнера.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

PACKAGES="org.postgresql:postgresql:42.7.4"

echo "==> [1/2] Spark JDBC read PG → Parquet в ./spark/output/"
docker exec \
  -e POSTGRES_HOST=postgres \
  -e POSTGRES_INTERNAL_PORT=5432 \
  -e POSTGRES_DWH_DB=dwh \
  -e POSTGRES_USER=football \
  -e POSTGRES_PASSWORD=football \
  football_spark_master \
  /opt/spark/bin/spark-submit \
    --master "local[*]" \
    --packages "${PACKAGES}" \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    /opt/spark/jobs/jobs/marts_pg_to_minio.py

echo ""
echo "==> [2/2] mc cp ./spark/output → MinIO bucket marts/"
docker run --rm \
  --network football_net \
  -v "$(pwd)/spark/output:/output:ro" \
  -e MC_HOST_local="http://minioadmin:minioadmin123@minio:9000" \
  --entrypoint sh \
  minio/mc:RELEASE.2024-10-02T08-27-28Z \
  -c '
    mc rm --recursive --force local/marts/ >/dev/null 2>&1 || true
    mc cp --recursive /output/mart_league_table          local/marts/
    mc cp --recursive /output/mart_top_scorers           local/marts/
    mc cp --recursive /output/mart_match_facts           local/marts/
    mc cp --recursive /output/mart_player_overperformers local/marts/
    mc cp --recursive /output/mart_team_xg_trend         local/marts/
    mc cp --recursive /output/mart_team_elo_current      local/marts/
    mc cp --recursive /output/mart_team_elo_history      local/marts/
    mc cp --recursive /output/mart_sb_la_liga_history    local/marts/
    echo "--- MinIO local/marts/ ---"
    mc ls --recursive local/marts/
  '

echo ""
echo "==> Готово. Триггерь DAG build_marts → clickhouse_load заберёт parquet."
