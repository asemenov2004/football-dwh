#!/usr/bin/env bash
# Spark-джоба расчёта Elo: PG (mart_match_facts) → cycle → PG (mart_team_elo_*).
# Запускается ВРУЧНУЮ перед run_spark_marts.sh: Elo нужен ДО переливки в parquet.
#
# Pipeline:
#   1. Spark JDBC read public_marts.mart_match_facts.
#   2. Сортировка по дате + Python-цикл (драйвер).
#   3. Spark JDBC write public_marts.mart_team_elo_history + mart_team_elo_current.
#
# Параметры (захардкожены в jobs/calculate_elo.py): K=20, home_adv=100,
# ln(|gd|+1) modifier при |gd|>=2, старт 1500. Стандарт ClubElo.

set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

PACKAGES="org.postgresql:postgresql:42.7.4"

echo "==> Spark Elo: PG.mart_match_facts → PG.mart_team_elo_{history,current}"
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
    /opt/spark/jobs/jobs/calculate_elo.py

echo ""
echo "==> Готово. Дальше: bash scripts/run_spark_marts.sh && триггер DAG build_marts."
