#!/bin/bash
# Инициализация: создаёт отдельную БД под метаданные Airflow и под DWH.
# POSTGRES_DB (postgres) создаётся образом по умолчанию — его не трогаем.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<-EOSQL
    CREATE DATABASE ${POSTGRES_AIRFLOW_DB};
    CREATE DATABASE ${POSTGRES_DWH_DB};
    GRANT ALL PRIVILEGES ON DATABASE ${POSTGRES_AIRFLOW_DB} TO ${POSTGRES_USER};
    GRANT ALL PRIVILEGES ON DATABASE ${POSTGRES_DWH_DB} TO ${POSTGRES_USER};
EOSQL