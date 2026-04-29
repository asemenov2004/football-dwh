"""Константы для ingestion-слоя.

Значения, которые меняются между окружениями, читаются из env.
Хардкод оставлен только для идентификаторов сущностей (лиги/турниры).
"""
from __future__ import annotations

import os

# ---------- MinIO (raw lake) ----------
# Внутри docker-сети всегда ходим на сервис `minio:9000`.
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

RAW_STATSBOMB_BUCKET = "raw-statsbomb"

# ---------- Postgres DWH (stage + raw vault + business vault) ----------
# Внутри docker-сети ходим на сервис `postgres:5432`. Для stage-слоя
# используется отдельная БД, указанная в POSTGRES_DWH_DB.
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_INTERNAL_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "football")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "football")
POSTGRES_DWH_DB = os.getenv("POSTGRES_DWH_DB", "dwh")

STAGE_SCHEMA = "stage"

# ---------- Этап 1: топ-5 лиг Европы + UCL ----------
# name      — slug для task_id и ключа S3 (league_id=<name>)
# statsbomb — (country_name, competition_name) для фильтрации
#             competitions() в StatsBomb Open Data; competition_id
#             резолвится динамически, т.к. покрытие неравномерное
LEAGUES = [
    {"name": "epl",        "statsbomb": ("England", "Premier League")},
    {"name": "la_liga",    "statsbomb": ("Spain", "La Liga")},
    {"name": "serie_a",    "statsbomb": ("Italy", "Serie A")},
    {"name": "bundesliga", "statsbomb": ("Germany", "1. Bundesliga")},
    {"name": "ligue_1",    "statsbomb": ("France", "Ligue 1")},
    {"name": "ucl",        "statsbomb": ("Europe", "Champions League")},
]

# ---------- Understat ----------
RAW_UNDERSTAT_BUCKET = "raw-understat"
DEFAULT_UNDERSTAT_SEASON = int(os.getenv("UNDERSTAT_SEASON", "2025"))

# Топ-5 лиг (UCL в understat нет)
UNDERSTAT_LEAGUES = [
    {"name": "epl",        "understat_id": "epl"},
    {"name": "la_liga",    "understat_id": "la_liga"},
    {"name": "serie_a",    "understat_id": "serie_a"},
    {"name": "bundesliga", "understat_id": "bundesliga"},
    {"name": "ligue_1",    "understat_id": "ligue_1"},
]

# ---------- Пути для локального состояния ingestion ----------
# Живёт рядом с логами Airflow, чтобы не плодить лишних томов.
INGESTION_STATE_DIR = os.getenv(
    "INGESTION_STATE_DIR", "/opt/airflow/logs/ingestion_state"
)
