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

RAW_API_FOOTBALL_BUCKET = "raw-api-football"
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

# ---------- API-Football ----------
API_FOOTBALL_BASE_URL = "https://v3.football.api-sports.io"
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")
API_FOOTBALL_DAILY_LIMIT = 100

# ---------- Этап 1: топ-5 лиг Европы + UCL ----------
# name            — slug для task_id и ключа S3 (league_id=<name>)
# api_football_id — ID лиги в API-Football
# statsbomb       — (country_name, competition_name) для фильтрации
#                   competitions() в StatsBomb Open Data; competition_id
#                   резолвится динамически, т.к. покрытие неравномерное
LEAGUES = [
    {
        "name": "epl",
        "api_football_id": 39,
        "statsbomb": ("England", "Premier League"),
    },
    {
        "name": "la_liga",
        "api_football_id": 140,
        "statsbomb": ("Spain", "La Liga"),
    },
    {
        "name": "serie_a",
        "api_football_id": 135,
        "statsbomb": ("Italy", "Serie A"),
    },
    {
        "name": "bundesliga",
        "api_football_id": 78,
        "statsbomb": ("Germany", "1. Bundesliga"),
    },
    {
        "name": "ligue_1",
        "api_football_id": 61,
        "statsbomb": ("France", "Ligue 1"),
    },
    {
        "name": "ucl",
        "api_football_id": 2,
        "statsbomb": ("Europe", "Champions League"),
    },
]

# API-Football: free tier даёт сезоны 2022-2024. Берём 2024 (сезон 2024/25 —
# полный, свежий). Override через env, если план обновится.
DEFAULT_API_FOOTBALL_SEASON = int(os.getenv("API_FOOTBALL_SEASON", "2024"))

# ---------- Пути для локального состояния ingestion ----------
# Живёт рядом с логами Airflow, чтобы не плодить лишних томов.
INGESTION_STATE_DIR = os.getenv(
    "INGESTION_STATE_DIR", "/opt/airflow/logs/ingestion_state"
)
API_FOOTBALL_USAGE_FILE = os.path.join(
    INGESTION_STATE_DIR, "api_football_usage.json"
)
