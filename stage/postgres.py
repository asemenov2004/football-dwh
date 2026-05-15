"""Подключение к Postgres DWH для stage-слоя.
"""
from __future__ import annotations

from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from ingestion import config


def _dsn() -> str:
    return (
        f"postgresql+psycopg2://{config.POSTGRES_USER}:{config.POSTGRES_PASSWORD}"
        f"@{config.POSTGRES_HOST}:{config.POSTGRES_PORT}/{config.POSTGRES_DWH_DB}"
    )


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    # pool_pre_ping — Airflow-воркер может простоять несколько часов между
    # тасками и получить stale connection. Пинг дёшев, разрывы молчаливы.
    return create_engine(_dsn(), pool_pre_ping=True, future=True)
