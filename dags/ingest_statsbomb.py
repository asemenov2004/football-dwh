"""DAG: загрузка StatsBomb Open Data по топ-5 лигам + UCL в raw lake.

StatsBomb Open Data — бесплатный репозиторий GitHub, без лимитов и ключей.
Покрытие неравномерное: La Liga — 100% сезонов Месси, UCL — часть финалов,
остальные лиги (EPL, Serie A, Bundesliga, Ligue 1) могут отсутствовать
вовсе либо иметь 1-2 сезона.

Схема задач:
    ingest_competitions
        ├─> matches__epl
        ├─> matches__la_liga
        ├─> matches__serie_a
        ├─> matches__bundesliga
        ├─> matches__ligue_1
        └─> matches__ucl

Если для лиги в Open Data ничего нет — task завершается успешно с WARNING
в логах. Это ожидаемое поведение: не хотим красить DAG в красный из-за
того, что StatsBomb не опубликовал условную Бундеслигу.

События (events) и lineups добавим на следующих итерациях — объём большой.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from ingestion import config, statsbomb_client
from ingestion.minio_writer import build_object_key, put_json, today_utc

log = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "football_dwh",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _ingest_competitions(**context) -> str:
    payload = statsbomb_client.get_competitions()
    key = build_object_key(
        source="statsbomb",
        endpoint="competitions",
        league_id=None,
        season=None,
        dt=today_utc(),
        filename="competitions.json",
    )
    return put_json(
        bucket=config.RAW_STATSBOMB_BUCKET,
        object_key=key,
        payload=payload,
    )


def _ingest_matches(
    league_name: str,
    country: str,
    competition: str,
    **context,
) -> list[str]:
    """Качает matches.json для всех сезонов лиги в StatsBomb Open Data.

    Фильтруем competitions по (country_name, competition_name) — надёжнее,
    чем хардкодить competition_id. Если совпадений нет — лига не
    опубликована, успешно выходим с предупреждением.
    """
    competitions = statsbomb_client.get_competitions()
    matched = [
        c for c in competitions
        if c.get("country_name") == country
        and c.get("competition_name") == competition
    ]
    if not matched:
        log.warning(
            "StatsBomb Open Data: нет данных для %s (%s / %s) — пропускаю",
            league_name, country, competition,
        )
        return []

    comp_id = matched[0]["competition_id"]
    seasons = sorted({c["season_id"] for c in matched})
    log.info(
        "StatsBomb %s: competition_id=%s, сезонов %d",
        league_name, comp_id, len(seasons),
    )

    written: list[str] = []
    for season_id in seasons:
        payload = statsbomb_client.get_matches(
            competition_id=comp_id, season_id=season_id
        )
        key = build_object_key(
            source="statsbomb",
            endpoint="matches",
            league_id=league_name,
            season=season_id,
            dt=today_utc(),
            filename="matches.json",
        )
        written.append(put_json(
            bucket=config.RAW_STATSBOMB_BUCKET,
            object_key=key,
            payload=payload,
        ))
    return written


with DAG(
    dag_id="ingest_statsbomb",
    description="Raw ingest: StatsBomb Open Data → MinIO (top-5 + UCL)",
    start_date=datetime(2026, 4, 1),
    # @weekly — Open Data обновляется редко
    schedule="@weekly",
    catchup=False,
    default_args=DEFAULT_ARGS,
    # statsbombpy тянет pandas DataFrame и жрёт память; 4 параллельные
    # таски на слабом хосте ловят zombie-kill от executor. Сериализуем.
    max_active_tasks=1,
    # единообразно с api-football: не даём scheduler'у запускать второй run.
    max_active_runs=1,
    tags=["ingestion", "statsbomb"],
) as dag:
    t_competitions = PythonOperator(
        task_id="ingest_competitions",
        python_callable=_ingest_competitions,
    )
    for league in config.LEAGUES:
        country, competition = league["statsbomb"]
        t_matches = PythonOperator(
            task_id=f"matches__{league['name']}",
            python_callable=_ingest_matches,
            op_kwargs={
                "league_name": league["name"],
                "country": country,
                "competition": competition,
            },
        )
        t_competitions >> t_matches
