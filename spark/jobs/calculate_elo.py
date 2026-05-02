"""Spark-джоба: расчёт Elo-рейтинга команд по матчам Understat.

Источник: public_marts.mart_match_facts (PG, ~6 901 матч, 5 лиг × 4 сезона).
Per-league: каждая лига — изолированный пул рейтингов, старт 1500. Если команда
играет только в одной лиге — её league_id фиксирован; для команд, которые в
течение проекта побывали в нескольких лигах (промоушн/релегейшн), считаем
независимые рейтинги per (league, team) — это упрощение, но в рамках курсовой
ок (overlap минимальный, на витрине будет видно отдельной строкой на лигу).

Формула — стандарт ClubElo:
  - K = 20
  - home advantage = 100 (хозяева получают +100 для расчёта expected)
  - goal-difference modifier: при |gd| >= 2 → K *= ln(|gd|+1) ≈ 1.099 (gd=2),
    1.386 (gd=3), 1.609 (gd=4), ...
  - score: win=1.0, draw=0.5, loss=0.0
  - Expected_home = 1 / (1 + 10^((R_away - R_home - 100) / 400))
  - R_new = R + K_eff * (S - E)

ВАЖНО: Elo строго последовательный (рейтинг N+1 зависит от N), поэтому
расчёт ведётся в Python-цикле на драйвере. Spark тут — для JDBC-IO + демонстрации
кластера в pipeline. На 6 901 матче цикл занимает <1 сек, проблем нет.

Output → public_marts (Postgres):
  - mart_team_elo_history: 2 строки на матч (одна per команду)
  - mart_team_elo_current: финальный рейтинг + peak per (team, league)

Запуск (через scripts/run_spark_elo.sh):
  spark-submit --master local[*] --packages org.postgresql:postgresql:42.7.4 \
    /opt/spark/jobs/jobs/calculate_elo.py
"""
from __future__ import annotations

import math
import os
import sys
from collections import defaultdict
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_PORT = os.getenv("POSTGRES_INTERNAL_PORT", "5432")
PG_DB = os.getenv("POSTGRES_DWH_DB", "dwh")
PG_USER = os.getenv("POSTGRES_USER", "football")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "football")

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"
JDBC_PROPS = {"user": PG_USER, "password": PG_PASS, "driver": "org.postgresql.Driver"}

K_FACTOR = 20.0
HOME_ADVANTAGE = 100.0
INITIAL_RATING = 1500.0


def expected_score(r_home: float, r_away: float) -> float:
    """Ожидание для хозяев с учётом home advantage."""
    return 1.0 / (1.0 + 10.0 ** ((r_away - r_home - HOME_ADVANTAGE) / 400.0))


def gd_modifier(gd: int) -> float:
    """Множитель K при разнице голов >= 2."""
    return math.log(gd + 1) if gd >= 2 else 1.0


HISTORY_SCHEMA = StructType([
    StructField("team_title", StringType(), nullable=False),
    StructField("league_id", StringType(), nullable=False),
    StructField("season_year", IntegerType(), nullable=False),
    StructField("match_date", DateType(), nullable=False),
    StructField("opponent_title", StringType(), nullable=False),
    StructField("is_home", IntegerType(), nullable=False),
    StructField("goals_for", IntegerType(), nullable=True),
    StructField("goals_against", IntegerType(), nullable=True),
    StructField("rating_before", DoubleType(), nullable=False),
    StructField("rating_after", DoubleType(), nullable=False),
    StructField("rating_delta", DoubleType(), nullable=False),
    # Флаг "команда в топ-3 своей лиги по финальному рейтингу".
    # Используется для line-чарта эволюции Elo в Superset (фильтр без хардкода имён).
    StructField("is_top3_in_league", IntegerType(), nullable=False),
])

CURRENT_SCHEMA = StructType([
    StructField("team_title", StringType(), nullable=False),
    StructField("league_id", StringType(), nullable=False),
    StructField("current_rating", DoubleType(), nullable=False),
    StructField("peak_rating", DoubleType(), nullable=False),
    StructField("peak_match_date", DateType(), nullable=False),
    StructField("matches_played", IntegerType(), nullable=False),
    StructField("last_match_date", DateType(), nullable=False),
])


def calc_elo(matches: list) -> tuple[list[tuple], list[tuple]]:
    """Возвращает (history_rows, current_rows)."""
    ratings: dict[tuple[str, str], float] = defaultdict(lambda: INITIAL_RATING)
    peaks: dict[tuple[str, str], tuple[float, date]] = {}
    matches_played: dict[tuple[str, str], int] = defaultdict(int)
    last_match: dict[tuple[str, str], date] = {}
    history: list[tuple] = []

    for m in matches:
        league = m["league_id"]
        season = int(m["season_year"])
        d = m["match_date"]
        home = m["home_team_title"]
        away = m["away_team_title"]
        gh = int(m["home_goals"])
        ga = int(m["away_goals"])

        key_h = (home, league)
        key_a = (away, league)
        rh, ra = ratings[key_h], ratings[key_a]

        eh = expected_score(rh, ra)
        ea = 1.0 - eh

        if gh > ga:
            sh, sa = 1.0, 0.0
        elif gh < ga:
            sh, sa = 0.0, 1.0
        else:
            sh, sa = 0.5, 0.5

        k_eff = K_FACTOR * gd_modifier(abs(gh - ga))

        rh_new = rh + k_eff * (sh - eh)
        ra_new = ra + k_eff * (sa - ea)

        ratings[key_h] = rh_new
        ratings[key_a] = ra_new

        for key, new_r in ((key_h, rh_new), (key_a, ra_new)):
            matches_played[key] += 1
            last_match[key] = d
            if key not in peaks or new_r > peaks[key][0]:
                peaks[key] = (new_r, d)

        history.append((home, league, season, d, away, 1, gh, ga, rh, rh_new, rh_new - rh, 0))
        history.append((away, league, season, d, home, 0, ga, gh, ra, ra_new, ra_new - ra, 0))

    # Финальный топ-3 per league
    by_league: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for (team, league), r in ratings.items():
        by_league[league].append((team, r))
    top3: set[tuple[str, str]] = set()
    for league, lst in by_league.items():
        lst.sort(key=lambda x: x[1], reverse=True)
        for team, _ in lst[:3]:
            top3.add((team, league))

    # Проставляем is_top3 в history (по (team, league))
    history = [
        row[:-1] + (1 if (row[0], row[1]) in top3 else 0,)
        for row in history
    ]

    current = []
    for (team, league), r in ratings.items():
        peak_r, peak_d = peaks[(team, league)]
        current.append((
            team, league, float(r), float(peak_r), peak_d,
            int(matches_played[(team, league)]),
            last_match[(team, league)],
        ))
    return history, current


def main() -> int:
    spark = (
        SparkSession.builder
        .appName("calculate_elo")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Читаем сыгранные матчи (есть счёт), сортируем по дате
        src_query = (
            "(SELECT league_id, season_year, match_date, "
            "home_team_title, away_team_title, home_goals, away_goals "
            "FROM public_marts.mart_match_facts "
            "WHERE home_goals IS NOT NULL AND away_goals IS NOT NULL) AS src"
        )
        df = (
            spark.read.format("jdbc")
            .option("url", JDBC_URL).option("dbtable", src_query)
            .option("user", PG_USER).option("password", PG_PASS)
            .option("driver", "org.postgresql.Driver")
            .load()
            .orderBy("match_date", "league_id", "home_team_title")
        )
        rows = [r.asDict() for r in df.collect()]
        print(f"[elo] matches loaded: {len(rows)}", flush=True)

        history, current = calc_elo(rows)
        print(f"[elo] history rows: {len(history)}, current teams: {len(current)}", flush=True)

        history_df = spark.createDataFrame(history, schema=HISTORY_SCHEMA)
        current_df = spark.createDataFrame(current, schema=CURRENT_SCHEMA)

        history_df.write.mode("overwrite").option(
            "createTableColumnTypes",
            "team_title VARCHAR(200), league_id VARCHAR(50), opponent_title VARCHAR(200)",
        ).jdbc(JDBC_URL, "public_marts.mart_team_elo_history", properties=JDBC_PROPS)

        current_df.write.mode("overwrite").option(
            "createTableColumnTypes",
            "team_title VARCHAR(200), league_id VARCHAR(50)",
        ).jdbc(JDBC_URL, "public_marts.mart_team_elo_current", properties=JDBC_PROPS)

        print("[elo] written to public_marts.mart_team_elo_{history,current}", flush=True)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
