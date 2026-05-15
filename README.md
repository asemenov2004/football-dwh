# Football DWH

Учебное хранилище данных по футбольной статистике на Data Vault 2.0. Курсовая работа 2026.

Источники — Understat (xG-разметка, топ-5 европейских лиг, сезоны 2022–2025) и StatsBomb Open Data (исторические данные La Liga). Данные проходят путь от сырого JSON в S3-хранилище до интерактивных дашбордов в Superset.

## Стек

| Слой | Технология |
|---|---|
| Оркестрация | Apache Airflow 2.9 (LocalExecutor) |
| Raw lake | MinIO (S3-compatible) |
| DWH (DV 2.0) | PostgreSQL 16 + dbt-core + datavault4dbt |
| Data marts | ClickHouse 24.8 |
| Обработка | PySpark 3.5 (Standalone) |
| BI | Apache Superset |
| CI | GitHub Actions |

Схема потока: `Source → MinIO → PG.stage → Raw Vault → Business Vault → ClickHouse marts → Superset`.

Архитектурные диаграммы лежат в [docs/diagrams/](docs/diagrams/) — C4 (Context, Containers), DFD, ER Data Vault, ER витрин.

## Требования

- Docker Desktop с WSL2 backend, минимум 12 ГБ оперативки.
- Около 20 ГБ свободного места под `./data/`.
- Python 3.11 локально — опционально, для запуска dbt/ingestion вне контейнеров.

## Подъём локально

```bash
# 1. Окружение
cp .env.example .env
# сгенерировать Fernet key для Airflow:
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# подставить значение в AIRFLOW_FERNET_KEY

# 2. Стек
docker compose up -d

# 3. Сервисы:
# Airflow:    http://localhost:18080  (admin/admin)
# MinIO:      http://localhost:9001   (minioadmin/minioadmin123)
# ClickHouse: http://localhost:8123/play
# Superset:   http://localhost:8088   (admin/admin)
# Spark UI:   http://localhost:8181
```

Порт Airflow перенесён на 18080, потому что на 8080 на Windows часто висит HP Smart / ApplicationWebServer.

### Лимит памяти WSL2

Docker на Windows работает через WSL2, и по умолчанию WSL забирает до 50% RAM, чего проекту не хватит. Создай `C:\Users\<user>\.wslconfig`:

```ini
[wsl2]
memory=12GB
processors=4
swap=4GB
```

Применить: `wsl --shutdown` и перезапуск Docker Desktop.

## Структура каталогов

```
dags/          — Airflow DAG'и
dbt/           — dbt-проект (stage / raw_vault / business_vault / marts)
ingestion/     — Python-клиенты Understat и StatsBomb
stage/         — DDL и loader'ы MinIO → Postgres.stage
spark/         — Spark-джобы (расчёт Elo, перелив витрин)
docker/        — Dockerfile'ы и init-скрипты сервисов
docs/          — диаграммы, скриншоты, материалы курсовой
data/          — bind-mounts (gitignored): postgres, minio, clickhouse, логи
```

## Пайплайн

### Ежедневная цепочка (Airflow Datasets)

```
ingest_understat_daily (cron 04:00)
    ↓ ds_understat_raw
stage_load_understat
    ↓ ds_understat_stage
dbt_raw_vault
```

DAG'и связаны через Airflow Datasets — каждый следующий запускается автоматически после публикации соответствующего датасета. В UI Airflow это видно во вкладке **Datasets**.

StatsBomb и историческая заливка Understat — отдельные one-off DAG'и с manual trigger, к daily-расписанию не привязаны.

### Витрины и ClickHouse (manual)

```bash
# 1. Spark — расчёт Elo по матчам, запись обратно в Postgres
bash scripts/run_spark_elo.sh

# 2. Spark — JDBC.read marts.* → parquet → MinIO
bash scripts/run_spark_marts.sh

# 3. Триггер DAG build_marts в Airflow:
docker exec football_airflow_scheduler airflow dags trigger build_marts
```

`build_marts` делает по шагам: `dbt run tag:bv` → `dbt run tag:mart` → `dbt test` → `clickhouse_load` (TRUNCATE + `INSERT FROM s3()` для каждой витрины).

Замечание про Spark: parquet пишется в локальный mount-volume, дальше переливается в MinIO через `mc cp`. Прямая запись через `s3a://` требует hadoop-aws + aws-java-sdk-bundle (~273 MB через Maven Central), что из РФ работает нестабильно.

### Superset

Поднимается как сервис в docker-compose (`docker/superset/Dockerfile` собирает образ с драйвером `clickhouse-connect`). Подключение к ClickHouse, датасеты, чарты и два дашборда собирались в UI на http://localhost:8088 (admin/admin). Описание дашбордов — ниже.

## Источники данных

**Understat** ([understat.com](https://understat.com)) — независимый ресурс с xG-разметкой. Покрытие: топ-5 лиг (АПЛ, Ла Лига, Серия A, Бундеслига, Лига 1), сезоны 2022–2025. Метрики: xG, xA, npxG, xPTS, PPDA. Доступ — HTML-парсинг через `soccerdata`, без лимитов.

**StatsBomb Open Data** ([github.com/statsbomb/open-data](https://github.com/statsbomb/open-data)) — исторические данные La Liga и других турниров. Используется как изолированная ветка для счёта и статуса матчей. Bridge с Understat — линк `lnk_match_same_as` (24 пары пересечений).

API-Football пробовали на старте, отказались: free tier 100 req/день и нет xG.

## Дашборды

### Football DWH (основной)

9 чартов поверх ClickHouse-витрин. В шапке два Native Filter — **Лига** (default `epl`) и **Сезон** (default 2025).

| Чарт | Тип | Витрина |
|---|---|---|
| Турнирная таблица | Table | mart_league_table |
| Топ-15 бомбардиров: goals vs xG | Grouped bar | mart_top_scorers |
| Avg total_xG по неделям | Area | mart_match_facts |
| Топ-5 overperformers | Table | mart_player_overperformers |
| Top-10 команд по avg xG | Dist bar | mart_team_xg_trend |
| Топ-10 матчей по total_xG | Table | mart_match_facts |
| Топ-10 команд по Elo (текущий) | Dist bar | mart_team_elo_current |
| Эволюция Elo: топ-3 команды лиги | Line | mart_team_elo_history |
| StatsBomb: Barcelona по сезонам La Liga | Table | mart_sb_la_liga_history |

### Football DWH — European Teams

Кросс-лиговый дашборд, без фильтра по лиге — на одном экране видна вся топ-5.

| Чарт | Тип | Витрина |
|---|---|---|
| Топ-20 клубов Европы по Elo | Dist bar | mart_team_elo_current |
| Топ-15 over/underperformers: PTS - xPTS | Table | mart_league_table |
| Топ-15 голеадоров Европы | Table | mart_top_scorers |
| Топ-апсеты: xG предсказал не того победителя | Table | mart_match_facts |

Скриншоты обоих дашбордов лежат в [docs/screenshots/](docs/screenshots/).

## Raw Vault

Схема `public_raw_vault` в Postgres. Генерация через макросы `datavault4dbt` (ScalefreeCOM v1.17.0). Hash-функция — MD5, ключи `bytea`.

| Объект | Строк | Описание |
|---|---|---|
| `hub_team` | 125 | Команды Understat (BK = `understat\|{lower(team_title)}`) |
| `hub_match` | 9 355 | Матчи Understat + SB (BK = `understat\|{id}` или `sb\|{id}`) |
| `hub_competition` | 6 | Турниры (общий BK = league slug) |
| `hub_season` | 4 | Сезоны 2022–2025 |
| `hub_player` | 4 982 | Игроки Understat |
| `lnk_match_team` | 13 790 | Матч ↔ команда + роль home/away |
| `lnk_team_competition_season` | 386 | Команда ↔ лига ↔ сезон |
| `lnk_player_team` | 11 360 | Игрок ↔ команда ↔ сезон, с мид-сезонными трансферами |
| `lnk_match_competition_season` | 6 895 | Матч ↔ лига ↔ сезон |
| `lnk_match_same_as` | 24 | Bridge SB ↔ Understat по (date, league, normalized teams) |
| `sat_team_details` | 125 | Название команды + лига |
| `sat_team_xg` | 386 | xG/NPxG/PPDA/OPPDA/xPTS/PTS на сезон |
| `sat_match_score` | 2 460 | SB: счёт + статус матча |
| `sat_match_score_understat` | 6 895 | Understat: счёт + datetime |
| `sat_match_xg` | 6 895 | Understat: home_xg / away_xg |
| `sat_player_xg` | 11 054 | xG/xA/npxG/goals/assists/minutes на сезон |

DV-stage views в схеме `public_stage_dv` читают `stage.*` и вычисляют MD5-хеши на лету, физически данных не хранят.

## Business Vault и Marts

Схемы `public_business_vault`, `public_marts` в Postgres + `marts` в ClickHouse.

| Объект | Слой | Строк | Описание |
|---|---|---|---|
| `pit_team_season` | BV | 386 | PIT-снапшот команды × лига × сезон |
| `pit_player_season` | BV | 11 054 | PIT-снапшот игрока × лига × сезон |
| `br_player_team_season` | BV | 11 360 | Bridge игрок × команда × лига × сезон (трансферы) |
| `br_team_competition_season` | BV | 386 | Bridge команда × лига × сезон |
| `mart_league_table` | Mart | 386 | Турнирная таблица: PTS/xPTS/xG/PPDA + position |
| `mart_top_scorers` | Mart | 11 054 | Топ-скореры с teams_concat для трансферов |
| `mart_match_facts` | Mart | 6 901 | Матч-уровень: дата/команды/счёт/xG/result |
| `mart_player_overperformers` | Mart | 7 832 | Игроки + goals−xG + percentile_rank, фильтр minutes ≥ 450 |
| `mart_team_xg_trend` | Mart | 386 | Агрегаты xG команды за сезон (avg/std/max) |
| `mart_team_elo_current` | Mart | 125 | Финальный Elo + peak per (team, league), кросс-сезонный |
| `mart_team_elo_history` | Mart | 13 802 | История Elo: 2 строки на матч (per команду), флаг is_top3_in_league |
| `mart_sb_la_liga_history` | Mart | 18 | StatsBomb La Liga по сезонам: matches/wins/draws/avg_total_goals |

### Elo

Формула ClubElo: K=20, home_adv=+100, gd-modifier `ln(|gd|+1)` при `|gd| ≥ 2`. Пул per-league, старт 1500. Расчёт строго последовательный (рейтинг N+1 зависит от N) — Python-цикл на драйвере Spark, ~1 секунда на 6 901 матч. Spark здесь в первую очередь для JDBC-IO и демонстрации работы кластера в pipeline.

## Примеры SQL

```sql
-- Турнирная таблица EPL 2025:
SELECT team_title, position, pts, round(xpts,2) AS xpts, ppda
FROM marts.mart_league_table
WHERE league_id='epl' AND season_year=2025 ORDER BY position LIMIT 5;

-- Топ-5 бомбардиров EPL 2025:
SELECT player_name, teams_concat, goals, round(xg,1) AS xg
FROM marts.mart_top_scorers
WHERE league_id='epl' AND season_year=2025 ORDER BY goals DESC LIMIT 5;

-- Топ команд по текущему Elo:
SELECT team_title, round(current_rating, 1) AS elo, round(peak_rating, 1) AS peak
FROM marts.mart_team_elo_current WHERE league_id='epl' ORDER BY current_rating DESC LIMIT 10;
```
