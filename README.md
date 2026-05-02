# Football DWH

Учебный DWH по футбольной статистике (топ-5 европейских лиг). Курсовая 2026, задел на диплом.

## Стек

| Слой | Технология |
|---|---|
| Оркестрация | Apache Airflow 2.9 (LocalExecutor) |
| Raw lake | MinIO (S3-compatible) |
| DWH (DV2.0) | Postgres 16 + dbt-core + datavault4dbt |
| Data marts | ClickHouse 24.8 |
| Обработка | PySpark 3.5 (Standalone) |
| BI | Apache Superset |
| CI | GitHub Actions |

Архитектура: `Source → MinIO → Postgres.stage → Raw Vault → Business Vault → ClickHouse marts → Superset`.

## Требования

- Docker Desktop (WSL2 backend), память ≥ 12 ГБ
- 20+ ГБ свободного места под `./data/`
- Python 3.11 локально (для dbt/ingestion вне контейнеров, опционально)
- Git, GitHub CLI (опционально)

## Подъём локально

```bash
# 1. Подготовить окружение
cp .env.example .env
# сгенерировать Fernet key для Airflow:
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# подставить значение в AIRFLOW_FERNET_KEY

# 2. Поднять стек
docker compose up -d

# 3. Проверить (должны открываться):
# Airflow:    http://localhost:18080  (admin/admin)  — 8080 часто занят HP Smart/ApplicationWebServer
# MinIO:      http://localhost:9001   (minioadmin/minioadmin123)
# ClickHouse: http://localhost:8123/play
# Superset:   http://localhost:8088   (admin/admin)
# Spark UI:   http://localhost:8181
```

## Лимит памяти WSL2

Docker на Windows работает через WSL2. По умолчанию WSL забирает до 50% RAM. Для этого проекта нужно минимум 12 ГБ.

Создай файл `C:\Users\<user>\.wslconfig`:

```ini
[wsl2]
memory=12GB
processors=4
swap=4GB
```

Применить: `wsl --shutdown`, затем перезапустить Docker Desktop.

## Структура

```
dags/          — Airflow DAG'и (Understat daily/historical, SB)
dbt/           — dbt-проект (stage / raw_vault / business_vault / marts)
ingestion/     — Python-клиенты Understat и StatsBomb
stage/         — DDL и loader'ы MinIO → Postgres.stage
spark/         — Spark-джобы (Elo-рейтинг)
docker/        — Dockerfile'ы и init-скрипты
docs/          — диаграммы, скриншоты, материалы для отчёта курсовой
data/          — bind-mounts (gitignored): postgres, minio, clickhouse, логи
```

## Этапы

Реализуется поэтапно, после каждого — git-коммит:

0. ✅ Скелет (Docker Compose, структура)
1. ✅ Extract (Understat + StatsBomb → MinIO)
2. ✅ Stage в Postgres (5 таблиц: `understat_matches/teams/players`, `sb_matches/competitions`)
3. ✅ Raw Vault — dbt + datavault4dbt (hubs/links/satellites)
4. ✅ Расширение RV: xG-стек Understat (matches/teams/players), bridge SB↔Understat, lnk_player_team с учётом мид-сезонных трансферов
5. ✅ Business Vault (PIT/bridge) + Marts в Postgres + перелив в ClickHouse через Spark/MinIO
6. Superset-дашборды поверх ClickHouse-витрин
7. Spark (расчёт Elo)
8. CI + DQ + документация

### Этап 5: BV + Marts + ClickHouse (как запускать)

После ingestion + Raw Vault конвейер до витрин:

```bash
# 1. Spark JDBC read Postgres marts.* → Parquet (./spark/output/) → MinIO bucket marts/
bash scripts/run_spark_marts.sh

# 2. Триггер DAG в Airflow UI (http://localhost:18080) или CLI:
docker exec football_airflow_scheduler airflow dags trigger build_marts
```

DAG `build_marts` шаги: `dbt run tag:bv` → `dbt run tag:mart` → `dbt test` → `clickhouse_load`
(TRUNCATE + `INSERT FROM s3()` для каждой март-таблицы).

ClickHouse-витрины:

```sql
-- Через clickhouse-client или http://localhost:8123/play
SELECT team_title, position, pts, round(xpts,2) AS xpts, ppda
FROM marts.mart_league_table
WHERE league_id='epl' AND season_year=2025 ORDER BY position LIMIT 5;

SELECT player_name, teams_concat, goals, round(xg,1) AS xg
FROM marts.mart_top_scorers
WHERE league_id='epl' AND season_year=2025 ORDER BY goals DESC LIMIT 5;

-- Топ-5 матчей EPL 2025 по совокупному xG (mart_match_facts):
SELECT match_date, home_team_title, home_goals, away_goals, away_team_title, round(total_xg,2) AS total_xg
FROM marts.mart_match_facts
WHERE league_id='epl' AND season_year=2025 ORDER BY total_xg DESC LIMIT 5;

-- "Удачливые" игроки EPL 2025 (goals - xG, минимум 450 минут):
SELECT player_name, team_title, goals, round(xg,2) AS xg, goals_minus_xg
FROM marts.mart_player_overperformers
WHERE league_id='epl' AND season_year=2025 ORDER BY goals_minus_xg DESC LIMIT 5;
```

Pragmatic-замечание: **Spark пишет parquet в локальный mount-volume**, заливка в MinIO — отдельный `mc cp`. Прямая запись через `s3a://` требует hadoop-aws + aws-java-sdk-bundle (~273MB через Maven Central) — из РФ упорно фейлится connection refused. Для курсовой обходной путь не критичен, Spark остаётся в pipeline (JDBC + parquet-сериализация, демонстрация кластера).

### Источники данных (Этап 4)

В рамках работы над xG-частью отказались от **API-Football** (free tier 100 req/день, нет xG). Остались два источника:

- **Understat** ([understat.com](https://understat.com)) — xG/xA/PPDA/xPTS на уровне матча, команды и игрока. Топ-5 лиг, сезоны 2022–2025, без лимитов (HTML-парсинг).
- **StatsBomb Open Data** ([github.com/statsbomb/open-data](https://github.com/statsbomb/open-data)) — исторические матчи (преимущественно 2015 и старее). Используется для счёта/статуса как изолированная ветка; bridge с Understat — `lnk_match_same_as` (24 пары на пересечениях).

### Raw Vault (на конец Этапа 4)

Схема `public_raw_vault` в Postgres. Генерация через макросы `datavault4dbt` (ScalefreeCOM v1.17.0). Hash = MD5.

| Объект | Строк | Описание |
|---|---|---|
| `hub_team` | 125 | Команды Understat (`understat\|{lower(team_title)}`) |
| `hub_match` | 9 355 | Матчи Understat + SB (`understat\|{id}`, `sb\|{id}`) |
| `hub_competition` | 6 | Турниры (общий BK = league slug) |
| `hub_season` | 4 | Сезоны 2022–2025 |
| `hub_player` | 4 982 | Игроки Understat (`understat\|{id}`) |
| `lnk_match_team` | 13 790 | Матч ↔ команда + роль home/away |
| `lnk_team_competition_season` | 386 | Команда ↔ лига ↔ сезон |
| `lnk_player_team` | 11 360 | Игрок ↔ команда ↔ сезон (с мид-сезонными трансферами) |
| `lnk_match_competition_season` | 6 895 | Матч ↔ лига ↔ сезон |
| `lnk_match_same_as` | 24 | Bridge SB ↔ Understat по (date, league, normalized teams) |
| `sat_team_details` | 125 | Название команды + лига |
| `sat_team_xg` | 386 | xG/NPxG/PPDA/OPPDA/xPTS/PTS на сезон |
| `sat_match_score` | 2 460 | SB: счёт + статус матча |
| `sat_match_score_understat` | 6 895 | Understat: счёт + datetime |
| `sat_match_xg` | 6 895 | Understat: home_xg / away_xg |
| `sat_player_xg` | 11 054 | xG/xA/npxG/goals/assists/minutes/player_name/team_title на сезон |

DV-stage views (`public_stage_dv`) читают `stage.*` и вычисляют MD5-хеши на лету — физически данных не хранят.

### Business Vault + Marts (Этап 5)

Схемы `public_business_vault` и `public_marts` в Postgres + `marts` в ClickHouse.

| Объект | Слой | Строк | Описание |
|---|---|---|---|
| `pit_team_season` | BV | 386 | PIT-снапшот: команда × лига × сезон, latest sat_team_xg + sat_team_details |
| `pit_player_season` | BV | 11 054 | PIT-снапшот: игрок × лига × сезон, latest sat_player_xg |
| `br_player_team_season` | BV | 11 360 | Bridge: игрок × команда × лига × сезон (трансферы) |
| `br_team_competition_season` | BV | 386 | Bridge: команда × лига × сезон (плоская копия PIT) |
| `mart_league_table` | Mart (PG + CH) | 386 | Турнирная таблица: PTS/xPTS/xG/PPDA + position |
| `mart_top_scorers` | Mart (PG + CH) | 11 054 | Топ-скореры с teams_concat для трансферов |
| `mart_match_facts` | Mart (PG + CH) | 6 901 | Матч-уровень (wide): дата/команды/счёт/xG/result. База для time-series в Superset |
| `mart_player_overperformers` | Mart (PG + CH) | 7 832 | Игроки + goals−xG + percentile_rank внутри (league, season). Фильтр minutes ≥ 450 |
| `mart_team_xg_trend` | Mart (PG + CH) | 386 | Агрегаты xG по матчам команды за сезон (avg/std/max). Дополняет league_table детализацией |
