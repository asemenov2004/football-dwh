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
5. Business Vault (PIT/bridge: команда-сезон, игрок-сезон, матч)
6. Data Marts в ClickHouse + Superset
7. Spark (расчёт Elo)
8. CI + DQ + документация

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
| `sat_player_xg` | 11 054 | xG/xA/npxG/goals/assists/minutes на сезон |

DV-stage views (`public_stage_dv`) читают `stage.*` и вычисляют MD5-хеши на лету — физически данных не хранят.
