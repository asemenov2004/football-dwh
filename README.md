# Football DWH

Учебный DWH по футбольной статистике (топ-5 европейских лиг + UCL). Курсовая 2026, задел на диплом.

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
dags/          — Airflow DAG'и (добавляются поэтапно)
dbt/           — dbt-проект (stage / raw_vault / business_vault / marts)
ingestion/     — Python-клиенты к API-Football и StatsBomb
spark/         — Spark-джобы (Elo-рейтинг)
docker/        — Dockerfile'ы и init-скрипты
docs/          — диаграммы, скриншоты, материалы для отчёта курсовой
data/          — bind-mounts (gitignored): postgres, minio, clickhouse, логи
```

## Этапы

Реализуется поэтапно, после каждого — git-коммит:

0. ✅ Скелет (Docker Compose, структура)
1. ✅ Extract (API-Football + StatsBomb → MinIO)
2. ✅ Stage в Postgres (7 таблиц: `af_fixtures/teams/leagues/standings/topscorers`, `sb_matches/competitions`)
3. ✅ Raw Vault — dbt + datavault4dbt (hubs/links/satellites для команд, матчей, лиг, сезонов, игроков)
4. Business Vault (PIT, bridge, same-as-link AF↔SB)
5. Data Marts в ClickHouse
6. Superset дашборды
7. Spark (расчёт Elo)
8. CI + DQ + документация

### Raw Vault (Этап 3 + 3.5)

Схема `public_raw_vault` в Postgres. Генерация через макросы `datavault4dbt` (ScalefreeCOM v1.17.0).

| Объект | Строк | Описание |
|---|---|---|
| `hub_team` | 157 | Команды AF (`af\|{team_id}`) |
| `hub_match` | 4 495 | Матчи AF + SB (`af\|{fixture_id}`, `sb\|{match_id}`) |
| `hub_competition` | 6 | Турниры AF+SB (общий, BK = slug) |
| `hub_season` | 1 | Сезоны (общий, BK = год) |
| `hub_player` | 103 | Игроки AF (`af\|{player_id}`) |
| `lnk_match_team` | 4 070 | Матч ↔ команда + роль home/away |
| `lnk_team_competition_season` | 179 | Команда ↔ лига ↔ сезон |
| `lnk_player_team` | 104 | Игрок ↔ команда |
| `sat_team_details` | 157 | Атрибуты команды (название, страна, стадион) |
| `sat_match_score` | 4 495 | Счёт + статус матча (AF + SB) |
| `sat_player_details` | 103 | Атрибуты игрока (имя, национальность, фото) |
| `sat_standing` | 132 | Снапшоты турнирных таблиц |
| `sat_topscorer` | 114 | Статистика бомбардиров |

DV-stage views (`public_stage_dv`) читают `stage.*` и вычисляют MD5-хеши на лету — физически данных не хранят.

Подробный план — в `docs/plan.md` (будет добавлен).

## Источники данных

- **StatsBomb Open Data** — исторические данные, без лимитов ([github.com/statsbomb/open-data](https://github.com/statsbomb/open-data))
- **API-Football free tier** — текущий сезон, 100 req/день ([api-football.com](https://www.api-football.com))
