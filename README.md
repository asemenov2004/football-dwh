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
# Airflow:    http://localhost:8080   (admin/admin)
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
1. ⏳ Extract (API-Football + StatsBomb → MinIO)
2. Stage в Postgres
3. Raw Vault (dbt + datavault4dbt)
4. Business Vault (PIT, bridge, computed sats)
5. Data Marts в ClickHouse
6. Superset дашборды
7. Spark (расчёт Elo)
8. CI + DQ + документация

Подробный план — в `docs/plan.md` (будет добавлен).

## Источники данных

- **StatsBomb Open Data** — исторические данные, без лимитов ([github.com/statsbomb/open-data](https://github.com/statsbomb/open-data))
- **API-Football free tier** — текущий сезон, 100 req/день ([api-football.com](https://www.api-football.com))
