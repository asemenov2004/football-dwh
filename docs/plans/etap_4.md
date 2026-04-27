# Этап 4 — Расширение источников + Business Vault

## Контекст решения

AF free tier ограничен сезонами 2021-2024, текущий сезон 2025-26 недоступен.
После Этапа 3.5 в RV: 157 команд, 4495 матчей, **1 сезон**, **103 игрока**, xG нет.
Для защищаемых витрин этого мало — нет продвинутой статистики, нет истории.

**Решение — Вариант 2: AF + FBref как параллельные источники.**
FBref (через `soccerdata`) даёт: 5+ сезонов × все топ-5 + UCL + текущий 2025-26 + xG.
Multi-source — главный DV2-аргумент защиты («зачем DV2 вместо DDS»).

---

## Этап 4а — выжимаем AF + исторические данные (~1-2 дня) ← В РАБОТЕ

### Что сделано (коммит этапа будет после 4а полностью)

**Python/ingestion:**
- `ingestion/api_football_client.py` — +`get_players(league_id, season, page)`
- `ingestion/config.py` — +`HISTORICAL_AF_SEASONS = [2022, 2023]`
- `stage/ddl/af_tables.sql` — +`stage.af_players` (PK без dt, UPSERT-friendly)
- `stage/loaders/api_football.py` — +`load_players()` с UPSERT-стратегией

**DAG-и (schedule=None, manual trigger):**
- `dags/ingest_af_players.py` — AF /players → MinIO, пагинация внутри task, Param: season
- `dags/stage_load_af_players.py` — MinIO → stage.af_players (UPSERT)
- `dags/ingest_af_historical.py` — standings+topscorers для старых сезонов
- `dags/stage_load_af_historical.py` — MinIO → stage (TRUNCATE, сразу dbt run)

**dbt:**
- `models/stage/stg_af_players.sql` — view: player_bk, hub_player_hk, birth_date, height, weight, position
- `models/raw_vault/hubs/hub_player.sql` — расширен: `{'stg_af_topscorers': {}, 'stg_af_players': {}}`
- `models/raw_vault/satellites/sat_player_profile.sql` — новый sat: birth_date, height, weight, position
- `models/sources.yml` — +af_players

### Порядок запуска backfill

```
# Исторические standings+topscorers (2022, 2023):
1. Trigger ingest_af_historical    (Param season=2022)
2. Trigger stage_load_af_historical (Param season=2022)
3. dbt run --select raw_vault       ← пока в stage лежат данные 2022

4. Trigger ingest_af_historical    (season=2023)
5. Trigger stage_load_af_historical (season=2023)
6. dbt run --select raw_vault

# Players ростеры (2022, 2023, 2024 — по одному run в день из-за квоты):
7. Trigger ingest_af_players       (season=2022)  ← ~90 req, 1 run/day
8. Trigger stage_load_af_players   (season=2022)
9. dbt run --select hub_player sat_player_profile
# Повторить для 2023, 2024
```

### Verify-критерии 4а
- `hub_player` count > 2000 (было 103)
- `hub_season` count = 3 (появились 2022, 2023)
- `sat_player_profile` не пустой, `dbt test` PASS
- Идемпотентность: повторный `dbt run` — row count не растёт

---

## Этап 4б — FBref как новый источник (~3-4 дня)

### Технология
- Пакет: `soccerdata` (MIT, scraping FBref)
- Данные: xG, xA, shots, key passes, progressive passes, defensive actions
- Покрытие: 5+ сезонов × все топ-5 + UCL + **текущий сезон 2025-26**

### Файлы для создания
- `ingestion/fbref_client.py` — обёртка над `soccerdata.FBref`
- `dags/ingest_fbref.py` — schedule=None, MinIO `source=fbref/...`
- `dags/stage_load_fbref.py`
- `stage/ddl/fbref_tables.sql`
- `stage/loaders/fbref.py`
- `stage/id_mapping/player_mapper.py` — fuzzy match AF↔FBref через `rapidfuzz`
- `dbt/models/stage/stg_fbref_player_stats.sql`
- `dbt/models/stage/stg_fbref_team_stats.sql`
- `dbt/models/raw_vault/satellites/sat_player_fbref_advanced.sql` — xG, xA, shots
- `dbt/models/raw_vault/satellites/sat_team_fbref_advanced.sql` — xG команды, прессинг

### ID-mapping стратегия
Fuzzy match по `(player_name + birth_date + current_team)` через `rapidfuzz`.
Результат → `stage.player_id_mapping (af_player_id, fbref_player_id, match_score)`.
Порог: score ≥ 85 → матч. Остальные — в лог. Целевое покрытие: 85-90%.

---

## Этап 4в — Business Vault (~2-3 дня)

### Конфиг dbt_project.yml

```yaml
business_vault:
  +schema: business_vault
  +materialized: table
```

### Модели

| Модель | Макрос | Назначение |
|---|---|---|
| `bv_match_result` | plain SQL table | winner, goal_diff, total_goals, is_finished, xg_winner, xg_overperformance |
| `pit_team` | `datavault4dbt.pit` | Срез команды на дату: details + standing + fbref advanced |
| `bridge_competition_team_season` | `datavault4dbt.bridge` | lnk_team_comp_season + sat_standing |
| `bridge_match_teams` | `datavault4dbt.bridge` | lnk_match_team + sat_match_score + bv_match_result |

### Verify-критерии 4в
1. `dbt build --select business_vault+` — PASS
2. Идемпотентность (повторный run — count не растёт)
3. `bv_match_result`: SUM(total_goals) = SUM(home_goals+away_goals) из sat_match_score
4. `pit_team`: Real Madrid за свежую load_date — имя+позиция совпадают со срезом sat_standing
5. `bridge_competition_team_season`: count = 179 (= lnk_team_competition_season)

---

## Коммиты
1. `Этап 4а: AF расширение — players ростеры, исторические сезоны, sat_player_profile`
2. `Этап 4б: FBref как источник — ingest, stage, ID-mapping, sat_*_fbref_advanced`
3. `Этап 4в: Business Vault — bv_match_result, pit_team, bridges`

## Технические решения
- Hash: MD5 (как в RV, не менять — сломает append)
- BV schema: отдельная `public_business_vault`
- BV materialization: `table` (объёмы маленькие, логика часто меняется)
- PIT/Bridge: макросы datavault4dbt (не руками — сильнее на защите)
- soccerdata кеш в Docker: монтировать как volume, не скачивать каждый раз
