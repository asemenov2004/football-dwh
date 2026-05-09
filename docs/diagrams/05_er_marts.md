# ER: Marts + lineage

8 финальных витрин (6 dbt + 2 Spark Elo). Зеркальные таблицы в ClickHouse
(`marts.*`) используют те же имена. Lineage показывает источники в DV.

## Lineage: как mart_* собираются из DV

```mermaid
flowchart LR
    classDef rv fill:#1168BD,stroke:#0B4884,color:#fff
    classDef bv fill:#0B5A9E,stroke:#062E50,color:#fff
    classDef stage fill:#FFE4A0,stroke:#7A5300,color:#000
    classDef mart fill:#1FA8C9,stroke:#0E6680,color:#fff
    classDef spark fill:#E25822,stroke:#7A2E11,color:#fff

    sat_match_xg["sat_match_xg<br/><i>RV</i>"]:::rv
    pit_team["pit_team_season<br/><i>BV</i>"]:::bv
    pit_player["pit_player_season<br/><i>BV</i>"]:::bv
    br_player["br_player_team_season<br/><i>BV</i>"]:::bv
    stg_sb["stg_sb_matches<br/><i>stage</i>"]:::stage

    m_league["mart_league_table"]:::mart
    m_top["mart_top_scorers"]:::mart
    m_facts["mart_match_facts"]:::mart
    m_over["mart_player_overperformers"]:::mart
    m_xg["mart_team_xg_trend"]:::mart
    m_sb["mart_sb_la_liga_history"]:::mart

    spark_elo["calculate_elo<br/><i>Spark job</i>"]:::spark
    m_elo_cur["mart_team_elo_current"]:::mart
    m_elo_hist["mart_team_elo_history"]:::mart

    pit_team --> m_league
    pit_player --> m_top
    br_player --> m_top
    sat_match_xg --> m_facts
    sat_match_xg --> m_xg
    pit_player --> m_over
    stg_sb --> m_sb

    m_facts --> spark_elo
    spark_elo --> m_elo_cur
    spark_elo --> m_elo_hist
```

## Список витрин

| Витрина | Грейн | Источник | Зачем |
|---|---|---|---|
| `mart_league_table` | team × competition × season | `pit_team_season` | Турнирная таблица: pts/xpts/gf/ga/ppda/position |
| `mart_top_scorers` | player × team × season | `pit_player_season + br_player_team_season` | Бомбардиры: goals/xg/assists/minutes |
| `mart_match_facts` | match | `sat_match_xg` | Факты матчей: home/away xg, total_xg, goal_diff |
| `mart_player_overperformers` | player × season | `pit_player_season` | Top по `goals - xg` (over/under) |
| `mart_team_xg_trend` | team × season | `sat_match_xg` | Avg/std xG по матчам команды за сезон |
| `mart_sb_la_liga_history` | season | `stg_sb_matches` (SB) | История Barcelona в La Liga (Этап 8) |
| `mart_team_elo_current` | team × league | `mart_match_facts → Spark calculate_elo` | Текущий Elo + peak |
| `mart_team_elo_history` | team × match × league | `mart_match_facts → Spark` | Эволюция Elo по матчам, флаг top-3 в лиге |

## Зеркала в ClickHouse

Все 8 витрин дублируются в схеме `marts.*` ClickHouse через путь:
`Postgres → Spark JDBC.read → Parquet в MinIO → mc cp → CH INSERT FROM s3()`.

DDL ClickHouse-таблиц лежит в `stage/ddl/clickhouse_marts.sql`. Grain и колонки
совпадают с PG-версией (типы `Float64`/`UInt*`/`Date`).

## Покрытие дашбордов

- **Football DWH** (9 чартов): `mart_league_table`, `mart_top_scorers`, `mart_player_overperformers`, `mart_match_facts`, `mart_team_xg_trend`, `mart_team_elo_current`, `mart_team_elo_history`, `mart_sb_la_liga_history`
- **European Teams** (4 чарта): `mart_team_elo_current`, `mart_league_table`, `mart_top_scorers`, `mart_match_facts`
