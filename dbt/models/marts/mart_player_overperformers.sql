-- Витрина "удачливые/невезучие" игроки: разница goals - xG за сезон в лиге,
-- percentile_rank внутри (league, season). Грейн: player × league × season.
-- Фильтр minutes >= 450 отрезает шум одиночных ударов (~5 матчей минимум).

{{ config(materialized='table', tags=['mart']) }}

SELECT
    p.hub_player_hk                                            AS player_hk,
    p.player_bk,
    p.player_name,
    p.team_title_raw                                           AS team_title,
    p.league_id,
    p.season_year,
    p.position,
    p.minutes,
    p.games,
    p.goals,
    p.xg,
    p.assists,
    p.xa,
    p.npxg,
    p.npg,
    ROUND((p.goals - p.xg)::numeric, 3)                        AS goals_minus_xg,
    ROUND((p.assists - p.xa)::numeric, 3)                      AS assists_minus_xa,
    ROUND(
        (PERCENT_RANK() OVER (
            PARTITION BY p.league_id, p.season_year
            ORDER BY (p.goals - p.xg)
        ))::numeric, 4
    )                                                          AS overperf_pct_rank
FROM {{ ref('pit_player_season') }} p
WHERE p.minutes >= 450
