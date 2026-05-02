-- Витрина матч-уровня (wide). 1 строка = 1 матч Understat.
-- Universal база для time-series и scatter в Superset (фильтры по дате,
-- лиге, команде; метрики xG/goals/diff). Источник один — sat_match_xg
-- (датавалт-сат уже содержит match_datetime, league_id, season_year, голы и xG).

{{ config(materialized='table', tags=['mart']) }}

SELECT
    hub_match_hk                                       AS match_hk,
    match_datetime,
    match_datetime::date                               AS match_date,
    league_id,
    season_year,
    home_team_title,
    away_team_title,
    home_goals,
    away_goals,
    home_xg,
    away_xg,
    (home_goals - away_goals)                          AS goal_diff,
    ROUND((home_xg - away_xg)::numeric, 3)             AS xg_diff,
    CASE
        WHEN home_goals > away_goals THEN 'home'
        WHEN home_goals < away_goals THEN 'away'
        ELSE 'draw'
    END                                                AS result,
    (home_goals + away_goals)                          AS total_goals,
    ROUND((home_xg + away_xg)::numeric, 3)             AS total_xg
FROM {{ ref('sat_match_xg') }}
