-- Витрина агрегатов xG по матчам команды (avg/std/max), отдельно от итогов сезона.
-- Грейн: team × league × season. UNION ALL раскладывает home/away в "плоский"
-- per-match team view. Дополняет mart_league_table детализацией матчей.

{{ config(materialized='table', tags=['mart']) }}

WITH match_team AS (
    SELECT
        league_id, season_year,
        home_team_title AS team_title,
        home_xg         AS xg_for,
        away_xg         AS xg_against,
        home_goals      AS gf,
        away_goals      AS ga
    FROM {{ ref('sat_match_xg') }}
    UNION ALL
    SELECT
        league_id, season_year,
        away_team_title,
        away_xg, home_xg,
        away_goals, home_goals
    FROM {{ ref('sat_match_xg') }}
)

SELECT
    league_id,
    season_year,
    team_title,
    COUNT(*)                                                          AS matches,
    ROUND(AVG(xg_for)::numeric, 3)                                    AS avg_xg_for,
    ROUND(AVG(xg_against)::numeric, 3)                                AS avg_xg_against,
    ROUND(STDDEV(xg_for)::numeric, 3)                                 AS std_xg_for,
    ROUND(STDDEV(xg_against)::numeric, 3)                             AS std_xg_against,
    ROUND(MAX(xg_for)::numeric, 3)                                    AS max_xg_for,
    ROUND(MAX(xg_against)::numeric, 3)                                AS max_xg_against,
    SUM(gf)                                                           AS total_gf,
    SUM(ga)                                                           AS total_ga,
    ROUND((SUM(gf)::numeric - SUM(xg_for)::numeric), 3)               AS gf_minus_xg,
    ROUND((SUM(ga)::numeric - SUM(xg_against)::numeric), 3)           AS ga_minus_xga
FROM match_team
GROUP BY league_id, season_year, team_title
