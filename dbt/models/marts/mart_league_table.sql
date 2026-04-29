-- Турнирная таблица: одна строка на (team, league, season).
-- Источник — pit_team_season. Position рассчитывается окном по (league, season)
-- с тай-брейком: PTS desc, разница голов, забитые.

{{ config(materialized='table', tags=['mart']) }}

WITH base AS (
    SELECT
        team_bk,
        team_title,
        league_id,
        season_year,
        matches    AS gp,
        wins       AS w,
        draws      AS d,
        losses     AS l,
        goals_for  AS gf,
        goals_against AS ga,
        (goals_for - goals_against) AS gd,
        points     AS pts,
        xpts,
        xg_for,
        xg_against,
        npxg_diff,
        ppda,
        oppda
    FROM {{ ref('pit_team_season') }}
)

SELECT
    team_bk,
    team_title,
    league_id,
    season_year,
    gp, w, d, l, gf, ga, gd, pts, xpts,
    xg_for,
    xg_against,
    npxg_diff,
    ppda,
    oppda,
    ROUND(pts - xpts, 2) AS pts_overperf,
    ROW_NUMBER() OVER (
        PARTITION BY league_id, season_year
        ORDER BY pts DESC, gd DESC, gf DESC
    ) AS position
FROM base
