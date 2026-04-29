-- PIT-снапшот команды на конец сезона.
-- Гранулярность: (team, competition, season). Берём latest строку каждого
-- сателлита по ldts. Understat отдаёт сезонные итоги, поэтому "конец сезона"
-- = последняя загруженная версия.

{{ config(materialized='table', tags=['bv']) }}

WITH lnk AS (
    SELECT
        lnk_team_comp_season_hk,
        hub_team_hk,
        hub_competition_hk,
        hub_season_hk
    FROM {{ ref('lnk_team_competition_season') }}
),

xg_latest AS (
    SELECT DISTINCT ON (hub_team_hk, league_id, season_year)
        hub_team_hk,
        league_id,
        season_year,
        xg_for, npxg_for, xg_against, npxg_against, npxg_diff,
        ppda, oppda, deep_completions, opp_deep_completions,
        xpts, matches, wins, draws, losses, points,
        goals_for, goals_against,
        ldts AS xg_ldts
    FROM {{ ref('sat_team_xg') }}
    ORDER BY hub_team_hk, league_id, season_year, ldts DESC
),

details_latest AS (
    SELECT DISTINCT ON (hub_team_hk)
        hub_team_hk,
        team_title,
        ldts AS details_ldts
    FROM {{ ref('sat_team_details') }}
    ORDER BY hub_team_hk, ldts DESC
),

comp AS (
    SELECT hub_competition_hk, league_id AS comp_bk
    FROM {{ ref('hub_competition') }}
),

seas AS (
    SELECT hub_season_hk, season_year AS season_bk
    FROM {{ ref('hub_season') }}
),

team AS (
    SELECT hub_team_hk, team_bk
    FROM {{ ref('hub_team') }}
)

SELECT
    md5(lnk.hub_team_hk || '||' || lnk.hub_competition_hk || '||' || lnk.hub_season_hk) AS pit_team_season_hk,

    lnk.hub_team_hk,
    lnk.hub_competition_hk,
    lnk.hub_season_hk,

    team.team_bk,
    comp.comp_bk           AS league_id,
    seas.season_bk         AS season_year,
    details.team_title,

    xg.xg_for,
    xg.npxg_for,
    xg.xg_against,
    xg.npxg_against,
    xg.npxg_diff,
    xg.ppda,
    xg.oppda,
    xg.deep_completions,
    xg.opp_deep_completions,
    xg.xpts,
    xg.matches,
    xg.wins,
    xg.draws,
    xg.losses,
    xg.points,
    xg.goals_for,
    xg.goals_against,

    GREATEST(xg.xg_ldts, details.details_ldts) AS pit_ldts

FROM lnk
JOIN comp ON comp.hub_competition_hk = lnk.hub_competition_hk
JOIN seas ON seas.hub_season_hk      = lnk.hub_season_hk
JOIN team ON team.hub_team_hk        = lnk.hub_team_hk
LEFT JOIN xg_latest xg
    ON  xg.hub_team_hk  = lnk.hub_team_hk
    AND xg.league_id    = comp.comp_bk
    AND xg.season_year  = seas.season_bk
LEFT JOIN details_latest details
    ON details.hub_team_hk = lnk.hub_team_hk
