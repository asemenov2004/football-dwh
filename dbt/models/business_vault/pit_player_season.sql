-- PIT-снапшот игрока на конец сезона.
-- Гранулярность: (player, competition, season). Источник — sat_player_xg
-- (latest по ldts). Understat отдаёт агрегат за сезон в одной лиге; если игрок
-- играл в нескольких лигах — будет несколько PIT-строк.

{{ config(materialized='table', tags=['bv']) }}

WITH xg_latest AS (
    SELECT DISTINCT ON (hub_player_hk, league_id, season_year)
        hub_player_hk,
        league_id,
        season_year,
        player_name,
        team_title,
        xg, xa, npxg, npg, xg_chain, xg_buildup,
        goals, assists, shots, key_passes,
        games, minutes, position,
        ldts AS xg_ldts
    FROM {{ ref('sat_player_xg') }}
    ORDER BY hub_player_hk, league_id, season_year, ldts DESC
),

player AS (
    SELECT hub_player_hk, player_bk
    FROM {{ ref('hub_player') }}
),

comp AS (
    SELECT hub_competition_hk, league_id AS comp_bk
    FROM {{ ref('hub_competition') }}
),

seas AS (
    SELECT hub_season_hk, season_year AS season_bk
    FROM {{ ref('hub_season') }}
)

SELECT
    md5(xg.hub_player_hk || '||' || comp.hub_competition_hk || '||' || seas.hub_season_hk) AS pit_player_season_hk,

    xg.hub_player_hk,
    comp.hub_competition_hk,
    seas.hub_season_hk,

    player.player_bk,
    xg.player_name,
    xg.team_title          AS team_title_raw,
    xg.league_id,
    xg.season_year,

    xg.xg,
    xg.xa,
    xg.npxg,
    xg.npg,
    xg.xg_chain,
    xg.xg_buildup,
    xg.goals,
    xg.assists,
    xg.shots,
    xg.key_passes,
    xg.games,
    xg.minutes,
    xg.position,

    xg.xg_ldts AS pit_ldts

FROM xg_latest xg
JOIN player ON player.hub_player_hk    = xg.hub_player_hk
JOIN comp   ON comp.comp_bk            = xg.league_id
JOIN seas   ON seas.season_bk          = xg.season_year
