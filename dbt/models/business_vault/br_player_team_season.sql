-- Bridge: игрок ↔ команда ↔ лига ↔ сезон (денорм).
-- У одного (player, league, season) может быть несколько строк, если игрок
-- сменил клуб посреди сезона внутри той же лиги. mart_top_scorers использует
-- этот bridge для склейки teams_concat ("AC Milan → Atalanta").
-- Лига для пары (player, team, season) определяется через lnk_team_competition_season.

{{ config(materialized='table', tags=['bv']) }}

WITH lpt AS (
    SELECT
        hub_player_hk,
        hub_team_hk,
        hub_season_hk
    FROM {{ ref('lnk_player_team') }}
),

ltcs AS (
    SELECT
        hub_team_hk,
        hub_competition_hk,
        hub_season_hk
    FROM {{ ref('lnk_team_competition_season') }}
),

team_details AS (
    SELECT DISTINCT ON (hub_team_hk)
        hub_team_hk, team_title
    FROM {{ ref('sat_team_details') }}
    ORDER BY hub_team_hk, ldts DESC
),

player AS (
    SELECT hub_player_hk, player_bk FROM {{ ref('hub_player') }}
),

team AS (
    SELECT hub_team_hk, team_bk FROM {{ ref('hub_team') }}
),

comp AS (
    SELECT hub_competition_hk, league_id AS comp_bk FROM {{ ref('hub_competition') }}
),

seas AS (
    SELECT hub_season_hk, season_year AS season_bk FROM {{ ref('hub_season') }}
),

pit_p AS (
    SELECT hub_player_hk, hub_competition_hk, hub_season_hk, player_name
    FROM {{ ref('pit_player_season') }}
)

SELECT
    md5(lpt.hub_player_hk || '||' || lpt.hub_team_hk || '||' ||
        ltcs.hub_competition_hk || '||' || lpt.hub_season_hk)        AS br_hk,

    lpt.hub_player_hk,
    lpt.hub_team_hk,
    ltcs.hub_competition_hk,
    lpt.hub_season_hk,

    player.player_bk,
    pit_p.player_name,
    team.team_bk,
    td.team_title,
    comp.comp_bk     AS league_id,
    seas.season_bk   AS season_year

FROM lpt
JOIN ltcs
  ON ltcs.hub_team_hk   = lpt.hub_team_hk
 AND ltcs.hub_season_hk = lpt.hub_season_hk
JOIN player ON player.hub_player_hk        = lpt.hub_player_hk
JOIN team   ON team.hub_team_hk            = lpt.hub_team_hk
JOIN comp   ON comp.hub_competition_hk     = ltcs.hub_competition_hk
JOIN seas   ON seas.hub_season_hk          = lpt.hub_season_hk
LEFT JOIN team_details td ON td.hub_team_hk = lpt.hub_team_hk
LEFT JOIN pit_p
  ON  pit_p.hub_player_hk      = lpt.hub_player_hk
  AND pit_p.hub_competition_hk = ltcs.hub_competition_hk
  AND pit_p.hub_season_hk      = lpt.hub_season_hk
-- Только пары (player, team, league, season), для которых есть PIT —
-- т.е. игрок реально записан в sat_player_xg как игравший в этой лиге
-- (отсекает фейковые связки player→team из lnk_player_team, где команда
-- играла в другой лиге, чем агрегат игрока).
WHERE pit_p.hub_player_hk IS NOT NULL
