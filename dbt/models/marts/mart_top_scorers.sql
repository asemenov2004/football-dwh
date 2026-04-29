-- Топ-скореры: одна строка на (player, league, season). Если игрок менял
-- клубы внутри лиги в течение сезона, teams_concat = "AC Milan → Atalanta".
-- Агрегаты xG/goals — из pit_player_season (sat_player_xg latest).

{{ config(materialized='table', tags=['mart']) }}

WITH teams_per_player AS (
    SELECT
        hub_player_hk,
        hub_competition_hk,
        hub_season_hk,
        STRING_AGG(team_title, ' → ' ORDER BY team_title) AS teams_concat,
        COUNT(*) AS teams_count
    FROM {{ ref('br_player_team_season') }}
    GROUP BY hub_player_hk, hub_competition_hk, hub_season_hk
),

pit AS (
    SELECT
        hub_player_hk,
        hub_competition_hk,
        hub_season_hk,
        player_bk,
        player_name,
        league_id,
        season_year,
        xg, xa, npxg, npg,
        goals, assists, shots, key_passes,
        games, minutes, position
    FROM {{ ref('pit_player_season') }}
)

SELECT
    pit.player_bk,
    pit.player_name,
    COALESCE(t.teams_concat, pit.player_name) AS teams_concat,
    COALESCE(t.teams_count, 1)                AS teams_count,
    pit.league_id,
    pit.season_year,
    pit.position,
    pit.games,
    pit.minutes,
    pit.goals,
    pit.assists,
    pit.shots,
    pit.key_passes,
    pit.xg,
    pit.xa,
    pit.npxg,
    pit.npg,
    CASE WHEN pit.xg > 0 THEN ROUND(pit.goals::numeric / pit.xg, 2) END AS goals_per_xg
FROM pit
LEFT JOIN teams_per_player t
  ON  t.hub_player_hk      = pit.hub_player_hk
  AND t.hub_competition_hk = pit.hub_competition_hk
  AND t.hub_season_hk      = pit.hub_season_hk
