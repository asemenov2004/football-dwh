-- StatsBomb: история выступлений Barcelona в La Liga по сезонам.
-- SB Open Data исторически фокусируется на матчах Barcelona (плюс полный сезон 2015/16).
-- Грейн: одна строка на сезон. Сезон = год начала (август-май → year при month>=7).

{{ config(materialized='table', tags=['mart']) }}

WITH barca_matches AS (
    SELECT
        home_team,
        away_team,
        home_goals,
        away_goals,
        CASE
            WHEN EXTRACT(MONTH FROM match_ts) >= 7
                THEN EXTRACT(YEAR FROM match_ts)::int
            ELSE EXTRACT(YEAR FROM match_ts)::int - 1
        END AS season_year,
        CASE WHEN home_team = 'Barcelona' THEN home_goals ELSE away_goals END AS goals_for,
        CASE WHEN home_team = 'Barcelona' THEN away_goals ELSE home_goals END AS goals_against
    FROM {{ ref('stg_sb_matches') }}
    WHERE league_id = 'la_liga'
      AND match_status = 'available'
      AND home_goals IS NOT NULL
      AND away_goals IS NOT NULL
      AND match_ts IS NOT NULL
      AND (home_team = 'Barcelona' OR away_team = 'Barcelona')
)

SELECT
    season_year,
    COUNT(*)::int                                                          AS matches_played,
    SUM(CASE WHEN goals_for > goals_against THEN 1 ELSE 0 END)::int        AS wins,
    SUM(CASE WHEN goals_for = goals_against THEN 1 ELSE 0 END)::int        AS draws,
    SUM(CASE WHEN goals_for < goals_against THEN 1 ELSE 0 END)::int        AS losses,
    SUM(goals_for)::int                                                    AS goals_scored,
    SUM(goals_against)::int                                                AS goals_conceded,
    ROUND(AVG(goals_for)::numeric, 2)::float                               AS avg_goals_scored,
    ROUND(AVG(goals_against)::numeric, 2)::float                           AS avg_goals_conceded
FROM barca_matches
GROUP BY season_year
ORDER BY season_year DESC
