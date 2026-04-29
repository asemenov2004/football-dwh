-- Bridge: команда ↔ турнир ↔ сезон (денорм-копия PIT).
-- По канону DV bridge — это плоско лежащая таблица для маршрута в марты.
-- Содержит ровно те же ключи что pit_team_season + основные дескрипторы.

{{ config(materialized='table', tags=['bv']) }}

SELECT
    pit_team_season_hk      AS br_hk,
    hub_team_hk,
    hub_competition_hk,
    hub_season_hk,
    team_bk,
    league_id,
    season_year,
    team_title
FROM {{ ref('pit_team_season') }}
