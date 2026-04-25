-- Satellite: снапшот статистики бомбардира (голы, ассисты, игры).
-- Plain SQL — multi-active (снапшоты по player×competition×season).
-- PK = (hub_player_hk, hub_competition_hk, hub_season_hk, ldts).
-- hub_competition_hk, hub_season_hk, hub_team_hk — де-нормализованные FK.

{{ config(materialized='incremental', unique_key=['hub_player_hk', 'hub_competition_hk', 'hub_season_hk', 'ldts']) }}

WITH src AS (
    SELECT
        hub_player_hk,
        hub_competition_hk,
        hub_season_hk,
        hub_team_hk,
        sat_topscorer_hashdiff,
        dt,
        goals,
        assists,
        appearances,
        rating,
        ldts,
        rsrc
    FROM {{ ref('stg_af_topscorers') }}
)

SELECT src.*
FROM src

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} existing
    WHERE existing.hub_player_hk      = src.hub_player_hk
      AND existing.hub_competition_hk = src.hub_competition_hk
      AND existing.hub_season_hk      = src.hub_season_hk
      AND existing.sat_topscorer_hashdiff = src.sat_topscorer_hashdiff
)
{% endif %}
