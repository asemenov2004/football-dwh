-- Satellite: снапшот турнирной таблицы (место, очки, статистика).
-- Plain SQL — multi-active satellite (несколько снапшотов в день разных лиг/сезонов).
-- PK = (hub_team_hk, hub_competition_hk, hub_season_hk, ldts).
-- Включает hub_competition_hk и hub_season_hk как FK-атрибуты (de-normalized для BI).

{{ config(materialized='incremental', unique_key=['hub_team_hk', 'hub_competition_hk', 'hub_season_hk', 'ldts']) }}

WITH src AS (
    SELECT
        hub_team_hk,
        hub_competition_hk,
        hub_season_hk,
        sat_standing_hashdiff,
        dt,
        standing_rank,
        points,
        played,
        wins,
        draws,
        losses,
        goals_for,
        goals_against,
        goals_diff,
        form,
        standing_description,
        ldts,
        rsrc
    FROM {{ ref('stg_af_standings') }}
)

SELECT src.*
FROM src

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} existing
    WHERE existing.hub_team_hk        = src.hub_team_hk
      AND existing.hub_competition_hk = src.hub_competition_hk
      AND existing.hub_season_hk      = src.hub_season_hk
      AND existing.sat_standing_hashdiff = src.sat_standing_hashdiff
)
{% endif %}
