-- Satellite: счёт и статус матча из обоих источников (AF + SB).
-- Plain SQL вместо datavault4dbt.sat() — нужен UNION двух источников
-- с одинаковым набором payload-колонок. Append-only через hashdiff.
-- PK = (hub_match_hk, ldts). AF и SB матчи — разные hub_match_hk.

{{ config(materialized='incremental', unique_key=['hub_match_hk', 'ldts']) }}

WITH af AS (
    SELECT
        hub_match_hk,
        sat_match_score_hashdiff,
        home_goals,
        away_goals,
        match_status,
        match_ts,
        ldts,
        rsrc
    FROM {{ ref('stg_af_fixtures') }}
),

sb AS (
    SELECT
        hub_match_hk,
        sat_match_score_hashdiff,
        home_goals,
        away_goals,
        match_status,
        match_ts,
        ldts,
        rsrc
    FROM {{ ref('stg_sb_matches') }}
),

all_scores AS (
    SELECT * FROM af
    UNION ALL
    SELECT * FROM sb
)

SELECT all_scores.*
FROM all_scores

{% if is_incremental() %}
-- Вставляем только если hashdiff изменился (или записи ещё нет)
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} existing
    WHERE existing.hub_match_hk = all_scores.hub_match_hk
      AND existing.sat_match_score_hashdiff = all_scores.sat_match_score_hashdiff
)
{% endif %}
