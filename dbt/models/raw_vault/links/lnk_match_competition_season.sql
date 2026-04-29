-- Link: матч ↔ турнир ↔ сезон.
-- Источник: stg_understat_matches. Каждый Understat-матч принадлежит ровно
-- одной паре (лига, сезон). Для SB-матчей аналогичная связь не строится
-- на этом этапе (Антон: SB остаётся изолированным).

{{ config(materialized='incremental', unique_key='lnk_match_comp_season_hk') }}

WITH incoming AS (
    SELECT
        lnk_match_comp_season_hk,
        hub_match_hk,
        hub_competition_hk,
        hub_season_hk,
        ldts,
        rsrc
    FROM {{ ref('stg_understat_matches') }}
)

SELECT * FROM incoming

{% if is_incremental() %}
WHERE lnk_match_comp_season_hk NOT IN (SELECT lnk_match_comp_season_hk FROM {{ this }})
{% endif %}
