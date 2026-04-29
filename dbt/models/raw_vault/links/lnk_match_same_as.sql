-- Same-as link: один и тот же матч в разных источниках (Understat ↔ StatsBomb).
-- Сматчинг по (date, league_id, нормализованные имена команд) делается в stage.
-- Используется в Business Vault для сшивки xG (Understat) и счёта (SB) по
-- редким пересечениям сезонов.

{{ config(materialized='incremental', unique_key='lnk_match_same_as_hk') }}

WITH incoming AS (
    SELECT
        lnk_match_same_as_hk,
        hub_match_hk_understat,
        hub_match_hk_sb,
        ldts,
        rsrc
    FROM {{ ref('stg_match_same_as') }}
)

SELECT * FROM incoming

{% if is_incremental() %}
WHERE lnk_match_same_as_hk NOT IN (SELECT lnk_match_same_as_hk FROM {{ this }})
{% endif %}
