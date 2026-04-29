-- Link: игрок ↔ команда ↔ сезон.
-- Источник: stg_understat_player_team. Учитывает мид-сезонные трансферы:
-- одна команда = одна строка (Understat склеивает через запятую,
-- разворачиваем в stage).

{{ config(materialized='incremental', unique_key='lnk_player_team_hk') }}

WITH incoming AS (
    SELECT
        lnk_player_team_hk,
        hub_player_hk,
        hub_team_hk,
        hub_season_hk,
        ldts,
        rsrc
    FROM {{ ref('stg_understat_player_team') }}
)

SELECT * FROM incoming

{% if is_incremental() %}
WHERE lnk_player_team_hk NOT IN (SELECT lnk_player_team_hk FROM {{ this }})
{% endif %}
