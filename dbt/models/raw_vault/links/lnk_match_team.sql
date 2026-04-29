-- Link: матч ↔ команда с ролью (home/away).
-- Пишем как plain SQL, а не через datavault4dbt.link(), потому что
-- нам нужно сохранить team_role — атрибут роли, которого нет в стандартном линке.
-- Источник: stg_understat_matches_teams (разворот home+away из Understat matches).

{{ config(materialized='incremental', unique_key='lnk_match_team_hk') }}

WITH incoming AS (
    SELECT
        lnk_match_team_hk,
        hub_match_hk,
        hub_team_hk,
        team_role,
        ldts,
        rsrc
    FROM {{ ref('stg_understat_matches_teams') }}
)

SELECT * FROM incoming

{% if is_incremental() %}
WHERE lnk_match_team_hk NOT IN (SELECT lnk_match_team_hk FROM {{ this }})
{% endif %}
