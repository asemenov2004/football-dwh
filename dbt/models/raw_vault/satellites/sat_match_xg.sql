-- Satellite: xG-статистика матча (Understat).
-- home_xg / away_xg — ожидаемые голы за матч.
-- Линковка с AF/SB матчами через fuzzy match — в Business Vault.

{{ config(materialized='incremental', unique_key=['hub_match_hk', 'ldts']) }}

{{ datavault4dbt.sat_v0(
    parent_hashkey='hub_match_hk',
    src_hashdiff='sat_match_xg_hashdiff',
    src_payload=[
        'home_xg', 'away_xg', 'home_goals', 'away_goals',
        'home_team_title', 'away_team_title',
        'match_datetime', 'league_id', 'season_year'
    ],
    src_ldts='ldts',
    src_rsrc='rsrc',
    source_model='stg_understat_matches'
) }}
