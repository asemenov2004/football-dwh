-- Satellite: счёт и статус матча из StatsBomb.
-- Append-only через hashdiff. PK = (hub_match_hk, ldts).

{{ config(materialized='incremental', unique_key=['hub_match_hk', 'ldts']) }}

{{ datavault4dbt.sat_v0(
    parent_hashkey='hub_match_hk',
    src_hashdiff='sat_match_score_hashdiff',
    src_payload=['home_goals', 'away_goals', 'match_status', 'match_ts'],
    src_ldts='ldts',
    src_rsrc='rsrc',
    source_model='stg_sb_matches'
) }}
