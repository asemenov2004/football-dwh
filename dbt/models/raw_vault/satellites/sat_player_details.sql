-- Satellite: описательные атрибуты игрока (имя, национальность, фото).
-- Источник: stg_af_topscorers. Append-only — новая строка только при изменении hashdiff.

{{ config(materialized='incremental', unique_key=['hub_player_hk', 'ldts']) }}

{{ datavault4dbt.sat_v0(
    parent_hashkey='hub_player_hk',
    src_hashdiff='sat_player_details_hashdiff',
    src_payload=['player_name', 'nationality', 'photo_url'],
    src_ldts='ldts',
    src_rsrc='rsrc',
    source_model='stg_af_topscorers'
) }}
