-- Hub для бизнес-сущности «Игрок».
-- BK = player_bk ('af|{player_id}') — только AF, SB player data не поддерживается.
-- Источник: stg_af_topscorers.

{{ config(materialized='incremental', unique_key='hub_player_hk') }}

{{ datavault4dbt.hub(
    hashkey='hub_player_hk',
    business_keys=['player_bk'],
    src_ldts='ldts',
    src_rsrc='rsrc',
    source_models='stg_af_topscorers'
) }}
