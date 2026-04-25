-- Hub для бизнес-сущности «Матч».
-- BK = (record_source, match_id) — AF и SB матчи хранятся РАЗДЕЛЬНО.
-- Источники: stg_af_fixtures (rsrc='af') + stg_sb_matches (rsrc='sb').

{{ config(materialized='incremental', unique_key='hub_match_hk') }}

{{ datavault4dbt.hub(
    hashkey='hub_match_hk',
    business_keys=['match_bk'],
    src_ldts='ldts',
    src_rsrc='rsrc',
    source_models={'stg_af_fixtures': {}, 'stg_sb_matches': {}}
) }}
