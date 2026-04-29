-- Hub для бизнес-сущности «Матч».
-- BK = match_bk ('sb|{match_id}' или 'understat|{match_id}'). SB и Understat хранятся РАЗДЕЛЬНО.

{{ config(materialized='incremental', unique_key='hub_match_hk') }}

{{ datavault4dbt.hub(
    hashkey='hub_match_hk',
    business_keys=['match_bk'],
    src_ldts='ldts',
    src_rsrc='rsrc',
    source_models={'stg_sb_matches': {}, 'stg_understat_matches': {}}
) }}
