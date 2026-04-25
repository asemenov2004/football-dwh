-- Hub для бизнес-сущности «Сезон».
-- BK = season_year (год, напр. 2024) БЕЗ record_source.
-- Источник: AF (season_year = числовой год). SB season_id ≠ год — маппинг отложен.

{{ config(materialized='incremental', unique_key='hub_season_hk') }}

{{ datavault4dbt.hub(
    hashkey='hub_season_hk',
    business_keys=['season_year'],
    src_ldts='ldts',
    src_rsrc='rsrc',
    source_models='stg_af_leagues'
) }}
