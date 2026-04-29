-- Hub для бизнес-сущности «Сезон».
-- BK = season_year (год, напр. 2024). Источник: Understat.

{{ config(materialized='incremental', unique_key='hub_season_hk') }}

{{ datavault4dbt.hub(
    hashkey='hub_season_hk',
    business_keys=['season_year'],
    src_ldts='ldts',
    src_rsrc='rsrc',
    source_models={
        'stg_understat_players': {},
        'stg_understat_teams': {}
    }
) }}
