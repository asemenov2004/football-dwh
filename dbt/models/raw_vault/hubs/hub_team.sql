-- Hub для бизнес-сущности «Команда».
-- BK = team_bk ('understat|{lower(team_title)}'). Источник: только Understat.

{{ config(materialized='incremental', unique_key='hub_team_hk') }}

{{ datavault4dbt.hub(
    hashkey='hub_team_hk',
    business_keys=['team_bk'],
    src_ldts='ldts',
    src_rsrc='rsrc',
    source_models={'stg_understat_teams': {}}
) }}
