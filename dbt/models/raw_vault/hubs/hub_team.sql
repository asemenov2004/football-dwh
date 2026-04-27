-- Hub для бизнес-сущности «Команда».
-- BK = (record_source, team_id) → AF и SB-команды хранятся как РАЗНЫЕ записи.
-- Источник: только AF (в SB team_id = NULL в stage.sb_matches).

{{ config(materialized='incremental', unique_key='hub_team_hk') }}

{{ datavault4dbt.hub(
    hashkey='hub_team_hk',
    business_keys=['team_bk'],
    src_ldts='ldts',
    src_rsrc='rsrc',
    source_models={'stg_af_teams': {}, 'stg_understat_teams': {}}
) }}
