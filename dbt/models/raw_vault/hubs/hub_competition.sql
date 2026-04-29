-- Hub для бизнес-сущности «Турнир/Лига».
-- BK = league_id slug (epl, la_liga, ...) — общий для SB и Understat.
-- Источники: stg_sb_competitions + stg_understat_teams + stg_understat_players.

{{ config(materialized='incremental', unique_key='hub_competition_hk') }}

{{ datavault4dbt.hub(
    hashkey='hub_competition_hk',
    business_keys=['league_id'],
    src_ldts='ldts',
    src_rsrc='rsrc',
    source_models={
        'stg_sb_competitions': {},
        'stg_understat_teams': {},
        'stg_understat_players': {}
    }
) }}
