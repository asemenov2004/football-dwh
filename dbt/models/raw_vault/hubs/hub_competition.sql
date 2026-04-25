-- Hub для бизнес-сущности «Турнир/Лига».
-- BK = league_id slug (epl, la_liga, ...) БЕЗ record_source.
-- Slug одинаков в AF и SB — намеренно — поэтому hub общий.
-- Источники: stg_af_leagues (AF) + stg_sb_competitions (SB).

{{ config(materialized='incremental', unique_key='hub_competition_hk') }}

{{ datavault4dbt.hub(
    hashkey='hub_competition_hk',
    business_keys=['league_id'],
    src_ldts='ldts',
    src_rsrc='rsrc',
    source_models={'stg_af_leagues': {}, 'stg_sb_competitions': {}}
) }}
