-- Link: игрок ↔ команда (участие в составе по данным бомбардиров AF).
-- Источник: stg_af_topscorers.

{{ config(materialized='incremental', unique_key='lnk_player_team_hk') }}

{{ datavault4dbt.link(
    link_hashkey='lnk_player_team_hk',
    foreign_hashkeys=['hub_player_hk', 'hub_team_hk'],
    src_ldts='ldts',
    src_rsrc='rsrc',
    source_models='stg_af_topscorers'
) }}
