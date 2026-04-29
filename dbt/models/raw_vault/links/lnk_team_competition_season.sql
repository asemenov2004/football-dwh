-- Link: команда ↔ турнир ↔ сезон.
-- Показывает участие команды в конкретном турнире/сезоне.
-- Источник: stg_understat_teams (содержит все три FK-хеша + link HK).

{{ config(materialized='incremental', unique_key='lnk_team_comp_season_hk') }}

{{ datavault4dbt.link(
    link_hashkey='lnk_team_comp_season_hk',
    foreign_hashkeys=['hub_team_hk', 'hub_competition_hk', 'hub_season_hk'],
    src_ldts='ldts',
    src_rsrc='rsrc',
    source_models='stg_understat_teams'
) }}
