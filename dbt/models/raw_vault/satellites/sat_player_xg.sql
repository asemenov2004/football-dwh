-- Satellite: xG-статистика игрока за сезон в лиге (Understat).
-- Один игрок × одна лига × один сезон = одна активная строка.
-- При смене данных (накопление голов по ходу сезона) hashdiff меняется
-- и в сат добавляется новая версия.

{{ config(materialized='incremental', unique_key=['hub_player_hk', 'ldts']) }}

{{ datavault4dbt.sat_v0(
    parent_hashkey='hub_player_hk',
    src_hashdiff='sat_player_xg_hashdiff',
    src_payload=[
        'xg', 'xa', 'npxg', 'npg', 'xg_chain', 'xg_buildup',
        'goals', 'assists', 'shots', 'key_passes',
        'games', 'minutes', 'position',
        'league_id', 'season_year'
    ],
    src_ldts='ldts',
    src_rsrc='rsrc',
    source_model='stg_understat_players'
) }}