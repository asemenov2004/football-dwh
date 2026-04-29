-- Satellite: xG/PPDA-статистика команды за сезон в лиге (Understat).
-- PPDA (passes per defensive action) — прокси интенсивности прессинга.
-- Один сезон × одна лига = одна активная строка (обновляется при смене данных).

{{ config(materialized='incremental', unique_key=['hub_team_hk', 'ldts']) }}

{{ datavault4dbt.sat_v0(
    parent_hashkey='hub_team_hk',
    src_hashdiff='sat_team_xg_hashdiff',
    src_payload=[
        'xg_for', 'npxg_for', 'xg_against', 'npxg_against', 'npxg_diff',
        'ppda', 'oppda', 'deep_completions', 'opp_deep_completions',
        'xpts', 'matches', 'wins', 'draws', 'losses', 'points',
        'goals_for', 'goals_against',
        'league_id', 'season_year'
    ],
    src_ldts='ldts',
    src_rsrc='rsrc',
    source_model='stg_understat_teams'
) }}
