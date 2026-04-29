-- Satellite: счёт Understat-матча.
-- Логически счёт — атрибут матча, а не xG, поэтому выделен из sat_match_xg.
-- Дублирование home_goals/away_goals с sat_match_xg временно допустимо
-- (sat append-only, миграция — в Этапе 5).

{{ config(materialized='incremental', unique_key=['hub_match_hk', 'ldts']) }}

{{ datavault4dbt.sat_v0(
    parent_hashkey='hub_match_hk',
    src_hashdiff='sat_match_score_understat_hashdiff',
    src_payload=['home_goals', 'away_goals', 'match_datetime'],
    src_ldts='ldts',
    src_rsrc='rsrc',
    source_model='stg_understat_matches'
) }}
