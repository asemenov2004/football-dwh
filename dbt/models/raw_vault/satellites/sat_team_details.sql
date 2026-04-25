-- Satellite: описательные атрибуты команды (название, страна, логотип, стадион).
-- Источник: stg_af_teams. Append-only — новая строка только при изменении hashdiff.

{{ config(materialized='incremental', unique_key=['hub_team_hk', 'ldts']) }}

{{ datavault4dbt.sat_v0(
    parent_hashkey='hub_team_hk',
    src_hashdiff='sat_team_details_hashdiff',
    src_payload=['team_name', 'country', 'logo_url', 'venue_name', 'venue_city', 'venue_capacity'],
    src_ldts='ldts',
    src_rsrc='rsrc',
    source_model='stg_af_teams'
) }}
