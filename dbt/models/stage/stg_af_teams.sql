-- Stage-модель для hub_team, lnk_team_competition_season, sat_team_details.
-- Источник: stage.af_teams (BK команды = 'af' || team_id).
-- team_id в AF — числовой, в BK кастуем в text для единообразия.

WITH src AS (
    SELECT
        team_id,
        league_id,
        season,
        loaded_at,
        raw_payload
    FROM {{ source('stage', 'af_teams') }}
),

extracted AS (
    SELECT
        team_id,
        lower(trim(league_id))                               AS league_id,
        season,
        loaded_at,

        -- Атрибуты команды из JSONB
        raw_payload -> 'team' ->> 'name'                    AS team_name,
        raw_payload -> 'team' ->> 'country'                 AS country,
        raw_payload -> 'team' ->> 'logo'                    AS logo_url,
        raw_payload -> 'venue' ->> 'name'                   AS venue_name,
        raw_payload -> 'venue' ->> 'city'                   AS venue_city,
        (raw_payload -> 'venue' ->> 'capacity')::int        AS venue_capacity
    FROM src
)

SELECT
    -- Hashkeys
    md5('af' || '||' || COALESCE(cast(team_id AS text), '^^'))         AS hub_team_hk,
    md5(COALESCE(league_id, '^^'))                                      AS hub_competition_hk,
    md5(COALESCE(cast(season AS text), '^^'))                           AS hub_season_hk,
    -- Link HK: хеш от всех FK-хешей
    md5(
        md5('af' || '||' || COALESCE(cast(team_id AS text), '^^'))
        || '||' || md5(COALESCE(league_id, '^^'))
        || '||' || md5(COALESCE(cast(season AS text), '^^'))
    )                                                                   AS lnk_team_comp_season_hk,

    -- Business keys
    cast(team_id AS text)                                               AS team_id,
    'af' || '|' || cast(team_id AS text)                               AS team_bk,
    league_id,
    cast(season AS text)                                                AS season_year,

    -- Payload for sat_team_details
    team_name,
    country,
    logo_url,
    venue_name,
    venue_city,
    venue_capacity,

    -- Hashdiff для sat_team_details
    md5(
        COALESCE(team_name, '^^') || '||' ||
        COALESCE(country, '^^') || '||' ||
        COALESCE(logo_url, '^^') || '||' ||
        COALESCE(venue_name, '^^') || '||' ||
        COALESCE(venue_city, '^^') || '||' ||
        COALESCE(cast(venue_capacity AS text), '^^')
    )                                                                   AS sat_team_details_hashdiff,

    -- Служебные
    loaded_at                                                           AS ldts,
    'af'                                                                AS rsrc

FROM extracted
