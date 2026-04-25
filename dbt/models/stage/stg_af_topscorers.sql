-- Stage-модель для hub_player, sat_player_details, sat_topscorer, lnk_player_team.
-- Источник: stage.af_topscorers (бомбардиры по лигам, снапшот).
-- BK игрока = 'af|{player_id}' — только AF, SB player data не поддерживается.

WITH src AS (
    SELECT
        player_id,
        league_id,
        season,
        dt,
        team_id,
        loaded_at,
        raw_payload
    FROM {{ source('stage', 'af_topscorers') }}
),

extracted AS (
    SELECT
        player_id,
        league_id,
        season,
        dt,
        team_id,
        loaded_at,

        -- Атрибуты игрока
        raw_payload -> 'player' ->> 'name'                                     AS player_name,
        raw_payload -> 'player' ->> 'nationality'                               AS nationality,
        raw_payload -> 'player' ->> 'photo'                                     AS photo_url,

        -- Статистика бомбардира
        (raw_payload -> 'statistics' -> 0 -> 'goals' ->> 'total')::int         AS goals,
        (raw_payload -> 'statistics' -> 0 -> 'goals' ->> 'assists')::int       AS assists,
        (raw_payload -> 'statistics' -> 0 -> 'games' ->> 'appearences')::int   AS appearances,
        raw_payload -> 'statistics' -> 0 -> 'games' ->> 'rating'               AS rating
    FROM src
)

SELECT
    -- Hashkeys
    md5('af' || '||' || COALESCE(cast(player_id AS text), '^^'))           AS hub_player_hk,
    md5('af' || '||' || COALESCE(cast(team_id AS text), '^^'))             AS hub_team_hk,
    md5(COALESCE(lower(trim(league_id)), '^^'))                            AS hub_competition_hk,
    md5(COALESCE(cast(season AS text), '^^'))                              AS hub_season_hk,

    -- Link HK: player ↔ team
    md5(
        md5('af' || '||' || COALESCE(cast(player_id AS text), '^^'))
        || '||' ||
        md5('af' || '||' || COALESCE(cast(team_id AS text), '^^'))
    )                                                                       AS lnk_player_team_hk,

    -- Business key
    'af' || '|' || cast(player_id AS text)                                 AS player_bk,

    -- Атрибуты игрока (для sat_player_details)
    player_name,
    nationality,
    photo_url,

    -- Статистика (для sat_topscorer)
    goals,
    assists,
    appearances,
    rating,
    dt,

    -- Hashdiff для sat_player_details
    md5(
        COALESCE(player_name, '^^')  || '||' ||
        COALESCE(nationality, '^^')  || '||' ||
        COALESCE(photo_url, '^^')
    )                                                                       AS sat_player_details_hashdiff,

    -- Hashdiff для sat_topscorer (включаем dt — каждый снапшот = новая запись)
    md5(
        COALESCE(cast(dt AS text), '^^')           || '||' ||
        COALESCE(cast(goals AS text), '^^')        || '||' ||
        COALESCE(cast(assists AS text), '^^')      || '||' ||
        COALESCE(cast(appearances AS text), '^^')  || '||' ||
        COALESCE(rating, '^^')
    )                                                                       AS sat_topscorer_hashdiff,

    -- Служебные
    loaded_at                                                               AS ldts,
    'af'                                                                    AS rsrc

FROM extracted
