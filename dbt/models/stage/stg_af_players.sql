-- Stage-модель для hub_player и sat_player_profile из AF /players endpoint.
-- Содержит полный профиль игрока: рождение, рост, вес, позиция.
-- BK игрока = 'af|{player_id}' — тот же формат что в stg_af_topscorers.

WITH src AS (
    SELECT
        player_id,
        league_id,
        season,
        loaded_at,
        raw_payload
    FROM {{ source('stage', 'af_players') }}
),

extracted AS (
    SELECT
        player_id,
        league_id,
        season,
        loaded_at,

        raw_payload -> 'player' ->> 'name'              AS player_name,
        raw_payload -> 'player' ->> 'nationality'       AS nationality,
        raw_payload -> 'player' ->> 'photo'             AS photo_url,
        raw_payload -> 'player' -> 'birth' ->> 'date'   AS birth_date,
        raw_payload -> 'player' ->> 'height'            AS height,
        raw_payload -> 'player' ->> 'weight'            AS weight,
        raw_payload -> 'statistics' -> 0
                     -> 'games' ->> 'position'          AS position
    FROM src
)

SELECT
    -- Hashkeys (идентичный формат с stg_af_topscorers)
    md5('af' || '||' || COALESCE(cast(player_id AS text), '^^'))  AS hub_player_hk,

    -- Business key
    'af' || '|' || cast(player_id AS text)                        AS player_bk,

    -- Payload для sat_player_profile
    player_name,
    nationality,
    photo_url,
    birth_date,
    height,
    weight,
    position,

    -- Hashdiff для sat_player_profile
    md5(
        COALESCE(player_name,  '^^') || '||' ||
        COALESCE(nationality,  '^^') || '||' ||
        COALESCE(birth_date,   '^^') || '||' ||
        COALESCE(height,       '^^') || '||' ||
        COALESCE(weight,       '^^') || '||' ||
        COALESCE(position,     '^^')
    )                                                             AS sat_player_profile_hashdiff,

    -- Служебные
    loaded_at       AS ldts,
    'af_players'    AS rsrc

FROM extracted
