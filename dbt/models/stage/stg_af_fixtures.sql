-- Stage-модель для hub_match и sat_match_score из API-Football.
-- Источник: stage.af_fixtures (BK матча = 'af' || fixture_id).
-- home_id / away_id — ключи команд, нужны для lnk_match_team (в отдельной модели).

WITH src AS (
    SELECT
        fixture_id,
        loaded_at,
        raw_payload
    FROM {{ source('stage', 'af_fixtures') }}
),

extracted AS (
    SELECT
        fixture_id,
        loaded_at,

        -- Счёт и статус из JSONB
        (raw_payload -> 'goals' ->> 'home')::int             AS home_goals,
        (raw_payload -> 'goals' ->> 'away')::int             AS away_goals,
        raw_payload -> 'fixture' -> 'status' ->> 'short'     AS match_status,
        (raw_payload -> 'fixture' ->> 'date')::timestamptz   AS match_ts
    FROM src
)

SELECT
    -- Hashkey для hub_match
    md5('af' || '||' || COALESCE(cast(fixture_id AS text), '^^'))   AS hub_match_hk,

    -- Business key
    cast(fixture_id AS text)                                         AS match_id,
    'af' || '|' || cast(fixture_id AS text)                         AS match_bk,

    -- Payload для sat_match_score
    home_goals,
    away_goals,
    match_status,
    match_ts,

    -- Hashdiff для sat_match_score
    md5(
        COALESCE(cast(home_goals AS text), '^^') || '||' ||
        COALESCE(cast(away_goals AS text), '^^') || '||' ||
        COALESCE(match_status, '^^') || '||' ||
        COALESCE(cast(match_ts AS text), '^^')
    )                                                                AS sat_match_score_hashdiff,

    -- Служебные
    loaded_at                                                        AS ldts,
    'af'                                                             AS rsrc

FROM extracted
