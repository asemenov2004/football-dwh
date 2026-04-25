-- Stage-модель для hub_match и sat_match_score из StatsBomb.
-- BK матча = 'sb' || match_id — отдельные записи от AF-матчей.
-- home_team_id/away_team_id в stage.sb_matches сейчас NULL —
-- статистика StatsBomb не содержит numeric team ID в matches endpoint.

WITH src AS (
    SELECT
        match_id,
        league_id,
        loaded_at,
        raw_payload
    FROM {{ source('stage', 'sb_matches') }}
),

extracted AS (
    SELECT
        match_id,
        league_id,
        loaded_at,

        -- Счёт из JSONB
        (raw_payload ->> 'home_score')::int                  AS home_goals,
        (raw_payload ->> 'away_score')::int                  AS away_goals,
        raw_payload ->> 'match_status'                       AS match_status,
        (raw_payload ->> 'match_date')::timestamptz          AS match_ts
    FROM src
)

SELECT
    -- Hashkey для hub_match
    md5('sb' || '||' || COALESCE(cast(match_id AS text), '^^'))     AS hub_match_hk,

    -- hub_competition через общий slug
    md5(COALESCE(lower(trim(league_id)), '^^'))                      AS hub_competition_hk,

    -- Business key
    cast(match_id AS text)                                           AS match_id,
    'sb' || '|' || cast(match_id AS text)                           AS match_bk,

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
    'sb'                                                             AS rsrc

FROM extracted
