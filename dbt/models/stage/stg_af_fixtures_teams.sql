-- Stage-модель для lnk_match_team.
-- Разворачивает один fixture в две строки (home + away) через UNION ALL.
-- SB пока не участвует — home_team_id/away_team_id в stage.sb_matches NULL.

WITH home_rows AS (
    SELECT
        fixture_id,
        home_id   AS team_id_num,
        'home'    AS team_role,
        loaded_at
    FROM {{ source('stage', 'af_fixtures') }}
    WHERE home_id IS NOT NULL
),

away_rows AS (
    SELECT
        fixture_id,
        away_id   AS team_id_num,
        'away'    AS team_role,
        loaded_at
    FROM {{ source('stage', 'af_fixtures') }}
    WHERE away_id IS NOT NULL
),

all_rows AS (
    SELECT * FROM home_rows
    UNION ALL
    SELECT * FROM away_rows
)

SELECT
    -- Link HK: хеш от (hub_match_hk || hub_team_hk || role)
    md5(
        md5('af' || '||' || COALESCE(cast(fixture_id AS text), '^^'))
        || '||' ||
        md5('af' || '||' || COALESCE(cast(team_id_num AS text), '^^'))
        || '||' ||
        COALESCE(team_role, '^^')
    )                                                                   AS lnk_match_team_hk,

    -- FK hashkeys
    md5('af' || '||' || COALESCE(cast(fixture_id AS text), '^^'))       AS hub_match_hk,
    md5('af' || '||' || COALESCE(cast(team_id_num AS text), '^^'))      AS hub_team_hk,

    -- Атрибут роли
    team_role,

    -- Служебные
    loaded_at                                                           AS ldts,
    'af'                                                                AS rsrc

FROM all_rows
