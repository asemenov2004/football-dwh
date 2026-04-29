-- Stage-модель для lnk_match_team из Understat.
-- Разворот home/away команд матча в long-формат через UNION ALL.
-- BK команды совпадает с stg_understat_teams: 'understat|{lower(team_title)}'.
-- BK матча совпадает с stg_understat_matches: 'understat|{match_id}'.

WITH src AS (
    SELECT
        match_id,
        loaded_at,
        raw_payload
    FROM {{ source('stage', 'understat_matches') }}
),

home AS (
    SELECT
        match_id,
        loaded_at,
        raw_payload -> 'h' ->> 'title' AS team_title,
        'home'                         AS team_role
    FROM src
),

away AS (
    SELECT
        match_id,
        loaded_at,
        raw_payload -> 'a' ->> 'title' AS team_title,
        'away'                         AS team_role
    FROM src
),

unioned AS (
    SELECT * FROM home
    UNION ALL
    SELECT * FROM away
)

SELECT
    md5('understat' || '||' || COALESCE(match_id, '^^'))            AS hub_match_hk,
    md5('understat' || '||' || COALESCE(lower(trim(team_title)), '^^'))
                                                                     AS hub_team_hk,

    -- PK линка: хеш от связки match_hk + team_hk + team_role
    md5(
        md5('understat' || '||' || COALESCE(match_id, '^^'))         || '||' ||
        md5('understat' || '||' || COALESCE(lower(trim(team_title)), '^^')) || '||' ||
        team_role
    )                                                                AS lnk_match_team_hk,

    team_role,
    loaded_at   AS ldts,
    'understat' AS rsrc

FROM unioned
WHERE team_title IS NOT NULL
