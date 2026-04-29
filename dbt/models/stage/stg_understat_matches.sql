-- Stage-модель для sat_match_xg, sat_match_score_understat,
-- lnk_match_competition_season из Understat.
-- Источник: stage.understat_matches (xG на уровне матча).
-- BK матча: 'understat|{match_id}' — собственный understat-ключ.
-- Линковка с SB hub_match через дату+команды — в lnk_match_same_as.

WITH src AS (
    SELECT
        match_id,
        league_id,
        season,
        loaded_at,
        raw_payload
    FROM {{ source('stage', 'understat_matches') }}
)

SELECT
    -- Hashkey для hub_match (understat BK)
    md5('understat' || '||' ||
        COALESCE(match_id, '^^'))                   AS hub_match_hk,

    -- Hashkey для hub_competition (BK = league_id slug)
    md5(COALESCE(lower(trim(league_id)), '^^'))     AS hub_competition_hk,

    -- Hashkey для hub_season (BK = season_year)
    md5(COALESCE(cast(season AS text), '^^'))       AS hub_season_hk,

    -- PK линка lnk_match_competition_season
    md5(
        md5('understat' || '||' || COALESCE(match_id, '^^'))    || '||' ||
        md5(COALESCE(lower(trim(league_id)), '^^'))             || '||' ||
        md5(COALESCE(cast(season AS text), '^^'))
    )                                               AS lnk_match_comp_season_hk,

    -- Business key
    'understat|' || match_id                        AS match_bk,

    -- BK для hub_season
    season                                          AS season_year,

    -- Контекст лиги
    league_id,

    -- Команды (для fuzzy-линковки с SB в lnk_match_same_as)
    raw_payload -> 'h' ->> 'title'                  AS home_team_title,
    raw_payload -> 'a' ->> 'title'                  AS away_team_title,
    raw_payload -> 'h' ->> 'id'                     AS home_understat_team_id,
    raw_payload -> 'a' ->> 'id'                     AS away_understat_team_id,

    -- Фактические голы
    (raw_payload -> 'goals' ->> 'h')::int           AS home_goals,
    (raw_payload -> 'goals' ->> 'a')::int           AS away_goals,

    -- xG
    (raw_payload -> 'xG' ->> 'h')::numeric          AS home_xg,
    (raw_payload -> 'xG' ->> 'a')::numeric          AS away_xg,

    -- Дата/время матча
    (raw_payload ->> 'datetime')::timestamp         AS match_datetime,

    -- Hashdiff для sat_match_xg
    md5(
        COALESCE(raw_payload -> 'xG' ->> 'h',    '^^') || '||' ||
        COALESCE(raw_payload -> 'xG' ->> 'a',    '^^') || '||' ||
        COALESCE(raw_payload -> 'goals' ->> 'h', '^^') || '||' ||
        COALESCE(raw_payload -> 'goals' ->> 'a', '^^') || '||' ||
        COALESCE(league_id, '^^')                       || '||' ||
        COALESCE(cast(season AS text), '^^')
    )                                               AS sat_match_xg_hashdiff,

    -- Hashdiff для sat_match_score_understat (только счёт + datetime)
    md5(
        COALESCE(raw_payload -> 'goals' ->> 'h', '^^') || '||' ||
        COALESCE(raw_payload -> 'goals' ->> 'a', '^^') || '||' ||
        COALESCE(raw_payload ->> 'datetime',     '^^')
    )                                               AS sat_match_score_understat_hashdiff,

    -- Служебные
    loaded_at   AS ldts,
    'understat' AS rsrc

FROM src
