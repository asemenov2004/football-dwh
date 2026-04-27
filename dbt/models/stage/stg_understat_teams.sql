-- Stage-модель для sat_team_xg из Understat.
-- Источник: stage.understat_teams (сезонная xG/PPDA-статистика команд).
-- BK команды: 'understat|{lower(team_title)}' — собственный understat-ключ.
-- Маппинг с AF hub_team через team_name выполняется в Business Vault.

WITH src AS (
    SELECT
        team_title,
        league_id,
        season,
        loaded_at,
        raw_payload
    FROM {{ source('stage', 'understat_teams') }}
)

SELECT
    -- Hashkey для hub_team (understat BK)
    md5('understat' || '||' ||
        COALESCE(lower(trim(team_title)), '^^'))  AS hub_team_hk,

    -- Business key
    'understat|' || lower(trim(team_title))       AS team_bk,

    -- BK для hub_season
    season                                        AS season_year,

    -- Контекст
    league_id,
    team_title,

    -- Payload: xG-метрики команды за сезон
    (raw_payload ->> 'xG')::numeric      AS xg_for,
    (raw_payload ->> 'NPxG')::numeric    AS npxg_for,
    (raw_payload ->> 'xGA')::numeric     AS xg_against,
    (raw_payload ->> 'NPxGA')::numeric   AS npxg_against,
    (raw_payload ->> 'NPxGD')::numeric   AS npxg_diff,
    (raw_payload ->> 'PPDA')::numeric    AS ppda,
    (raw_payload ->> 'OPPDA')::numeric   AS oppda,
    (raw_payload ->> 'DC')::int          AS deep_completions,
    (raw_payload ->> 'ODC')::int         AS opp_deep_completions,
    (raw_payload ->> 'xPTS')::numeric    AS xpts,
    (raw_payload ->> 'M')::int           AS matches,
    (raw_payload ->> 'W')::int           AS wins,
    (raw_payload ->> 'D')::int           AS draws,
    (raw_payload ->> 'L')::int           AS losses,
    (raw_payload ->> 'PTS')::int         AS points,

    -- Hashdiff
    md5(
        COALESCE(raw_payload ->> 'xG',    '^^') || '||' ||
        COALESCE(raw_payload ->> 'NPxG',  '^^') || '||' ||
        COALESCE(raw_payload ->> 'xGA',   '^^') || '||' ||
        COALESCE(raw_payload ->> 'PPDA',  '^^') || '||' ||
        COALESCE(raw_payload ->> 'OPPDA', '^^') || '||' ||
        COALESCE(raw_payload ->> 'xPTS',  '^^') || '||' ||
        COALESCE(raw_payload ->> 'M',     '^^') || '||' ||
        COALESCE(league_id, '^^')               || '||' ||
        COALESCE(cast(season AS text), '^^')
    )                                            AS sat_team_xg_hashdiff,

    -- Служебные
    loaded_at   AS ldts,
    'understat' AS rsrc

FROM src