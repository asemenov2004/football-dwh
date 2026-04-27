-- Stage-модель для sat_standing из API-Football.
-- Источник: stage.af_standings (снапшот турнирной таблицы по лигам).
-- Один снапшот в день, dt — дата данных.

WITH src AS (
    SELECT
        team_id,
        league_id,
        season,
        dt,
        loaded_at,
        raw_payload
    FROM {{ source('stage', 'af_standings') }}
),

extracted AS (
    SELECT
        team_id,
        league_id,
        season,
        dt,
        loaded_at,

        (raw_payload ->> 'rank')::int                              AS standing_rank,
        (raw_payload ->> 'points')::int                            AS points,
        (raw_payload -> 'all' ->> 'played')::int                   AS played,
        (raw_payload -> 'all' ->> 'win')::int                      AS wins,
        (raw_payload -> 'all' ->> 'draw')::int                     AS draws,
        (raw_payload -> 'all' ->> 'lose')::int                     AS losses,
        (raw_payload -> 'all' -> 'goals' ->> 'for')::int           AS goals_for,
        (raw_payload -> 'all' -> 'goals' ->> 'against')::int       AS goals_against,
        (raw_payload ->> 'goalsDiff')::int                         AS goals_diff,
        raw_payload ->> 'form'                                     AS form,
        raw_payload ->> 'description'                              AS standing_description
    FROM src
)

SELECT
    -- Hashkeys
    md5('af' || '||' || COALESCE(cast(team_id AS text), '^^'))     AS hub_team_hk,
    md5(COALESCE(lower(trim(league_id)), '^^'))                    AS hub_competition_hk,
    md5(COALESCE(cast(season AS text), '^^'))                      AS hub_season_hk,

    -- Business keys (нужны hub_season как multi-source)
    season                                                         AS season_year,

    -- Snapshot date (payload, enters hashdiff — новая строка при смене dt)
    dt,

    -- Payload для sat_standing
    standing_rank,
    points,
    played,
    wins,
    draws,
    losses,
    goals_for,
    goals_against,
    goals_diff,
    form,
    standing_description,

    -- Hashdiff: включаем dt, чтобы каждый новый снапшот = новая строка
    md5(
        COALESCE(cast(dt AS text), '^^')             || '||' ||
        COALESCE(cast(standing_rank AS text), '^^')  || '||' ||
        COALESCE(cast(points AS text), '^^')         || '||' ||
        COALESCE(cast(played AS text), '^^')         || '||' ||
        COALESCE(cast(wins AS text), '^^')           || '||' ||
        COALESCE(cast(draws AS text), '^^')          || '||' ||
        COALESCE(cast(losses AS text), '^^')         || '||' ||
        COALESCE(cast(goals_for AS text), '^^')      || '||' ||
        COALESCE(cast(goals_against AS text), '^^')  || '||' ||
        COALESCE(cast(goals_diff AS text), '^^')     || '||' ||
        COALESCE(form, '^^')                         || '||' ||
        COALESCE(standing_description, '^^')
    )                                                              AS sat_standing_hashdiff,

    -- Служебные
    loaded_at                                                      AS ldts,
    'af'                                                           AS rsrc

FROM extracted
