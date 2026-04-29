-- Stage-модель для sat_player_xg из Understat.
-- Источник: stage.understat_players (сезонная xG-статистика по лигам).
-- BK игрока: 'understat|{player_id}' — собственный understat-ключ.
-- ВАЖНО: team_title в Understat для трансферных игроков = строка через запятую
-- ("AC Milan,Atalanta"). Здесь храним как есть в саттелите. Развёртку для
-- lnk_player_team делает stg_understat_player_team.

WITH src AS (
    SELECT
        player_id,
        player_name,
        league_id,
        season,
        loaded_at,
        raw_payload
    FROM {{ source('stage', 'understat_players') }}
),

extracted AS (
    SELECT
        player_id,
        player_name,
        league_id,
        season,
        loaded_at,

        raw_payload ->> 'team_title'             AS team_title,

        (raw_payload ->> 'xG')::numeric          AS xg,
        (raw_payload ->> 'xA')::numeric          AS xa,
        (raw_payload ->> 'npxG')::numeric        AS npxg,
        (raw_payload ->> 'xGChain')::numeric     AS xg_chain,
        (raw_payload ->> 'xGBuildup')::numeric   AS xg_buildup,
        (raw_payload ->> 'npg')::int             AS npg,
        (raw_payload ->> 'goals')::int           AS goals,
        (raw_payload ->> 'assists')::int         AS assists,
        (raw_payload ->> 'shots')::int           AS shots,
        (raw_payload ->> 'key_passes')::int      AS key_passes,
        (raw_payload ->> 'games')::int           AS games,
        (raw_payload ->> 'time')::int            AS minutes,
        raw_payload ->> 'position'               AS position
    FROM src
)

SELECT
    -- Hashkey для hub_player (understat BK)
    md5('understat' || '||' ||
        COALESCE(player_id, '^^'))              AS hub_player_hk,

    -- Hashkey для hub_season
    md5(COALESCE(cast(season AS text), '^^'))  AS hub_season_hk,

    -- Hashkey для hub_competition (BK = league_id slug)
    md5(COALESCE(lower(trim(league_id)), '^^'))
                                                AS hub_competition_hk,

    -- Business key
    'understat|' || player_id                  AS player_bk,

    -- BK для hub_season (год начала сезона, напр. 2025)
    season                                     AS season_year,

    -- Контекст (входит в hashdiff — один игрок в разных лигах = разные строки)
    league_id,
    player_name,
    team_title,

    -- Payload
    xg,
    xa,
    npxg,
    npg,
    xg_chain,
    xg_buildup,
    goals,
    assists,
    shots,
    key_passes,
    games,
    minutes,
    position,

    -- Hashdiff: league+season делают строки уникальными по контексту
    md5(
        COALESCE(cast(xg AS text), '^^')         || '||' ||
        COALESCE(cast(xa AS text), '^^')         || '||' ||
        COALESCE(cast(npxg AS text), '^^')       || '||' ||
        COALESCE(cast(xg_chain AS text), '^^')   || '||' ||
        COALESCE(cast(xg_buildup AS text), '^^') || '||' ||
        COALESCE(cast(goals AS text), '^^')      || '||' ||
        COALESCE(cast(assists AS text), '^^')    || '||' ||
        COALESCE(cast(games AS text), '^^')      || '||' ||
        COALESCE(cast(minutes AS text), '^^')    || '||' ||
        COALESCE(league_id, '^^')                || '||' ||
        COALESCE(cast(season AS text), '^^')
    )                                            AS sat_player_xg_hashdiff,

    -- Служебные
    loaded_at   AS ldts,
    'understat' AS rsrc

FROM extracted