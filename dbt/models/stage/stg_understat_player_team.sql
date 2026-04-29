-- Stage-модель для lnk_player_team.
-- Разворачивает team_title (строка через запятую для трансферных игроков)
-- в long-формат: одна строка = один (player, team, season).
-- Гранулярность линка: player × team × season (с учётом мид-сезонных трансферов).

WITH src AS (
    SELECT
        player_id,
        season,
        loaded_at,
        raw_payload
    FROM {{ source('stage', 'understat_players') }}
    WHERE raw_payload ->> 'team_title' IS NOT NULL
),

exploded AS (
    SELECT
        player_id,
        season,
        loaded_at,
        trim(team) AS team_title
    FROM src,
         LATERAL regexp_split_to_table(raw_payload ->> 'team_title', ',') AS team
    WHERE trim(team) <> ''
)

SELECT DISTINCT
    md5('understat' || '||' || COALESCE(player_id, '^^'))            AS hub_player_hk,
    md5('understat' || '||' || COALESCE(lower(team_title), '^^'))    AS hub_team_hk,
    md5(COALESCE(cast(season AS text), '^^'))                        AS hub_season_hk,

    md5(
        md5('understat' || '||' || COALESCE(player_id, '^^'))           || '||' ||
        md5('understat' || '||' || COALESCE(lower(team_title), '^^'))   || '||' ||
        md5(COALESCE(cast(season AS text), '^^'))
    )                                                                 AS lnk_player_team_hk,

    team_title,
    season                                                            AS season_year,

    loaded_at                                                         AS ldts,
    'understat'                                                       AS rsrc

FROM exploded
