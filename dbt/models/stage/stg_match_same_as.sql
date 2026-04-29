-- Stage-модель для lnk_match_same_as (Understat ↔ StatsBomb).
-- Same-as bridge через fuzzy match по (date, normalized home, normalized away, league).
-- Overlap по сезонам мизерный (SB ~2015 и старше, Understat 2022+),
-- ожидаем единицы–десятки пар на ligue_1 2022 (32 SB-матча).

WITH understat AS (
    SELECT
        league_id,
        date(match_datetime)             AS match_date,
        lower(trim(home_team_title))     AS home_norm,
        lower(trim(away_team_title))     AS away_norm,
        ldts                              AS loaded_at,
        hub_match_hk                      AS hub_match_hk_u
    FROM {{ ref('stg_understat_matches') }}
),

sb AS (
    SELECT
        match_id,
        league_id,
        match_date,
        -- Нормализуем (убираем префиксы клубов, типичные для SB)
        regexp_replace(
            lower(trim(raw_payload ->> 'home_team')),
            '^(afc |fc |cf |ac |as )', ''
        )                                AS home_norm,
        regexp_replace(
            lower(trim(raw_payload ->> 'away_team')),
            '^(afc |fc |cf |ac |as )', ''
        )                                AS away_norm,
        loaded_at,
        md5('sb' || '||' || COALESCE(cast(match_id AS text), '^^')) AS hub_match_hk_sb
    FROM {{ source('stage', 'sb_matches') }}
)

SELECT
    md5(u.hub_match_hk_u || '||' || s.hub_match_hk_sb)  AS lnk_match_same_as_hk,
    u.hub_match_hk_u                                    AS hub_match_hk_understat,
    s.hub_match_hk_sb                                   AS hub_match_hk_sb,
    GREATEST(u.loaded_at, s.loaded_at)                  AS ldts,
    'derived'                                           AS rsrc

FROM understat u
JOIN sb s
  ON u.match_date = s.match_date
 AND u.league_id = s.league_id
 AND (
        u.home_norm = s.home_norm
     OR u.home_norm = regexp_replace(s.home_norm, '^(afc |fc |cf |ac |as )', '')
     OR regexp_replace(u.home_norm, '^(afc |fc |cf |ac |as )', '') = s.home_norm
 )
 AND (
        u.away_norm = s.away_norm
     OR u.away_norm = regexp_replace(s.away_norm, '^(afc |fc |cf |ac |as )', '')
     OR regexp_replace(u.away_norm, '^(afc |fc |cf |ac |as )', '') = s.away_norm
 )
