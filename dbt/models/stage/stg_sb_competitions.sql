-- Stage-модель для hub_competition из StatsBomb.
-- Преобразует (country_name, competition_name) → league_id slug.
-- Маппинг жёстко зашит — конфигурация лиг менялась в ingestion/config.py.

WITH src AS (
    SELECT
        competition_id,
        season_id,
        country_name,
        competition_name,
        loaded_at
    FROM {{ source('stage', 'sb_competitions') }}
),

with_slug AS (
    SELECT
        *,
        CASE
            WHEN country_name = 'England'  AND competition_name = 'Premier League'  THEN 'epl'
            WHEN country_name = 'Spain'    AND competition_name = 'La Liga'          THEN 'la_liga'
            WHEN country_name = 'Italy'    AND competition_name = 'Serie A'          THEN 'serie_a'
            WHEN country_name = 'Germany'  AND competition_name = '1. Bundesliga'    THEN 'bundesliga'
            WHEN country_name = 'France'   AND competition_name = 'Ligue 1'          THEN 'ligue_1'
            WHEN country_name = 'Europe'   AND competition_name = 'Champions League' THEN 'ucl'
        END AS league_id
    FROM src
)

SELECT
    -- Hashkey (общий с AF — только slug в BK, без record_source)
    md5(COALESCE(league_id, '^^'))     AS hub_competition_hk,

    -- Business key
    league_id,

    -- Служебные
    loaded_at                          AS ldts,
    'sb'                               AS rsrc

FROM with_slug
WHERE league_id IS NOT NULL
