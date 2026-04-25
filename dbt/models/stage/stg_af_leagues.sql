-- Stage-модель для hub_competition и hub_season из API-Football.
-- Источник: stage.af_leagues (один snapshot в день, distinct league_id/season).
-- Не использует datavault4dbt.stage() — hashkeys вычисляются через md5() напрямую,
-- чтобы не создавать промежуточную модель-обёртку над source().

SELECT
    -- Hashkeys
    md5(COALESCE(lower(trim(league_id)), '^^'))               AS hub_competition_hk,
    md5(COALESCE(cast(season AS text), '^^'))                 AS hub_season_hk,

    -- Business keys
    lower(trim(league_id))                                    AS league_id,
    season                                                    AS season_year,

    -- Служебные
    loaded_at                                                 AS ldts,
    'af'                                                      AS rsrc

FROM {{ source('stage', 'af_leagues') }}
