-- Проверка загрузки stage-слоя

-- 1. Кол-во строк и дата снимка по каждой таблице
SELECT 'af_leagues'      AS tbl, count(*) AS rows, max(dt) AS last_dt FROM stage.af_leagues
UNION ALL
SELECT 'af_teams',        count(*), max(dt) FROM stage.af_teams
UNION ALL
SELECT 'af_fixtures',     count(*), max(dt) FROM stage.af_fixtures
UNION ALL
SELECT 'af_standings',    count(*), max(dt) FROM stage.af_standings
UNION ALL
SELECT 'af_topscorers',   count(*), max(dt) FROM stage.af_topscorers
UNION ALL
SELECT 'sb_competitions', count(*), max(dt) FROM stage.sb_competitions
UNION ALL
SELECT 'sb_matches',      count(*), max(dt) FROM stage.sb_matches
ORDER BY tbl;

-- 2. Null-проверка ключевых полей af_fixtures
SELECT
    count(*) FILTER (WHERE league_id   IS NULL) AS null_league,
    count(*) FILTER (WHERE fixture_id  IS NULL) AS null_fixture,
    count(*) FILTER (WHERE raw_payload IS NULL) AS null_payload
FROM stage.af_fixtures;

-- 3. Null-проверка af_teams
SELECT
    count(*) FILTER (WHERE team_id     IS NULL) AS null_team_id,
    count(*) FILTER (WHERE raw_payload IS NULL) AS null_payload
FROM stage.af_teams;

-- 4. sb_matches: покрытие competition_id и диапазон дат
SELECT
    league_id,
    count(*)                         AS total_matches,
    count(competition_id)            AS with_comp_id,
    count(*) - count(competition_id) AS missing_comp_id,
    min(match_date)                  AS earliest,
    max(match_date)                  AS latest
FROM stage.sb_matches
GROUP BY league_id
ORDER BY league_id;

-- 5. Дубли в PK (должно вернуть 0 строк)
SELECT match_id, dt, count(*) FROM stage.sb_matches
GROUP BY match_id, dt HAVING count(*) > 1;

SELECT fixture_id, dt, count(*) FROM stage.af_fixtures
GROUP BY fixture_id, dt HAVING count(*) > 1;

-- 6. Семпл: referee без NaN (проверяем что _nan_to_null сработал)
SELECT match_id, raw_payload->>'referee' AS referee
FROM stage.sb_matches
WHERE raw_payload ? 'referee'
LIMIT 5;
