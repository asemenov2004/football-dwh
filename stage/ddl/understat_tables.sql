-- Stage-таблицы для Understat (xG-статистика).
-- Стратегия: UPSERT по натуральному ключу — разные сезоны накапливаются,
-- повторный запуск за тот же сезон обновляет данные.

-- ---------- players ----------
CREATE TABLE IF NOT EXISTS stage.understat_players (
    player_id   TEXT        NOT NULL,
    player_name TEXT        NOT NULL,
    league_id   TEXT        NOT NULL,
    season      INTEGER     NOT NULL,
    dt          DATE        NOT NULL,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_payload JSONB       NOT NULL,
    PRIMARY KEY (player_id, league_id, season)
);

-- ---------- teams ----------
CREATE TABLE IF NOT EXISTS stage.understat_teams (
    team_title  TEXT        NOT NULL,
    league_id   TEXT        NOT NULL,
    season      INTEGER     NOT NULL,
    dt          DATE        NOT NULL,
    loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_payload JSONB       NOT NULL,
    PRIMARY KEY (team_title, league_id, season)
);