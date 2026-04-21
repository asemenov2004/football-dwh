-- Stage-таблицы для API-Football.
-- Каждая строка = одна запись из payload["response"] + служебные колонки.
-- raw_payload держит исходный JSON объект целиком, ключевые поля вынесены
-- в колонки ради PK и удобных JOIN-ов с Vault-слоем.
-- Стратегия загрузки: full refresh (TRUNCATE + INSERT) внутри одной транзакции.

CREATE SCHEMA IF NOT EXISTS stage;

-- ---------- leagues ----------
CREATE TABLE IF NOT EXISTS stage.af_leagues (
    league_id     TEXT        NOT NULL,
    season        INTEGER     NOT NULL,
    dt            DATE        NOT NULL,
    source_file   TEXT        NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_payload   JSONB       NOT NULL,
    PRIMARY KEY (league_id, season, dt)
);

-- ---------- teams ----------
CREATE TABLE IF NOT EXISTS stage.af_teams (
    team_id       BIGINT      NOT NULL,
    league_id     TEXT        NOT NULL,
    season        INTEGER     NOT NULL,
    dt            DATE        NOT NULL,
    source_file   TEXT        NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_payload   JSONB       NOT NULL,
    PRIMARY KEY (team_id, league_id, season, dt)
);

-- ---------- fixtures ----------
CREATE TABLE IF NOT EXISTS stage.af_fixtures (
    fixture_id    BIGINT      NOT NULL,
    league_id     TEXT        NOT NULL,
    season        INTEGER     NOT NULL,
    dt            DATE        NOT NULL,
    event_date    TIMESTAMPTZ,
    home_id       BIGINT,
    away_id       BIGINT,
    source_file   TEXT        NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_payload   JSONB       NOT NULL,
    PRIMARY KEY (fixture_id, dt)
);

-- ---------- standings ----------
CREATE TABLE IF NOT EXISTS stage.af_standings (
    league_id     TEXT        NOT NULL,
    season        INTEGER     NOT NULL,
    team_id       BIGINT      NOT NULL,
    dt            DATE        NOT NULL,
    rank          INTEGER,
    points        INTEGER,
    source_file   TEXT        NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_payload   JSONB       NOT NULL,
    PRIMARY KEY (league_id, season, team_id, dt)
);

-- ---------- topscorers ----------
CREATE TABLE IF NOT EXISTS stage.af_topscorers (
    player_id     BIGINT      NOT NULL,
    league_id     TEXT        NOT NULL,
    season        INTEGER     NOT NULL,
    dt            DATE        NOT NULL,
    team_id       BIGINT,
    goals         INTEGER,
    source_file   TEXT        NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_payload   JSONB       NOT NULL,
    PRIMARY KEY (player_id, league_id, season, dt)
);
