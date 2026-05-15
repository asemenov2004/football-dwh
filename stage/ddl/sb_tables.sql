CREATE SCHEMA IF NOT EXISTS stage;

-- ---------- competitions ----------
-- Одна строка = одна пара (competition_id, season_id).
-- Payload хранит описание турнира целиком (country_name, gender и пр.).
CREATE TABLE IF NOT EXISTS stage.sb_competitions (
    competition_id     INTEGER     NOT NULL,
    season_id          INTEGER     NOT NULL,
    dt                 DATE        NOT NULL,
    country_name       TEXT,
    competition_name   TEXT,
    source_file        TEXT        NOT NULL,
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_payload        JSONB       NOT NULL,
    PRIMARY KEY (competition_id, season_id, dt)
);

-- ---------- matches ----------

CREATE TABLE IF NOT EXISTS stage.sb_matches (
    match_id           BIGINT      NOT NULL,
    dt                 DATE        NOT NULL,
    league_id          TEXT        NOT NULL,
    competition_id     INTEGER,
    season_id          INTEGER     NOT NULL,
    match_date         DATE,
    home_team_id       BIGINT,
    away_team_id       BIGINT,
    source_file        TEXT        NOT NULL,
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_payload        JSONB       NOT NULL,
    PRIMARY KEY (match_id, dt)
);

