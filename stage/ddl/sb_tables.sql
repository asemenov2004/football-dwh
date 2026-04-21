-- Stage-таблицы для StatsBomb Open Data.
-- Покрытие StatsBomb неравномерное: La Liga имеет все сезоны Месси,
-- остальные топ-5 лиг могут иметь 0-2 сезона. PK включает dt для того
-- же паттерна full-refresh, что и в af_*.

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
-- Одна строка = один матч. league_id здесь — наш slug (epl/la_liga/...),
-- а не competition_id, чтобы джойниться с af_* через общий бизнес-ключ.
--
-- competition_id — nullable: в matches.json statsbombpy его не хранит,
-- резолвим через competitions.json. Если резолв не нашёл пару — NULL.
-- home_team_id/away_team_id тоже nullable: statsbombpy в matches отдаёт
-- только названия команд, id команд есть только в lineups endpoint
-- (добавим позже при необходимости).
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

-- Миграция для уже созданных инстансов: снимаем NOT NULL, если он стоял.
-- ALTER DROP NOT NULL идемпотентен — на nullable-колонке это no-op.
ALTER TABLE stage.sb_matches ALTER COLUMN competition_id DROP NOT NULL;
