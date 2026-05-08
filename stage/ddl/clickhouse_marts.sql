-- ClickHouse DDL для март-витрин Этапа 5.
-- Загрузка данных идёт DAG'ом build_marts через INSERT FROM s3() из MinIO.
-- Идемпотентно: CREATE DATABASE/TABLE IF NOT EXISTS.

CREATE DATABASE IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS marts.mart_league_table
(
    team_bk        String,
    team_title     String,
    league_id      LowCardinality(String),
    season_year    UInt16,
    gp             UInt8,
    w              UInt8,
    d              UInt8,
    l              UInt8,
    gf             UInt16,
    ga             UInt16,
    gd             Int16,
    pts            UInt8,
    xpts           Float64,
    xg_for         Float64,
    xg_against     Float64,
    npxg_diff      Float64,
    ppda           Float64,
    oppda          Float64,
    pts_overperf   Float64,
    position       UInt8
)
ENGINE = MergeTree()
ORDER BY (league_id, season_year, position);

CREATE TABLE IF NOT EXISTS marts.mart_top_scorers
(
    player_bk      String,
    player_name    String,
    teams_concat   String,
    teams_count    UInt8,
    league_id      LowCardinality(String),
    season_year    UInt16,
    position       Nullable(String),
    games          Nullable(UInt8),
    minutes        Nullable(UInt16),
    goals          Nullable(UInt8),
    assists        Nullable(UInt8),
    shots          Nullable(UInt16),
    key_passes     Nullable(UInt16),
    xg             Nullable(Float64),
    xa             Nullable(Float64),
    npxg           Nullable(Float64),
    npg            Nullable(UInt8),
    goals_per_xg   Nullable(Float64)
)
ENGINE = MergeTree()
ORDER BY (league_id, season_year, player_bk);

CREATE TABLE IF NOT EXISTS marts.mart_match_facts
(
    match_hk          String,
    match_datetime    DateTime,
    match_date        Date,
    league_id         LowCardinality(String),
    season_year       UInt16,
    home_team_title   String,
    away_team_title   String,
    home_goals        UInt8,
    away_goals        UInt8,
    home_xg           Float64,
    away_xg           Float64,
    goal_diff         Int8,
    xg_diff           Float64,
    result            LowCardinality(String),
    total_goals       UInt8,
    total_xg          Float64
)
ENGINE = MergeTree()
ORDER BY (league_id, season_year, match_date);

CREATE TABLE IF NOT EXISTS marts.mart_player_overperformers
(
    player_hk          String,
    player_bk          String,
    player_name        String,
    team_title         Nullable(String),
    league_id          LowCardinality(String),
    season_year        UInt16,
    position           Nullable(String),
    minutes            Nullable(UInt16),
    games              Nullable(UInt8),
    goals              Nullable(UInt8),
    xg                 Nullable(Float64),
    assists            Nullable(UInt8),
    xa                 Nullable(Float64),
    npxg               Nullable(Float64),
    npg                Nullable(UInt8),
    goals_minus_xg     Nullable(Float64),
    assists_minus_xa   Nullable(Float64),
    overperf_pct_rank  Nullable(Float64)
)
ENGINE = MergeTree()
ORDER BY (league_id, season_year, player_hk);

CREATE TABLE IF NOT EXISTS marts.mart_team_xg_trend
(
    league_id        LowCardinality(String),
    season_year      UInt16,
    team_title       String,
    matches          UInt16,
    avg_xg_for       Nullable(Float64),
    avg_xg_against   Nullable(Float64),
    std_xg_for       Nullable(Float64),
    std_xg_against   Nullable(Float64),
    max_xg_for       Nullable(Float64),
    max_xg_against   Nullable(Float64),
    total_gf         UInt32,
    total_ga         UInt32,
    gf_minus_xg      Nullable(Float64),
    ga_minus_xga     Nullable(Float64)
)
ENGINE = MergeTree()
ORDER BY (league_id, season_year, team_title);

-- Этап 7: Elo-рейтинг (Spark calculate_elo.py)
CREATE TABLE IF NOT EXISTS marts.mart_team_elo_history
(
    team_title         String,
    league_id          LowCardinality(String),
    season_year        UInt16,
    match_date         Date,
    opponent_title     String,
    is_home            UInt8,
    goals_for          Nullable(UInt8),
    goals_against      Nullable(UInt8),
    rating_before      Float64,
    rating_after       Float64,
    rating_delta       Float64,
    is_top3_in_league  UInt8
)
ENGINE = MergeTree()
ORDER BY (league_id, team_title, match_date);

CREATE TABLE IF NOT EXISTS marts.mart_team_elo_current
(
    team_title        String,
    league_id         LowCardinality(String),
    current_rating    Float64,
    peak_rating       Float64,
    peak_match_date   Date,
    matches_played    UInt16,
    last_match_date   Date
)
ENGINE = MergeTree()
ORDER BY (league_id, current_rating);

-- Этап 8: StatsBomb — история Barcelona в La Liga (демо мульти-источника).
CREATE TABLE IF NOT EXISTS marts.mart_sb_la_liga_history
(
    season_year         UInt16,
    matches_played      UInt16,
    wins                UInt16,
    draws               UInt16,
    losses              UInt16,
    goals_scored        UInt16,
    goals_conceded      UInt16,
    avg_goals_scored    Float64,
    avg_goals_conceded  Float64
)
ENGINE = MergeTree()
ORDER BY season_year;
