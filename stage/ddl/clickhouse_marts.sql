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
