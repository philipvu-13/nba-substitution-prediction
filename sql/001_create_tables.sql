-- ============================================
-- MINNESOTA SUBSTITUTION PROJECT - CANONICAL SCHEMA
-- ============================================
-- Safe to run multiple times.

BEGIN;

-- --------------------------------------------
-- 1) games
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS games (
    game_id VARCHAR(20) PRIMARY KEY,
    game_date DATE NOT NULL,
    season VARCHAR(10),
    home_team VARCHAR(10),
    away_team VARCHAR(10),
    matchup VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- --------------------------------------------
-- 2) players
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS players (
    player_id BIGINT PRIMARY KEY,
    full_name TEXT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    is_active BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- --------------------------------------------
-- 3) play_by_play_v3_raw
-- Raw PlayByPlayV3 payload shape (camelCase kept intentionally)
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS play_by_play_v3_raw (
    "gameId" VARCHAR(20) NOT NULL,
    "actionNumber" INTEGER NOT NULL,
    "clock" VARCHAR(20),
    "period" INTEGER,
    "teamId" BIGINT,
    "teamTricode" VARCHAR(10),
    "personId" BIGINT,
    "playerName" TEXT,
    "playerNameI" TEXT,
    "xLegacy" DOUBLE PRECISION,
    "yLegacy" DOUBLE PRECISION,
    "shotDistance" DOUBLE PRECISION,
    "shotResult" TEXT,
    "isFieldGoal" BOOLEAN,
    "scoreHome" INTEGER,
    "scoreAway" INTEGER,
    "pointsTotal" INTEGER,
    "location" TEXT,
    "description" TEXT,
    "actionType" TEXT,
    "subType" TEXT,
    "videoAvailable" BOOLEAN,
    "actionId" BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT play_by_play_v3_raw_uniq UNIQUE ("gameId", "actionId")
);

-- --------------------------------------------
-- 4) player_rotations
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS player_rotations (
    id BIGSERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL,
    team_id BIGINT NOT NULL,
    team_city TEXT,
    team_name TEXT,
    player_id BIGINT NOT NULL,
    player_first TEXT,
    player_last TEXT,
    in_time_real INTEGER NOT NULL,
    out_time_real INTEGER NOT NULL,
    player_pts DOUBLE PRECISION,
    pt_diff DOUBLE PRECISION,
    usg_pct DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uniq_rotation UNIQUE (game_id, team_id, player_id, in_time_real, out_time_real)
);

-- --------------------------------------------
-- 5) player_boxscore_minutes
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS player_boxscore_minutes (
    game_id VARCHAR(20) NOT NULL,
    player_id BIGINT NOT NULL,
    player_name TEXT,
    minutes_text VARCHAR(20) NOT NULL,
    minutes_seconds INTEGER NOT NULL,
    source VARCHAR(50) DEFAULT 'boxscoretraditionalv3',
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (game_id, player_id)
);

-- --------------------------------------------
-- 6) excluded_games
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS excluded_games (
    game_id VARCHAR(20) PRIMARY KEY,
    reason TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- --------------------------------------------
-- 7) player_stints
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS player_stints (
    id BIGSERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL,
    quarter INTEGER NOT NULL,
    sub_in_time INTEGER NOT NULL,
    sub_out_time INTEGER NOT NULL,
    stint_length_seconds INTEGER NOT NULL,
    score_diff INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uniq_player_stints UNIQUE (game_id, quarter, sub_in_time, sub_out_time)
);

-- --------------------------------------------
-- Indexes for common query paths
-- --------------------------------------------
CREATE INDEX IF NOT EXISTS idx_games_season ON games (season);
CREATE INDEX IF NOT EXISTS idx_games_game_date ON games (game_date);

CREATE INDEX IF NOT EXISTS idx_pbp_game_period_action
    ON play_by_play_v3_raw ("gameId", "period", "actionNumber");

CREATE INDEX IF NOT EXISTS idx_rotations_game_player_team
    ON player_rotations (game_id, player_id, team_id);

CREATE INDEX IF NOT EXISTS idx_stints_game_quarter
    ON player_stints (game_id, quarter);

COMMIT;
