-- ============================================
-- MINNESOTA SUBSTITUTION PROJECT - PHASE 1
-- Database Schema
-- ============================================

-- --------------------------------------------
-- 1. GAMES TABLE
-- One row per Minnesota game
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
-- 2. PLAYERS TABLE
-- One row per player encountered
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS players (
    player_id BIGINT PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    team_abbrev VARCHAR(10),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- --------------------------------------------
-- 3. PLAY_BY_PLAY TABLE
-- One row per play-by-play event
-- --------------------------------------------
CREATE TABLE IF NOT EXISTS play_by_play (
    id BIGSERIAL PRIMARY KEY,

    game_id VARCHAR(20) NOT NULL,
    event_num INTEGER NOT NULL,

    event_type INTEGER,
    period INTEGER,
    clock VARCHAR(10),

    description_home TEXT,
    description_away TEXT,
    description_neutral TEXT,

    player1_id BIGINT,
    player2_id BIGINT,
    player3_id BIGINT,

    score VARCHAR(10),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Prevent duplicate events
    CONSTRAINT unique_game_event UNIQUE (game_id, event_num),

    -- Foreign key to games table
    CONSTRAINT fk_game
        FOREIGN KEY (game_id)
        REFERENCES games (game_id)
        ON DELETE CASCADE
);
