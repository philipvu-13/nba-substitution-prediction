BEGIN;

CREATE TABLE IF NOT EXISTS core.games (
    game_id VARCHAR(20) PRIMARY KEY,
    game_date DATE NOT NULL,
    season VARCHAR(7) NOT NULL,
    season_id VARCHAR(10) NOT NULL,
    season_type VARCHAR(20) NOT NULL,
    home_team_abbrev VARCHAR(3) NOT NULL,
    away_team_abbrev VARCHAR(3) NOT NULL,
    matchup VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS core.players (
    player_id BIGINT PRIMARY KEY,
    full_name TEXT NOT NULL,
    first_name TEXT,
    last_name TEXT,
    is_active BOOLEAN,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS core.excluded_games (
    game_id VARCHAR(20) PRIMARY KEY
        REFERENCES core.games(game_id),
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_games_season_type_date
    ON core.games (season, season_type, game_date);

COMMIT;