BEGIN;

CREATE TABLE IF NOT EXISTS raw.play_by_play (
    game_id VARCHAR(20) NOT NULL,
    action_id BIGINT NOT NULL,
    action_number INTEGER,
    period INTEGER NOT NULL,
    clock VARCHAR(20) NOT NULL,
    team_id BIGINT,
    team_tricode VARCHAR(3),
    person_id BIGINT,
    player_name TEXT,
    description TEXT,
    action_type TEXT,
    sub_type TEXT,
    score_home INTEGER,
    score_away INTEGER,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (game_id, action_id)
);

CREATE TABLE IF NOT EXISTS raw.player_rotations (
    game_id VARCHAR(20) NOT NULL,
    team_id BIGINT NOT NULL,
    player_id BIGINT NOT NULL,
    player_first TEXT,
    player_last TEXT,
    in_time_tenths INTEGER NOT NULL,
    out_time_tenths INTEGER NOT NULL,
    player_points NUMERIC,
    point_differential NUMERIC,
    usage_percentage NUMERIC,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (
        game_id,
        team_id,
        player_id,
        in_time_tenths,
        out_time_tenths
    ),

    CHECK (out_time_tenths > in_time_tenths)
);

CREATE TABLE IF NOT EXISTS raw.player_boxscores (
    game_id VARCHAR(20) NOT NULL,
    team_id BIGINT NOT NULL,
    player_id BIGINT NOT NULL,
    player_name TEXT NOT NULL,
    minutes_text VARCHAR(20),
    minutes_seconds INTEGER,
    points INTEGER,
    personal_fouls INTEGER,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (game_id, player_id)
);

CREATE INDEX IF NOT EXISTS idx_pbp_game_period_action
    ON raw.play_by_play (game_id, period, action_number);

CREATE INDEX IF NOT EXISTS idx_rotations_game_player
    ON raw.player_rotations (game_id, player_id);

COMMIT;