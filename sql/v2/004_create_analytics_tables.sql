BEGIN;

CREATE TABLE IF NOT EXISTS analytics.player_stints (
    stint_id BIGSERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL
        REFERENCES core.games(game_id),
    team_id BIGINT NOT NULL,
    player_id BIGINT NOT NULL
        REFERENCES core.players(player_id),
    stint_number INTEGER NOT NULL,
    sub_in_game_second INTEGER NOT NULL,
    sub_out_game_second INTEGER NOT NULL,
    stint_length_seconds INTEGER GENERATED ALWAYS AS
        (sub_out_game_second - sub_in_game_second) STORED,
    start_period INTEGER NOT NULL,
    end_period INTEGER NOT NULL,
    score_diff_start INTEGER,
    score_diff_end INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (game_id, player_id, stint_number),
    CHECK (sub_out_game_second > sub_in_game_second),
    CHECK (start_period >= 1),
    CHECK (end_period >= start_period)
);

CREATE TABLE IF NOT EXISTS analytics.player_stint_segments (
    segment_id BIGSERIAL PRIMARY KEY,
    stint_id BIGINT NOT NULL
        REFERENCES analytics.player_stints(stint_id)
        ON DELETE CASCADE,
    period INTEGER NOT NULL,
    segment_start_game_second INTEGER NOT NULL,
    segment_end_game_second INTEGER NOT NULL,
    segment_length_seconds INTEGER GENERATED ALWAYS AS
        (segment_end_game_second - segment_start_game_second) STORED,

    UNIQUE (stint_id, period),
    CHECK (segment_end_game_second > segment_start_game_second),
    CHECK (period >= 1)
);

CREATE INDEX IF NOT EXISTS idx_stints_player_game
    ON analytics.player_stints (player_id, game_id);

CREATE INDEX IF NOT EXISTS idx_stint_segments_period
    ON analytics.player_stint_segments (period);

COMMIT;