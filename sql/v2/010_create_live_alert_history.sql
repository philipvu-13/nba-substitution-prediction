BEGIN;

CREATE TABLE IF NOT EXISTS analytics.live_alert_history (
    alert_id BIGSERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL,
    player_id BIGINT NOT NULL,
    game_second DOUBLE PRECISION NOT NULL,
    alert_status VARCHAR(30) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT live_alert_history_game_fkey
        FOREIGN KEY (game_id)
        REFERENCES core.games (game_id),

    CONSTRAINT live_alert_history_player_fkey
        FOREIGN KEY (player_id)
        REFERENCES core.players (player_id),

    CONSTRAINT live_alert_history_unique
        UNIQUE (game_id, player_id, game_second)
);

CREATE INDEX IF NOT EXISTS idx_live_alert_history_game_time
    ON analytics.live_alert_history (
        game_id,
        game_second DESC
    );

CREATE INDEX IF NOT EXISTS idx_live_alert_history_game_player_time
    ON analytics.live_alert_history (
        game_id,
        player_id,
        game_second DESC
    );

COMMIT;