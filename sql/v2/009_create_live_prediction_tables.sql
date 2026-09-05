BEGIN;

CREATE TABLE IF NOT EXISTS analytics.live_predictions (
    prediction_id BIGSERIAL PRIMARY KEY,
    game_id VARCHAR(20) NOT NULL
        REFERENCES core.games(game_id),
    period INTEGER NOT NULL
        CHECK (period >= 1),
    snapshot_game_millisecond INTEGER NOT NULL
        CHECK (snapshot_game_millisecond >= 0),
    seconds_remaining NUMERIC(6, 1) NOT NULL,
    wolves_score INTEGER NOT NULL,
    opponent_score INTEGER NOT NULL,
    score_diff INTEGER NOT NULL,
    predicted_player_id BIGINT NOT NULL
        REFERENCES core.players(player_id),
    predicted_probability DOUBLE PRECISION NOT NULL
        CHECK (
            predicted_probability >= 0
            AND predicted_probability <= 1
        ),
    alert_threshold DOUBLE PRECISION NOT NULL
        CHECK (
            alert_threshold >= 0
            AND alert_threshold <= 1
        ),
    should_alert BOOLEAN NOT NULL,
    alert_status TEXT NOT NULL,
    model_version TEXT NOT NULL,
    horizon_seconds INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT live_predictions_snapshot_unique
        UNIQUE (
            game_id,
            snapshot_game_millisecond
        )
);

CREATE TABLE IF NOT EXISTS analytics.live_prediction_players (
    prediction_id BIGINT NOT NULL
        REFERENCES analytics.live_predictions(prediction_id)
        ON DELETE CASCADE,
    player_id BIGINT NOT NULL
        REFERENCES core.players(player_id),
    prediction_rank INTEGER NOT NULL
        CHECK (
            prediction_rank >= 1
            AND prediction_rank <= 5
        ),
    probability DOUBLE PRECISION NOT NULL
        CHECK (
            probability >= 0
            AND probability <= 1
        ),
    current_stint_seconds INTEGER NOT NULL
        CHECK (current_stint_seconds >= 0),

    PRIMARY KEY (
        prediction_id,
        player_id
    ),

    CONSTRAINT live_prediction_rank_unique
        UNIQUE (
            prediction_id,
            prediction_rank
        )
);

CREATE INDEX IF NOT EXISTS idx_live_predictions_game_time
    ON analytics.live_predictions (
        game_id,
        snapshot_game_millisecond
    );

CREATE INDEX IF NOT EXISTS idx_live_predictions_alerts
    ON analytics.live_predictions (
        game_id,
        should_alert,
        alert_status
    );

CREATE INDEX IF NOT EXISTS idx_live_prediction_players_player
    ON analytics.live_prediction_players (
        player_id,
        prediction_id
    );

COMMIT;