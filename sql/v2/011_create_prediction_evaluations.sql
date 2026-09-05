BEGIN;

CREATE TABLE IF NOT EXISTS analytics.live_prediction_evaluations (
    prediction_id BIGINT PRIMARY KEY,

    substitution_occurred BOOLEAN NOT NULL,
    predicted_player_exited BOOLEAN NOT NULL,

    actual_outgoing_player_ids BIGINT[]
        NOT NULL
        DEFAULT ARRAY[]::BIGINT[],

    evaluated_at TIMESTAMPTZ
        NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT live_prediction_evaluations_prediction_fkey
        FOREIGN KEY (prediction_id)
        REFERENCES analytics.live_predictions (prediction_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_live_evaluations_substitution
    ON analytics.live_prediction_evaluations (
        substitution_occurred,
        predicted_player_exited
    );

COMMIT;