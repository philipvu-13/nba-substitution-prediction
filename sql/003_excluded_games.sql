-- Games to exclude from downstream modeling/analytics because of known data-quality issues.
CREATE TABLE IF NOT EXISTS excluded_games (
    game_id VARCHAR(20) PRIMARY KEY,
    reason TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Example:
-- INSERT INTO excluded_games (game_id, reason)
-- VALUES ('0022500498', 'Incomplete rotations: only one stint row')
-- ON CONFLICT (game_id) DO UPDATE SET reason = EXCLUDED.reason;
