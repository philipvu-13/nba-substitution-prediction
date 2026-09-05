BEGIN;

ALTER TABLE raw.player_boxscores
    ADD COLUMN IF NOT EXISTS position VARCHAR(10);

ALTER TABLE raw.player_boxscores
    ADD COLUMN IF NOT EXISTS is_starter BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE raw.player_boxscores
    ADD COLUMN IF NOT EXISTS comment TEXT;

COMMIT;