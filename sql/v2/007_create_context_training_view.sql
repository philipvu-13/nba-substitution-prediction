BEGIN;

CREATE OR REPLACE VIEW analytics.substitution_training_context_v AS

WITH pbp_timeline AS (
    SELECT
        pbp.*,

        CASE
            WHEN period <= 4
                THEN (period - 1) * 720
            ELSE 2880 + (period - 5) * 300
        END

        +

        CASE
            WHEN period <= 4 THEN 720
            ELSE 300
        END

        -

        (
            SUBSTRING(clock FROM 'PT([0-9]+)M')::NUMERIC * 60
            + SUBSTRING(clock FROM 'M([0-9.]+)S')::NUMERIC
        ) AS event_game_second

    FROM raw.play_by_play pbp
)

SELECT
    training.*,

    COALESCE(
        CASE
            WHEN games.home_team_abbrev = 'MIN'
                THEN latest_score.score_home
                     - latest_score.score_away
            ELSE latest_score.score_away
                 - latest_score.score_home
        END,
        0
    ) AS score_diff

FROM analytics.substitution_training_examples_v training

JOIN core.games games
    ON games.game_id = training.game_id

LEFT JOIN LATERAL (
    SELECT
        timeline.score_home,
        timeline.score_away
    FROM pbp_timeline timeline
    WHERE timeline.game_id = training.game_id
      AND timeline.event_game_second
            <= training.snapshot_game_second
      AND timeline.score_home IS NOT NULL
      AND timeline.score_away IS NOT NULL
    ORDER BY
        timeline.event_game_second DESC,
        timeline.action_number DESC,
        timeline.action_id DESC
    LIMIT 1
) latest_score ON TRUE;

COMMIT;