-- =========================================================
-- PHASE 3: Pattern Modeling (Analytics Layer)
-- Anthony Edwards rotation profile (regular season only)
-- =========================================================

CREATE OR REPLACE VIEW edwards_rotation_profile_v AS
WITH eligible_stints AS (
    SELECT
        s.game_id,
        s.quarter,
        s.sub_in_time,
        s.sub_out_time,
        s.stint_length_seconds,
        s.score_diff
    FROM player_stints s
    JOIN games g
      ON g.game_id = s.game_id
    LEFT JOIN excluded_games eg
      ON eg.game_id = s.game_id
    WHERE eg.game_id IS NULL
      AND (
            g.season ~ '^[2][0-9]{4}$'  -- regular season SEASON_ID style (e.g. 22025)
            OR g.season ~ '^[0-9]{4}-[0-9]{2}$'  -- fallback text season style (e.g. 2025-26)
      )
),
games_agg AS (
    SELECT
        game_id,
        SUM(stint_length_seconds) AS total_seconds
    FROM eligible_stints
    GROUP BY game_id
),
first_rest_by_quarter AS (
    SELECT
        game_id,
        quarter,
        GREATEST(
            0,
            CASE
                WHEN quarter <= 4 THEN (quarter * 720) - sub_out_time
                ELSE 300 - (sub_out_time - 2880 - ((quarter - 5) * 300))
            END
        ) AS first_rest_seconds_left
    FROM (
        SELECT
            game_id,
            quarter,
            sub_out_time,
            ROW_NUMBER() OVER (
                PARTITION BY game_id, quarter
                ORDER BY sub_in_time ASC
            ) AS rn
        FROM eligible_stints
        WHERE quarter BETWEEN 1 AND 4
    ) q
    WHERE rn = 1
),
profile AS (
    SELECT
        'Anthony Edwards'::TEXT AS player_name,
        (SELECT COUNT(DISTINCT game_id) FROM eligible_stints) AS regular_season_games,

        (SELECT AVG(first_rest_seconds_left)::NUMERIC(10,2) FROM first_rest_by_quarter WHERE quarter = 1)
            AS avg_first_rest_q1_seconds_left,
        (SELECT AVG(first_rest_seconds_left)::NUMERIC(10,2) FROM first_rest_by_quarter WHERE quarter = 2)
            AS avg_first_rest_q2_seconds_left,
        (SELECT AVG(first_rest_seconds_left)::NUMERIC(10,2) FROM first_rest_by_quarter WHERE quarter = 3)
            AS avg_first_rest_q3_seconds_left,
        (SELECT AVG(first_rest_seconds_left)::NUMERIC(10,2) FROM first_rest_by_quarter WHERE quarter = 4)
            AS avg_first_rest_q4_seconds_left,

        (SELECT AVG(total_seconds)::NUMERIC(10,2) FROM games_agg) AS avg_total_seconds_per_game,
        (SELECT (AVG(total_seconds) / 60.0)::NUMERIC(10,2) FROM games_agg) AS avg_total_minutes_per_game,

        (SELECT AVG(stint_length_seconds)::NUMERIC(10,2) FROM eligible_stints WHERE quarter = 1)
            AS avg_stint_q1_seconds,
        (SELECT AVG(stint_length_seconds)::NUMERIC(10,2) FROM eligible_stints WHERE quarter = 2)
            AS avg_stint_q2_seconds,
        (SELECT AVG(stint_length_seconds)::NUMERIC(10,2) FROM eligible_stints WHERE quarter = 3)
            AS avg_stint_q3_seconds,
        (SELECT AVG(stint_length_seconds)::NUMERIC(10,2) FROM eligible_stints WHERE quarter = 4)
            AS avg_stint_q4_seconds,

        (SELECT AVG(stint_length_seconds)::NUMERIC(10,2)
           FROM eligible_stints
          WHERE ABS(score_diff) < 5) AS avg_stint_close_game_seconds
)
SELECT
    player_name,
    regular_season_games,
    avg_first_rest_q1_seconds_left,
    CONCAT(
        FLOOR(avg_first_rest_q1_seconds_left / 60)::INT,
        ':',
        LPAD((ROUND(avg_first_rest_q1_seconds_left)::INT % 60)::TEXT, 2, '0')
    ) AS avg_first_rest_q1_clock_left,
    avg_first_rest_q2_seconds_left,
    CONCAT(
        FLOOR(avg_first_rest_q2_seconds_left / 60)::INT,
        ':',
        LPAD((ROUND(avg_first_rest_q2_seconds_left)::INT % 60)::TEXT, 2, '0')
    ) AS avg_first_rest_q2_clock_left,
    avg_first_rest_q3_seconds_left,
    CONCAT(
        FLOOR(avg_first_rest_q3_seconds_left / 60)::INT,
        ':',
        LPAD((ROUND(avg_first_rest_q3_seconds_left)::INT % 60)::TEXT, 2, '0')
    ) AS avg_first_rest_q3_clock_left,
    avg_first_rest_q4_seconds_left,
    CONCAT(
        FLOOR(avg_first_rest_q4_seconds_left / 60)::INT,
        ':',
        LPAD((ROUND(avg_first_rest_q4_seconds_left)::INT % 60)::TEXT, 2, '0')
    ) AS avg_first_rest_q4_clock_left,
    avg_total_seconds_per_game,
    avg_total_minutes_per_game,
    avg_stint_q1_seconds,
    avg_stint_q2_seconds,
    avg_stint_q3_seconds,
    avg_stint_q4_seconds,
    avg_stint_close_game_seconds
FROM profile;

-- Example checkpoint query:
-- SELECT
--   'Based on all his regular season games (' || regular_season_games || '), '
--   || player_name || '''s average first rest is '
--   || avg_first_rest_q1_clock_left || ' left in Q1.' AS checkpoint_sentence
-- FROM edwards_rotation_profile_v;
