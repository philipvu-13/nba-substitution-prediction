BEGIN;

CREATE OR REPLACE VIEW analytics.substitution_training_examples_v AS

WITH segment_context AS (
    SELECT
        s.game_id,
        s.team_id,
        s.player_id,
        s.stint_id,
        s.stint_number,
        s.sub_in_game_second,
        s.sub_out_game_second,
        seg.period,
        seg.segment_start_game_second,
        seg.segment_end_game_second,

        CASE
            WHEN seg.period <= 4
                THEN (seg.period - 1) * 720
            ELSE 2880 + (seg.period - 5) * 300
        END AS period_start_second,

        CASE
            WHEN seg.period <= 4 THEN 720
            ELSE 300
        END AS period_length_seconds

    FROM analytics.player_stints s
    JOIN analytics.player_stint_segments seg
        ON seg.stint_id = s.stint_id

    WHERE s.team_id = 1610612750
),

snapshots AS (
    SELECT
        context.*,
        snapshot_second::INTEGER AS snapshot_game_second

    FROM segment_context context

    CROSS JOIN LATERAL generate_series(
        ((context.segment_start_game_second + 29) / 30) * 30,
        context.segment_end_game_second - 1,
        30
    ) AS snapshot_second
)

SELECT
    snapshots.game_id,
    games.game_date,
    snapshots.player_id,
    players.full_name AS player_name,

    snapshots.snapshot_game_second,
    snapshots.period,

    snapshots.period_start_second
        + snapshots.period_length_seconds
        - snapshots.snapshot_game_second
        AS seconds_remaining,

    snapshots.snapshot_game_second
        - snapshots.sub_in_game_second
        AS current_stint_seconds,

    snapshots.stint_number,

    snapshots.segment_start_game_second
        = snapshots.period_start_second
        AS started_period,

    COALESCE(boxscores.is_starter, FALSE) AS started_game,

    games.home_team_abbrev = 'MIN' AS is_home,

    CASE
        WHEN games.home_team_abbrev = 'MIN'
            THEN games.away_team_abbrev
        ELSE games.home_team_abbrev
    END AS opponent,

    snapshots.sub_out_game_second
        <= snapshots.snapshot_game_second + 120
        AND snapshots.sub_out_game_second
            < snapshots.period_start_second
              + snapshots.period_length_seconds
        AS exits_within_120_seconds

FROM snapshots

JOIN core.games games
    ON games.game_id = snapshots.game_id

JOIN core.players players
    ON players.player_id = snapshots.player_id

LEFT JOIN raw.player_boxscores boxscores
    ON boxscores.game_id = snapshots.game_id
   AND boxscores.player_id = snapshots.player_id

WHERE snapshots.period_start_second
        + snapshots.period_length_seconds
        - snapshots.snapshot_game_second > 120;

COMMIT;