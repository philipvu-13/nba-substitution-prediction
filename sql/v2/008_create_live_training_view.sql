BEGIN;

CREATE OR REPLACE VIEW analytics.substitution_live_training_v AS

SELECT
    context.*,

    stints.sub_out_game_second
        <= context.snapshot_game_second + 60

    AND stints.sub_out_game_second
        < context.snapshot_game_second
          + context.seconds_remaining

        AS exits_within_60_seconds

FROM analytics.substitution_training_context_v context

JOIN analytics.player_stints stints
    ON stints.game_id = context.game_id
   AND stints.player_id = context.player_id
   AND context.snapshot_game_second
        >= stints.sub_in_game_second
   AND context.snapshot_game_second
        < stints.sub_out_game_second

WHERE stints.team_id = 1610612750;

COMMIT;