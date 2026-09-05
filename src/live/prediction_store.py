from src.config import get_connection
from src.live.read_state import event_game_second


MODEL_VERSION = "substitution_live_v1"


def save_prediction(
    state,
    prediction,
    alert_status,
):
    top_prediction = prediction["top_prediction"]

    game_second = event_game_second(
        state["period"],
        state["seconds_remaining"],
    )

    snapshot_game_millisecond = round(
        game_second * 1000
    )

    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO analytics.live_predictions (
                    game_id,
                    period,
                    snapshot_game_millisecond,
                    seconds_remaining,
                    wolves_score,
                    opponent_score,
                    score_diff,
                    predicted_player_id,
                    predicted_probability,
                    alert_threshold,
                    should_alert,
                    alert_status,
                    model_version,
                    horizon_seconds
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (
                    game_id,
                    snapshot_game_millisecond
                )
                DO UPDATE SET
                    period = EXCLUDED.period,
                    seconds_remaining =
                        EXCLUDED.seconds_remaining,
                    wolves_score = EXCLUDED.wolves_score,
                    opponent_score =
                        EXCLUDED.opponent_score,
                    score_diff = EXCLUDED.score_diff,
                    predicted_player_id =
                        EXCLUDED.predicted_player_id,
                    predicted_probability =
                        EXCLUDED.predicted_probability,
                    alert_threshold =
                        EXCLUDED.alert_threshold,
                    should_alert =
                        EXCLUDED.should_alert,
                    alert_status =
                        EXCLUDED.alert_status,
                    model_version =
                        EXCLUDED.model_version,
                    horizon_seconds =
                        EXCLUDED.horizon_seconds,
                    created_at = CURRENT_TIMESTAMP
                RETURNING prediction_id
                """,
                (
                    state["game_id"],
                    state["period"],
                    snapshot_game_millisecond,
                    state["seconds_remaining"],
                    state["wolves_score"],
                    state["opponent_score"],
                    state["score_diff"],
                    int(top_prediction["player_id"]),
                    float(top_prediction["probability"]),
                    float(prediction["threshold"]),
                    bool(prediction["should_alert"]),
                    alert_status,
                    MODEL_VERSION,
                    int(prediction["horizon_seconds"]),
                ),
            )

            prediction_id = cursor.fetchone()[0]

            cursor.execute(
                """
                DELETE FROM analytics.live_prediction_players
                WHERE prediction_id = %s
                """,
                (prediction_id,),
            )

            player_rows = []

            for row in prediction["rankings"].itertuples():
                player_rows.append(
                    (
                        prediction_id,
                        int(row.player_id),
                        int(row.rank),
                        float(row.probability),
                        int(row.current_stint_seconds),
                    )
                )

            cursor.executemany(
                """
                INSERT INTO analytics.live_prediction_players (
                    prediction_id,
                    player_id,
                    prediction_rank,
                    probability,
                    current_stint_seconds
                )
                VALUES (%s, %s, %s, %s, %s)
                """,
                player_rows,
            )

        connection.commit()
        return prediction_id

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()