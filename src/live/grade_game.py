import argparse

from src.config import get_connection


DELIVERED_ALERT_STATUSES = {
    "discord_sent",
    "dry_run_alert",
}


def load_game_information(cursor, game_id):
    cursor.execute(
        """
        SELECT
            game_date,
            matchup
        FROM core.games
        WHERE game_id = %s;
        """,
        (game_id,),
    )

    row = cursor.fetchone()

    if row is None:
        raise RuntimeError(
            f"Game {game_id} was not found in core.games"
        )

    return {
        "game_date": row[0],
        "matchup": row[1],
    }


def verify_game_data(cursor, game_id):
    cursor.execute(
        """
        SELECT COUNT(*)
        FROM analytics.live_predictions
        WHERE game_id = %s;
        """,
        (game_id,),
    )

    prediction_count = cursor.fetchone()[0]

    if prediction_count == 0:
        raise RuntimeError(
            f"No stored live predictions found for {game_id}"
        )

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM analytics.player_stints
        WHERE game_id = %s;
        """,
        (game_id,),
    )

    stint_count = cursor.fetchone()[0]

    if stint_count == 0:
        raise RuntimeError(
            "No completed player stints were found. "
            f"Run: python -m src.pipeline.run_game {game_id}"
        )


def load_prediction_outcomes(cursor, game_id):
    cursor.execute(
        """
        SELECT
            predictions.prediction_id,
            predictions.period,
            predictions.seconds_remaining,
            predictions.predicted_player_id,
            players.full_name,
            predictions.predicted_probability,
            predictions.should_alert,
            predictions.alert_status,

            COALESCE(
                ARRAY_AGG(
                    DISTINCT stints.player_id
                ) FILTER (
                    WHERE
                        stints.sub_out_game_second
                            <= (
                                predictions.snapshot_game_millisecond
                                / 1000.0
                            )
                            + predictions.horizon_seconds

                    AND stints.sub_out_game_second
                            < (
                                predictions.snapshot_game_millisecond
                                / 1000.0
                            )
                            + predictions.seconds_remaining
                ),
                ARRAY[]::BIGINT[]
            ) AS actual_outgoing_player_ids

        FROM analytics.live_predictions predictions

        JOIN analytics.live_prediction_players
            prediction_players
            ON prediction_players.prediction_id
                = predictions.prediction_id

        JOIN core.players players
            ON players.player_id
                = predictions.predicted_player_id

        LEFT JOIN analytics.player_stints stints
            ON stints.game_id = predictions.game_id
           AND stints.player_id
                = prediction_players.player_id
           AND (
                predictions.snapshot_game_millisecond
                / 1000.0
           ) >= stints.sub_in_game_second
           AND (
                predictions.snapshot_game_millisecond
                / 1000.0
           ) < stints.sub_out_game_second

        WHERE predictions.game_id = %s

        GROUP BY
            predictions.prediction_id,
            predictions.period,
            predictions.seconds_remaining,
            predictions.predicted_player_id,
            players.full_name,
            predictions.predicted_probability,
            predictions.should_alert,
            predictions.alert_status

        ORDER BY
            predictions.snapshot_game_millisecond;
        """,
        (game_id,),
    )

    outcomes = []

    for row in cursor.fetchall():
        outgoing_player_ids = [
            int(player_id)
            for player_id in row[8]
        ]

        predicted_player_id = int(row[3])

        outcomes.append({
            "prediction_id": int(row[0]),
            "period": int(row[1]),
            "seconds_remaining": float(row[2]),
            "predicted_player_id": predicted_player_id,
            "predicted_player_name": row[4],
            "predicted_probability": float(row[5]),
            "should_alert": bool(row[6]),
            "alert_status": row[7],
            "actual_outgoing_player_ids": (
                outgoing_player_ids
            ),
            "substitution_occurred": bool(
                outgoing_player_ids
            ),
            "predicted_player_exited": (
                predicted_player_id
                in outgoing_player_ids
            ),
        })

    return outcomes


def save_evaluations(cursor, outcomes):
    rows = [
        (
            outcome["prediction_id"],
            outcome["substitution_occurred"],
            outcome["predicted_player_exited"],
            outcome["actual_outgoing_player_ids"],
        )
        for outcome in outcomes
    ]

    cursor.executemany(
        """
        INSERT INTO analytics.live_prediction_evaluations (
            prediction_id,
            substitution_occurred,
            predicted_player_exited,
            actual_outgoing_player_ids
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (prediction_id)
        DO UPDATE SET
            substitution_occurred
                = EXCLUDED.substitution_occurred,
            predicted_player_exited
                = EXCLUDED.predicted_player_exited,
            actual_outgoing_player_ids
                = EXCLUDED.actual_outgoing_player_ids,
            evaluated_at
                = CURRENT_TIMESTAMP;
        """,
        rows,
    )


def safe_percentage(numerator, denominator):
    if denominator == 0:
        return 0.0

    return numerator / denominator


def format_clock(seconds_remaining):
    total_seconds = int(round(seconds_remaining))

    minutes = total_seconds // 60
    seconds = total_seconds % 60

    return f"{minutes}:{seconds:02d}"


def print_results(game_id, game_information, outcomes):
    threshold_predictions = [
        outcome
        for outcome in outcomes
        if outcome["should_alert"]
    ]

    delivered_alerts = [
        outcome
        for outcome in outcomes
        if outcome["alert_status"]
        in DELIVERED_ALERT_STATUSES
    ]

    correct_substitution_alerts = sum(
        outcome["substitution_occurred"]
        for outcome in delivered_alerts
    )

    correct_player_alerts = sum(
        outcome["predicted_player_exited"]
        for outcome in delivered_alerts
    )

    print(game_information["game_date"])
    print(game_information["matchup"])
    print(f"Game ID: {game_id}\n")

    print(f"Stored predictions: {len(outcomes)}")
    print(
        f"Snapshots above threshold: "
        f"{len(threshold_predictions)}"
    )
    print(
        f"Delivered alerts: "
        f"{len(delivered_alerts)}"
    )

    print(
        "Alert substitution precision: "
        f"{safe_percentage(
            correct_substitution_alerts,
            len(delivered_alerts),
        ):.1%}"
    )

    print(
        "Alert player precision: "
        f"{safe_percentage(
            correct_player_alerts,
            len(delivered_alerts),
        ):.1%}"
    )

    if not delivered_alerts:
        return

    print("\nDelivered alert results")

    print(
        f"{'Q':3}"
        f"{'Clock':8}"
        f"{'Player':25}"
        f"{'Probability':14}"
        f"{'Sub happened':14}"
        f"{'Player exited'}"
    )

    for outcome in delivered_alerts:
        print(
            f"{outcome['period']:<3}"
            f"{format_clock(
                outcome['seconds_remaining']
            ):<8}"
            f"{outcome['predicted_player_name']:<25}"
            f"{outcome['predicted_probability']:<14.1%}"
            f"{str(
                outcome['substitution_occurred']
            ):<14}"
            f"{outcome['predicted_player_exited']}"
        )


def grade_game(game_id):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            game_information = load_game_information(
                cursor,
                game_id,
            )

            verify_game_data(
                cursor,
                game_id,
            )

            outcomes = load_prediction_outcomes(
                cursor,
                game_id,
            )

            save_evaluations(
                cursor,
                outcomes,
            )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

    print_results(
        game_id,
        game_information,
        outcomes,
    )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Grade stored live substitution predictions"
        )
    )

    parser.add_argument("game_id")

    args = parser.parse_args()

    grade_game(args.game_id)


if __name__ == "__main__":
    main()