from pathlib import Path

from src.config import get_connection
from src.notify.send_discord_alert import post_to_discord


def safe_percentage(numerator, denominator):
    if denominator == 0:
        return 0.0

    return numerator / denominator


def load_postgame_results(game_id):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    games.game_date,
                    games.matchup,

                    COUNT(
                        predictions.prediction_id
                    ) AS stored_predictions,

                    COUNT(
                        predictions.prediction_id
                    ) FILTER (
                        WHERE predictions.should_alert
                    ) AS threshold_predictions,

                    COUNT(
                        predictions.prediction_id
                    ) FILTER (
                        WHERE predictions.alert_status
                            IN (
                                'discord_sent',
                                'dry_run_alert'
                            )
                    ) AS delivered_alerts,

                    COUNT(
                        predictions.prediction_id
                    ) FILTER (
                        WHERE predictions.alert_status
                            IN (
                                'discord_sent',
                                'dry_run_alert'
                            )
                          AND evaluations.substitution_occurred
                    ) AS correct_substitution_alerts,

                    COUNT(
                        predictions.prediction_id
                    ) FILTER (
                        WHERE predictions.alert_status
                            IN (
                                'discord_sent',
                                'dry_run_alert'
                            )
                          AND evaluations.predicted_player_exited
                    ) AS correct_player_alerts

                FROM core.games games

                JOIN analytics.live_predictions predictions
                    ON predictions.game_id = games.game_id

                JOIN analytics.live_prediction_evaluations evaluations
                    ON evaluations.prediction_id
                        = predictions.prediction_id

                WHERE games.game_id = %s

                GROUP BY
                    games.game_date,
                    games.matchup;
                """,
                (game_id,),
            )

            row = cursor.fetchone()

            if row is None:
                raise RuntimeError(
                    "No evaluated predictions were found "
                    f"for game {game_id}"
                )

            return {
                "game_date": row[0],
                "matchup": row[1],
                "stored_predictions": int(row[2]),
                "threshold_predictions": int(row[3]),
                "delivered_alerts": int(row[4]),
                "correct_substitution_alerts": int(
                    row[5]
                ),
                "correct_player_alerts": int(row[6]),
            }

    finally:
        connection.close()


def send_discord_postgame_summary(
    game_id,
    image_path=None,
    suggested_caption=None,
):
    results = load_postgame_results(game_id)

    delivered_alerts = results["delivered_alerts"]

    substitution_precision = safe_percentage(
        results["correct_substitution_alerts"],
        delivered_alerts,
    )

    player_precision = safe_percentage(
        results["correct_player_alerts"],
        delivered_alerts,
    )

    if delivered_alerts == 0:
        description = (
            "The game finished, but no alerts "
            "were delivered."
        )
        color = 9807270
    else:
        description = (
            "The live substitution predictions "
            "have been graded."
        )
        color = 5763719

    fields = [
        {
            "name": "Game",
            "value": results["matchup"],
            "inline": True,
        },
        {
            "name": "Date",
            "value": str(results["game_date"]),
            "inline": True,
        },
        {
            "name": "Alerts Sent",
            "value": str(delivered_alerts),
            "inline": True,
        },
        {
            "name": "Substitution Accuracy",
            "value": (
                f"{results[
                    'correct_substitution_alerts'
                ]}/{delivered_alerts} "
                f"({substitution_precision:.1%})"
            ),
            "inline": True,
        },
        {
            "name": "Exact Player Accuracy",
            "value": (
                f"{results[
                    'correct_player_alerts'
                ]}/{delivered_alerts} "
                f"({player_precision:.1%})"
            ),
            "inline": True,
        },
        {
            "name": "Stored Predictions",
            "value": str(
                results["stored_predictions"]
            ),
            "inline": True,
        },
    ]

    if suggested_caption:
        fields.append({
            "name": "Suggested X Post",
            "value": suggested_caption,
            "inline": False,
        })

    embed = {
        "title": "Timberwolves Postgame Report",
        "description": description,
        "color": color,
        "fields": fields,
        "footer": {
            "text": (
                f"Game ID {game_id} | "
                "NBA substitution prediction V2"
            )
        },
    }

    if image_path is not None:
        image_path = Path(image_path)

        embed["image"] = {
            "url": (
                f"attachment://{image_path.name}"
            )
        }

    payload = {
        "embeds": [embed],
    }

    post_to_discord(
        payload,
        image_path=image_path,
    )

    print("Discord postgame summary sent.")