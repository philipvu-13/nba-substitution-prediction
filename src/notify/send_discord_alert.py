import os

import requests
from dotenv import load_dotenv

from src.live.read_state import format_clock


load_dotenv()


def get_webhook_url():
    webhook_url = os.getenv("DISCORD_WEBHOOK_URL")

    if not webhook_url:
        raise RuntimeError(
            "DISCORD_WEBHOOK_URL is missing from .env"
        )

    return webhook_url


def post_to_discord(payload):
    response = requests.post(
        get_webhook_url(),
        json=payload,
        timeout=20,
    )

    response.raise_for_status()


def send_discord_alert(state, prediction):
    top_prediction = prediction["top_prediction"]

    player_name = top_prediction["player_name"]
    probability = float(top_prediction["probability"])
    horizon_seconds = prediction["horizon_seconds"]

    payload = {
        "embeds": [
            {
                "title": "Timberwolves Substitution Alert",
                "description": (
                    f"**{player_name}** is the most likely "
                    f"player to exit within {horizon_seconds} seconds."
                ),
                "color": 2369838,
                "fields": [
                    {
                        "name": "Probability",
                        "value": f"{probability:.1%}",
                        "inline": True,
                    },
                    {
                        "name": "Game Clock",
                        "value": (
                            f"Q{state['period']} "
                            f"{format_clock(
                                state['seconds_remaining']
                            )}"
                        ),
                        "inline": True,
                    },
                    {
                        "name": "Score",
                        "value": (
                            f"MIN {state['wolves_score']} "
                            f"{state['opponent']} "
                            f"{state['opponent_score']}"
                        ),
                        "inline": True,
                    },
                ],
            }
        ]
    }

    post_to_discord(payload)


def send_test_notification():
    payload = {
        "embeds": [
            {
                "title": "Discord Connection Successful",
                "description": (
                    "The NBA substitution prediction project "
                    "can send live alerts."
                ),
                "color": 5763719,
            }
        ]
    }

    post_to_discord(payload)
    print("Discord test notification sent.")


if __name__ == "__main__":
    send_test_notification()