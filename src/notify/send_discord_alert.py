import json
import os
from pathlib import Path

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


def post_to_discord(
    payload,
    image_path=None,
):
    if image_path is None:
        response = requests.post(
            get_webhook_url(),
            json=payload,
            timeout=20,
        )

        response.raise_for_status()
        return

    image_path = Path(image_path)

    if not image_path.exists():
        raise FileNotFoundError(
            f"Discord image was not found: {image_path}"
        )

    with image_path.open("rb") as image_file:
        response = requests.post(
            get_webhook_url(),
            data={
                "payload_json": json.dumps(payload),
            },
            files={
                "files[0]": (
                    image_path.name,
                    image_file,
                    "image/png",
                ),
            },
            timeout=30,
        )

    response.raise_for_status()


def send_discord_alert(
    state,
    prediction,
    image_path=None,
):
    top_prediction = prediction["top_prediction"]

    player_name = top_prediction["player_name"]
    probability = float(
        top_prediction["probability"]
    )

    horizon_seconds = prediction[
        "horizon_seconds"
    ]

    embed = {
        "title": "Timberwolves Substitution Alert",
        "description": (
            f"**{player_name}** is the most likely "
            f"player to exit within "
            f"{horizon_seconds} seconds."
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
            {
                "name": "Current Stint",
                "value": format_clock(
                    top_prediction[
                        "current_stint_seconds"
                    ]
                ),
                "inline": True,
            },
        ],
        "footer": {
            "text": (
                "Experimental NBA substitution model"
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