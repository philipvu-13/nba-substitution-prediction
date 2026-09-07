import argparse
import time

import joblib

from src.live.alert_memory import AlertMemory
from src.live.predict_live import (
    MODEL_PATH,
    predict_state,
    print_prediction,
)
from src.live.prediction_store import save_prediction
from src.live.read_state import (
    build_state,
    event_game_second,
    parse_clock,
)
from src.notify.send_discord_alert import (
    send_discord_alert,
)
from src.visualization.live_probability import (
    build_output_path,
    create_live_probability_graphic,
)


MINIMUM_SECONDS_REMAINING = 120


def build_state_key(state):
    player_ids = tuple(sorted(
        player["player_id"]
        for player in state["players"]
    ))

    return (
        state["period"],
        state["seconds_remaining"],
        state["wolves_score"],
        state["opponent_score"],
        player_ids,
    )


def game_is_finished(state):
    return (
        state["period"] >= 4
        and state["seconds_remaining"] <= 0
        and state["wolves_score"] != state["opponent_score"]
    )


def can_make_prediction(state):
    return (
        state["seconds_remaining"]
        > MINIMUM_SECONDS_REMAINING
    )


def create_alert_graphic(
    game_id,
    state,
    prediction,
):
    output_path = build_output_path(
        game_id,
        state,
    )

    create_live_probability_graphic(
        state,
        prediction,
        output_path,
    )

    return output_path


def process_alert(
    game_id,
    state,
    prediction,
    alert_memory,
    discord_enabled,
):
    if not prediction["should_alert"]:
        print("\nAlert status: Threshold not reached")
        return "below_threshold"

    top_prediction = prediction["top_prediction"]

    player_id = int(top_prediction["player_id"])
    player_name = top_prediction["player_name"]
    probability = float(
        top_prediction["probability"]
    )

    game_second = event_game_second(
        state["period"],
        state["seconds_remaining"],
    )

    alert_allowed, reason = alert_memory.can_alert(
        player_id,
        game_second,
    )

    if not alert_allowed:
        print(f"\nAlert suppressed: {reason}")
        return "cooldown_suppressed"

    print("\nALERT READY")

    print(
        f"Substitution likely within "
        f"{prediction['horizon_seconds']} seconds"
    )

    print(f"Most likely player: {player_name}")
    print(f"Probability: {probability:.1%}")

    if discord_enabled:
        image_path = None

        try:
            image_path = create_alert_graphic(
                game_id,
                state,
                prediction,
            )

            print(
                f"Created alert graphic: "
                f"{image_path}"
            )

        except Exception as error:
            print(
                f"Alert graphic creation failed: "
                f"{error}"
            )

            print(
                "Sending the Discord alert "
                "without an image."
            )

        try:
            send_discord_alert(
                state,
                prediction,
                image_path=image_path,
            )

        except Exception as error:
            print(f"Discord alert failed: {error}")
            return "discord_failed"

        print("Discord alert sent.")
        alert_status = "discord_sent"

    else:
        print("Discord delivery disabled.")
        alert_status = "dry_run_alert"

    alert_memory.record_alert(
        player_id,
        game_second,
        alert_status,
    )

    return alert_status


def store_prediction(
    state,
    prediction,
    alert_status,
):
    prediction_id = save_prediction(
        state,
        prediction,
        alert_status,
    )

    print(
        f"Stored prediction {prediction_id} "
        f"in PostgreSQL."
    )


def run_once(
    game_id,
    model_artifact,
    alert_memory,
    discord_enabled,
    store_enabled,
    period=None,
    clock_seconds=None,
):
    state = build_state(
        game_id,
        period=period,
        clock_seconds=clock_seconds,
    )

    if not can_make_prediction(state):
        print(
            f"Q{state['period']} has "
            f"{state['seconds_remaining']:.1f} "
            f"seconds remaining."
        )

        print(
            "Skipping prediction because the model was "
            "not trained on the final two minutes."
        )

        return state

    prediction = predict_state(
        state,
        model_artifact,
    )

    print_prediction(
        state,
        prediction,
    )

    alert_status = process_alert(
        game_id,
        state,
        prediction,
        alert_memory,
        discord_enabled,
    )

    if store_enabled:
        store_prediction(
            state,
            prediction,
            alert_status,
        )

    return state


def poll_game(
    game_id,
    interval_seconds,
    discord_enabled,
):
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found at {MODEL_PATH}"
        )

    model_artifact = joblib.load(MODEL_PATH)

    alert_memory = AlertMemory(
        game_id=game_id
    )

    previous_state_key = None

    print(f"Watching game {game_id}")

    print(
        f"Checking for new events every "
        f"{interval_seconds} seconds"
    )

    print(
        "Discord alerts: "
        + (
            "enabled"
            if discord_enabled
            else "disabled"
        )
    )

    print("PostgreSQL storage: enabled")
    print("Persistent alert cooldowns: enabled")

    print(
        "Discord alert graphics: "
        + (
            "enabled"
            if discord_enabled
            else "disabled"
        )
    )

    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            try:
                state = build_state(game_id)

                current_state_key = build_state_key(
                    state
                )

                if current_state_key != previous_state_key:
                    print("\n" + "=" * 60)

                    if can_make_prediction(state):
                        prediction = predict_state(
                            state,
                            model_artifact,
                        )

                        print_prediction(
                            state,
                            prediction,
                        )

                        alert_status = process_alert(
                            game_id,
                            state,
                            prediction,
                            alert_memory,
                            discord_enabled,
                        )

                        store_prediction(
                            state,
                            prediction,
                            alert_status,
                        )

                    else:
                        print(
                            f"Q{state['period']} has "
                            f"{state['seconds_remaining']:.1f} "
                            f"seconds remaining."
                        )

                        print(
                            "Waiting because the model was not "
                            "trained on the final two minutes."
                        )

                    previous_state_key = (
                        current_state_key
                    )

                if game_is_finished(state):
                    print(
                        "\nGame finished. "
                        "Polling stopped."
                    )

                    return True

            except Exception as error:
                print(
                    f"Could not process the current "
                    f"game state: {error}"
                )

                print(
                    "Trying again on the next check."
                )

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\nPolling stopped by user.")
        return False


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Continuously watch a game "
            "for substitutions"
        )
    )

    parser.add_argument("game_id")

    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Seconds between live feed checks",
    )

    parser.add_argument(
        "--discord",
        action="store_true",
        help="Send approved alerts to Discord",
    )

    parser.add_argument(
        "--store",
        action="store_true",
        help="Store a historical snapshot test",
    )

    parser.add_argument("--period", type=int)
    parser.add_argument("--clock")

    args = parser.parse_args()

    if args.interval <= 0:
        parser.error(
            "--interval must be greater than zero"
        )

    if (args.period is None) != (args.clock is None):
        parser.error(
            "--period and --clock must be supplied together"
        )

    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found at {MODEL_PATH}"
        )

    model_artifact = joblib.load(MODEL_PATH)

    if args.period is not None:
        alert_memory = AlertMemory()

        run_once(
            args.game_id,
            model_artifact,
            alert_memory,
            args.discord,
            args.store,
            period=args.period,
            clock_seconds=parse_clock(
                args.clock
            ),
        )

        return

    poll_game(
        args.game_id,
        args.interval,
        args.discord,
    )


if __name__ == "__main__":
    main()