import argparse
import time

import joblib

from src.live.alert_memory import AlertMemory
from src.live.predict_live import (
    MODEL_PATH,
    predict_state,
    print_prediction,
)
from src.live.read_state import (
    build_state,
    event_game_second,
    parse_clock,
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


def process_alert(state, prediction, alert_memory):
    if not prediction["should_alert"]:
        print("\nAlert status: Threshold not reached")
        return False

    top_prediction = prediction["top_prediction"]

    player_id = int(top_prediction["player_id"])
    player_name = top_prediction["player_name"]
    probability = float(top_prediction["probability"])

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
        return False

    alert_memory.record_alert(
        player_id,
        game_second,
    )

    print("\nALERT READY")
    print(
        f"Substitution likely within "
        f"{prediction['horizon_seconds']} seconds"
    )
    print(f"Most likely player: {player_name}")
    print(f"Probability: {probability:.1%}")

    return True


def run_once(
    game_id,
    model_artifact,
    alert_memory,
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
            f"{state['seconds_remaining']:.1f} seconds remaining."
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

    process_alert(
        state,
        prediction,
        alert_memory,
    )

    return state


def poll_game(game_id, interval_seconds):
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found at {MODEL_PATH}"
        )

    model_artifact = joblib.load(MODEL_PATH)
    alert_memory = AlertMemory()
    previous_state_key = None

    print(f"Watching game {game_id}")
    print(
        f"Checking for new events every "
        f"{interval_seconds} seconds"
    )
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            try:
                state = build_state(game_id)
                current_state_key = build_state_key(state)

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

                        process_alert(
                            state,
                            prediction,
                            alert_memory,
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

                    previous_state_key = current_state_key

                if game_is_finished(state):
                    print("\nGame finished. Polling stopped.")
                    return

            except Exception as error:
                print(
                    f"Could not read the current game state: "
                    f"{error}"
                )
                print("Trying again on the next check.")

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        print("\nPolling stopped by user.")


def main():
    parser = argparse.ArgumentParser(
        description="Continuously watch a game for substitutions"
    )

    parser.add_argument("game_id")

    parser.add_argument(
        "--interval",
        type=int,
        default=30,
        help="Seconds between live feed checks",
    )

    parser.add_argument("--period", type=int)
    parser.add_argument("--clock")

    args = parser.parse_args()

    if args.interval <= 0:
        parser.error("--interval must be greater than zero")

    if (args.period is None) != (args.clock is None):
        parser.error(
            "--period and --clock must be supplied together"
        )

    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found at {MODEL_PATH}"
        )

    model_artifact = joblib.load(MODEL_PATH)
    alert_memory = AlertMemory()

    if args.period is not None:
        run_once(
            args.game_id,
            model_artifact,
            alert_memory,
            period=args.period,
            clock_seconds=parse_clock(args.clock),
        )
        return

    poll_game(
        args.game_id,
        args.interval,
    )


if __name__ == "__main__":
    main()