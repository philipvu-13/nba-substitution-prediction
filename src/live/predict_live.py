import argparse
from pathlib import Path

import joblib
import pandas as pd

from src.live.read_state import (
    build_state,
    format_clock,
    parse_clock,
)


MODEL_PATH = Path("models/substitution_live_v1.joblib")


def build_feature_frame(state):
    rows = []

    for player in state["players"]:
        rows.append({
            "player_id": str(player["player_id"]),
            "player_name": player["player_name"],
            "period": state["period"],
            "seconds_remaining": state["seconds_remaining"],
            "current_stint_seconds": (
                player["current_stint_seconds"]
            ),
            "stint_number": player["stint_number"],
            "started_period": player["started_period"],
            "started_game": player["started_game"],
            "is_home": state["is_home"],
            "opponent": state["opponent"],
            "score_diff": state["score_diff"],
            "abs_score_diff": state["abs_score_diff"],
        })

    return pd.DataFrame(rows)


def predict_state(state, model_artifact):
    model = model_artifact["model"]
    features = model_artifact["features"]
    threshold = model_artifact["snapshot_threshold"]

    frame = build_feature_frame(state)

    missing_features = [
        feature
        for feature in features
        if feature not in frame.columns
    ]

    if missing_features:
        raise RuntimeError(
            "Missing model features: "
            + ", ".join(missing_features)
        )

    frame["probability"] = model.predict_proba(
        frame[features]
    )[:, 1]

    frame = frame.sort_values(
        "probability",
        ascending=False,
    ).reset_index(drop=True)

    frame["rank"] = frame.index + 1

    top_prediction = frame.iloc[0]
    should_alert = (
        top_prediction["probability"] >= threshold
    )

    return {
        "rankings": frame,
        "top_prediction": top_prediction,
        "should_alert": should_alert,
        "threshold": threshold,
        "horizon_seconds": model_artifact[
            "horizon_seconds"
        ],
    }


def print_prediction(state, prediction):
    top_prediction = prediction["top_prediction"]
    horizon_seconds = prediction["horizon_seconds"]
    threshold = prediction["threshold"]

    print(
        f"{state['game_date']} "
        f"{state['location']} {state['opponent']}"
    )

    print(
        f"Q{state['period']} with "
        f"{format_clock(state['seconds_remaining'])} remaining"
    )

    print(
        f"Score: MIN {state['wolves_score']} "
        f"{state['opponent']} {state['opponent_score']}"
    )

    print(f"Score difference: {state['score_diff']:+d}")

    alert_text = (
        "YES"
        if prediction["should_alert"]
        else "NO"
    )

    print(
        f"\nSubstitution likely within "
        f"{horizon_seconds} seconds: {alert_text}"
    )

    print(
        f"Most likely player: "
        f"{top_prediction['player_name']} "
        f"({top_prediction['probability']:.1%})"
    )

    print(f"Required threshold: {threshold:.0%}")

    print(
        f"\n{'Rank':6}"
        f"{'Player':24}"
        f"{'Probability':14}"
        f"{'Current stint'}"
    )

    for row in prediction["rankings"].itertuples():
        print(
            f"{row.rank:<6}"
            f"{row.player_name:24}"
            f"{row.probability:<14.1%}"
            f"{format_clock(row.current_stint_seconds)}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Predict a Timberwolves substitution"
    )

    parser.add_argument("game_id")
    parser.add_argument("--period", type=int)
    parser.add_argument("--clock")

    args = parser.parse_args()

    if (args.period is None) != (args.clock is None):
        parser.error(
            "--period and --clock must be supplied together"
        )

    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found at {MODEL_PATH}"
        )

    clock_seconds = (
        parse_clock(args.clock)
        if args.clock is not None
        else None
    )

    model_artifact = joblib.load(MODEL_PATH)

    state = build_state(
        args.game_id,
        period=args.period,
        clock_seconds=clock_seconds,
    )

    prediction = predict_state(
        state,
        model_artifact,
    )

    print_prediction(state, prediction)


if __name__ == "__main__":
    main()