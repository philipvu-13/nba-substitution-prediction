import argparse
import math
from pathlib import Path

import joblib
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter

from src.live.predict_live import (
    MODEL_PATH,
    predict_state,
)
from src.live.read_state import (
    build_state,
    format_clock,
    parse_clock,
)


BACKGROUND_COLOR = "#071A2B"
PANEL_COLOR = "#0C2340"
TEXT_COLOR = "#F5F7FA"
MUTED_TEXT_COLOR = "#AAB7C4"
TOP_PLAYER_COLOR = "#236192"
OTHER_PLAYER_COLOR = "#78BE20"
THRESHOLD_COLOR = "#F5A623"
GRID_COLOR = "#29445E"
ALERT_COLOR = "#78BE20"
NO_ALERT_COLOR = "#AAB7C4"


def build_output_path(game_id, state):
    clock_seconds = int(
        round(state["seconds_remaining"])
    )

    clock_tag = (
        f"{clock_seconds // 60:02d}"
        f"{clock_seconds % 60:02d}"
    )

    filename = (
        f"{game_id}_q{state['period']}_"
        f"{clock_tag}_probability.png"
    )

    return (
        Path("outputs")
        / "live_probabilities"
        / filename
    )


def calculate_probability_limit(
    maximum_probability,
    threshold,
):
    largest_value = max(
        maximum_probability,
        threshold,
    )

    rounded_limit = (
        math.ceil(
            (largest_value + 0.08) * 10
        ) / 10
    )

    return min(
        1.0,
        max(0.5, rounded_limit),
    )


def create_live_probability_graphic(
    state,
    prediction,
    output_path,
):
    rankings = prediction["rankings"].copy()

    probabilities = (
        rankings["probability"]
        .astype(float)
        .tolist()
    )

    threshold = float(prediction["threshold"])

    maximum_probability = max(probabilities)

    probability_limit = (
        calculate_probability_limit(
            maximum_probability,
            threshold,
        )
    )

    player_count = len(rankings)

    y_positions = list(
        range(player_count - 1, -1, -1)
    )

    colors = [
        TOP_PLAYER_COLOR
        if rank == 1
        else OTHER_PLAYER_COLOR
        for rank in rankings["rank"]
    ]

    figure, axis = plt.subplots(
        figsize=(16, 9),
        dpi=100,
    )

    figure.patch.set_facecolor(BACKGROUND_COLOR)
    axis.set_facecolor(PANEL_COLOR)

    bars = axis.barh(
        y_positions,
        probabilities,
        height=0.58,
        color=colors,
        edgecolor=BACKGROUND_COLOR,
        linewidth=1.2,
        zorder=3,
    )

    y_labels = []

    for row in rankings.itertuples():
        y_labels.append(
            f"{row.rank}. {row.player_name}   "
            f"Stint {format_clock(row.current_stint_seconds)}"
        )

    axis.set_yticks(y_positions)
    axis.set_yticklabels(
        y_labels,
        color=TEXT_COLOR,
        fontsize=13,
    )

    axis.axvline(
        threshold,
        color=THRESHOLD_COLOR,
        linewidth=2.2,
        zorder=4,
    )

    axis.text(
        threshold,
        player_count - 0.05,
        f"Alert threshold {threshold:.0%}",
        color=THRESHOLD_COLOR,
        fontsize=11,
        fontweight="bold",
        horizontalalignment="center",
        verticalalignment="bottom",
    )

    label_offset = probability_limit * 0.015

    for bar, probability in zip(
        bars,
        probabilities,
    ):
        axis.text(
            probability + label_offset,
            bar.get_y() + bar.get_height() / 2,
            f"{probability:.1%}",
            color=TEXT_COLOR,
            fontsize=14,
            fontweight="bold",
            verticalalignment="center",
            horizontalalignment="left",
        )

    axis.set_xlim(0, probability_limit)
    axis.set_ylim(-0.75, player_count + 0.55)

    axis.xaxis.set_major_formatter(
        PercentFormatter(
            xmax=1.0,
            decimals=0,
        )
    )

    axis.grid(
        axis="x",
        color=GRID_COLOR,
        linewidth=0.8,
        alpha=0.75,
        zorder=1,
    )

    axis.tick_params(
        axis="x",
        colors=MUTED_TEXT_COLOR,
        labelsize=11,
        length=0,
        pad=8,
    )

    axis.tick_params(
        axis="y",
        colors=TEXT_COLOR,
        length=0,
        pad=12,
    )

    for spine in axis.spines.values():
        spine.set_visible(False)

    axis.set_xlabel(
        (
            "Probability player exits within "
            f"{prediction['horizon_seconds']} seconds"
        ),
        color=MUTED_TEXT_COLOR,
        fontsize=12,
        labelpad=14,
    )

    top_prediction = prediction["top_prediction"]

    player_name = str(
        top_prediction["player_name"]
    )

    top_probability = float(
        top_prediction["probability"]
    )

    should_alert = bool(
        prediction["should_alert"]
    )

    alert_text = (
        "SUBSTITUTION LIKELY"
        if should_alert
        else "NO ALERT"
    )

    alert_color = (
        ALERT_COLOR
        if should_alert
        else NO_ALERT_COLOR
    )

    game_date = str(state["game_date"])

    figure.suptitle(
        "TIMBERWOLVES SUBSTITUTION WATCH",
        color=TEXT_COLOR,
        fontsize=23,
        fontweight="bold",
        x=0.06,
        y=0.955,
        horizontalalignment="left",
    )

    figure.text(
        0.06,
        0.895,
        (
            f"{game_date}  •  "
            f"MIN {state['location']} "
            f"{state['opponent']}  •  "
            f"Q{state['period']} "
            f"{format_clock(state['seconds_remaining'])}"
        ),
        color=MUTED_TEXT_COLOR,
        fontsize=13,
    )

    figure.text(
        0.06,
        0.85,
        (
            f"Score: MIN {state['wolves_score']}  "
            f"{state['opponent']} "
            f"{state['opponent_score']}  •  "
            f"Difference {state['score_diff']:+d}"
        ),
        color=TEXT_COLOR,
        fontsize=14,
        fontweight="bold",
    )

    figure.text(
        0.94,
        0.92,
        alert_text,
        color=alert_color,
        fontsize=17,
        fontweight="bold",
        horizontalalignment="right",
    )

    figure.text(
        0.94,
        0.875,
        (
            f"Top pick: {player_name} "
            f"({top_probability:.1%})"
        ),
        color=TEXT_COLOR,
        fontsize=13,
        horizontalalignment="right",
    )

    figure.text(
        0.06,
        0.035,
        (
            "Experimental model estimate based on live "
            "lineup, rotation timing, score and opponent."
        ),
        color=MUTED_TEXT_COLOR,
        fontsize=10,
    )

    figure.subplots_adjust(
        left=0.285,
        right=0.95,
        top=0.75,
        bottom=0.14,
    )

    output_path = Path(output_path)

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    figure.savefig(
        output_path,
        dpi=100,
        facecolor=figure.get_facecolor(),
    )

    plt.close(figure)

    return output_path


def generate_live_probability_graphic(
    game_id,
    period=None,
    clock_seconds=None,
    output_path=None,
):
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found at {MODEL_PATH}"
        )

    model_artifact = joblib.load(MODEL_PATH)

    state = build_state(
        game_id,
        period=period,
        clock_seconds=clock_seconds,
    )

    prediction = predict_state(
        state,
        model_artifact,
    )

    if output_path is None:
        output_path = build_output_path(
            game_id,
            state,
        )

    return create_live_probability_graphic(
        state,
        prediction,
        output_path,
    )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Generate a live substitution "
            "probability graphic"
        )
    )

    parser.add_argument("game_id")
    parser.add_argument("--period", type=int)
    parser.add_argument("--clock")

    parser.add_argument(
        "--output",
        help="Optional output PNG path",
    )

    args = parser.parse_args()

    if (args.period is None) != (args.clock is None):
        parser.error(
            "--period and --clock must be supplied together"
        )

    clock_seconds = (
        parse_clock(args.clock)
        if args.clock is not None
        else None
    )

    output_path = generate_live_probability_graphic(
        args.game_id,
        period=args.period,
        clock_seconds=clock_seconds,
        output_path=args.output,
    )

    print(
        f"Saved live probability graphic to "
        f"{output_path}"
    )


if __name__ == "__main__":
    main()