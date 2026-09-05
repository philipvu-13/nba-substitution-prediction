import argparse
from pathlib import Path

import joblib
import pandas as pd

from src.config import get_connection


MODEL_PATH = Path(
    "models/substitution_logistic_validated.joblib"
)


def parse_clock(clock: str) -> int:
    try:
        minutes, seconds = clock.split(":")
        minutes = int(minutes)
        seconds = int(seconds)
    except ValueError as error:
        raise ValueError(
            "Clock must use MM:SS, such as 06:00"
        ) from error

    if minutes < 0 or seconds < 0 or seconds >= 60:
        raise ValueError("Invalid game clock")

    return minutes * 60 + seconds


def period_start_second(period: int) -> int:
    if period <= 4:
        return (period - 1) * 720

    return 2880 + (period - 5) * 300


def period_length(period: int) -> int:
    return 720 if period <= 4 else 300


def get_snapshot_second(period: int, clock: str) -> int:
    seconds_remaining = parse_clock(clock)

    if seconds_remaining > period_length(period):
        raise ValueError("Clock exceeds the period length")

    exact_game_second = (
        period_start_second(period)
        + period_length(period)
        - seconds_remaining
    )

    # Use the most recent 30-second snapshot,
    # never a snapshot from the future.
    return (exact_game_second // 30) * 30


def load_snapshot(game_id: str, snapshot_second: int):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    game_id,
                    game_date,
                    snapshot_game_second,
                    player_id,
                    player_name,
                    opponent,
                    period,
                    seconds_remaining,
                    current_stint_seconds,
                    stint_number,
                    started_period,
                    started_game,
                    is_home,
                    score_diff,
                    exits_within_120_seconds
                FROM analytics.substitution_training_context_v
                WHERE game_id = %s
                  AND snapshot_game_second = %s
                ORDER BY player_name
                """,
                (game_id, snapshot_second),
            )

            rows = cursor.fetchall()
            columns = [
                description.name
                for description in cursor.description
            ]
    finally:
        connection.close()

    return pd.DataFrame(rows, columns=columns)


def format_seconds(total_seconds: int) -> str:
    minutes, seconds = divmod(int(total_seconds), 60)
    return f"{minutes}:{seconds:02d}"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("game_id")
    parser.add_argument("--period", type=int, required=True)
    parser.add_argument("--clock", required=True)
    args = parser.parse_args()

    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            "Run the validated model training script first"
        )

    snapshot_second = get_snapshot_second(
        args.period,
        args.clock,
    )

    snapshot = load_snapshot(
        args.game_id,
        snapshot_second,
    )

    if snapshot.empty:
        raise ValueError(
            "No training snapshot exists for that game and clock"
        )

    if len(snapshot) != 5:
        raise ValueError(
            f"Expected five players but found {len(snapshot)}"
        )

    artifact = joblib.load(MODEL_PATH)
    model = artifact["model"]
    features = artifact["features"]
    threshold = artifact["threshold"]

    snapshot["player_id"] = snapshot["player_id"].astype(str)
    snapshot["abs_score_diff"] = snapshot["score_diff"].abs()

    snapshot["probability"] = model.predict_proba(
        snapshot[features]
    )[:, 1]

    snapshot["alert"] = (
        snapshot["probability"] >= threshold
    )

    snapshot = snapshot.sort_values(
        "probability",
        ascending=False,
    ).reset_index(drop=True)

    first_row = snapshot.iloc[0]

    location = "vs" if first_row["is_home"] else "at"

    print(
        f"\n{first_row['game_date']} "
        f"{location} {first_row['opponent']}"
    )

    print(
        f"Q{first_row['period']} "
        f"with {format_seconds(first_row['seconds_remaining'])} "
        f"remaining"
    )

    print(f"Score difference: {first_row['score_diff']:+d}")
    print(f"Alert threshold: {threshold:.0%}\n")

    print(
        f"{'Rank':<6}"
        f"{'Player':<24}"
        f"{'Probability':<14}"
        f"{'Current stint':<16}"
        f"{'Alert':<8}"
        f"{'Actually exited'}"
    )

    for index, row in snapshot.iterrows():
        print(
            f"{index + 1:<6}"
            f"{row['player_name']:<24}"
            f"{row['probability']:<14.1%}"
            f"{format_seconds(row['current_stint_seconds']):<16}"
            f"{str(bool(row['alert'])):<8}"
            f"{bool(row['exits_within_120_seconds'])}"
        )


if __name__ == "__main__":
    main()