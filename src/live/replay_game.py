import argparse

import joblib

from src.config import get_connection
from src.live.alert_memory import AlertMemory
from src.live.predict_live import MODEL_PATH
from src.live.read_state import format_clock
from src.modeling.train_baseline import load_dataset


TARGET = "exits_within_60_seconds"


def load_player_names():
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT player_id, full_name
                FROM core.players
                """
            )

            return {
                int(player_id): full_name
                for player_id, full_name
                in cursor.fetchall()
            }

    finally:
        connection.close()


def replay_game(game_id):
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found at {MODEL_PATH}"
        )

    artifact = joblib.load(MODEL_PATH)

    model = artifact["model"]
    features = artifact["features"]
    threshold = artifact["snapshot_threshold"]

    dataset = load_dataset()

    game_data = dataset[
        dataset["game_id"].astype(str) == game_id
    ].copy()

    if game_data.empty:
        raise RuntimeError(
            f"No training snapshots found for {game_id}"
        )

    player_names = load_player_names()

    game_data["probability"] = model.predict_proba(
        game_data[features]
    )[:, 1]

    game_data = game_data.sort_values(
        [
            "snapshot_game_second",
            "player_id",
        ]
    )

    alert_memory = AlertMemory()
    alerts = []
    raw_threshold_alerts = 0

    snapshot_seconds = sorted(
        game_data["snapshot_game_second"].unique()
    )

    for snapshot_game_second in snapshot_seconds:
        snapshot = game_data[
            game_data["snapshot_game_second"]
            == snapshot_game_second
        ]

        top_index = snapshot["probability"].idxmax()
        top_player = snapshot.loc[top_index]

        player_id = int(top_player["player_id"])

        probability = float(
            top_player["probability"]
        )

        if probability < threshold:
            continue

        raw_threshold_alerts += 1

        alert_allowed, _ = alert_memory.can_alert(
            player_id,
            float(snapshot_game_second),
        )

        if not alert_allowed:
            continue

        alert_memory.record_alert(
            player_id,
            float(snapshot_game_second),
        )

        actual_substitution = bool(
            snapshot[TARGET].astype(bool).any()
        )

        actual_player = bool(
            top_player[TARGET]
        )

        alerts.append({
            "period": int(top_player["period"]),
            "seconds_remaining": float(
                top_player["seconds_remaining"]
            ),
            "player_name": player_names.get(
                player_id,
                f"Unknown player {player_id}",
            ),
            "probability": probability,
            "actual_substitution": actual_substitution,
            "actual_player": actual_player,
        })

    game_information = game_data.iloc[0]

    location = (
        "vs"
        if bool(game_information["is_home"])
        else "at"
    )

    print(
        f"{game_information['game_date']} "
        f"{location} {game_information['opponent']}"
    )

    print(f"Game ID: {game_id}")
    print(f"Alert threshold: {threshold:.0%}")
    print(f"Eligible snapshots: {len(snapshot_seconds)}")

    print(
        f"Snapshots above threshold: "
        f"{raw_threshold_alerts}"
    )

    print(
        f"Alerts after cooldowns: {len(alerts)}"
    )

    if not alerts:
        print("No alerts would have been sent.")
        return

    correct_substitutions = sum(
        alert["actual_substitution"]
        for alert in alerts
    )

    correct_players = sum(
        alert["actual_player"]
        for alert in alerts
    )

    print(
        "Alert substitution precision: "
        f"{correct_substitutions / len(alerts):.1%}"
    )

    print(
        "Alert player precision: "
        f"{correct_players / len(alerts):.1%}"
    )

    print(
        f"\n{'Q':3}"
        f"{'Clock':8}"
        f"{'Player':24}"
        f"{'Probability':14}"
        f"{'Sub happened':14}"
        f"{'Player exited'}"
    )

    for alert in alerts:
        print(
            f"{alert['period']:<3}"
            f"{format_clock(alert['seconds_remaining']):8}"
            f"{alert['player_name']:24}"
            f"{alert['probability']:<14.1%}"
            f"{str(alert['actual_substitution']):14}"
            f"{alert['actual_player']}"
        )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Replay live alert behavior for one historical game"
        )
    )

    parser.add_argument("game_id")

    args = parser.parse_args()

    replay_game(args.game_id)


if __name__ == "__main__":
    main()