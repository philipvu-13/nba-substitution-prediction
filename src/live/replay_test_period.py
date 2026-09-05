import joblib

from src.live.alert_memory import AlertMemory
from src.live.predict_live import MODEL_PATH
from src.modeling.train_baseline import load_dataset
from src.modeling.train_validated_model import split_dataset


TARGET = "exits_within_60_seconds"


def evaluate_game(game_data, threshold):
    alert_memory = AlertMemory()

    raw_threshold_alerts = 0
    delivered_alerts = 0
    correct_substitution_alerts = 0
    correct_player_alerts = 0

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

        probability = float(
            top_player["probability"]
        )

        if probability < threshold:
            continue

        raw_threshold_alerts += 1

        player_id = int(top_player["player_id"])

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

        delivered_alerts += 1

        if snapshot[TARGET].astype(bool).any():
            correct_substitution_alerts += 1

        if bool(top_player[TARGET]):
            correct_player_alerts += 1

    first_row = game_data.iloc[0]

    return {
        "game_id": str(first_row["game_id"]),
        "game_date": first_row["game_date"],
        "opponent": first_row["opponent"],
        "raw_alerts": raw_threshold_alerts,
        "delivered_alerts": delivered_alerts,
        "correct_substitutions": (
            correct_substitution_alerts
        ),
        "correct_players": correct_player_alerts,
    }


def safe_percentage(numerator, denominator):
    if denominator == 0:
        return 0.0

    return numerator / denominator


def main():
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found at {MODEL_PATH}"
        )

    artifact = joblib.load(MODEL_PATH)

    model = artifact["model"]
    features = artifact["features"]
    threshold = artifact["snapshot_threshold"]

    dataset = load_dataset()
    _, _, testing = split_dataset(dataset)

    testing = testing.copy()

    testing["probability"] = model.predict_proba(
        testing[features]
    )[:, 1]

    results = []

    for game_id, game_data in testing.groupby(
        "game_id",
        sort=False,
    ):
        results.append(
            evaluate_game(
                game_data,
                threshold,
            )
        )

    results.sort(
        key=lambda row: (
            row["game_date"],
            row["game_id"],
        )
    )

    print("Production alert replay")
    print(f"Test games: {len(results)}")
    print(f"Alert threshold: {threshold:.0%}\n")

    print(
        f"{'Date':12}"
        f"{'Opponent':10}"
        f"{'Raw':6}"
        f"{'Sent':7}"
        f"{'Sub correct':13}"
        f"{'Player correct'}"
    )

    for result in results:
        print(
            f"{str(result['game_date']):12}"
            f"{result['opponent']:10}"
            f"{result['raw_alerts']:<6}"
            f"{result['delivered_alerts']:<7}"
            f"{result['correct_substitutions']:<13}"
            f"{result['correct_players']}"
        )

    total_raw = sum(
        row["raw_alerts"]
        for row in results
    )

    total_delivered = sum(
        row["delivered_alerts"]
        for row in results
    )

    total_correct_substitutions = sum(
        row["correct_substitutions"]
        for row in results
    )

    total_correct_players = sum(
        row["correct_players"]
        for row in results
    )

    substitution_precision = safe_percentage(
        total_correct_substitutions,
        total_delivered,
    )

    player_precision = safe_percentage(
        total_correct_players,
        total_delivered,
    )

    average_alerts = safe_percentage(
        total_delivered,
        len(results),
    )

    print("\nOverall results")
    print(f"Raw threshold alerts: {total_raw}")

    print(
        f"Alerts after cooldowns: "
        f"{total_delivered}"
    )

    print(
        f"Average alerts per game: "
        f"{average_alerts:.2f}"
    )

    print(
        f"Correct substitution alerts: "
        f"{total_correct_substitutions}"
    )

    print(
        f"Correct player alerts: "
        f"{total_correct_players}"
    )

    print(
        f"Substitution precision: "
        f"{substitution_precision:.1%}"
    )

    print(
        f"Player precision: "
        f"{player_precision:.1%}"
    )


if __name__ == "__main__":
    main()