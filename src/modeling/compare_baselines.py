import joblib
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    roc_auc_score,
)

from src.live.alert_memory import AlertMemory
from src.live.predict_live import MODEL_PATH
from src.modeling.train_baseline import load_dataset
from src.modeling.train_validated_model import split_dataset


TARGET = "exits_within_60_seconds"


def build_snapshot_results(dataset, probabilities):
    scored = dataset.copy()
    scored["probability"] = probabilities
    scored[TARGET] = scored[TARGET].astype(bool)

    results = []

    for keys, snapshot in scored.groupby(
        ["game_id", "snapshot_game_second"],
        sort=False,
    ):
        model_index = snapshot["probability"].idxmax()
        model_player = snapshot.loc[model_index]

        maximum_stint = int(
            snapshot["current_stint_seconds"].max()
        )

        longest_players = snapshot[
            snapshot["current_stint_seconds"]
            == maximum_stint
        ].sort_values("player_id")

        longest_player = longest_players.iloc[0]

        exiting_player_ids = set(
            snapshot.loc[
                snapshot[TARGET],
                "player_id",
            ].astype(int)
        )

        longest_tied_ids = set(
            longest_players["player_id"].astype(int)
        )

        if exiting_player_ids & longest_tied_ids:
            longest_tie_credit = (
                1.0 / len(longest_tied_ids)
            )
        else:
            longest_tie_credit = 0.0

        first_row = snapshot.iloc[0]

        results.append({
            "game_id": str(keys[0]),
            "snapshot_game_second": float(keys[1]),
            "game_date": first_row["game_date"],
            "opponent": first_row["opponent"],
            "any_exit": bool(exiting_player_ids),
            "actual_player_ids": exiting_player_ids,
            "model_player_id": int(
                model_player["player_id"]
            ),
            "model_probability": float(
                model_player["probability"]
            ),
            "model_player_correct": (
                int(model_player["player_id"])
                in exiting_player_ids
            ),
            "longest_player_id": int(
                longest_player["player_id"]
            ),
            "longest_stint_seconds": maximum_stint,
            "longest_player_correct": (
                int(longest_player["player_id"])
                in exiting_player_ids
            ),
            "longest_tie_credit": longest_tie_credit,
            "longest_tie_count": len(
                longest_tied_ids
            ),
        })

    return pd.DataFrame(results)


def evaluate_alert_strategy(
    snapshots,
    score_column,
    threshold,
    player_column,
):
    raw_alerts = 0
    delivered_alerts = 0
    correct_substitutions = 0
    correct_players = 0

    game_count = snapshots["game_id"].nunique()

    for _, game_snapshots in snapshots.groupby(
        "game_id",
        sort=False,
    ):
        alert_memory = AlertMemory()

        game_snapshots = game_snapshots.sort_values(
            "snapshot_game_second"
        )

        for _, snapshot in game_snapshots.iterrows():
            score = float(snapshot[score_column])

            if score < threshold:
                continue

            raw_alerts += 1

            player_id = int(snapshot[player_column])
            game_second = float(
                snapshot["snapshot_game_second"]
            )

            alert_allowed, _ = alert_memory.can_alert(
                player_id,
                game_second,
            )

            if not alert_allowed:
                continue

            alert_memory.record_alert(
                player_id,
                game_second,
            )

            delivered_alerts += 1

            if bool(snapshot["any_exit"]):
                correct_substitutions += 1

            if (
                player_id
                in snapshot["actual_player_ids"]
            ):
                correct_players += 1

    return {
        "raw_alerts": raw_alerts,
        "delivered_alerts": delivered_alerts,
        "alerts_per_game": (
            delivered_alerts / game_count
            if game_count
            else 0.0
        ),
        "correct_substitutions": (
            correct_substitutions
        ),
        "correct_players": correct_players,
        "substitution_precision": (
            correct_substitutions
            / delivered_alerts
            if delivered_alerts
            else 0.0
        ),
        "player_precision": (
            correct_players / delivered_alerts
            if delivered_alerts
            else 0.0
        ),
    }


def choose_matched_longest_stint_threshold(
    validation_snapshots,
    model_threshold,
):
    model_results = evaluate_alert_strategy(
        validation_snapshots,
        score_column="model_probability",
        threshold=model_threshold,
        player_column="model_player_id",
    )

    target_delivered_alerts = model_results[
        "delivered_alerts"
    ]

    target_raw_alerts = model_results[
        "raw_alerts"
    ]

    thresholds = sorted(
        validation_snapshots[
            "longest_stint_seconds"
        ].unique()
    )

    best_choice = None

    for threshold in thresholds:
        baseline_results = evaluate_alert_strategy(
            validation_snapshots,
            score_column="longest_stint_seconds",
            threshold=float(threshold),
            player_column="longest_player_id",
        )

        delivered_difference = abs(
            baseline_results["delivered_alerts"]
            - target_delivered_alerts
        )

        raw_difference = abs(
            baseline_results["raw_alerts"]
            - target_raw_alerts
        )

        comparison_key = (
            delivered_difference,
            raw_difference,
            int(threshold),
        )

        if (
            best_choice is None
            or comparison_key
            < best_choice["comparison_key"]
        ):
            best_choice = {
                "threshold": int(threshold),
                "results": baseline_results,
                "comparison_key": comparison_key,
            }

    return {
        "model_results": model_results,
        "baseline_threshold": best_choice[
            "threshold"
        ],
        "baseline_results": best_choice[
            "results"
        ],
    }


def format_seconds(seconds):
    seconds = int(round(seconds))
    return f"{seconds // 60}:{seconds % 60:02d}"


def print_alert_results(name, results):
    print(f"\n{name}")

    print(
        f"Raw threshold alerts: "
        f"{results['raw_alerts']}"
    )

    print(
        f"Alerts after cooldowns: "
        f"{results['delivered_alerts']}"
    )

    print(
        f"Average alerts per game: "
        f"{results['alerts_per_game']:.2f}"
    )

    print(
        f"Correct substitution alerts: "
        f"{results['correct_substitutions']}"
    )

    print(
        f"Correct player alerts: "
        f"{results['correct_players']}"
    )

    print(
        f"Substitution precision: "
        f"{results['substitution_precision']:.1%}"
    )

    print(
        f"Exact player precision: "
        f"{results['player_precision']:.1%}"
    )


def print_percentage_difference(label, difference):
    print(
        f"{label}: "
        f"{difference:+.1%}"
    )


def main():
    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"Model not found at {MODEL_PATH}"
        )

    artifact = joblib.load(MODEL_PATH)

    model = artifact["model"]
    features = artifact["features"]

    model_threshold = float(
        artifact["snapshot_threshold"]
    )

    dataset = load_dataset()
    _, validation, testing = split_dataset(dataset)

    validation = validation.copy()
    testing = testing.copy()

    validation_probabilities = model.predict_proba(
        validation[features]
    )[:, 1]

    testing_probabilities = model.predict_proba(
        testing[features]
    )[:, 1]

    validation_snapshots = build_snapshot_results(
        validation,
        validation_probabilities,
    )

    testing_snapshots = build_snapshot_results(
        testing,
        testing_probabilities,
    )

    threshold_selection = (
        choose_matched_longest_stint_threshold(
            validation_snapshots,
            model_threshold,
        )
    )

    longest_threshold = threshold_selection[
        "baseline_threshold"
    ]

    validation_model_results = threshold_selection[
        "model_results"
    ]

    validation_baseline_results = (
        threshold_selection["baseline_results"]
    )

    player_target = testing[TARGET].astype(int)

    model_roc_auc = roc_auc_score(
        player_target,
        testing_probabilities,
    )

    stint_roc_auc = roc_auc_score(
        player_target,
        testing["current_stint_seconds"],
    )

    model_average_precision = average_precision_score(
        player_target,
        testing_probabilities,
    )

    stint_average_precision = (
        average_precision_score(
            player_target,
            testing["current_stint_seconds"],
        )
    )

    positive_snapshots = testing_snapshots[
        testing_snapshots["any_exit"]
    ]

    model_ranking_accuracy = (
        positive_snapshots[
            "model_player_correct"
        ].mean()
    )

    longest_ranking_accuracy = (
        positive_snapshots[
            "longest_tie_credit"
        ].mean()
    )

    model_alert_results = evaluate_alert_strategy(
        testing_snapshots,
        score_column="model_probability",
        threshold=model_threshold,
        player_column="model_player_id",
    )

    longest_alert_results = evaluate_alert_strategy(
        testing_snapshots,
        score_column="longest_stint_seconds",
        threshold=longest_threshold,
        player_column="longest_player_id",
    )

    print("Machine learning versus longest stint")
    print("Matched alert-volume comparison")

    print(
        f"Validation games: "
        f"{validation_snapshots['game_id'].nunique()}"
    )

    print(
        f"Test games: "
        f"{testing_snapshots['game_id'].nunique()}"
    )

    print(
        f"Test player rows: {len(testing)}"
    )

    print(
        f"Test snapshots: "
        f"{len(testing_snapshots)}"
    )

    print("\nValidation alert-volume matching")

    print(
        f"Machine-learning threshold: "
        f"{model_threshold:.0%}"
    )

    print(
        f"Machine-learning alerts: "
        f"{validation_model_results['delivered_alerts']}"
    )

    print(
        f"Selected longest-stint threshold: "
        f"{format_seconds(longest_threshold)}"
    )

    print(
        f"Longest-stint alerts: "
        f"{validation_baseline_results['delivered_alerts']}"
    )

    print("\nPlayer-level prediction quality")

    print(
        f"{'Strategy':24}"
        f"{'ROC AUC':12}"
        f"{'Average precision'}"
    )

    print(
        f"{'Machine learning':24}"
        f"{model_roc_auc:<12.4f}"
        f"{model_average_precision:.4f}"
    )

    print(
        f"{'Current stint only':24}"
        f"{stint_roc_auc:<12.4f}"
        f"{stint_average_precision:.4f}"
    )

    print("\nExact-player ranking")

    print(
        "Measured when at least one player "
        "exited within 60 seconds."
    )

    print(
        f"Positive snapshots: "
        f"{len(positive_snapshots)}"
    )

    print(
        f"Machine-learning accuracy: "
        f"{model_ranking_accuracy:.1%}"
    )

    print(
        f"Longest-stint accuracy: "
        f"{longest_ranking_accuracy:.1%}"
    )

    print(
        "Longest-stint ties receive fractional credit."
    )

    print_alert_results(
        (
            "Machine-learning strategy "
            f"(threshold {model_threshold:.0%})"
        ),
        model_alert_results,
    )

    print_alert_results(
        (
            "Longest-stint strategy "
            f"(threshold "
            f"{format_seconds(longest_threshold)})"
        ),
        longest_alert_results,
    )

    ranking_difference = (
        model_ranking_accuracy
        - longest_ranking_accuracy
    )

    substitution_precision_difference = (
        model_alert_results[
            "substitution_precision"
        ]
        - longest_alert_results[
            "substitution_precision"
        ]
    )

    player_precision_difference = (
        model_alert_results[
            "player_precision"
        ]
        - longest_alert_results[
            "player_precision"
        ]
    )

    alert_count_difference = (
        model_alert_results["delivered_alerts"]
        - longest_alert_results["delivered_alerts"]
    )

    print("\nMachine-learning lift")

    print_percentage_difference(
        "Exact-player ranking difference",
        ranking_difference,
    )

    print_percentage_difference(
        "Substitution precision difference",
        substitution_precision_difference,
    )

    print_percentage_difference(
        "Exact-player alert precision difference",
        player_precision_difference,
    )

    print(
        f"Test alert-count difference: "
        f"{alert_count_difference:+d}"
    )

    print("\nConclusion")

    if (
        ranking_difference > 0
        and player_precision_difference > 0
    ):
        print(
            "The machine-learning model "
            "outperformed the longest-stint rule."
        )

    elif ranking_difference > 0:
        print(
            "The model ranked players better overall, "
            "but did not improve exact-player precision "
            "for the delivered alerts."
        )

    elif player_precision_difference > 0:
        print(
            "The model improved delivered-alert "
            "precision, but not overall player ranking."
        )

    else:
        print(
            "The longest-stint rule matched or "
            "outperformed the machine-learning model."
        )


if __name__ == "__main__":
    main()