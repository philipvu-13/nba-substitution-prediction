from pathlib import Path

import joblib
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

from src.modeling.train_baseline import (
    FEATURES,
    build_full_model,
    load_dataset,
)
from src.modeling.train_validated_model import split_dataset


MODEL_PATH = Path("models/substitution_live_v1.joblib")
LIVE_TARGET = "exits_within_60_seconds"
MINIMUM_ALERT_PRECISION = 0.65


def build_snapshot_results(dataset, probabilities):
    scored = dataset[
        ["game_id", "snapshot_game_second"]
    ].copy()

    scored["target"] = (
        dataset[LIVE_TARGET]
        .astype(bool)
        .to_numpy()
    )
    scored["probability"] = probabilities

    results = []

    for keys, group in scored.groupby(
        ["game_id", "snapshot_game_second"],
        sort=False,
    ):
        top_index = group["probability"].idxmax()

        top_pick_correct = bool(
            group.loc[top_index, "target"]
        )

        results.append({
            "game_id": keys[0],
            "snapshot_game_second": keys[1],
            "any_exit": bool(group["target"].any()),
            "maximum_probability": float(
                group["probability"].max()
            ),
            "top_pick_correct": top_pick_correct,
        })

    return pd.DataFrame(results)


def choose_snapshot_threshold(snapshot_results):
    target = snapshot_results["any_exit"]
    probabilities = snapshot_results["maximum_probability"]

    best_threshold = None
    best_recall = -1.0
    best_f1 = 0.0

    for threshold_number in range(5, 96):
        threshold = threshold_number / 100
        predictions = probabilities >= threshold

        precision = precision_score(
            target,
            predictions,
            zero_division=0,
        )

        recall = recall_score(
            target,
            predictions,
            zero_division=0,
        )

        score = f1_score(
            target,
            predictions,
            zero_division=0,
        )

        if (
            precision >= MINIMUM_ALERT_PRECISION
            and recall > best_recall
        ):
            best_threshold = threshold
            best_recall = recall
            best_f1 = score

    if best_threshold is None:
        raise ValueError(
            "No threshold reached the minimum alert precision"
        )

    return best_threshold, best_f1


def evaluate(model, testing, threshold):
    player_target = testing[LIVE_TARGET].astype(int)

    probabilities = model.predict_proba(
        testing[FEATURES]
    )[:, 1]

    snapshots = build_snapshot_results(
        testing,
        probabilities,
    )

    substitution_target = snapshots["any_exit"]

    substitution_predictions = (
        snapshots["maximum_probability"] >= threshold
    )

    alerted_snapshots = snapshots[
        substitution_predictions
    ]

    metrics = {
        "player_roc_auc": roc_auc_score(
            player_target,
            probabilities,
        ),
        "player_average_precision": average_precision_score(
            player_target,
            probabilities,
        ),
        "substitution_precision": precision_score(
            substitution_target,
            substitution_predictions,
            zero_division=0,
        ),
        "substitution_recall": recall_score(
            substitution_target,
            substitution_predictions,
            zero_division=0,
        ),
        "substitution_f1": f1_score(
            substitution_target,
            substitution_predictions,
            zero_division=0,
        ),
        "top_pick_accuracy": snapshots.loc[
            snapshots["any_exit"],
            "top_pick_correct",
        ].mean(),
        "correct_alert_precision": (
            alerted_snapshots["top_pick_correct"].mean()
            if not alerted_snapshots.empty
            else 0.0
        ),
        "alert_rate": substitution_predictions.mean(),
    }

    return metrics


def main():
    dataset = load_dataset()
    training, validation, testing = split_dataset(dataset)

    selection_model = build_full_model()

    selection_model.fit(
        training[FEATURES],
        training[LIVE_TARGET],
    )

    validation_probabilities = selection_model.predict_proba(
        validation[FEATURES]
    )[:, 1]

    validation_snapshots = build_snapshot_results(
        validation,
        validation_probabilities,
    )

    threshold, validation_f1 = choose_snapshot_threshold(
        validation_snapshots
    )

    print(f"Selected live threshold: {threshold:.2f}")
    print(
        f"Minimum required precision: "
        f"{MINIMUM_ALERT_PRECISION:.0%}"
    )
    print(
        f"Validation substitution F1: "
        f"{validation_f1:.4f}"
    )

    final_training = pd.concat(
        [training, validation],
        ignore_index=True,
    )

    final_model = build_full_model()

    final_model.fit(
        final_training[FEATURES],
        final_training[LIVE_TARGET],
    )

    metrics = evaluate(
        final_model,
        testing,
        threshold,
    )

    print("\nFinal live test metrics")

    for metric, value in metrics.items():
        print(f"{metric}: {value:.4f}")

    MODEL_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    joblib.dump(
        {
            "model": final_model,
            "features": FEATURES,
            "snapshot_threshold": threshold,
            "horizon_seconds": 60,
            "minimum_alert_precision": (
                MINIMUM_ALERT_PRECISION
            ),
            "trained_through": str(
                validation["game_date"].max()
            ),
            "metrics": metrics,
        },
        MODEL_PATH,
    )

    print(f"\nSaved live model to {MODEL_PATH}")


if __name__ == "__main__":
    main()