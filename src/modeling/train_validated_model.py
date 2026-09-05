from pathlib import Path
import pandas as pd

import joblib
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

from src.modeling.train_baseline import (
    FEATURES,
    TARGET,
    build_full_model,
    load_dataset,
    top_one_accuracy,
)


MODEL_PATH = Path(
    "models/substitution_logistic_validated.joblib"
)


def split_dataset(dataset):
    games = (
        dataset[["game_id", "game_date"]]
        .drop_duplicates()
        .sort_values(["game_date", "game_id"])
    )

    training_end = int(len(games) * 0.70)
    validation_end = int(len(games) * 0.85)

    training_ids = set(
        games.iloc[:training_end]["game_id"]
    )
    validation_ids = set(
        games.iloc[training_end:validation_end]["game_id"]
    )
    testing_ids = set(
        games.iloc[validation_end:]["game_id"]
    )

    training = dataset[
        dataset["game_id"].isin(training_ids)
    ].copy()

    validation = dataset[
        dataset["game_id"].isin(validation_ids)
    ].copy()

    testing = dataset[
        dataset["game_id"].isin(testing_ids)
    ].copy()

    return training, validation, testing


def choose_threshold(target, probabilities):
    best_threshold = 0.50
    best_f1 = 0.0

    for threshold_number in range(10, 91):
        threshold = threshold_number / 100
        predictions = probabilities >= threshold
        score = f1_score(
            target,
            predictions,
            zero_division=0,
        )

        if score > best_f1:
            best_f1 = score
            best_threshold = threshold

    return best_threshold, best_f1


def evaluate(model, testing, threshold):
    target = testing[TARGET].astype(int)

    probabilities = model.predict_proba(
        testing[FEATURES]
    )[:, 1]

    predictions = probabilities >= threshold

    return {
        "roc_auc": roc_auc_score(target, probabilities),
        "average_precision": average_precision_score(
            target,
            probabilities,
        ),
        "precision": precision_score(
            target,
            predictions,
            zero_division=0,
        ),
        "recall": recall_score(
            target,
            predictions,
            zero_division=0,
        ),
        "f1": f1_score(
            target,
            predictions,
            zero_division=0,
        ),
        "top_one_accuracy": top_one_accuracy(
            testing,
            probabilities,
        ),
    }


def main():
    dataset = load_dataset()
    training, validation, testing = split_dataset(dataset)

    print(f"Training games: {training['game_id'].nunique()}")
    print(
        f"Validation games: "
        f"{validation['game_id'].nunique()}"
    )
    print(f"Testing games: {testing['game_id'].nunique()}")

    print(
        f"Training through: {training['game_date'].max()}"
    )
    print(
        f"Validation period: "
        f"{validation['game_date'].min()} to "
        f"{validation['game_date'].max()}"
    )
    print(
        f"Testing begins: {testing['game_date'].min()}"
    )

    model = build_full_model()

    model.fit(
        training[FEATURES],
        training[TARGET],
    )

    validation_probabilities = model.predict_proba(
        validation[FEATURES]
    )[:, 1]

    threshold, validation_f1 = choose_threshold(
        validation[TARGET].astype(int),
        validation_probabilities,
    )

    print(f"\nSelected threshold: {threshold:.2f}")
    print(f"Validation F1: {validation_f1:.4f}")

    final_training = pd.concat(
        [training, validation],
       ignore_index=True,
    )

    final_model = build_full_model()

    final_model.fit(
        final_training[FEATURES],
        final_training[TARGET],
    )

    metrics = evaluate(
        final_model,
        testing,
        threshold,
    )

    print("\nFinal test metrics")

    for metric, value in metrics.items():
        print(f"{metric}: {value:.4f}")

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)

    joblib.dump(
        {
            "model": final_model,
            "features": FEATURES,
            "threshold": threshold,
            "horizon_seconds": 120,
            "trained_through": str(
                validation["game_date"].max()
            ),
            "metrics": metrics,
        },
        MODEL_PATH,
    )

    print(f"\nSaved model to {MODEL_PATH}")


if __name__ == "__main__":
    main()