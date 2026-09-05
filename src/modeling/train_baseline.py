from pathlib import Path

import joblib
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from src.config import get_connection


MODEL_PATH = Path("models/substitution_logistic_v1.joblib")

NUMERIC_FEATURES = [
    "period",
    "seconds_remaining",
    "current_stint_seconds",
    "stint_number",
    "started_period",
    "started_game",
    "is_home",
    "score_diff",
    "abs_score_diff",
]

CATEGORICAL_FEATURES = [
    "player_id",
    "opponent",
]

FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES
TARGET = "exits_within_120_seconds"


def load_dataset() -> pd.DataFrame:
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
                ORDER BY game_date, game_id, snapshot_game_second
                """
            )

            rows = cursor.fetchall()
            columns = [
                description.name
                for description in cursor.description
            ]
    finally:
        connection.close()

    dataset = pd.DataFrame(rows, columns=columns)
    dataset["player_id"] = dataset["player_id"].astype(str)
    dataset["abs_score_diff"] = dataset["score_diff"].abs()

    return dataset


def temporal_split(dataset: pd.DataFrame):
    games = (
        dataset[["game_id", "game_date"]]
        .drop_duplicates()
        .sort_values(["game_date", "game_id"])
    )

    split_position = int(len(games) * 0.80)

    training_games = set(
        games.iloc[:split_position]["game_id"]
    )
    testing_games = set(
        games.iloc[split_position:]["game_id"]
    )

    training = dataset[
        dataset["game_id"].isin(training_games)
    ].copy()

    testing = dataset[
        dataset["game_id"].isin(testing_games)
    ].copy()

    return training, testing


def build_timing_model() -> Pipeline:
    return Pipeline([
        ("scale", StandardScaler()),
        (
            "model",
            LogisticRegression(max_iter=2000),
        ),
    ])


def build_full_model() -> Pipeline:
    preprocessing = ColumnTransformer([
        (
            "numeric",
            StandardScaler(),
            NUMERIC_FEATURES,
        ),
        (
            "categorical",
            OneHotEncoder(handle_unknown="ignore"),
            CATEGORICAL_FEATURES,
        ),
    ])

    return Pipeline([
        ("preprocessing", preprocessing),
        (
            "model",
            LogisticRegression(max_iter=2000),
        ),
    ])


def top_one_accuracy(
    testing: pd.DataFrame,
    probabilities,
) -> float:
    scored = testing[
        ["game_id", "snapshot_game_second"]
    ].copy()

    scored["target"] = testing[TARGET].astype(bool).to_numpy()
    scored["probability"] = probabilities

    eligible_snapshots = 0
    correct_snapshots = 0

    for _, group in scored.groupby(
        ["game_id", "snapshot_game_second"]
    ):
        if not group["target"].any():
            continue

        eligible_snapshots += 1
        predicted_index = group["probability"].idxmax()

        if group.loc[predicted_index, "target"]:
            correct_snapshots += 1

    return correct_snapshots / eligible_snapshots


def evaluate(
    name: str,
    model: Pipeline,
    testing: pd.DataFrame,
    features,
):
    probabilities = model.predict_proba(
        testing[features]
    )[:, 1]

    predictions = probabilities >= 0.50
    target = testing[TARGET].astype(int)

    metrics = {
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

    print(f"\n{name}")

    for metric, value in metrics.items():
        print(f"{metric}: {value:.4f}")

    return metrics


def main() -> None:
    dataset = load_dataset()
    training, testing = temporal_split(dataset)

    print(f"Total rows: {len(dataset)}")
    print(f"Training rows: {len(training)}")
    print(f"Testing rows: {len(testing)}")
    print(
        f"Training through: {training['game_date'].max()}"
    )
    print(
        f"Testing begins: {testing['game_date'].min()}"
    )

    timing_features = [
        "period",
        "seconds_remaining",
        "current_stint_seconds",
    ]

    timing_model = build_timing_model()
    timing_model.fit(
        training[timing_features],
        training[TARGET],
    )

    evaluate(
        "Timing-only baseline",
        timing_model,
        testing,
        timing_features,
    )

    full_model = build_full_model()
    full_model.fit(
        training[FEATURES],
        training[TARGET],
    )

    metrics = evaluate(
        "Full substitution model",
        full_model,
        testing,
        FEATURES,
    )

    MODEL_PATH.parent.mkdir(parents=True, exist_ok=True)

    joblib.dump(
        {
            "model": full_model,
            "features": FEATURES,
            "horizon_seconds": 120,
            "trained_through": str(
                training["game_date"].max()
            ),
            "metrics": metrics,
        },
        MODEL_PATH,
    )

    print(f"\nSaved model to {MODEL_PATH}")


if __name__ == "__main__":
    main()