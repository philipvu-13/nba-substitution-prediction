import argparse
import subprocess
import sys

from src.config import get_connection


MAX_SECONDS_DIFFERENCE = 2

PIPELINE_STAGES = [
    ("Play by play", "src.collect.load_play_by_play"),
    ("Box scores", "src.collect.load_boxscores"),
    ("Player stints", "src.processing.build_player_stints_v2"),
]


def run_stage(label: str, module: str, game_id: str) -> None:
    print(f"\nRunning: {label}")

    subprocess.run(
        [sys.executable, "-m", module, game_id],
        check=True,
    )


def validate_game(game_id: str) -> None:
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH boxscore AS (
                    SELECT
                        player_id,
                        player_name,
                        COALESCE(minutes_seconds, 0) AS boxscore_seconds
                    FROM raw.player_boxscores
                    WHERE game_id = %s
                ),
                calculated AS (
                    SELECT
                        player_id,
                        SUM(stint_length_seconds) AS calculated_seconds
                    FROM analytics.player_stints
                    WHERE game_id = %s
                    GROUP BY player_id
                )
                SELECT
                    COALESCE(b.player_id, c.player_id) AS player_id,
                    COALESCE(b.player_name, p.full_name) AS player_name,
                    COALESCE(b.boxscore_seconds, 0) AS boxscore_seconds,
                    COALESCE(c.calculated_seconds, 0) AS calculated_seconds
                FROM boxscore b
                FULL OUTER JOIN calculated c
                    ON c.player_id = b.player_id
                LEFT JOIN core.players p
                    ON p.player_id = COALESCE(b.player_id, c.player_id)
                ORDER BY player_name
                """,
                (game_id, game_id),
            )

            rows = cursor.fetchall()
    finally:
        connection.close()

    if not rows:
        raise RuntimeError("Validation failed because no player rows were found")

    mismatches = []

    for player_id, player_name, boxscore, calculated in rows:
        difference = calculated - boxscore

        if abs(difference) > MAX_SECONDS_DIFFERENCE:
            mismatches.append(
                (
                    player_id,
                    player_name,
                    boxscore,
                    calculated,
                    difference,
                )
            )

    if mismatches:
        for mismatch in mismatches:
            print("Mismatch:", mismatch)

        raise RuntimeError(
            f"Validation failed for {len(mismatches)} players"
        )

    largest_difference = max(
        abs(calculated - boxscore)
        for _, _, boxscore, calculated in rows
    )

    print("\nValidation passed")
    print(f"Players checked: {len(rows)}")
    print(f"Largest difference: {largest_difference} seconds")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("game_id")
    args = parser.parse_args()

    for label, module in PIPELINE_STAGES:
        run_stage(label, module, args.game_id)

    validate_game(args.game_id)

    print(f"\nPipeline completed for {args.game_id}")


if __name__ == "__main__":
    main()