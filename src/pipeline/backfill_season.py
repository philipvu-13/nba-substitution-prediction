import argparse
import subprocess
import sys
import time

from src.config import get_connection


MAX_SECONDS_DIFFERENCE = 2


def fetch_games(season: str, season_type: str):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT game_id, game_date, matchup
                FROM core.games
                WHERE season = %s
                  AND season_type = %s
                ORDER BY game_date, game_id
                """,
                (season, season_type),
            )

            return cursor.fetchall()
    finally:
        connection.close()


def game_is_complete(game_id: str) -> bool:
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                WITH boxscore AS (
                    SELECT
                        player_id,
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
                ),
                comparison AS (
                    SELECT
                        COALESCE(b.player_id, c.player_id) AS player_id,
                        COALESCE(b.boxscore_seconds, 0) AS boxscore_seconds,
                        COALESCE(c.calculated_seconds, 0) AS calculated_seconds
                    FROM boxscore b
                    FULL OUTER JOIN calculated c
                        ON c.player_id = b.player_id
                )
                SELECT
                    COUNT(*) > 0 AS has_players,
                    COALESCE(SUM(calculated_seconds), 0) AS total_seconds,
                    COALESCE(
                        BOOL_AND(
                            ABS(calculated_seconds - boxscore_seconds) <= %s
                        ),
                        FALSE
                    ) AS matches_boxscore
                FROM comparison
                """,
                (
                    game_id,
                    game_id,
                    MAX_SECONDS_DIFFERENCE,
                ),
            )

            has_players, total_seconds, matches_boxscore = cursor.fetchone()

            return (
                has_players
                and total_seconds > 0
                and matches_boxscore
            )
    finally:
        connection.close()


def mark_failure(game_id: str, reason: str) -> None:
    connection = get_connection()

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO core.excluded_games (
                        game_id,
                        reason
                    )
                    VALUES (%s, %s)
                    ON CONFLICT (game_id)
                    DO UPDATE SET
                        reason = EXCLUDED.reason,
                        created_at = CURRENT_TIMESTAMP
                    """,
                    (game_id, reason),
                )
    finally:
        connection.close()


def clear_failure(game_id: str) -> None:
    connection = get_connection()

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM core.excluded_games
                    WHERE game_id = %s
                    """,
                    (game_id,),
                )
    finally:
        connection.close()


def run_game(game_id: str):
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "src.pipeline.run_game",
            game_id,
        ],
        text=True,
        capture_output=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", default="2025-26")
    parser.add_argument(
        "--season-type",
        default="Regular Season",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of unfinished games to attempt",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=3,
        help="Seconds between games",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess completed games",
    )
    args = parser.parse_args()

    games = fetch_games(args.season, args.season_type)

    attempted = 0
    completed = 0
    skipped = 0
    failed = 0

    print(f"Found {len(games)} games")

    try:
        for game_id, game_date, matchup in games:
            if args.limit is not None and attempted >= args.limit:
                break

            if not args.force and game_is_complete(game_id):
                clear_failure(game_id)
                print(f"Skipping completed game {game_id}")
                skipped += 1
                continue

            attempted += 1

            print(
                f"\nProcessing {game_id}: "
                f"{game_date} {matchup}"
            )

            result = run_game(game_id)

            if result.stdout:
                print(result.stdout.rstrip())

            if result.stderr:
                print(result.stderr.rstrip(), file=sys.stderr)

            if result.returncode == 0:
                clear_failure(game_id)
                completed += 1
            else:
                output = result.stderr or result.stdout
                reason = (
                    output[-2000:]
                    if output
                    else f"Pipeline exited with code {result.returncode}"
                )

                mark_failure(game_id, reason)
                failed += 1

            if args.delay > 0:
                time.sleep(args.delay)

    except KeyboardInterrupt:
        print("\nBackfill stopped safely by user")

    print("\nBackfill summary")
    print(f"Attempted: {attempted}")
    print(f"Completed: {completed}")
    print(f"Skipped: {skipped}")
    print(f"Failed: {failed}")


if __name__ == "__main__":
    main()