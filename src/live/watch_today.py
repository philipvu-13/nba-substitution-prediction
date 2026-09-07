import argparse
import subprocess
import sys
import time
from datetime import date, datetime, timezone

from src.config import get_connection
from src.content.postgame_rotation_post import (
    generate_postgame_rotation_post,
)
from src.live.find_game import (
    find_wolves_game,
    get_central_date,
)
from src.live.grade_game import grade_game
from src.live.poll_game import poll_game
from src.notify.send_discord_postgame import (
    send_discord_postgame_summary,
)


SCHEDULED_STATUS = 1
LIVE_STATUS = 2
FINAL_STATUS = 3

PRE_GAME_WINDOW_SECONDS = 15 * 60
MAX_SCHEDULED_SLEEP_SECONDS = 60 * 60

POSTGAME_ATTEMPTS = 10
POSTGAME_RETRY_SECONDS = 60


def get_season_start_year(game_date):
    if game_date.month >= 7:
        return game_date.year

    return game_date.year - 1


def get_season_information(game):
    start_year = get_season_start_year(
        game["game_date"]
    )

    season = (
        f"{start_year}-"
        f"{str(start_year + 1)[-2:]}"
    )

    game_prefix = game["game_id"][:3]

    if game_prefix == "001":
        season_type = "Pre Season"
        season_id = f"1{start_year}"
    elif game_prefix == "002":
        season_type = "Regular Season"
        season_id = f"2{start_year}"
    elif game_prefix == "004":
        season_type = "Playoffs"
        season_id = f"4{start_year}"
    else:
        season_type = "Unknown"
        season_id = f"0{start_year}"

    return {
        "season": season,
        "season_id": season_id,
        "season_type": season_type,
    }


def register_game(game):
    season_information = get_season_information(game)

    if game["is_home"]:
        matchup = f"MIN vs. {game['opponent']}"
    else:
        matchup = f"MIN @ {game['opponent']}"

    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO core.games (
                    game_id,
                    game_date,
                    season,
                    season_id,
                    season_type,
                    home_team_abbrev,
                    away_team_abbrev,
                    matchup
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                )
                ON CONFLICT (game_id)
                DO UPDATE SET
                    game_date = EXCLUDED.game_date,
                    season = EXCLUDED.season,
                    season_id = EXCLUDED.season_id,
                    season_type = EXCLUDED.season_type,
                    home_team_abbrev = EXCLUDED.home_team_abbrev,
                    away_team_abbrev = EXCLUDED.away_team_abbrev,
                    matchup = EXCLUDED.matchup
                """,
                (
                    game["game_id"],
                    game["game_date"],
                    season_information["season"],
                    season_information["season_id"],
                    season_information["season_type"],
                    game["home_team"],
                    game["away_team"],
                    matchup,
                ),
            )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def stored_predictions_exist(game_id):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM analytics.live_predictions
                    WHERE game_id = %s
                );
                """,
                (game_id,),
            )

            return bool(cursor.fetchone()[0])

    finally:
        connection.close()


def run_final_pipeline(game_id):
    subprocess.run(
        [
            sys.executable,
            "-u",
            "-m",
            "src.pipeline.run_game",
            game_id,
        ],
        check=True,
    )


def create_postgame_content(game_id):
    try:
        print(
            "\nGenerating rotation timeline "
            "and X caption."
        )

        result = generate_postgame_rotation_post(
            game_id
        )

        print(
            f"Created rotation timeline: "
            f"{result['graphic_path']}"
        )

        print(
            f"Created X caption: "
            f"{result['caption_path']}"
        )

        return result

    except Exception as error:
        print(
            f"Postgame content creation failed: "
            f"{error}"
        )

        print(
            "Continuing without postgame "
            "social content."
        )

        return {
            "caption": None,
            "caption_path": None,
            "graphic_path": None,
        }


def finalize_game(
    game_id,
    discord_enabled,
):
    if not stored_predictions_exist(game_id):
        print(
            "\nNo stored live predictions were found. "
            "Skipping postgame grading."
        )
        return

    print("\nStarting postgame processing.")

    for attempt in range(
        1,
        POSTGAME_ATTEMPTS + 1,
    ):
        try:
            print(
                f"\nPostgame attempt "
                f"{attempt}/{POSTGAME_ATTEMPTS}"
            )

            run_final_pipeline(game_id)

            print("\nGrading stored predictions.")
            grade_game(game_id)

            postgame_content = (
                create_postgame_content(game_id)
            )

            if discord_enabled:
                print(
                    "\nSending postgame report "
                    "to Discord."
                )

                try:
                    send_discord_postgame_summary(
                        game_id,
                        image_path=postgame_content[
                            "graphic_path"
                        ],
                        suggested_caption=postgame_content[
                            "caption"
                        ],
                    )

                except Exception as error:
                    print(
                        "Discord postgame summary failed: "
                        f"{error}"
                    )

            print("\nPostgame processing completed.")
            return

        except Exception as error:
            print(
                f"\nPostgame processing failed: {error}"
            )

            if attempt == POSTGAME_ATTEMPTS:
                raise RuntimeError(
                    "Postgame processing failed after "
                    f"{POSTGAME_ATTEMPTS} attempts"
                ) from error

            print(
                f"Trying again in "
                f"{POSTGAME_RETRY_SECONDS} seconds."
            )

            time.sleep(POSTGAME_RETRY_SECONDS)


def parse_game_time_utc(game_time_text):
    cleaned = str(game_time_text).strip()

    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"

    game_time = datetime.fromisoformat(cleaned)

    if game_time.tzinfo is None:
        game_time = game_time.replace(
            tzinfo=timezone.utc
        )

    return game_time.astimezone(timezone.utc)


def calculate_scoreboard_wait(
    game,
    regular_interval,
):
    if game["game_status"] != SCHEDULED_STATUS:
        return regular_interval

    try:
        game_time = parse_game_time_utc(
            game["game_time_utc"]
        )
    except (TypeError, ValueError):
        return regular_interval

    seconds_until_game = (
        game_time - datetime.now(timezone.utc)
    ).total_seconds()

    if seconds_until_game <= PRE_GAME_WINDOW_SECONDS:
        return regular_interval

    sleep_seconds = (
        seconds_until_game
        - PRE_GAME_WINDOW_SECONDS
    )

    return max(
        regular_interval,
        min(
            round(sleep_seconds),
            MAX_SCHEDULED_SLEEP_SECONDS,
        ),
    )


def format_wait_time(seconds):
    if seconds >= 3600:
        hours = seconds / 3600
        return f"{hours:.1f} hours"

    if seconds >= 60:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"

    return f"{seconds} seconds"


def watch_game_day(
    game_date,
    scoreboard_interval,
    game_interval,
    discord_enabled,
):
    previous_status = None

    print(f"Watching scoreboard for {game_date}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            game = find_wolves_game(game_date)

            if game is None:
                print(
                    "No Timberwolves game was found "
                    "for this date."
                )
                return

            register_game(game)

            current_status = (
                game["game_status"],
                game["game_status_text"],
            )

            if current_status != previous_status:
                location = (
                    "vs" if game["is_home"] else "at"
                )

                print(
                    f"Found {game['game_id']}: "
                    f"MIN {location} "
                    f"{game['opponent']}"
                )

                print(
                    f"Status: "
                    f"{game['game_status_text']}"
                )

                print(
                    f"Scheduled UTC: "
                    f"{game['game_time_utc']}"
                )

                previous_status = current_status

            if game["game_status"] == LIVE_STATUS:
                print(
                    "\nGame is live. "
                    "Starting predictions."
                )

                game_finished = poll_game(
                    game["game_id"],
                    game_interval,
                    discord_enabled,
                )

                if game_finished:
                    finalize_game(
                        game["game_id"],
                        discord_enabled,
                    )
                else:
                    print(
                        "\nLive polling stopped before "
                        "the game finished."
                    )

                return

            if game["game_status"] == FINAL_STATUS:
                print("Game is already finished.")

                finalize_game(
                    game["game_id"],
                    discord_enabled,
                )

                return

            wait_seconds = calculate_scoreboard_wait(
                game,
                scoreboard_interval,
            )

            if (
                game["game_status"]
                == SCHEDULED_STATUS
            ):
                print(
                    "Game has not started. "
                    f"Checking again in "
                    f"{format_wait_time(wait_seconds)}."
                )
            else:
                print(
                    f"Unknown game status "
                    f"{game['game_status']}. "
                    f"Checking again in "
                    f"{format_wait_time(wait_seconds)}."
                )

            time.sleep(wait_seconds)

    except KeyboardInterrupt:
        print(
            "\nGame day watcher stopped by user."
        )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Find and watch today's "
            "Timberwolves game"
        )
    )

    parser.add_argument(
        "--date",
        help="Game date in YYYY-MM-DD format",
    )

    parser.add_argument(
        "--scoreboard-interval",
        type=int,
        default=60,
        help=(
            "Seconds between nearby "
            "scoreboard checks"
        ),
    )

    parser.add_argument(
        "--game-interval",
        type=int,
        default=30,
        help=(
            "Seconds between live "
            "prediction checks"
        ),
    )

    parser.add_argument(
        "--discord",
        action="store_true",
        help=(
            "Send live alerts and the "
            "postgame report to Discord"
        ),
    )

    args = parser.parse_args()

    if args.scoreboard_interval <= 0:
        parser.error(
            "--scoreboard-interval must be "
            "greater than zero"
        )

    if args.game_interval <= 0:
        parser.error(
            "--game-interval must be "
            "greater than zero"
        )

    game_date = (
        date.fromisoformat(args.date)
        if args.date
        else get_central_date()
    )

    watch_game_day(
        game_date,
        args.scoreboard_interval,
        args.game_interval,
        args.discord,
    )


if __name__ == "__main__":
    main()