import argparse
import time
from datetime import date

from src.config import get_connection
from src.live.find_game import (
    find_wolves_game,
    get_central_date,
)
from src.live.poll_game import poll_game


SCHEDULED_STATUS = 1
LIVE_STATUS = 2
FINAL_STATUS = 3


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
                    f"MIN {location} {game['opponent']}"
                )

                print(
                    f"Status: {game['game_status_text']}"
                )

                print(
                    f"Scheduled UTC: "
                    f"{game['game_time_utc']}"
                )

                previous_status = current_status

            if game["game_status"] == LIVE_STATUS:
                print("\nGame is live. Starting predictions.")

                poll_game(
                    game["game_id"],
                    game_interval,
                    discord_enabled,
                )

                return

            if game["game_status"] == FINAL_STATUS:
                print("Game is already finished.")
                return

            if game["game_status"] == SCHEDULED_STATUS:
                print(
                    f"Game has not started. Checking again "
                    f"in {scoreboard_interval} seconds."
                )
            else:
                print(
                    f"Unknown game status "
                    f"{game['game_status']}. Checking again."
                )

            time.sleep(scoreboard_interval)

    except KeyboardInterrupt:
        print("\nGame day watcher stopped by user.")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Find and watch today's Timberwolves game"
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
        help="Seconds between scoreboard checks",
    )

    parser.add_argument(
        "--game-interval",
        type=int,
        default=30,
        help="Seconds between live prediction checks",
    )

    parser.add_argument(
        "--discord",
        action="store_true",
        help="Send approved alerts to Discord",
    )

    args = parser.parse_args()

    if args.scoreboard_interval <= 0:
        parser.error(
            "--scoreboard-interval must be greater than zero"
        )

    if args.game_interval <= 0:
        parser.error(
            "--game-interval must be greater than zero"
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