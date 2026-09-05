import argparse
from datetime import date, datetime
from zoneinfo import ZoneInfo

from nba_api.stats.endpoints import scoreboardv3

from src.nba_http import configure_nba_http


WOLVES_TEAM_ID = 1610612750
WOLVES_TRICODE = "MIN"
CENTRAL_TIMEZONE = ZoneInfo("America/Chicago")


def get_central_date():
    return datetime.now(CENTRAL_TIMEZONE).date()


def fetch_scoreboard(game_date: date):
    configure_nba_http()

    response = scoreboardv3.ScoreboardV3(
        game_date=game_date.isoformat(),
        league_id="00",
        timeout=60,
    )

    frames = response.get_data_frames()

    return {
        "games": frames[1],
        "teams": frames[2],
    }


def parse_home_and_away(game_code):
    matchup_code = str(game_code).split("/")[-1]

    if len(matchup_code) != 6:
        raise RuntimeError(
            f"Unexpected game code: {game_code}"
        )

    return {
        "away_team": matchup_code[:3],
        "home_team": matchup_code[3:],
    }


def find_wolves_game(game_date: date):
    scoreboard = fetch_scoreboard(game_date)

    games = scoreboard["games"]
    teams = scoreboard["teams"]

    if games.empty or teams.empty:
        return None

    wolves_rows = teams[
        teams["teamId"] == WOLVES_TEAM_ID
    ]

    if wolves_rows.empty:
        return None

    game_id = str(wolves_rows.iloc[0]["gameId"])

    game_rows = games[
        games["gameId"].astype(str) == game_id
    ]

    if game_rows.empty:
        raise RuntimeError(
            f"Game {game_id} was missing from game data"
        )

    game = game_rows.iloc[0]

    matchup = parse_home_and_away(
        game["gameCode"]
    )

    opponent_rows = teams[
        (teams["gameId"].astype(str) == game_id)
        & (teams["teamId"] != WOLVES_TEAM_ID)
    ]

    if opponent_rows.empty:
        raise RuntimeError(
            f"Opponent was missing for game {game_id}"
        )

    opponent = opponent_rows.iloc[0]

    wolves_are_home = (
        matchup["home_team"] == WOLVES_TRICODE
    )

    return {
        "game_id": game_id,
        "game_date": game_date,
        "game_status": int(game["gameStatus"]),
        "game_status_text": str(
            game["gameStatusText"]
        ),
        "period": int(game["period"]),
        "game_clock": str(game["gameClock"]),
        "game_time_utc": str(game["gameTimeUTC"]),
        "home_team": matchup["home_team"],
        "away_team": matchup["away_team"],
        "opponent": str(opponent["teamTricode"]),
        "is_home": wolves_are_home,
    }


def print_game(game):
    if game is None:
        print("No Timberwolves game found.")
        return

    location = "vs" if game["is_home"] else "at"

    print(f"Game ID: {game['game_id']}")
    print(
        f"Matchup: MIN {location} "
        f"{game['opponent']}"
    )
    print(f"Status: {game['game_status_text']}")
    print(f"Start time UTC: {game['game_time_utc']}")

    if game["game_status"] == 2:
        print(
            f"Current state: Q{game['period']} "
            f"{game['game_clock']}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Find a Timberwolves game by date"
    )

    parser.add_argument(
        "--date",
        help="Game date in YYYY-MM-DD format",
    )

    args = parser.parse_args()

    game_date = (
        date.fromisoformat(args.date)
        if args.date
        else get_central_date()
    )

    print(f"Checking {game_date}")

    game = find_wolves_game(game_date)
    print_game(game)


if __name__ == "__main__":
    main()