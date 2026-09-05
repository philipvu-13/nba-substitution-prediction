import argparse
import time
from typing import Any

import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder
from psycopg2.extras import execute_values

from src.config import get_connection


TIMBERWOLVES_TEAM_ID = 1610612750


def fetch_games(
    season: str,
    season_type: str,
    max_retries: int = 3,
) -> pd.DataFrame:
    for attempt in range(1, max_retries + 1):
        try:
            response = leaguegamefinder.LeagueGameFinder(
                team_id_nullable=TIMBERWOLVES_TEAM_ID,
                season_nullable=season,
                season_type_nullable=season_type,
                timeout=60,
            )

            games = response.get_data_frames()[0]

            if games.empty:
                raise RuntimeError("NBA API returned no games")

            return games

        except Exception:
            if attempt == max_retries:
                raise

            wait_seconds = 2**attempt
            print(
                f"Attempt {attempt} failed. "
                f"Retrying in {wait_seconds} seconds..."
            )
            time.sleep(wait_seconds)

    raise RuntimeError("Game request failed")


def parse_matchup(matchup: str) -> tuple[str, str]:
    if " vs. " in matchup:
        home_team, away_team = matchup.split(" vs. ", maxsplit=1)
    elif " @ " in matchup:
        away_team, home_team = matchup.split(" @ ", maxsplit=1)
    else:
        raise ValueError(f"Unexpected matchup format: {matchup}")

    return home_team.strip(), away_team.strip()


def normalize_games(
    games: pd.DataFrame,
    season: str,
    season_type: str,
) -> list[tuple[Any, ...]]:
    rows = []

    for game in games.to_dict(orient="records"):
        matchup = str(game["MATCHUP"])
        home_team, away_team = parse_matchup(matchup)

        rows.append(
            (
                str(game["GAME_ID"]),
                pd.to_datetime(game["GAME_DATE"]).date(),
                season,
                str(game["SEASON_ID"]),
                season_type,
                home_team,
                away_team,
                matchup,
            )
        )

    return rows


def upsert_games(rows: list[tuple[Any, ...]]) -> int:
    sql = """
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
        VALUES %s
        ON CONFLICT (game_id)
        DO UPDATE SET
            game_date = EXCLUDED.game_date,
            season = EXCLUDED.season,
            season_id = EXCLUDED.season_id,
            season_type = EXCLUDED.season_type,
            home_team_abbrev = EXCLUDED.home_team_abbrev,
            away_team_abbrev = EXCLUDED.away_team_abbrev,
            matchup = EXCLUDED.matchup,
            loaded_at = CURRENT_TIMESTAMP;
    """

    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            execute_values(cursor, sql, rows, page_size=100)

        connection.commit()
    finally:
        connection.close()

    return len(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", default="2025-26")
    parser.add_argument("--season-type", default="Regular Season")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    games = fetch_games(args.season, args.season_type)
    rows = normalize_games(games, args.season, args.season_type)
    loaded_count = upsert_games(rows)

    print(f"Fetched {len(games)} games")
    print(f"Upserted {loaded_count} rows into core.games")


if __name__ == "__main__":
    main()