import argparse
import time
from typing import Any

import pandas as pd
from nba_api.stats.endpoints import boxscoretraditionalv3
from psycopg2.extras import execute_values

from src.config import get_connection
from src.nba_http import configure_nba_http


TIMBERWOLVES_TEAM_ID = 1610612750

configure_nba_http()


def optional_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()
    return text or None


def optional_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None

    return int(value)


def minutes_to_seconds(value: Any) -> int:
    text = optional_text(value)

    if not text:
        return 0

    if ":" in text:
        minutes, seconds = text.split(":", maxsplit=1)
        return int(minutes) * 60 + int(float(seconds))

    if text.startswith("PT") and text.endswith("S"):
        body = text[2:-1]

        if "M" in body:
            minutes, seconds = body.split("M", maxsplit=1)
            return int(minutes) * 60 + int(float(seconds))

        return int(float(body))

    raise ValueError(f"Unexpected minutes format: {text}")


def fetch_boxscore(
    game_id: str,
    max_retries: int = 3,
) -> pd.DataFrame:
    for attempt in range(1, max_retries + 1):
        try:
            response = boxscoretraditionalv3.BoxScoreTraditionalV3(
                game_id=game_id,
                timeout=60,
            )

            players = response.get_data_frames()[0]

            wolves = players[
                players["teamId"] == TIMBERWOLVES_TEAM_ID
            ].copy()

            if wolves.empty:
                raise RuntimeError(
                    f"No Timberwolves box score found for {game_id}"
                )

            return wolves

        except Exception:
            if attempt == max_retries:
                raise

            wait_seconds = 2**attempt
            print(
                f"Attempt {attempt} failed. "
                f"Retrying in {wait_seconds} seconds..."
            )
            time.sleep(wait_seconds)

    raise RuntimeError("Box score request failed")


def normalize_boxscore(
    players: pd.DataFrame,
    game_id: str,
) -> list[tuple[Any, ...]]:
    rows = []

    for player in players.to_dict(orient="records"):
        first_name = optional_text(player.get("firstName")) or ""
        last_name = optional_text(player.get("familyName")) or ""
        player_name = f"{first_name} {last_name}".strip()

        position = optional_text(player.get("position"))
        minutes_text = optional_text(player.get("minutes"))

        rows.append(
            (
                game_id,
                int(player["teamId"]),
                int(player["personId"]),
                player_name,
                minutes_text,
                minutes_to_seconds(minutes_text),
                optional_int(player.get("points")),
                optional_int(player.get("foulsPersonal")),
                position,
                position is not None,
                optional_text(player.get("comment")),
            )
        )

    return rows


def upsert_boxscore(rows: list[tuple[Any, ...]]) -> int:
    sql = """
        INSERT INTO raw.player_boxscores (
            game_id,
            team_id,
            player_id,
            player_name,
            minutes_text,
            minutes_seconds,
            points,
            personal_fouls,
            position,
            is_starter,
            comment
        )
        VALUES %s
        ON CONFLICT (game_id, player_id)
        DO UPDATE SET
            team_id = EXCLUDED.team_id,
            player_name = EXCLUDED.player_name,
            minutes_text = EXCLUDED.minutes_text,
            minutes_seconds = EXCLUDED.minutes_seconds,
            points = EXCLUDED.points,
            personal_fouls = EXCLUDED.personal_fouls,
            position = EXCLUDED.position,
            is_starter = EXCLUDED.is_starter,
            comment = EXCLUDED.comment,
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
    parser.add_argument("game_id")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    players = fetch_boxscore(args.game_id)
    rows = normalize_boxscore(players, args.game_id)
    loaded_count = upsert_boxscore(rows)

    print(f"Fetched {len(players)} Timberwolves box score rows")
    print(
        f"Upserted {loaded_count} rows into raw.player_boxscores "
        f"for game {args.game_id}"
    )


if __name__ == "__main__":
    main()