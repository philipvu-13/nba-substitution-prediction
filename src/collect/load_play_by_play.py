import argparse
import time
from typing import Any

import pandas as pd
from nba_api.stats.endpoints import playbyplayv3
from psycopg2.extras import execute_values

from src.config import get_connection


def optional_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def optional_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None

    return str(value)


def fetch_play_by_play(
    game_id: str,
    max_retries: int = 3,
) -> pd.DataFrame:
    for attempt in range(1, max_retries + 1):
        try:
            response = playbyplayv3.PlayByPlayV3(
                game_id=game_id,
                timeout=60,
            )

            plays = response.get_data_frames()[0]

            if plays.empty:
                raise RuntimeError(
                    f"NBA API returned no plays for {game_id}"
                )

            return plays

        except Exception:
            if attempt == max_retries:
                raise

            wait_seconds = 2**attempt
            print(
                f"Attempt {attempt} failed. "
                f"Retrying in {wait_seconds} seconds..."
            )
            time.sleep(wait_seconds)

    raise RuntimeError("Play by play request failed")


def normalize_plays(
    plays: pd.DataFrame,
    game_id: str,
) -> list[tuple[Any, ...]]:
    plays = plays.drop_duplicates(
        subset=["gameId", "actionId"],
        keep="last",
    )

    rows = []

    for play in plays.to_dict(orient="records"):
        rows.append(
            (
                game_id,
                int(play["actionId"]),
                optional_int(play.get("actionNumber")),
                int(play["period"]),
                str(play["clock"]),
                optional_int(play.get("teamId")),
                optional_text(play.get("teamTricode")),
                optional_int(play.get("personId")),
                optional_text(play.get("playerName")),
                optional_text(play.get("description")),
                optional_text(play.get("actionType")),
                optional_text(play.get("subType")),
                optional_int(play.get("scoreHome")),
                optional_int(play.get("scoreAway")),
            )
        )

    return rows


def upsert_plays(rows: list[tuple[Any, ...]]) -> int:
    sql = """
        INSERT INTO raw.play_by_play (
            game_id,
            action_id,
            action_number,
            period,
            clock,
            team_id,
            team_tricode,
            person_id,
            player_name,
            description,
            action_type,
            sub_type,
            score_home,
            score_away
        )
        VALUES %s
        ON CONFLICT (game_id, action_id)
        DO UPDATE SET
            action_number = EXCLUDED.action_number,
            period = EXCLUDED.period,
            clock = EXCLUDED.clock,
            team_id = EXCLUDED.team_id,
            team_tricode = EXCLUDED.team_tricode,
            person_id = EXCLUDED.person_id,
            player_name = EXCLUDED.player_name,
            description = EXCLUDED.description,
            action_type = EXCLUDED.action_type,
            sub_type = EXCLUDED.sub_type,
            score_home = EXCLUDED.score_home,
            score_away = EXCLUDED.score_away,
            loaded_at = CURRENT_TIMESTAMP;
    """

    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            execute_values(cursor, sql, rows, page_size=1000)

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

    plays = fetch_play_by_play(args.game_id)
    rows = normalize_plays(plays, args.game_id)
    loaded_count = upsert_plays(rows)

    print(f"Fetched {len(plays)} play by play events")
    print(
        f"Upserted {loaded_count} rows into raw.play_by_play "
        f"for game {args.game_id}"
    )


if __name__ == "__main__":
    main()