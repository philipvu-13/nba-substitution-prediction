from typing import Any

from nba_api.stats.static import players as nba_players
from psycopg2.extras import execute_values

from src.config import get_connection


def fetch_players() -> list[dict[str, Any]]:
    return nba_players.get_players()


def upsert_players(players: list[dict[str, Any]]) -> int:
    rows = [
        (
            int(player["id"]),
            player["full_name"],
            player.get("first_name"),
            player.get("last_name"),
            bool(player.get("is_active")),
        )
        for player in players
    ]

    sql = """
        INSERT INTO core.players (
            player_id,
            full_name,
            first_name,
            last_name,
            is_active
        )
        VALUES %s
        ON CONFLICT (player_id)
        DO UPDATE SET
            full_name = EXCLUDED.full_name,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            is_active = EXCLUDED.is_active,
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


def main() -> None:
    players = fetch_players()
    loaded_count = upsert_players(players)

    print(f"Fetched {len(players)} NBA players")
    print(f"Upserted {loaded_count} rows into core.players")


if __name__ == "__main__":
    main()