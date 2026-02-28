# src/collect/load_players.py
"""
Populate players dimension table using nba_api static players list.

Run:
  python src/collect/load_players.py
"""

import os
import psycopg2
from dotenv import load_dotenv
from nba_api.stats.static import players as nba_players_static

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", "5433"))

# -----------------------------
# DB
# -----------------------------
def get_connection():
    missing = [k for k in ["DB_NAME", "DB_USER", "DB_PASSWORD"] if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing env vars in .env: {missing}")

    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT,
    )

def ensure_players_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS players (
                player_id BIGINT PRIMARY KEY,
                full_name TEXT NOT NULL,
                first_name TEXT,
                last_name TEXT,
                is_active BOOLEAN,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
    conn.commit()

# -----------------------------
# Load players
# -----------------------------
def load_players():
    conn = get_connection()
    ensure_players_table(conn)
    cur = conn.cursor()

    nba_players = nba_players_static.get_players()
    print(f"Fetched {len(nba_players)} players from nba_api.")

    inserted = 0

    for p in nba_players:
        cur.execute(
            """
            INSERT INTO players (
                player_id,
                full_name,
                first_name,
                last_name,
                is_active
            )
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (player_id) DO UPDATE SET
                full_name = EXCLUDED.full_name,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                is_active = EXCLUDED.is_active;
            """,
            (
                p["id"],
                p["full_name"],
                p["first_name"],
                p["last_name"],
                p["is_active"],
            ),
        )
        inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Upserted {inserted} players into the players table.")

# -----------------------------
# CLI
# -----------------------------
if __name__ == "__main__":
    load_players()