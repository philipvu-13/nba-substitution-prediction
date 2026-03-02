import os
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", "5433"))
WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")

def fmt(sec):
    sec = int(round(sec or 0))
    return f"{sec//60}:{sec%60:02d}"

def get_conn():
    return psycopg2.connect(
        host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=DB_PORT
    )

if not WEBHOOK:
    raise RuntimeError("Missing DISCORD_WEBHOOK_URL in .env")

conn = get_conn()
try:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT player_name, avg_first_rest_q1_seconds_left
            FROM edwards_rotation_profile_v
            LIMIT 1;
        """)
        row = cur.fetchone()
        if not row:
            raise RuntimeError("No row in edwards_rotation_profile_v")

        player_name, avg_q1 = row

        cur.execute("""
            WITH last_game AS (
                SELECT s.game_id
                FROM player_stints s
                JOIN games g ON g.game_id = s.game_id
                LEFT JOIN excluded_games eg ON eg.game_id = s.game_id
                WHERE eg.game_id IS NULL
                ORDER BY g.game_date DESC, s.game_id DESC
                LIMIT 1
            ),
            first_q1 AS (
                SELECT s.sub_out_time
                FROM player_stints s
                JOIN last_game lg ON lg.game_id = s.game_id
                WHERE s.quarter = 1
                ORDER BY s.sub_in_time ASC
                LIMIT 1
            )
            SELECT 720 - sub_out_time AS tonight_q1_rest_left
            FROM first_q1;
        """)
        row2 = cur.fetchone()
        if not row2:
            raise RuntimeError("No Q1 stint found for latest game")
        tonight = row2[0]
finally:
    conn.close()

delta = tonight - avg_q1
if delta > 10:
    verdict = "Longer than normal."
elif delta < -10:
    verdict = "Shorter than normal."
else:
    verdict = "About normal."

msg = (
    f"{player_name} average first rest: {fmt(avg_q1)} Q1\n"
    f"Tonight: {fmt(tonight)} Q1\n"
    f"{verdict}"
)

r = requests.post(WEBHOOK, json={"content": msg}, timeout=20)
r.raise_for_status()
print("Discord notification sent.")
