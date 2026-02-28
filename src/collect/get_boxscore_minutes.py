"""
Fetch official player minutes from NBA box score for a single game.

Usage:
  python src/collect/get_boxscore_minutes.py <game_id> [player_id] [--no-write-db]

Examples:
  python src/collect/get_boxscore_minutes.py 0022500498
  python src/collect/get_boxscore_minutes.py 0022500498 1630162
  python src/collect/get_boxscore_minutes.py 0022500498 1630162 --no-write-db
"""

import os
import sys
import requests
import psycopg2
from dotenv import load_dotenv
from nba_api.stats.endpoints import boxscoretraditionalv3

load_dotenv()

DEFAULT_PLAYER_ID = 1630162  # Anthony Edwards
BOX_TABLE = "player_boxscore_minutes"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", "5433"))

_HEADERS_PATCHED = False


def _patch_requests_headers():
    global _HEADERS_PATCHED
    if _HEADERS_PATCHED:
        return
    _HEADERS_PATCHED = True

    _orig_request = requests.sessions.Session.request

    def _patched_request(self, method, url, **kwargs):
        headers = kwargs.pop("headers", {}) or {}
        headers.setdefault(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        )
        headers.setdefault("Referer", "https://www.nba.com/")
        headers.setdefault("Origin", "https://www.nba.com")
        headers.setdefault("Accept", "application/json, text/plain, */*")
        headers.setdefault("Accept-Language", "en-US,en;q=0.9")
        kwargs["headers"] = headers
        return _orig_request(self, method, url, **kwargs)

    requests.sessions.Session.request = _patched_request


def _pick_col(columns, *candidates):
    for c in candidates:
        if c in columns:
            return c
    return None


def _minutes_to_seconds(minutes_text: str) -> int:
    """
    Convert minute strings into whole seconds.
    Supports 'MM:SS', 'PTMMMSS.SS', and 'PTSS.SS'.
    """
    text = str(minutes_text).strip()
    if not text:
        return 0

    if ":" in text:
        mins, secs = text.split(":", 1)
        return int(mins) * 60 + int(float(secs))

    if text.startswith("PT") and text.endswith("S"):
        body = text[2:-1]
        if "M" in body:
            mins, secs = body.split("M", 1)
            return int(mins) * 60 + int(float(secs))
        return int(float(body))

    return int(float(text))


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


def ensure_boxscore_minutes_table():
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {BOX_TABLE} (
        game_id VARCHAR(20) NOT NULL,
        player_id BIGINT NOT NULL,
        player_name TEXT,
        minutes_text VARCHAR(20) NOT NULL,
        minutes_seconds INTEGER NOT NULL,
        source VARCHAR(50) DEFAULT 'boxscoretraditionalv3',
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (game_id, player_id)
    );
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def upsert_boxscore_minutes(game_id: str, player_id: int, player_name: str, minutes_text: str, minutes_seconds: int):
    sql = f"""
    INSERT INTO {BOX_TABLE} (
        game_id,
        player_id,
        player_name,
        minutes_text,
        minutes_seconds,
        source
    )
    VALUES (%s, %s, %s, %s, %s, 'boxscoretraditionalv3')
    ON CONFLICT (game_id, player_id)
    DO UPDATE SET
        player_name = EXCLUDED.player_name,
        minutes_text = EXCLUDED.minutes_text,
        minutes_seconds = EXCLUDED.minutes_seconds,
        source = EXCLUDED.source,
        loaded_at = CURRENT_TIMESTAMP;
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (game_id, int(player_id), str(player_name), str(minutes_text), int(minutes_seconds)))
        conn.commit()
    finally:
        conn.close()


def fetch_player_minutes(game_id: str, player_id: int):
    _patch_requests_headers()
    box = boxscoretraditionalv3.BoxScoreTraditionalV3(
        game_id=game_id,
        timeout=90,
        get_request=True,
    )

    # nba_api shape can vary by version, so handle both patterns.
    if hasattr(box, "player_stats"):
        df = box.player_stats.get_data_frame()
    else:
        df = box.get_data_frames()[0]

    pid_col = _pick_col(df.columns, "personId", "PLAYER_ID", "playerId")
    min_col = _pick_col(df.columns, "minutes", "MIN", "min")
    name_col = _pick_col(df.columns, "name", "playerName", "PLAYER_NAME")

    if pid_col is None or min_col is None:
        raise RuntimeError(f"Could not find expected columns. Got: {list(df.columns)}")

    row = df[df[pid_col] == int(player_id)]
    if row.empty:
        raise RuntimeError(f"player_id={player_id} not found in box score for game_id={game_id}")

    r = row.iloc[0]
    player_name = r[name_col] if name_col else str(player_id)
    return player_name, r[min_col], df


def main():
    args = [a.strip() for a in sys.argv[1:]]
    write_db = True
    if "--no-write-db" in args:
        write_db = False
        args = [a for a in args if a != "--no-write-db"]

    if len(args) not in (1, 2):
        print("Usage: python src/collect/get_boxscore_minutes.py <game_id> [player_id] [--no-write-db]")
        sys.exit(1)

    game_id = args[0]
    player_id = int(args[1]) if len(args) == 2 else DEFAULT_PLAYER_ID

    player_name, minutes, _ = fetch_player_minutes(game_id, player_id)
    minutes_seconds = _minutes_to_seconds(str(minutes))

    print(f"game_id={game_id}")
    print(f"player_id={player_id}")
    print(f"player_name={player_name}")
    print(f"official_boxscore_minutes={minutes}")
    print(f"official_boxscore_minutes_seconds={minutes_seconds}")

    if write_db:
        ensure_boxscore_minutes_table()
        upsert_boxscore_minutes(game_id, player_id, player_name, str(minutes), minutes_seconds)
        print(f"db_write=ok table={BOX_TABLE}")
    else:
        print("db_write=skipped (--no-write-db)")


if __name__ == "__main__":
    main()
