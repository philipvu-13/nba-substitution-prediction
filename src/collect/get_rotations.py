# src/collect/get_rotations.py
"""
Use GameRotation to store reliable player stints.
This version:
- concatenates ALL returned dataframes (so you don’t accidentally load only one team)
- filters to Minnesota Timberwolves ONLY (team_id = 1610612750)
"""

import sys
import os
import time
import random
import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv
from nba_api.stats.endpoints import gamerotation

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", "5433"))

MIN_TEAM_ID = 1610612750  # Timberwolves

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

def ensure_rotations_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS player_rotations (
                id BIGSERIAL PRIMARY KEY,
                game_id VARCHAR(20) NOT NULL,
                team_id BIGINT NOT NULL,
                team_city TEXT,
                team_name TEXT,
                player_id BIGINT NOT NULL,
                player_first TEXT,
                player_last TEXT,

                -- NOTE: often tenths of seconds from game start (max ~28800 for regulation)
                in_time_real INTEGER NOT NULL,
                out_time_real INTEGER NOT NULL,

                player_pts DOUBLE PRECISION,
                pt_diff DOUBLE PRECISION,
                usg_pct DOUBLE PRECISION,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                CONSTRAINT uniq_rotation UNIQUE (game_id, team_id, player_id, in_time_real, out_time_real)
            );
            """
        )
        # Backfill columns for existing tables created before these fields were added.
        cur.execute("ALTER TABLE player_rotations ADD COLUMN IF NOT EXISTS team_city TEXT;")
        cur.execute("ALTER TABLE player_rotations ADD COLUMN IF NOT EXISTS team_name TEXT;")
        cur.execute("ALTER TABLE player_rotations ADD COLUMN IF NOT EXISTS player_first TEXT;")
        cur.execute("ALTER TABLE player_rotations ADD COLUMN IF NOT EXISTS player_last TEXT;")
        cur.execute("ALTER TABLE player_rotations ADD COLUMN IF NOT EXISTS player_pts DOUBLE PRECISION;")
        cur.execute("ALTER TABLE player_rotations ADD COLUMN IF NOT EXISTS pt_diff DOUBLE PRECISION;")
        cur.execute("ALTER TABLE player_rotations ADD COLUMN IF NOT EXISTS usg_pct DOUBLE PRECISION;")
        # Remove legacy columns no longer needed.
        cur.execute("ALTER TABLE player_rotations DROP COLUMN IF EXISTS in_period;")
        cur.execute("ALTER TABLE player_rotations DROP COLUMN IF EXISTS out_period;")
        cur.execute("ALTER TABLE player_rotations DROP COLUMN IF EXISTS in_clock;")
        cur.execute("ALTER TABLE player_rotations DROP COLUMN IF EXISTS out_clock;")
    conn.commit()

# -----------------------------
# Make stats.nba.com less flaky
# -----------------------------
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

_patch_requests_headers()

# -----------------------------
# Helpers
# -----------------------------
def _col(df: pd.DataFrame, *names: str):
    for n in names:
        if n in df.columns:
            return n
    return None

def _clean(x):
    return None if (x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x)) else x

# -----------------------------
# Fetch (GameRotation)
# -----------------------------
def fetch_game_rotation_min_only(game_id: str, max_retries: int = 5) -> pd.DataFrame:
    base_sleep = 2

    for attempt in range(1, max_retries + 1):
        try:
            time.sleep(random.uniform(0.5, 1.25))

            rot = gamerotation.GameRotation(game_id=game_id, timeout=90, get_request=True)
            dfs = rot.get_data_frames()

            # keep all non-empty frames and concatenate
            frames = [d for d in dfs if d is not None and not d.empty]
            if not frames:
                raise RuntimeError("GameRotation returned no non-empty DataFrames.")

            df_all = pd.concat(frames, ignore_index=True)

            # detect team column and filter to Minnesota only
            team_col = _col(df_all, "TEAM_ID", "teamId")
            if team_col is None:
                raise RuntimeError(f"Could not find TEAM_ID column. Columns: {df_all.columns.tolist()}")

            df_min = df_all[df_all[team_col] == MIN_TEAM_ID].copy()

            if df_min.empty:
                teams = sorted(pd.unique(df_all[team_col].dropna()).tolist())
                raise RuntimeError(
                    f"No Minnesota rows found for game_id={game_id}. "
                    f"Teams present in rotation df: {teams}"
                )

            return df_min

        except Exception as e:
            if attempt == max_retries:
                raise
            sleep = min(base_sleep * (2 ** (attempt - 1)), 60) * random.uniform(0.8, 1.2)
            print(f"[Attempt {attempt}/{max_retries}] GameRotation fetch failed: {e}")
            print(f"Retrying in {sleep:.1f}s...")
            time.sleep(sleep)

# -----------------------------
# Insert rotations
# -----------------------------
def insert_rotations(game_id: str, df: pd.DataFrame):
    conn = get_connection()
    ensure_rotations_table(conn)
    cur = conn.cursor()

    print("GameRotation (MIN only) rows:", len(df))
    print("Columns:", df.columns.tolist())

    game_col   = _col(df, "GAME_ID", "gameId")
    team_col   = _col(df, "TEAM_ID", "teamId")
    team_city_col = _col(df, "TEAM_CITY", "teamCity")
    team_name_col = _col(df, "TEAM_NAME", "teamName")
    player_col = _col(df, "PLAYER_ID", "playerId", "PERSON_ID", "personId")
    player_first_col = _col(df, "PLAYER_FIRST", "playerFirst")
    player_last_col = _col(df, "PLAYER_LAST", "playerLast")

    in_real_col  = _col(df, "IN_TIME_REAL", "inTimeReal", "IN_TIME_SECONDS", "inTimeSeconds", "IN_TIME")
    out_real_col = _col(df, "OUT_TIME_REAL", "outTimeReal", "OUT_TIME_SECONDS", "outTimeSeconds", "OUT_TIME")
    player_pts_col = _col(df, "PLAYER_PTS", "playerPts")
    pt_diff_col = _col(df, "PT_DIFF", "ptDiff")
    usg_pct_col = _col(df, "USG_PCT", "usgPct")

    if player_col is None or in_real_col is None or out_real_col is None:
        raise RuntimeError(
            f"Missing required rotation columns. Columns: {df.columns.tolist()}"
        )

    inserted = 0

    for _, row in df.iterrows():
        db_game_id = _clean(row.get(game_col)) if game_col else game_id
        team_id = _clean(row.get(team_col)) if team_col else None
        team_city = _clean(row.get(team_city_col)) if team_city_col else None
        team_name = _clean(row.get(team_name_col)) if team_name_col else None
        player_id = _clean(row.get(player_col))
        player_first = _clean(row.get(player_first_col)) if player_first_col else None
        player_last = _clean(row.get(player_last_col)) if player_last_col else None
        in_time_real = _clean(row.get(in_real_col))
        out_time_real = _clean(row.get(out_real_col))
        player_pts = _clean(row.get(player_pts_col)) if player_pts_col else None
        pt_diff = _clean(row.get(pt_diff_col)) if pt_diff_col else None
        usg_pct = _clean(row.get(usg_pct_col)) if usg_pct_col else None

        cur.execute(
            """
            INSERT INTO player_rotations (
                game_id,
                team_id,
                team_city,
                team_name,
                player_id,
                player_first,
                player_last,
                in_time_real,
                out_time_real,
                player_pts,
                pt_diff,
                usg_pct
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT ON CONSTRAINT uniq_rotation DO NOTHING;
            """,
            (
                str(db_game_id),
                team_id,
                team_city,
                team_name,
                player_id,
                player_first,
                player_last,
                in_time_real,
                out_time_real,
                player_pts,
                pt_diff,
                usg_pct,
            ),
        )

        if cur.rowcount > 0:
            inserted += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted {inserted} Minnesota rotation rows into player_rotations for game_id={game_id}.")

# -----------------------------
# CLI
# -----------------------------
def main():
    if len(sys.argv) != 2:
        print("Usage: python src/collect/get_rotations.py <game_id>")
        sys.exit(1)

    game_id = sys.argv[1]
    print(f"Fetching GameRotation (Minnesota only) for game_id={game_id}...")

    df = fetch_game_rotation_min_only(game_id)
    print(f"Fetched {len(df)} MIN rotation rows. Inserting...")

    insert_rotations(game_id, df)
    print("Done.")

if __name__ == "__main__":
    main()
