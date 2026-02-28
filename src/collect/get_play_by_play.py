# src/collect/get_play_by_play.py
import sys
import os
import time
import random
import pandas as pd
import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv
from nba_api.stats.endpoints import playbyplayv3

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

# -----------------------------
# Make stats.nba.com less flaky
# -----------------------------
_HEADERS_PATCHED = False
def _patch_requests_headers():
    """
    nba_api uses requests under the hood. stats.nba.com is picky.
    Patch requests to include browser-like headers unless the caller overrides.
    """
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
# V3 expected raw columns (exact names from nba_api PlayByPlayV3)
# -----------------------------
V3_PBP_COLS = [
    "gameId",
    "actionNumber",
    "clock",
    "period",
    "teamId",
    "teamTricode",
    "personId",
    "playerName",
    "playerNameI",
    "xLegacy",
    "yLegacy",
    "shotDistance",
    "shotResult",
    "isFieldGoal",
    "scoreHome",
    "scoreAway",
    "pointsTotal",
    "location",
    "description",
    "actionType",
    "subType",
    "videoAvailable",
    "actionId",
]

RAW_TABLE = "play_by_play_v3_raw"

# -----------------------------
# Fetch (V3)
# -----------------------------
def fetch_play_by_play_v3(game_id: str, max_retries: int = 5) -> pd.DataFrame:
    """
    Pull V3 PBP via nba_api with retries/backoff.
    Returns a DataFrame with the PlayByPlay dataset.
    """
    base_sleep = 2

    for attempt in range(1, max_retries + 1):
        try:
            time.sleep(random.uniform(0.5, 1.25))

            pbp = playbyplayv3.PlayByPlayV3(
                game_id=game_id,
                timeout=90,
                get_request=True,
            )

            # nba_api returns datasets in order; dataset 0 is PlayByPlay for this endpoint
            df = pbp.get_data_frames()[0]
            if df is None or df.empty:
                raise RuntimeError("PlayByPlayV3 returned empty DataFrame.")

            return df

        except Exception as e:
            if attempt == max_retries:
                raise
            sleep = min(base_sleep * (2 ** (attempt - 1)), 60) * random.uniform(0.8, 1.2)
            print(f"[Attempt {attempt}/{max_retries}] V3 fetch failed: {e}")
            print(f"Retrying in {sleep:.1f}s...")
            time.sleep(sleep)

# -----------------------------
# Ensure raw table exists (columns match API names)
# IMPORTANT: we use quoted identifiers to preserve camelCase column names.
# -----------------------------
def ensure_raw_table():
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
        "gameId" VARCHAR(20) NOT NULL,
        "actionNumber" INTEGER NOT NULL,
        "clock" VARCHAR(20),
        "period" INTEGER,
        "teamId" BIGINT,
        "teamTricode" VARCHAR(10),
        "personId" BIGINT,
        "playerName" TEXT,
        "playerNameI" TEXT,
        "xLegacy" DOUBLE PRECISION,
        "yLegacy" DOUBLE PRECISION,
        "shotDistance" DOUBLE PRECISION,
        "shotResult" TEXT,
        "isFieldGoal" BOOLEAN,
        "scoreHome" INTEGER,
        "scoreAway" INTEGER,
        "pointsTotal" INTEGER,
        "location" TEXT,
        "description" TEXT,
        "actionType" TEXT,
        "subType" TEXT,
        "videoAvailable" BOOLEAN,
        "actionId" BIGINT,

        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        CONSTRAINT {RAW_TABLE}_uniq UNIQUE ("gameId", "actionId")
    );
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()

# -----------------------------
# Insert raw rows exactly as returned by API
# -----------------------------
def _coerce_value(col: str, val):
    """
    Clean up pandas/numpy nulls into Python None so psycopg2 inserts NULLs.
    Minimal coercion (we're keeping things raw).
    """
    if pd.isna(val):
        return None

    # Booleans sometimes come through as 0/1 or True/False
    if col in ("isFieldGoal", "videoAvailable"):
        if isinstance(val, (bool,)):
            return val
        if str(val).strip().lower() in ("1", "true", "t", "yes", "y"):
            return True
        if str(val).strip().lower() in ("0", "false", "f", "no", "n"):
            return False
        return None

    # Integers: keep as int if possible, otherwise None
    if col in ("actionNumber", "period", "scoreHome", "scoreAway", "pointsTotal"):
        try:
            return int(val)
        except Exception:
            return None

    # BIGINT-ish ids
    if col in ("teamId", "personId", "actionId"):
        try:
            return int(val)
        except Exception:
            return None

    # Floats
    if col in ("xLegacy", "yLegacy", "shotDistance"):
        try:
            return float(val)
        except Exception:
            return None

    # Everything else: keep as string/text-ish
    return val

def insert_raw_play_by_play(df_v3: pd.DataFrame) -> int:
    """
    Bulk insert into play_by_play_v3_raw using exact API columns.
    Returns number of rows inserted (best-effort; ON CONFLICT ignores duplicates).
    """
    # Make sure all expected columns exist (if nba_api changes, we won't crash)
    for c in V3_PBP_COLS:
        if c not in df_v3.columns:
            df_v3[c] = None

    # Keep only the raw cols in the exact order
    df = df_v3[V3_PBP_COLS].copy()

    # Prevent duplicates within the same bulk insert: if the API produced
    # multiple rows with the same (gameId, actionNumber) we'll keep the
    # last occurrence and drop earlier ones. This avoids the
    # "ON CONFLICT DO UPDATE command cannot affect row a second time" error
    # when psycopg2 executes many rows in a single VALUES batch.
    original_count = len(df)
    # Deduplicate by (gameId, actionId) where possible. actionId is the
    # API's stable identifier for the action and avoids collapsing distinct
    # rows that share the same actionNumber (which can happen for sub-events).
    if "gameId" in df.columns and "actionId" in df.columns:
        df = df.drop_duplicates(subset=["gameId", "actionId"], keep="last")
    elif "actionId" in df.columns:
        df = df.drop_duplicates(subset=["actionId"], keep="last")
    else:
        df = df.drop_duplicates(subset=["actionNumber"], keep="last")
    deduped_count = len(df)
    if deduped_count != original_count:
        print(f"Deduplicated rows: {original_count} -> {deduped_count} (kept last occurrence per key)")

    rows = []
    for _, r in df.iterrows():
        rows.append([_coerce_value(col, r[col]) for col in V3_PBP_COLS])

    if not rows:
        return 0

    # Quoted column list to preserve camelCase
    cols_sql = ", ".join([f'"{c}"' for c in V3_PBP_COLS])
    # Build DO UPDATE clause to ensure DB rows reflect latest API values
    update_cols = [c for c in V3_PBP_COLS if c not in ("gameId", "actionId")]
    update_sql = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])
    insert_sql = f"""
        INSERT INTO {RAW_TABLE} ({cols_sql})
        VALUES %s
        ON CONFLICT ("gameId", "actionId") DO UPDATE SET
        {update_sql};
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                insert_sql,
                rows,
                page_size=1000
            )
        conn.commit()

        # NOTE: psycopg2 can't reliably tell "inserted row count" with execute_values + ON CONFLICT
        # We'll return the number of attempted rows; duplicates are just ignored.
        return len(rows)
    finally:
        conn.close()

# -----------------------------
# CLI
# -----------------------------
def main():
    if len(sys.argv) != 2:
        print("Usage: python src/collect/get_play_by_play.py <game_id>")
        sys.exit(1)

    game_id = sys.argv[1]
    print(f"Ensuring raw table exists: {RAW_TABLE} ...")
    ensure_raw_table()

    print(f"Fetching V3 play-by-play for game_id={game_id} ...")
    df = fetch_play_by_play_v3(game_id)
    print(f"Fetched {len(df)} events. Inserting raw rows...")

    attempted = insert_raw_play_by_play(df)
    print(f"Attempted insert of {attempted} rows into {RAW_TABLE} (duplicates auto-ignored).")
    print("Done.")

if __name__ == "__main__":
    main()