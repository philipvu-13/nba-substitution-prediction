import os
import time
import sys
import pandas as pd
import psycopg2
import requests
import random
import json
import pathlib
from urllib3.exceptions import ReadTimeoutError
from dotenv import load_dotenv
from nba_api.stats.endpoints import leaguegamefinder

# Load environment variables
load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

TIMBERWOLVES_TEAM_ID = 1610612750  # Minnesota Timberwolves

# Simple cache directory in project root
CACHE_DIR = os.path.join(os.getcwd(), ".cache")
os.makedirs(CACHE_DIR, exist_ok=True)

# Patch requests.Session.request once to add browser-like headers so stats.nba.com
# treats our program more like a real browser (reduces blocking/rate-limit issues).
_HEADERS_PATCHED = False
def _patch_requests_headers():
    global _HEADERS_PATCHED
    if _HEADERS_PATCHED:
        return
    _HEADERS_PATCHED = True

    _orig_request = requests.sessions.Session.request

    def _patched_request(self, method, url, **kwargs):
        headers = kwargs.pop("headers", {}) or {}
        # add common browser headers, but allow explicit overrides by callers
        headers.setdefault(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        )
        headers.setdefault("Referer", "https://www.nba.com/")
        headers.setdefault("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        headers.setdefault("Accept-Language", "en-US,en;q=0.9")
        kwargs["headers"] = headers
        return _orig_request(self, method, url, **kwargs)

    requests.sessions.Session.request = _patched_request

# apply patch eagerly
_patch_requests_headers()


def get_team_games(season="2025-26", max_retries=3, backoff_factor=2, force_refresh=False, cache_ttl=3600):
    print(f"Fetching Timberwolves games for season {season}...")

    cache_path = os.path.join(CACHE_DIR, f"games_{season}.pkl")
    # return cached DataFrame when fresh
    if not force_refresh and os.path.exists(cache_path):
        mtime = os.path.getmtime(cache_path)
        if (time.time() - mtime) < cache_ttl:
            try:
                print(f"Using cached games for {season} (cached {(time.time()-mtime):.0f}s ago)")
                return pd.read_pickle(cache_path)
            except Exception:
                print("Failed to read cache; refetching.")

    for attempt in range(1, max_retries + 1):
        try:
            gamefinder = leaguegamefinder.LeagueGameFinder(
                team_id_nullable=TIMBERWOLVES_TEAM_ID,
                season_nullable=season
            )

            games_df = gamefinder.get_data_frames()[0]
            # save to cache (best-effort)
            try:
                games_df.to_pickle(cache_path)
            except Exception:
                pass
            return games_df

        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError, ReadTimeoutError) as e:
            raw_wait = backoff_factor ** (attempt - 1)
            wait = min(raw_wait, 60) * random.uniform(0.8, 1.2)
            print(f"Request failed (attempt {attempt}/{max_retries}): {e}. Retrying in {wait:.1f}s...")
            if attempt == max_retries:
                print("Max retries reached; returning empty result instead of crashing.")
                return pd.DataFrame()
            time.sleep(wait)


def insert_games_into_db(games_df):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    inserted_count = 0

    for _, row in games_df.iterrows():
        game_id = row["GAME_ID"]
        game_date = row["GAME_DATE"]
        matchup = row["MATCHUP"]
        season = row["SEASON_ID"]

        home_team = matchup.split(" vs. ")[0] if "vs." in matchup else matchup.split(" @ ")[1]
        away_team = matchup.split(" @ ")[0] if "@" in matchup else matchup.split(" vs. ")[1]

        cur.execute("""
            INSERT INTO games (game_id, game_date, season, home_team, away_team, matchup)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (game_id) DO NOTHING;
        """, (
            game_id,
            game_date,
            season,
            home_team,
            away_team,
            matchup
        ))

        if cur.rowcount > 0:
            inserted_count += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Inserted {inserted_count} new games.")


if __name__ == "__main__":
    season = "2025-26"   # Change as needed
    games_df = get_team_games(season)

    if games_df is None or (hasattr(games_df, 'empty') and games_df.empty):
        print("No games fetched (timeout or network issue). Exiting without DB updates.")
        sys.exit(0)

    insert_games_into_db(games_df)
