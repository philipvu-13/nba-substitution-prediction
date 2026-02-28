# src/collect/backfill_season.py
"""
Backfill a whole season of Timberwolves games into play_by_play_v3_raw.

What it does:
1) Fetch games for a season (via get_minnesota_games.py) and insert into games table
2) Ensure play_by_play_v3_raw exists
3) Loop game_ids for that season and load V3 play-by-play into play_by_play_v3_raw
4) Skip games already loaded (unless you force reload)
5) Log successes/failures without crashing the whole run
"""

import os
import sys
import time
import random
from typing import List, Optional, Tuple

# Allow running as: python src/collect/backfill_season.py
# even if cwd is repo root (so imports resolve).
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(THIS_DIR, "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# Your existing modules
from src.collect.get_minnesota_games import get_team_games, insert_games_into_db  # noqa: E402
from src.collect.get_play_by_play import (  # noqa: E402
    get_connection,
    ensure_raw_table,
    fetch_play_by_play_v3,
    insert_raw_play_by_play,
    RAW_TABLE,
)

# -----------------------------
# Helpers
# -----------------------------
def _fetch_game_ids_for_season(season: str) -> List[str]:
    """
    Pull game_ids from `games` table for the given season.
    NOTE: your games table stores season from LeagueGameFinder SEASON_ID (e.g., '22025').
    If you insert 'SEASON_ID' values, then season filter must match those.
    We'll handle both:
      - if user passes '2025-26' => we try to convert to '22025' (NBA season id format)
      - otherwise we also try exact match on season column
    """
    season_id_guess = _season_to_season_id_guess(season)

    sql = """
        SELECT game_id
        FROM games
        WHERE season = %s OR season = %s
        ORDER BY game_date ASC;
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (season, season_id_guess))
            rows = cur.fetchall()
            return [r[0] for r in rows]
    finally:
        conn.close()


def _season_to_season_id_guess(season: str) -> str:
    """
    Convert '2025-26' -> '22025' (NBA stats SEASON_ID format for regular season).
    If it doesn't match that pattern, return as-is.
    """
    try:
        # season like '2025-26'
        start_year = int(season.split("-")[0])
        return f"2{start_year}"
    except Exception:
        return season


def _pbp_already_loaded(game_id: str) -> bool:
    """
    True if RAW_TABLE already has at least one row for this game.
    """
    sql = f'SELECT 1 FROM {RAW_TABLE} WHERE "gameId" = %s LIMIT 1;'
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (game_id,))
            return cur.fetchone() is not None
    finally:
        conn.close()


def _count_pbp_rows(game_id: str) -> int:
    """
    Return count of rows for this game in RAW_TABLE (for sanity/logging).
    """
    sql = f'SELECT COUNT(*) FROM {RAW_TABLE} WHERE "gameId" = %s;'
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (game_id,))
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _delete_pbp_for_game(game_id: str) -> int:
    """
    Deletes existing raw rows for a game (only used when force_reload_pbp=True).
    Returns deleted row count.
    """
    sql = f'DELETE FROM {RAW_TABLE} WHERE "gameId" = %s;'
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (game_id,))
            deleted = cur.rowcount
        conn.commit()
        return deleted
    finally:
        conn.close()


def _sleep_jitter(min_s: float, max_s: float) -> None:
    time.sleep(random.uniform(min_s, max_s))


def _parse_args(argv: List[str]) -> dict:
    """
    Minimal arg parsing (no external deps).
    Usage:
      python src/collect/backfill_season.py --season 2025-26
      python src/collect/backfill_season.py --season 2025-26 --force-reload-pbp
      python src/collect/backfill_season.py --season 2025-26 --limit 10
      python src/collect/backfill_season.py --season 2025-26 --start-index 25
    """
    args = {
        "season": "2025-26",
        "force_refresh_games": False,
        "force_reload_pbp": False,
        "limit": None,
        "start_index": 0,
        "sleep_min": 0.8,
        "sleep_max": 2.0,
        "max_failures": 25,
    }

    i = 1
    while i < len(argv):
        a = argv[i].strip()
        if a == "--season":
            i += 1
            args["season"] = argv[i]
        elif a == "--force-refresh-games":
            args["force_refresh_games"] = True
        elif a == "--force-reload-pbp":
            args["force_reload_pbp"] = True
        elif a == "--limit":
            i += 1
            args["limit"] = int(argv[i])
        elif a == "--start-index":
            i += 1
            args["start_index"] = int(argv[i])
        elif a == "--sleep-min":
            i += 1
            args["sleep_min"] = float(argv[i])
        elif a == "--sleep-max":
            i += 1
            args["sleep_max"] = float(argv[i])
        elif a == "--max-failures":
            i += 1
            args["max_failures"] = int(argv[i])
        elif a in ("-h", "--help"):
            print(
                "Usage: python src/collect/backfill_season.py [options]\n\n"
                "Options:\n"
                "  --season <YYYY-YY>          Season to backfill (default: 2025-26)\n"
                "  --force-refresh-games       Ignore cached games_*.pkl and refetch\n"
                "  --force-reload-pbp          Delete and reload play-by-play per game\n"
                "  --start-index <N>           Start at Nth game in season list\n"
                "  --limit <N>                 Only process N games\n"
                "  --sleep-min <float>         Min jitter sleep between games (default: 0.8)\n"
                "  --sleep-max <float>         Max jitter sleep between games (default: 2.0)\n"
                "  --max-failures <N>          Stop after N failures (default: 25)\n"
            )
            sys.exit(0)
        else:
            print(f"Unknown arg: {a} (use --help)")
            sys.exit(1)
        i += 1

    if args["sleep_max"] < args["sleep_min"]:
        args["sleep_max"] = args["sleep_min"] + 0.1

    return args


# -----------------------------
# Main
# -----------------------------
def main():
    args = _parse_args(sys.argv)

    season = args["season"]
    force_refresh_games = args["force_refresh_games"]
    force_reload_pbp = args["force_reload_pbp"]
    start_index = args["start_index"]
    limit = args["limit"]
    sleep_min = args["sleep_min"]
    sleep_max = args["sleep_max"]
    max_failures = args["max_failures"]

    print("=" * 80)
    print(f"Backfill season: {season}")
    print(f"RAW table: {RAW_TABLE}")
    print(f"force_refresh_games={force_refresh_games} force_reload_pbp={force_reload_pbp}")
    print(f"start_index={start_index} limit={limit} sleep=[{sleep_min},{sleep_max}] max_failures={max_failures}")
    print("=" * 80)

    # 1) Get games and insert into DB
    games_df = get_team_games(season=season, force_refresh=force_refresh_games)
    if games_df is None or (hasattr(games_df, "empty") and games_df.empty):
        print("No games fetched (timeout/network). Exiting without backfill.")
        sys.exit(0)

    insert_games_into_db(games_df)

    # 2) Ensure raw table exists once
    print(f"Ensuring raw table exists: {RAW_TABLE} ...")
    ensure_raw_table()

    # 3) Pull game_ids for that season from DB
    game_ids = _fetch_game_ids_for_season(season)
    if not game_ids:
        print("No games found in DB for this season after insert. Check your `games.season` values.")
        sys.exit(1)

    # apply start/limit
    game_ids = game_ids[start_index:]
    if limit is not None:
        game_ids = game_ids[:limit]

    total = len(game_ids)
    print(f"Will process {total} games.")

    successes: List[Tuple[str, int]] = []
    failures: List[Tuple[str, str]] = []
    skipped: List[str] = []

    for idx, game_id in enumerate(game_ids, start=1):
        print("-" * 80)
        print(f"[{idx}/{total}] game_id={game_id}")

        try:
            if _pbp_already_loaded(game_id) and not force_reload_pbp:
                existing = _count_pbp_rows(game_id)
                print(f"SKIP: already loaded ({existing} rows).")
                skipped.append(game_id)
                _sleep_jitter(sleep_min, sleep_max)
                continue

            if force_reload_pbp and _pbp_already_loaded(game_id):
                deleted = _delete_pbp_for_game(game_id)
                print(f"force_reload_pbp=True -> deleted {deleted} existing rows for {game_id}")

            # fetch + insert
            df = fetch_play_by_play_v3(game_id)
            attempted = insert_raw_play_by_play(df)
            now_rows = _count_pbp_rows(game_id)

            print(f"Fetched {len(df)} events. Attempted insert {attempted}. Rows now in DB: {now_rows}")
            successes.append((game_id, now_rows))

        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            print(f"FAIL: {msg}")
            failures.append((game_id, msg))

            if len(failures) >= max_failures:
                print(f"Stopping early: hit max_failures={max_failures}.")
                break

        # jitter between games to reduce bans/rate limits
        _sleep_jitter(sleep_min, sleep_max)

    print("\n" + "=" * 80)
    print("BACKFILL SUMMARY")
    print("=" * 80)
    print(f"Successes: {len(successes)}")
    print(f"Skipped:   {len(skipped)}")
    print(f"Failures:  {len(failures)}")

    if failures:
        print("\nFailures (game_id -> error):")
        for gid, err in failures[:50]:
            print(f"  {gid} -> {err}")
        if len(failures) > 50:
            print(f"  ... and {len(failures) - 50} more")

    print("\nDone.")


if __name__ == "__main__":
    main()