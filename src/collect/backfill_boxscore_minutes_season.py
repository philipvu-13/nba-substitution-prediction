"""
Backfill official boxscore minutes for one player across a season's games.

What it does:
1) Pull game_ids from games table for the season
2) For each game_id, fetch player's official minutes from BoxScoreTraditionalV3
3) Upsert into player_boxscore_minutes
4) Skip already-loaded rows unless --force-reload
"""

import os
import sys
import time
import random
from typing import List, Tuple

# Allow running as: python src/collect/backfill_boxscore_minutes_season.py
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(THIS_DIR, "..", ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.collect.get_boxscore_minutes import (  # noqa: E402
    BOX_TABLE,
    DEFAULT_PLAYER_ID,
    ensure_boxscore_minutes_table,
    fetch_player_minutes,
    get_connection,
    upsert_boxscore_minutes,
    _minutes_to_seconds,
)


def _season_to_season_id_guess(season: str) -> str:
    try:
        start_year = int(season.split("-")[0])
        return f"2{start_year}"
    except Exception:
        return season


def _fetch_game_ids_for_season(season: str) -> List[str]:
    season_guess = _season_to_season_id_guess(season)
    sql = """
        SELECT game_id
        FROM games
        WHERE season = %s OR season = %s
        ORDER BY game_date ASC;
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (season, season_guess))
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def _already_loaded(game_id: str, player_id: int) -> bool:
    sql = f"SELECT 1 FROM {BOX_TABLE} WHERE game_id = %s AND player_id = %s LIMIT 1;"
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (game_id, player_id))
            return cur.fetchone() is not None
    finally:
        conn.close()


def _delete_row(game_id: str, player_id: int) -> int:
    sql = f"DELETE FROM {BOX_TABLE} WHERE game_id = %s AND player_id = %s;"
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (game_id, player_id))
            deleted = cur.rowcount
        conn.commit()
        return deleted
    finally:
        conn.close()


def _parse_args(argv: List[str]) -> dict:
    args = {
        "season": "2025-26",
        "player_id": DEFAULT_PLAYER_ID,
        "force_reload": False,
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
        elif a == "--player-id":
            i += 1
            args["player_id"] = int(argv[i])
        elif a == "--force-reload":
            args["force_reload"] = True
        elif a == "--start-index":
            i += 1
            args["start_index"] = int(argv[i])
        elif a == "--limit":
            i += 1
            args["limit"] = int(argv[i])
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
                "Usage: python src/collect/backfill_boxscore_minutes_season.py [options]\n\n"
                "Options:\n"
                "  --season <YYYY-YY>       Season to backfill (default: 2025-26)\n"
                "  --player-id <id>         Target player_id (default from get_boxscore_minutes)\n"
                "  --force-reload           Delete+reload existing player/game rows\n"
                "  --start-index <N>        Start from Nth game\n"
                "  --limit <N>              Process only N games\n"
                "  --sleep-min <float>      Min sleep between requests (default: 0.8)\n"
                "  --sleep-max <float>      Max sleep between requests (default: 2.0)\n"
                "  --max-failures <N>       Stop after N failures (default: 25)\n"
            )
            sys.exit(0)
        else:
            print(f"Unknown arg: {a} (use --help)")
            sys.exit(1)
        i += 1

    if args["sleep_max"] < args["sleep_min"]:
        args["sleep_max"] = args["sleep_min"] + 0.1

    return args


def main():
    args = _parse_args(sys.argv)
    season = args["season"]
    player_id = args["player_id"]
    force_reload = args["force_reload"]
    start_index = args["start_index"]
    limit = args["limit"]
    sleep_min = args["sleep_min"]
    sleep_max = args["sleep_max"]
    max_failures = args["max_failures"]

    print("=" * 80)
    print(f"Backfill official boxscore minutes | season={season} player_id={player_id}")
    print(f"table={BOX_TABLE} force_reload={force_reload}")
    print(
        f"start_index={start_index} limit={limit} sleep=[{sleep_min},{sleep_max}] max_failures={max_failures}"
    )
    print("=" * 80)

    ensure_boxscore_minutes_table()

    game_ids = _fetch_game_ids_for_season(season)
    if not game_ids:
        print("No game_ids found for season in games table.")
        sys.exit(1)

    game_ids = game_ids[start_index:]
    if limit is not None:
        game_ids = game_ids[:limit]

    successes: List[Tuple[str, int]] = []
    skipped: List[str] = []
    failures: List[Tuple[str, str]] = []

    for idx, game_id in enumerate(game_ids, start=1):
        print("-" * 80)
        print(f"[{idx}/{len(game_ids)}] game_id={game_id}")
        try:
            if _already_loaded(game_id, player_id) and not force_reload:
                print("SKIP: already loaded.")
                skipped.append(game_id)
                time.sleep(random.uniform(sleep_min, sleep_max))
                continue

            if force_reload and _already_loaded(game_id, player_id):
                deleted = _delete_row(game_id, player_id)
                print(f"force_reload=True -> deleted {deleted} old row(s)")

            player_name, minutes_text, _ = fetch_player_minutes(game_id, player_id)
            minutes_seconds = _minutes_to_seconds(str(minutes_text))
            upsert_boxscore_minutes(
                game_id=game_id,
                player_id=player_id,
                player_name=str(player_name),
                minutes_text=str(minutes_text),
                minutes_seconds=int(minutes_seconds),
            )

            print(
                f"UPSERT ok: player_name={player_name} "
                f"minutes_text={minutes_text} minutes_seconds={minutes_seconds}"
            )
            successes.append((game_id, int(minutes_seconds)))

        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            print(f"FAIL: {msg}")
            failures.append((game_id, msg))
            if len(failures) >= max_failures:
                print(f"Stopping early: hit max_failures={max_failures}.")
                break

        time.sleep(random.uniform(sleep_min, sleep_max))

    print("\n" + "=" * 80)
    print("BOX SCORE MINUTES BACKFILL SUMMARY")
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
