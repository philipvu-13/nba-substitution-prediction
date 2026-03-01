"""
Query and display Anthony Edwards' rotation profile checkpoint sentence.

Usage examples:
  python src/processing/report_edwards_rotation_profile.py
  python src/processing/report_edwards_rotation_profile.py --ensure-view
  python src/processing/report_edwards_rotation_profile.py --sql-file sql/004_phase3_rotation_profile.sql --ensure-view
"""

import os
import sys
from typing import Dict, List

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", "5433"))

DEFAULT_SQL_FILE = "sql/004_phase3_rotation_profile.sql"


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


def parse_args(argv: List[str]) -> Dict:
    args = {
        "ensure_view": False,
        "sql_file": DEFAULT_SQL_FILE,
    }

    i = 1
    while i < len(argv):
        a = argv[i].strip()
        if a == "--ensure-view":
            args["ensure_view"] = True
        elif a == "--sql-file":
            i += 1
            args["sql_file"] = argv[i]
        elif a in ("-h", "--help"):
            print(
                "Usage: python src/processing/report_edwards_rotation_profile.py [options]\n\n"
                "Options:\n"
                "  --ensure-view               Create/replace edwards_rotation_profile_v first\n"
                "  --sql-file <path>           SQL file used with --ensure-view\n"
            )
            sys.exit(0)
        else:
            print(f"Unknown arg: {a} (use --help)")
            sys.exit(1)
        i += 1

    return args


def ensure_profile_view(sql_file: str):
    if not os.path.exists(sql_file):
        raise FileNotFoundError(f"SQL file not found: {sql_file}")

    with open(sql_file, "r", encoding="utf-8") as f:
        sql = f.read()

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Recreate view cleanly so column shape changes are allowed.
            cur.execute("DROP VIEW IF EXISTS edwards_rotation_profile_v;")
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def fetch_profile_row():
    sql = """
    SELECT
        player_name,
        regular_season_games,
        avg_first_rest_q1_clock_left,
        avg_first_rest_q2_clock_left,
        avg_first_rest_q3_clock_left,
        avg_first_rest_q4_clock_left,
        avg_total_minutes_per_game,
        avg_stint_q1_seconds,
        avg_stint_q2_seconds,
        avg_stint_q3_seconds,
        avg_stint_q4_seconds,
        avg_stint_close_game_seconds
    FROM edwards_rotation_profile_v
    LIMIT 1;
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()
    finally:
        conn.close()


def main():
    args = parse_args(sys.argv)

    if args["ensure_view"]:
        ensure_profile_view(args["sql_file"])
        print(f"Ensured view via {args['sql_file']}")

    row = fetch_profile_row()
    if not row:
        print("No profile row found. Ensure stints exist and the view is created.")
        return

    (
        player_name,
        regular_season_games,
        avg_first_rest_q1_clock_left,
        avg_first_rest_q2_clock_left,
        avg_first_rest_q3_clock_left,
        avg_first_rest_q4_clock_left,
        avg_total_minutes_per_game,
        avg_stint_q1_seconds,
        avg_stint_q2_seconds,
        avg_stint_q3_seconds,
        avg_stint_q4_seconds,
        avg_stint_close_game_seconds,
    ) = row

    print(
        f"Based on all his regular season games ({regular_season_games}), "
        f"{player_name}'s average first rest is {avg_first_rest_q1_clock_left} left in Q1."
    )
    print(
        f"Based on all his regular season games ({regular_season_games}), "
        f"{player_name}'s average first rest is {avg_first_rest_q2_clock_left} left in Q2."
    )
    print(
        f"Based on all his regular season games ({regular_season_games}), "
        f"{player_name}'s average first rest is {avg_first_rest_q3_clock_left} left in Q3."
    )
    print(
        f"Based on all his regular season games ({regular_season_games}), "
        f"{player_name}'s average first rest is {avg_first_rest_q4_clock_left} left in Q4."
    )
    print(f"Avg total minutes per game: {avg_total_minutes_per_game}")
    print(
        "Avg stint length by quarter (seconds): "
        f"Q1={avg_stint_q1_seconds}, Q2={avg_stint_q2_seconds}, "
        f"Q3={avg_stint_q3_seconds}, Q4={avg_stint_q4_seconds}"
    )
    print(f"Avg stint length when |score diff| < 5 (seconds): {avg_stint_close_game_seconds}")


if __name__ == "__main__":
    main()
