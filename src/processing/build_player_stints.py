"""
Build player_stints from existing raw tables.

Phase 2 flow in this project:
1) Read rotation intervals from player_rotations
2) Read scoreboard snapshots from play_by_play_v3_raw
3) Read home/away context from games
4) Write clean rows into player_stints

Table shape (as requested):
  game_id
  quarter
  sub_in_time
  sub_out_time
  stint_length_seconds
  score_diff

Notes:
- sub_in_time/sub_out_time are stored as integer game seconds elapsed.
- quarter is the stint start quarter.
- score_diff is captured at sub_out_time (latest known score at or before that moment).
"""

import os
import re
import sys
from bisect import bisect_right
from typing import Dict, List, Optional, Tuple

import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = int(os.getenv("DB_PORT", "5433"))

ROTATIONS_TABLE = "player_rotations"
PBP_TABLE = "play_by_play_v3_raw"
GAMES_TABLE = "games"
STINTS_TABLE = "player_stints"
EXCLUDED_GAMES_TABLE = "excluded_games"
BOX_TABLE = "player_boxscore_minutes"

DEFAULT_PLAYER_ID = 1630162
DEFAULT_TEAM_ID = 1610612750
DEFAULT_TEAM_ABBREV = "MIN"


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


def ensure_player_stints_table():
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {STINTS_TABLE} (
        id BIGSERIAL PRIMARY KEY,
        game_id VARCHAR(20) NOT NULL,
        quarter INTEGER NOT NULL,
        sub_in_time INTEGER NOT NULL,
        sub_out_time INTEGER NOT NULL,
        stint_length_seconds INTEGER NOT NULL,
        score_diff INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT uniq_player_stints UNIQUE (game_id, quarter, sub_in_time, sub_out_time)
    );
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def ensure_excluded_games_table():
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {EXCLUDED_GAMES_TABLE} (
        game_id VARCHAR(20) PRIMARY KEY,
        reason TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()
    finally:
        conn.close()


def season_to_season_id_guess(season: str) -> str:
    try:
        start_year = int(season.split("-")[0])
        return f"2{start_year}"
    except Exception:
        return season


def fetch_target_games(
    player_id: int,
    team_id: int,
    game_id: Optional[str] = None,
    season: Optional[str] = None,
    limit: Optional[int] = None,
    include_excluded: bool = False,
) -> List[str]:
    params: List = [player_id, team_id]
    where_clauses = ["r.player_id = %s", "r.team_id = %s"]

    if game_id:
        where_clauses.append("r.game_id = %s")
        params.append(game_id)

    if season:
        season_guess = season_to_season_id_guess(season)
        where_clauses.append("(g.season = %s OR g.season = %s)")
        params.extend([season, season_guess])

    excluded_join = ""
    if not include_excluded:
        excluded_join = (
            f"LEFT JOIN {EXCLUDED_GAMES_TABLE} eg ON eg.game_id = r.game_id"
        )
        where_clauses.append("eg.game_id IS NULL")

    sql = f"""
        SELECT DISTINCT r.game_id
        FROM {ROTATIONS_TABLE} r
        LEFT JOIN {GAMES_TABLE} g ON g.game_id = r.game_id
        {excluded_join}
        WHERE {" AND ".join(where_clauses)}
        ORDER BY r.game_id;
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = [r[0] for r in cur.fetchall()]
    finally:
        conn.close()

    if limit is not None:
        return rows[:limit]
    return rows


def fetch_boxscore_minutes_seconds(game_id: str, player_id: int) -> Optional[int]:
    sql = f"""
        SELECT minutes_seconds
        FROM {BOX_TABLE}
        WHERE game_id = %s AND player_id = %s
        LIMIT 1;
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (game_id, player_id))
            row = cur.fetchone()
            if not row:
                return None
            return int(row[0])
    finally:
        conn.close()


def infer_rotation_time_scale(
    rows: List[Tuple[int, int]],
    expected_total_seconds: Optional[int] = None,
) -> int:
    max_val = 0
    raw_total = 0
    for in_raw, out_raw in rows:
        if in_raw is not None:
            max_val = max(max_val, int(in_raw))
        if out_raw is not None:
            max_val = max(max_val, int(out_raw))
        if in_raw is not None and out_raw is not None and int(out_raw) > int(in_raw):
            raw_total += int(out_raw) - int(in_raw)

    # Prefer official boxscore minutes for scale selection when available.
    if expected_total_seconds is not None and expected_total_seconds > 0 and raw_total > 0:
        err_scale_1 = abs(raw_total - expected_total_seconds)
        err_scale_10 = abs(int(round(raw_total / 10.0)) - expected_total_seconds)
        if err_scale_10 < err_scale_1:
            return 10
        if err_scale_1 < err_scale_10:
            return 1

    # Fallback heuristic if boxscore minutes are unavailable.
    return 10 if max_val > 4000 else 1


def quarter_from_game_seconds(game_seconds: int) -> int:
    if game_seconds < 0:
        return 1
    if game_seconds < 2880:
        return (game_seconds // 720) + 1
    return 5 + ((game_seconds - 2880) // 300)


def parse_clock_to_seconds_remaining(clock_text: str) -> Optional[float]:
    if clock_text is None:
        return None
    t = str(clock_text).strip()
    if not t:
        return None

    # NBA v3 usually looks like PT11M34.00S or PT34.20S.
    if t.startswith("PT") and t.endswith("S"):
        body = t[2:-1]
        if "M" in body:
            mins, secs = body.split("M", 1)
            try:
                return int(mins) * 60 + float(secs)
            except Exception:
                return None
        try:
            return float(body)
        except Exception:
            return None

    # Fallback: MM:SS or MM:SS.S
    m = re.match(r"^(\d+):(\d+(?:\.\d+)?)$", t)
    if m:
        try:
            mins = int(m.group(1))
            secs = float(m.group(2))
            return mins * 60 + secs
        except Exception:
            return None

    return None


def game_seconds_from_period_clock(period: int, clock_text: str) -> Optional[int]:
    sec_remain = parse_clock_to_seconds_remaining(clock_text)
    if sec_remain is None or period is None:
        return None

    if int(period) <= 4:
        elapsed_before = (int(period) - 1) * 720
        elapsed_in_period = 720 - sec_remain
    else:
        elapsed_before = 2880 + (int(period) - 5) * 300
        elapsed_in_period = 300 - sec_remain

    game_seconds = int(round(elapsed_before + elapsed_in_period))
    if game_seconds < 0:
        return 0
    return game_seconds


def fetch_game_home_away(game_id: str) -> Tuple[Optional[str], Optional[str]]:
    sql = f"SELECT home_team, away_team FROM {GAMES_TABLE} WHERE game_id = %s;"
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (game_id,))
            row = cur.fetchone()
            if not row:
                return None, None
            return row[0], row[1]
    finally:
        conn.close()


def fetch_rotation_rows(game_id: str, player_id: int, team_id: int) -> List[Tuple[int, int]]:
    sql = f"""
        SELECT in_time_real, out_time_real
        FROM {ROTATIONS_TABLE}
        WHERE game_id = %s AND player_id = %s AND team_id = %s
        ORDER BY in_time_real ASC, out_time_real ASC;
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (game_id, player_id, team_id))
            return cur.fetchall()
    finally:
        conn.close()


def fetch_score_events(game_id: str) -> List[Tuple[int, int, int]]:
    sql = f"""
        SELECT "period", "clock", "scoreHome", "scoreAway", "actionNumber"
        FROM {PBP_TABLE}
        WHERE "gameId" = %s
          AND "scoreHome" IS NOT NULL
          AND "scoreAway" IS NOT NULL
        ORDER BY "period" ASC, "actionNumber" ASC;
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (game_id,))
            rows = cur.fetchall()
    finally:
        conn.close()

    out: List[Tuple[int, int, int]] = []
    for period, clock, score_home, score_away, _action_number in rows:
        gs = game_seconds_from_period_clock(period, clock)
        if gs is None:
            continue
        out.append((gs, int(score_home), int(score_away)))

    out.sort(key=lambda x: x[0])
    return out


def compute_score_diff_at_time(
    target_sec: int,
    score_events: List[Tuple[int, int, int]],
    team_is_home: Optional[bool],
) -> Optional[int]:
    if team_is_home is None or not score_events:
        return None

    sec_list = [e[0] for e in score_events]
    idx = bisect_right(sec_list, target_sec) - 1
    if idx < 0:
        return None

    _, score_home, score_away = score_events[idx]
    if team_is_home:
        return int(score_home - score_away)
    return int(score_away - score_home)


def build_stints_for_game(
    game_id: str,
    player_id: int,
    team_id: int,
    team_abbrev: str,
) -> List[Tuple[str, int, int, int, int, Optional[int]]]:
    rotation_rows = fetch_rotation_rows(game_id, player_id, team_id)
    if not rotation_rows:
        return []

    expected_total_seconds = fetch_boxscore_minutes_seconds(game_id, player_id)
    scale = infer_rotation_time_scale(
        rotation_rows,
        expected_total_seconds=expected_total_seconds,
    )
    score_events = fetch_score_events(game_id)

    home_team, away_team = fetch_game_home_away(game_id)
    home_team_u = str(home_team).upper() if home_team else None
    away_team_u = str(away_team).upper() if away_team else None
    team_abbrev_u = team_abbrev.upper()

    if home_team_u == team_abbrev_u:
        team_is_home = True
    elif away_team_u == team_abbrev_u:
        team_is_home = False
    else:
        team_is_home = None

    stints: List[Tuple[str, int, int, int, int, Optional[int]]] = []
    for in_raw, out_raw in rotation_rows:
        if in_raw is None or out_raw is None:
            continue

        sub_in = int(round(float(in_raw) / scale))
        sub_out = int(round(float(out_raw) / scale))

        if sub_out <= sub_in:
            continue

        quarter = quarter_from_game_seconds(sub_in)
        stint_len = sub_out - sub_in
        score_diff = compute_score_diff_at_time(sub_out, score_events, team_is_home)

        stints.append((game_id, quarter, sub_in, sub_out, stint_len, score_diff))

    return stints


def upsert_player_stints(rows: List[Tuple[str, int, int, int, int, Optional[int]]]) -> int:
    if not rows:
        return 0

    sql = f"""
    INSERT INTO {STINTS_TABLE} (
        game_id, quarter, sub_in_time, sub_out_time, stint_length_seconds, score_diff
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (game_id, quarter, sub_in_time, sub_out_time)
    DO UPDATE SET
        stint_length_seconds = EXCLUDED.stint_length_seconds,
        score_diff = EXCLUDED.score_diff;
    """

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
    finally:
        conn.close()
    return len(rows)


def parse_args(argv: List[str]) -> Dict:
    args = {
        "player_id": DEFAULT_PLAYER_ID,
        "team_id": DEFAULT_TEAM_ID,
        "team_abbrev": DEFAULT_TEAM_ABBREV,
        "game_id": None,
        "season": None,
        "limit": None,
        "include_excluded": False,
    }

    i = 1
    while i < len(argv):
        a = argv[i].strip()
        if a == "--player-id":
            i += 1
            args["player_id"] = int(argv[i])
        elif a == "--team-id":
            i += 1
            args["team_id"] = int(argv[i])
        elif a == "--team-abbrev":
            i += 1
            args["team_abbrev"] = str(argv[i]).upper()
        elif a == "--game-id":
            i += 1
            args["game_id"] = str(argv[i])
        elif a == "--season":
            i += 1
            args["season"] = str(argv[i])
        elif a == "--limit":
            i += 1
            args["limit"] = int(argv[i])
        elif a == "--include-excluded":
            args["include_excluded"] = True
        elif a in ("-h", "--help"):
            print(
                "Usage: python src/processing/build_player_stints.py [options]\n\n"
                "Options:\n"
                "  --player-id <id>         Target player_id (default: 1630162)\n"
                "  --team-id <id>           Target team_id (default: 1610612750)\n"
                "  --team-abbrev <ABC>      Team tricode in games table (default: MIN)\n"
                "  --game-id <id>           Build stints for one game only\n"
                "  --season <YYYY-YY>       Build stints for games in this season\n"
                "  --limit <N>              Process only first N games after filters\n"
                "  --include-excluded       Process games even if listed in excluded_games\n"
            )
            sys.exit(0)
        else:
            print(f"Unknown arg: {a} (use --help)")
            sys.exit(1)
        i += 1

    return args


def main():
    args = parse_args(sys.argv)

    player_id = args["player_id"]
    team_id = args["team_id"]
    team_abbrev = args["team_abbrev"]
    game_id = args["game_id"]
    season = args["season"]
    limit = args["limit"]
    include_excluded = args["include_excluded"]

    print(f"Ensuring table exists: {STINTS_TABLE}")
    ensure_player_stints_table()
    print(f"Ensuring table exists: {EXCLUDED_GAMES_TABLE}")
    ensure_excluded_games_table()

    game_ids = fetch_target_games(
        player_id=player_id,
        team_id=team_id,
        game_id=game_id,
        season=season,
        limit=limit,
        include_excluded=include_excluded,
    )
    if not game_ids:
        print("No matching games found in player_rotations for provided filters.")
        return

    total_inserted = 0
    for idx, gid in enumerate(game_ids, start=1):
        stints = build_stints_for_game(
            game_id=gid,
            player_id=player_id,
            team_id=team_id,
            team_abbrev=team_abbrev,
        )
        inserted = upsert_player_stints(stints)
        total_inserted += inserted
        print(f"[{idx}/{len(game_ids)}] game_id={gid} stints_upserted={inserted}")

    print(f"Done. Total stints upserted={total_inserted}")


if __name__ == "__main__":
    main()
