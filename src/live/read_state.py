import argparse
from collections import defaultdict

from pbpstats.client import Client

from src.config import get_connection


WOLVES_TEAM_ID = 1610612750


def parse_clock(clock_text: str) -> float:
    minutes, seconds = clock_text.split(":")
    return int(minutes) * 60 + float(seconds)


def format_clock(seconds: float) -> str:
    minutes = int(seconds // 60)
    remaining_seconds = seconds % 60

    if remaining_seconds.is_integer():
        return f"{minutes}:{int(remaining_seconds):02d}"

    return f"{minutes}:{remaining_seconds:04.1f}"


def event_game_second(period: int, seconds_remaining: float) -> float:
    if period <= 4:
        period_start = (period - 1) * 720
        period_length = 720
    else:
        period_start = 2880 + (period - 5) * 300
        period_length = 300

    return period_start + period_length - seconds_remaining


def fetch_game_events(game_id: str):
    client = Client({
        "EnhancedPbp": {
            "source": "web",
            "data_provider": "live",
        }
    })

    game = client.Game(game_id)
    return game.enhanced_pbp.items


def select_event(events, period=None, clock_seconds=None):
    if period is None:
        usable_indexes = [
            index
            for index, event in enumerate(events)
            if getattr(event, "action_type", None) != "game"
        ]

        if not usable_indexes:
            raise RuntimeError("No usable game events were found")

        index = usable_indexes[-1]
        return index, events[index]

    usable_indexes = [
        index
        for index, event in enumerate(events)
        if event.period == period
        and event.seconds_remaining >= clock_seconds
    ]

    if not usable_indexes:
        raise RuntimeError(
            f"No event found at or before Q{period} "
            f"with {format_clock(clock_seconds)} remaining"
        )

    index = usable_indexes[-1]
    return index, events[index]


def load_game_information(game_id: str):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    game_date,
                    home_team_abbrev,
                    away_team_abbrev
                FROM core.games
                WHERE game_id = %s
                """,
                (game_id,),
            )

            game = cursor.fetchone()

            if game is None:
                raise RuntimeError(
                    f"Game {game_id} is not in core.games"
                )

            cursor.execute(
                """
                SELECT player_id, full_name
                FROM core.players
                """
            )

            player_names = {
                player_id: full_name
                for player_id, full_name in cursor.fetchall()
            }

        return {
            "game_date": game[0],
            "home_team": game[1],
            "away_team": game[2],
            "player_names": player_names,
        }

    finally:
        connection.close()


def build_lineup_snapshots(events, ending_index):
    snapshots = {}

    for event in events[:ending_index + 1]:
        try:
            lineup = event.current_players.get(
                WOLVES_TEAM_ID,
                [],
            )
        except (AttributeError, TypeError):
            continue

        if len(lineup) != 5:
            continue

        key = (
            event.period,
            float(event.seconds_remaining),
        )

        snapshots[key] = set(lineup)

    return [
        {
            "period": period,
            "seconds_remaining": seconds_remaining,
            "game_second": event_game_second(
                period,
                seconds_remaining,
            ),
            "lineup": lineup,
        }
        for (period, seconds_remaining), lineup
        in snapshots.items()
    ]


def calculate_player_states(snapshots):
    previous_lineup = set()
    open_stints = {}
    stint_numbers = defaultdict(int)
    period_starters = {}
    game_starters = set()

    for snapshot in snapshots:
        period = snapshot["period"]
        game_second = snapshot["game_second"]
        lineup = snapshot["lineup"]

        if period not in period_starters:
            period_starters[period] = set(lineup)

        if period == 1 and not game_starters:
            game_starters = set(lineup)

        outgoing_players = previous_lineup - lineup
        incoming_players = lineup - previous_lineup

        for player_id in outgoing_players:
            open_stints.pop(player_id, None)

        for player_id in incoming_players:
            stint_numbers[player_id] += 1

            if not previous_lineup and period == 1:
                open_stints[player_id] = 0.0
            else:
                open_stints[player_id] = game_second

        previous_lineup = set(lineup)

    if not snapshots:
        raise RuntimeError("No valid lineup snapshots were found")

    current_snapshot = snapshots[-1]
    current_game_second = current_snapshot["game_second"]
    current_period = current_snapshot["period"]
    current_lineup = current_snapshot["lineup"]

    player_states = {}

    for player_id in current_lineup:
        stint_start = open_stints.get(player_id)

        if stint_start is None:
            raise RuntimeError(
                f"No open stint found for player {player_id}"
            )

        player_states[player_id] = {
            "current_stint_seconds": round(
                current_game_second - stint_start
            ),
            "stint_number": stint_numbers[player_id],
            "started_period": (
                player_id
                in period_starters.get(current_period, set())
            ),
            "started_game": player_id in game_starters,
        }

    return player_states


def build_state(game_id, period=None, clock_seconds=None):
    events = fetch_game_events(game_id)

    if not events:
        raise RuntimeError("The play by play feed returned no events")

    event_index, event = select_event(
        events,
        period=period,
        clock_seconds=clock_seconds,
    )

    information = load_game_information(game_id)

    lineup_snapshots = build_lineup_snapshots(
        events,
        event_index,
    )

    player_states = calculate_player_states(
        lineup_snapshots
    )

    wolves_lineup = list(player_states)

    if len(wolves_lineup) != 5:
        raise RuntimeError(
            f"Expected 5 Timberwolves players, "
            f"but found {len(wolves_lineup)}"
        )

    wolves_are_home = information["home_team"] == "MIN"

    if wolves_are_home:
        opponent = information["away_team"]
        wolves_score = event.home_score
        opponent_score = event.away_score
        location = "vs"
    else:
        opponent = information["home_team"]
        wolves_score = event.away_score
        opponent_score = event.home_score
        location = "at"

    score_diff = wolves_score - opponent_score

    players = []

    for player_id in wolves_lineup:
        player_state = player_states[player_id]

        players.append({
            "player_id": player_id,
            "player_name": information["player_names"].get(
                player_id,
                f"Unknown player {player_id}",
            ),
            "current_stint_seconds": (
                player_state["current_stint_seconds"]
            ),
            "stint_number": player_state["stint_number"],
            "started_period": player_state["started_period"],
            "started_game": player_state["started_game"],
        })

    return {
        "game_id": game_id,
        "game_date": information["game_date"],
        "period": event.period,
        "seconds_remaining": float(event.seconds_remaining),
        "wolves_score": wolves_score,
        "opponent_score": opponent_score,
        "score_diff": score_diff,
        "abs_score_diff": abs(score_diff),
        "opponent": opponent,
        "location": location,
        "is_home": wolves_are_home,
        "players": players,
    }


def print_state(state):
    print(
        f"{state['game_date']} "
        f"{state['location']} {state['opponent']}"
    )

    print(
        f"Q{state['period']} with "
        f"{format_clock(state['seconds_remaining'])} remaining"
    )

    print(
        f"Score: MIN {state['wolves_score']} "
        f"{state['opponent']} {state['opponent_score']}"
    )

    print(f"Score difference: {state['score_diff']:+d}")

    print("\nTimberwolves players on court")

    print(
        f"{'Player':24}"
        f"{'Current stint':16}"
        f"{'Stint':8}"
        f"{'Period starter':16}"
        f"{'Game starter'}"
    )

    for player in sorted(
        state["players"],
        key=lambda row: row["player_name"],
    ):
        print(
            f"{player['player_name']:24}"
            f"{format_clock(player['current_stint_seconds']):16}"
            f"{player['stint_number']:<8}"
            f"{str(player['started_period']):16}"
            f"{player['started_game']}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Read the Timberwolves game state"
    )

    parser.add_argument("game_id")
    parser.add_argument("--period", type=int)
    parser.add_argument("--clock")

    args = parser.parse_args()

    if (args.period is None) != (args.clock is None):
        parser.error(
            "--period and --clock must be supplied together"
        )

    clock_seconds = (
        parse_clock(args.clock)
        if args.clock is not None
        else None
    )

    state = build_state(
        args.game_id,
        period=args.period,
        clock_seconds=clock_seconds,
    )

    print_state(state)


if __name__ == "__main__":
    main()