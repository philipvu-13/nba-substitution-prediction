import argparse
from collections import defaultdict
from itertools import groupby

from pbpstats.client import Client

from src.config import get_connection


WOLVES_TEAM_ID = 1610612750


def period_length(period: int) -> int:
    return 720 if period <= 4 else 300


def period_start_second(period: int) -> int:
    if period <= 4:
        return (period - 1) * 720

    return 2880 + (period - 5) * 300


def event_game_second(period: int, seconds_remaining: float) -> int:
    return round(
        period_start_second(period)
        + period_length(period)
        - seconds_remaining
    )


def fetch_enhanced_pbp(game_id: str):
    client = Client({
        "EnhancedPbp": {
            "source": "web",
            "data_provider": "live",
        }
    })

    return client.Game(game_id).enhanced_pbp.items


def get_period_starters(events, team_id: int) -> dict[int, set[int]]:
    starters = {}

    for event in events:
        if (
            getattr(event, "action_type", None) == "period"
            and getattr(event, "sub_type", None) == "start"
        ):
            team_starters = event.get_period_starters().get(team_id)

            if team_starters is None or len(team_starters) != 5:
                raise ValueError(
                    f"Could not determine five starters for period "
                    f"{event.period}"
                )

            starters[event.period] = {
                int(player_id) for player_id in team_starters
            }

    return starters


def get_substitution_snapshots(events, team_id: int):
    substitutions = [
        event
        for event in events
        if getattr(event, "action_type", None) == "substitution"
    ]

    snapshots = defaultdict(list)

    for key, grouped_events in groupby(
        substitutions,
        key=lambda event: (
            event.period,
            round(event.seconds_remaining, 3),
        ),
    ):
        grouped_events = list(grouped_events)
        final_event = grouped_events[-1]

        lineup = {
            int(player_id)
            for player_id in final_event.current_players.get(team_id, [])
        }

        if len(lineup) != 5:
            raise ValueError(
                f"Incomplete lineup in period {key[0]} "
                f"with {key[1]} seconds remaining: {lineup}"
            )

        snapshots[key[0]].append({
            "game_second": event_game_second(key[0], key[1]),
            "seconds_remaining": key[1],
            "lineup": lineup,
        })

    return snapshots


def create_segments(
    start_second: int,
    end_second: int,
    max_period: int,
):
    segments = []

    for period in range(1, max_period + 1):
        period_start = period_start_second(period)
        period_end = period_start + period_length(period)

        segment_start = max(start_second, period_start)
        segment_end = min(end_second, period_end)

        if segment_end > segment_start:
            segments.append({
                "period": period,
                "start": segment_start,
                "end": segment_end,
            })

    return segments


def build_stints(events, team_id: int):
    period_starters = get_period_starters(events, team_id)
    substitution_snapshots = get_substitution_snapshots(events, team_id)

    max_period = max(period_starters)
    game_end_second = (
        period_start_second(max_period)
        + period_length(max_period)
    )

    current_lineup = set()
    open_stints = {}
    stint_counts = defaultdict(int)
    completed_stints = []

    def open_player(player_id: int, game_second: int):
        if player_id in open_stints:
            raise ValueError(f"Player {player_id} already has an open stint")

        stint_counts[player_id] += 1
        open_stints[player_id] = {
            "stint_number": stint_counts[player_id],
            "start": game_second,
        }

    def close_player(player_id: int, game_second: int):
        open_stint = open_stints.pop(player_id, None)

        if open_stint is None:
            raise ValueError(f"Player {player_id} has no open stint")

        if game_second < open_stint["start"]:
            raise ValueError(f"Invalid stint length for player {player_id}")

        if game_second == open_stint["start"]:
            return

        completed_stints.append({
            "team_id": team_id,
            "player_id": player_id,
            "stint_number": open_stint["stint_number"],
            "start": open_stint["start"],
            "end": game_second,
        })

    def change_lineup(new_lineup: set[int], game_second: int):
        nonlocal current_lineup

        for player_id in current_lineup - new_lineup:
            close_player(player_id, game_second)

        for player_id in new_lineup - current_lineup:
            open_player(player_id, game_second)

        current_lineup = set(new_lineup)

    for period in range(1, max_period + 1):
        period_start = period_start_second(period)
        change_lineup(period_starters[period], period_start)

        for snapshot in substitution_snapshots.get(period, []):
            if snapshot["seconds_remaining"] <= 0:
                continue

            change_lineup(
                snapshot["lineup"],
                snapshot["game_second"],
            )

    for player_id in list(current_lineup):
        close_player(player_id, game_end_second)

    expected_seconds = game_end_second * 5
    actual_seconds = sum(
        stint["end"] - stint["start"]
        for stint in completed_stints
    )

    if actual_seconds != expected_seconds:
        raise ValueError(
            f"Expected {expected_seconds} team seconds, "
            f"but calculated {actual_seconds}"
        )

    for stint in completed_stints:
        stint["segments"] = create_segments(
            stint["start"],
            stint["end"],
            max_period,
        )

    return completed_stints


def save_stints(game_id: str, stints):
    connection = get_connection()

    try:
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM analytics.player_stints
                    WHERE game_id = %s
                      AND team_id = %s
                    """,
                    (game_id, WOLVES_TEAM_ID),
                )

                for stint in stints:
                    segments = stint["segments"]

                    cursor.execute(
                        """
                        INSERT INTO analytics.player_stints (
                            game_id,
                            team_id,
                            player_id,
                            stint_number,
                            sub_in_game_second,
                            sub_out_game_second,
                            start_period,
                            end_period
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING stint_id
                        """,
                        (
                            game_id,
                            stint["team_id"],
                            stint["player_id"],
                            stint["stint_number"],
                            stint["start"],
                            stint["end"],
                            segments[0]["period"],
                            segments[-1]["period"],
                        ),
                    )

                    stint_id = cursor.fetchone()[0]

                    for segment in segments:
                        cursor.execute(
                            """
                            INSERT INTO analytics.player_stint_segments (
                                stint_id,
                                period,
                                segment_start_game_second,
                                segment_end_game_second
                            )
                            VALUES (%s, %s, %s, %s)
                            """,
                            (
                                stint_id,
                                segment["period"],
                                segment["start"],
                                segment["end"],
                            ),
                        )
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("game_id")
    args = parser.parse_args()

    events = fetch_enhanced_pbp(args.game_id)
    stints = build_stints(events, WOLVES_TEAM_ID)
    save_stints(args.game_id, stints)

    total_seconds = sum(
        stint["end"] - stint["start"]
        for stint in stints
    )

    print(f"Created {len(stints)} player stints")
    print(f"Total team minutes: {total_seconds / 60:.2f}")


if __name__ == "__main__":
    main()