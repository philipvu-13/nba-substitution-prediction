import argparse
from pathlib import Path

from src.config import get_connection
from src.visualization.rotation_timeline import (
    format_duration,
    generate_rotation_timeline,
    load_game_information,
    load_player_stints,
    summarize_players,
)


TEAM_NAMES = {
    "ATL": "Atlanta",
    "BOS": "Boston",
    "BKN": "Brooklyn",
    "CHA": "Charlotte",
    "CHI": "Chicago",
    "CLE": "Cleveland",
    "DAL": "Dallas",
    "DEN": "Denver",
    "DET": "Detroit",
    "GSW": "Golden State",
    "HOU": "Houston",
    "IND": "Indiana",
    "LAC": "the Clippers",
    "LAL": "the Lakers",
    "MEM": "Memphis",
    "MIA": "Miami",
    "MIL": "Milwaukee",
    "MIN": "Minnesota",
    "NOP": "New Orleans",
    "NYK": "New York",
    "OKC": "Oklahoma City",
    "ORL": "Orlando",
    "PHI": "Philadelphia",
    "PHX": "Phoenix",
    "POR": "Portland",
    "SAC": "Sacramento",
    "SAS": "San Antonio",
    "TOR": "Toronto",
    "UTA": "Utah",
    "WAS": "Washington",
}


def load_final_score(game_id):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    score_home,
                    score_away
                FROM raw.play_by_play
                WHERE game_id = %s
                  AND score_home IS NOT NULL
                  AND score_away IS NOT NULL
                ORDER BY
                    period DESC,
                    action_number DESC NULLS LAST,
                    action_id DESC
                LIMIT 1
                """,
                (game_id,),
            )

            row = cursor.fetchone()

            if row is None:
                return None

            return {
                "home_score": int(row[0]),
                "away_score": int(row[1]),
            }

    finally:
        connection.close()


def get_opponent_abbreviation(game_information):
    if game_information["home_team"] == "MIN":
        return game_information["away_team"]

    return game_information["home_team"]


def get_opponent_name(game_information):
    abbreviation = get_opponent_abbreviation(
        game_information
    )

    return TEAM_NAMES.get(
        abbreviation,
        abbreviation,
    )


def get_score_information(
    game_information,
    final_score,
):
    if final_score is None:
        return None

    opponent_abbreviation = (
        get_opponent_abbreviation(
            game_information
        )
    )

    opponent_name = TEAM_NAMES.get(
        opponent_abbreviation,
        opponent_abbreviation,
    )

    if game_information["home_team"] == "MIN":
        wolves_score = final_score["home_score"]
        opponent_score = final_score["away_score"]
    else:
        wolves_score = final_score["away_score"]
        opponent_score = final_score["home_score"]

    if wolves_score > opponent_score:
        result = "win"
    elif wolves_score < opponent_score:
        result = "loss"
    else:
        result = "tie"

    return {
        "wolves_score": wolves_score,
        "opponent_score": opponent_score,
        "opponent_name": opponent_name,
        "result": result,
    }


def build_opening(
    game_information,
    score_information,
):
    opponent_name = get_opponent_name(
        game_information
    )

    if score_information is None:
        return (
            f"Wolves rotation from their game "
            f"against {opponent_name}."
        )

    wolves_score = score_information[
        "wolves_score"
    ]

    opponent_score = score_information[
        "opponent_score"
    ]

    if score_information["result"] == "win":
        return (
            f"Wolves beat {opponent_name} "
            f"{wolves_score} to {opponent_score}."
        )

    if score_information["result"] == "loss":
        return (
            f"{opponent_name} beat the Wolves "
            f"{opponent_score} to {wolves_score}."
        )

    return (
        f"The Wolves and {opponent_name} finished "
        f"tied at {wolves_score}."
    )


def build_caption(
    game_information,
    players,
    score_information,
):
    minutes_leader = max(
        players,
        key=lambda player: player["total_seconds"],
    )

    bench_players = [
        player
        for player in players
        if not player["is_starter"]
    ]

    bench_leader = (
        max(
            bench_players,
            key=lambda player: (
                player["total_seconds"]
            ),
        )
        if bench_players
        else None
    )

    opening = build_opening(
        game_information,
        score_information,
    )

    rotation_facts = (
        f"{minutes_leader['player_name']} "
        f"led Minnesota in minutes at "
        f"{format_duration(
            minutes_leader['total_seconds']
        )}."
    )

    if bench_leader is not None:
        rotation_facts += (
            f" {bench_leader['player_name']} "
            f"led the bench with "
            f"{format_duration(
                bench_leader['total_seconds']
            )}."
        )

    player_count_text = (
        f"{len(players)} Wolves saw the floor."
    )

    return "\n\n".join([
        opening,
        rotation_facts,
        player_count_text,
        "Here is how the rotation unfolded 👇",
    ])


def save_caption(
    game_id,
    caption,
):
    output_path = (
        Path("outputs")
        / "postgame_posts"
        / f"{game_id}_rotation_post.txt"
    )

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path.write_text(
        caption,
        encoding="utf-8",
    )

    return output_path


def generate_postgame_rotation_post(game_id):
    game_information = load_game_information(
        game_id
    )

    stints = load_player_stints(game_id)
    players = summarize_players(stints)

    final_score = load_final_score(game_id)

    score_information = get_score_information(
        game_information,
        final_score,
    )

    caption = build_caption(
        game_information,
        players,
        score_information,
    )

    graphic_path = generate_rotation_timeline(
        game_id
    )

    caption_path = save_caption(
        game_id,
        caption,
    )

    return {
        "caption": caption,
        "caption_path": caption_path,
        "graphic_path": graphic_path,
    }


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Generate a postgame Wolves rotation "
            "graphic and X caption"
        )
    )

    parser.add_argument("game_id")

    args = parser.parse_args()

    result = generate_postgame_rotation_post(
        args.game_id
    )

    print("\nReady to copy X post\n")
    print(result["caption"])

    print(
        f"\nCaption saved to: "
        f"{result['caption_path']}"
    )

    print(
        f"Graphic saved to: "
        f"{result['graphic_path']}"
    )


if __name__ == "__main__":
    main()