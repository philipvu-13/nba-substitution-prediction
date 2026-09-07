import argparse
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import Patch

from src.config import get_connection


REGULATION_PERIOD_SECONDS = 12 * 60
OVERTIME_PERIOD_SECONDS = 5 * 60
REGULATION_GAME_SECONDS = 48 * 60

BACKGROUND_COLOR = "#071A2B"
PANEL_COLOR = "#0C2340"
TEXT_COLOR = "#F5F7FA"
MUTED_TEXT_COLOR = "#AAB7C4"
STARTER_COLOR = "#236192"
BENCH_COLOR = "#78BE20"
PERIOD_LINE_COLOR = "#8FA3B7"
GRID_COLOR = "#29445E"


def format_duration(seconds):
    seconds = int(round(seconds))
    minutes, remaining_seconds = divmod(seconds, 60)

    return f"{minutes}:{remaining_seconds:02d}"


def load_game_information(game_id):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    game_id,
                    game_date,
                    matchup,
                    home_team_abbrev,
                    away_team_abbrev
                FROM core.games
                WHERE game_id = %s
                """,
                (game_id,),
            )

            row = cursor.fetchone()

            if row is None:
                raise ValueError(
                    f"Game {game_id} was not found in core.games"
                )

            return {
                "game_id": str(row[0]),
                "game_date": row[1],
                "matchup": row[2],
                "home_team": row[3],
                "away_team": row[4],
            }

    finally:
        connection.close()


def load_player_stints(game_id):
    connection = get_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    stints.player_id,
                    COALESCE(
                        boxscores.player_name,
                        players.full_name,
                        stints.player_id::TEXT
                    ) AS player_name,
                    stints.stint_number,
                    stints.sub_in_game_second,
                    stints.sub_out_game_second,
                    COALESCE(
                        boxscores.is_starter,
                        FALSE
                    ) AS is_starter
                FROM analytics.player_stints stints

                LEFT JOIN raw.player_boxscores boxscores
                    ON boxscores.game_id = stints.game_id
                   AND boxscores.player_id = stints.player_id

                LEFT JOIN core.players players
                    ON players.player_id = stints.player_id

                WHERE stints.game_id = %s
                  AND stints.team_id = 1610612750

                ORDER BY
                    stints.sub_in_game_second,
                    stints.player_id,
                    stints.stint_number
                """,
                (game_id,),
            )

            rows = cursor.fetchall()

    finally:
        connection.close()

    if not rows:
        raise ValueError(
            f"No player stints were found for game {game_id}"
        )

    return [
        {
            "player_id": int(row[0]),
            "player_name": str(row[1]),
            "stint_number": int(row[2]),
            "start_second": int(row[3]),
            "end_second": int(row[4]),
            "is_starter": bool(row[5]),
        }
        for row in rows
    ]


def summarize_players(stints):
    player_stints = defaultdict(list)

    for stint in stints:
        player_stints[stint["player_id"]].append(
            stint
        )

    players = []

    for player_id, player_rows in player_stints.items():
        total_seconds = sum(
            row["end_second"] - row["start_second"]
            for row in player_rows
        )

        first_game_second = min(
            row["start_second"]
            for row in player_rows
        )

        players.append({
            "player_id": player_id,
            "player_name": player_rows[0]["player_name"],
            "is_starter": any(
                row["is_starter"]
                for row in player_rows
            ),
            "total_seconds": total_seconds,
            "first_game_second": first_game_second,
            "stints": sorted(
                player_rows,
                key=lambda row: row["start_second"],
            ),
        })

    players.sort(
        key=lambda player: (
            not player["is_starter"],
            player["first_game_second"],
            -player["total_seconds"],
            player["player_name"],
        )
    )

    return players


def build_periods(game_length_seconds):
    periods = []
    current_start = 0
    period = 1

    while current_start < game_length_seconds:
        if period <= 4:
            period_length = REGULATION_PERIOD_SECONDS
            label = f"Q{period}"
        else:
            period_length = OVERTIME_PERIOD_SECONDS
            label = f"OT{period - 4}"

        current_end = min(
            current_start + period_length,
            game_length_seconds,
        )

        periods.append({
            "label": label,
            "start_second": current_start,
            "end_second": current_end,
        })

        current_start += period_length
        period += 1

    return periods


def create_rotation_timeline(
    game_information,
    stints,
    output_path,
):
    players = summarize_players(stints)

    game_length_seconds = max(
        REGULATION_GAME_SECONDS,
        max(stint["end_second"] for stint in stints),
    )

    periods = build_periods(game_length_seconds)

    figure, axis = plt.subplots(
        figsize=(16, 9),
        dpi=100,
    )

    figure.patch.set_facecolor(BACKGROUND_COLOR)
    axis.set_facecolor(PANEL_COLOR)

    player_count = len(players)
    bar_height = 0.62

    y_positions = list(
        range(player_count - 1, -1, -1)
    )

    for period_index, period in enumerate(periods):
        start_minute = (
            period["start_second"] / 60
        )

        end_minute = (
            period["end_second"] / 60
        )

        if period_index % 2 == 1:
            axis.axvspan(
                start_minute,
                end_minute,
                color=BACKGROUND_COLOR,
                alpha=0.26,
                zorder=0,
            )

        if period_index > 0:
            axis.axvline(
                start_minute,
                color=PERIOD_LINE_COLOR,
                linewidth=1.1,
                alpha=0.65,
                zorder=1,
            )

        midpoint = (
            start_minute + end_minute
        ) / 2

        axis.text(
            midpoint,
            player_count - 0.05,
            period["label"],
            color=MUTED_TEXT_COLOR,
            fontsize=12,
            fontweight="bold",
            horizontalalignment="center",
            verticalalignment="bottom",
        )

    y_labels = []

    for player, y_position in zip(
        players,
        y_positions,
    ):
        color = (
            STARTER_COLOR
            if player["is_starter"]
            else BENCH_COLOR
        )

        for stint in player["stints"]:
            start_minute = (
                stint["start_second"] / 60
            )

            length_minutes = (
                stint["end_second"]
                - stint["start_second"]
            ) / 60

            axis.broken_barh(
                [(start_minute, length_minutes)],
                (
                    y_position - bar_height / 2,
                    bar_height,
                ),
                facecolors=color,
                edgecolors=BACKGROUND_COLOR,
                linewidth=1.2,
                zorder=3,
            )

        starter_label = (
            "S" if player["is_starter"] else "B"
        )

        y_labels.append(
            f"{player['player_name']}  "
            f"{format_duration(player['total_seconds'])}  "
            f"[{starter_label}]"
        )

    axis.set_yticks(y_positions)
    axis.set_yticklabels(
        y_labels,
        color=TEXT_COLOR,
        fontsize=11,
    )

    game_length_minutes = game_length_seconds / 60

    period_boundaries = [
        period["start_second"] / 60
        for period in periods
    ]

    period_boundaries.append(game_length_minutes)

    axis.set_xticks(period_boundaries)

    axis.set_xticklabels(
        [
            format_duration(
                round(minute * 60)
            )
            for minute in period_boundaries
        ],
        color=MUTED_TEXT_COLOR,
        fontsize=10,
    )

    axis.set_xlim(0, game_length_minutes)
    axis.set_ylim(-0.8, player_count + 0.6)

    axis.grid(
        axis="x",
        color=GRID_COLOR,
        linewidth=0.8,
        alpha=0.7,
        zorder=1,
    )

    axis.tick_params(
        axis="x",
        colors=MUTED_TEXT_COLOR,
        length=0,
        pad=8,
    )

    axis.tick_params(
        axis="y",
        colors=TEXT_COLOR,
        length=0,
        pad=10,
    )

    for spine in axis.spines.values():
        spine.set_visible(False)

    axis.set_xlabel(
        "Elapsed game time",
        color=MUTED_TEXT_COLOR,
        fontsize=11,
        labelpad=12,
    )

    game_date = game_information[
        "game_date"
    ].strftime("%B %d, %Y")

    figure.suptitle(
        "MINNESOTA TIMBERWOLVES ROTATION TIMELINE",
        color=TEXT_COLOR,
        fontsize=22,
        fontweight="bold",
        x=0.06,
        y=0.955,
        horizontalalignment="left",
    )

    axis.set_title(
        (
            f"{game_date}  •  "
            f"{game_information['matchup']}  •  "
            f"Game {game_information['game_id']}"
        ),
        color=MUTED_TEXT_COLOR,
        fontsize=12,
        loc="left",
        pad=30,
    )

    legend = axis.legend(
        handles=[
            Patch(
                facecolor=STARTER_COLOR,
                label="Game starter",
            ),
            Patch(
                facecolor=BENCH_COLOR,
                label="Bench player",
            ),
        ],
        loc="upper right",
        bbox_to_anchor=(1.0, 1.075),
        frameon=False,
        ncol=2,
        fontsize=10,
    )

    for text in legend.get_texts():
        text.set_color(TEXT_COLOR)

    figure.text(
        0.06,
        0.035,
        (
            "Bars show when each player was on the court. "
            "Minutes are reconstructed from play by play "
            "and validated against official box scores."
        ),
        color=MUTED_TEXT_COLOR,
        fontsize=10,
    )

    figure.subplots_adjust(
        left=0.235,
        right=0.965,
        top=0.83,
        bottom=0.13,
    )

    output_path = Path(output_path)
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    figure.savefig(
        output_path,
        dpi=100,
        facecolor=figure.get_facecolor(),
    )

    plt.close(figure)

    return output_path


def generate_rotation_timeline(
    game_id,
    output_path=None,
):
    game_information = load_game_information(
        game_id
    )

    stints = load_player_stints(game_id)

    if output_path is None:
        output_path = (
            Path("outputs")
            / "rotation_timelines"
            / f"{game_id}_rotation_timeline.png"
        )

    return create_rotation_timeline(
        game_information,
        stints,
        output_path,
    )


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Generate a Timberwolves rotation timeline"
        )
    )

    parser.add_argument("game_id")

    parser.add_argument(
        "--output",
        help="Optional output PNG path",
    )

    args = parser.parse_args()

    output_path = generate_rotation_timeline(
        args.game_id,
        args.output,
    )

    print(f"Saved rotation timeline to {output_path}")


if __name__ == "__main__":
    main()