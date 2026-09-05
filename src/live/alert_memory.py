from dataclasses import dataclass, field

from src.config import get_connection


GLOBAL_COOLDOWN_SECONDS = 90
PLAYER_COOLDOWN_SECONDS = 240


@dataclass
class AlertMemory:
    game_id: str | None = None
    last_alert_game_second: float | None = None
    player_alert_times: dict[int, float] = field(
        default_factory=dict
    )

    def __post_init__(self):
        if self.game_id is not None:
            self.load_from_database()

    def load_from_database(self):
        connection = get_connection()

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT MAX(game_second)
                    FROM analytics.live_alert_history
                    WHERE game_id = %s;
                    """,
                    (self.game_id,),
                )

                row = cursor.fetchone()

                if row and row[0] is not None:
                    self.last_alert_game_second = float(row[0])

                cursor.execute(
                    """
                    SELECT
                        player_id,
                        MAX(game_second) AS last_game_second
                    FROM analytics.live_alert_history
                    WHERE game_id = %s
                    GROUP BY player_id;
                    """,
                    (self.game_id,),
                )

                self.player_alert_times = {
                    int(player_id): float(last_game_second)
                    for player_id, last_game_second
                    in cursor.fetchall()
                }
        finally:
            connection.close()

    def can_alert(
        self,
        player_id: int,
        game_second: float,
    ):
        if self.last_alert_game_second is not None:
            seconds_since_last_alert = (
                game_second - self.last_alert_game_second
            )

            if seconds_since_last_alert < GLOBAL_COOLDOWN_SECONDS:
                remaining = round(
                    GLOBAL_COOLDOWN_SECONDS
                    - seconds_since_last_alert
                )

                return (
                    False,
                    f"Global cooldown has {remaining} seconds left",
                )

        player_last_alert = self.player_alert_times.get(
            player_id
        )

        if player_last_alert is not None:
            seconds_since_player_alert = (
                game_second - player_last_alert
            )

            if seconds_since_player_alert < PLAYER_COOLDOWN_SECONDS:
                remaining = round(
                    PLAYER_COOLDOWN_SECONDS
                    - seconds_since_player_alert
                )

                return (
                    False,
                    f"Player cooldown has {remaining} seconds left",
                )

        return True, "Alert allowed"

    def record_alert(
        self,
        player_id: int,
        game_second: float,
        alert_status: str = "alert_sent",
    ):
        self.last_alert_game_second = game_second
        self.player_alert_times[player_id] = game_second

        if self.game_id is None:
            return

        connection = get_connection()

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO analytics.live_alert_history (
                        game_id,
                        player_id,
                        game_second,
                        alert_status
                    )
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (
                        game_id,
                        player_id,
                        game_second
                    )
                    DO NOTHING;
                    """,
                    (
                        self.game_id,
                        player_id,
                        game_second,
                        alert_status,
                    ),
                )

            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()