from dataclasses import dataclass, field


GLOBAL_COOLDOWN_SECONDS = 90
PLAYER_COOLDOWN_SECONDS = 240


@dataclass
class AlertMemory:
    last_alert_game_second: float | None = None
    player_alert_times: dict[int, float] = field(
        default_factory=dict
    )

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
    ):
        self.last_alert_game_second = game_second
        self.player_alert_times[player_id] = game_second