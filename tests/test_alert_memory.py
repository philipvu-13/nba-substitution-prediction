from src.live.alert_memory import AlertMemory


def test_first_alert_is_allowed():
    memory = AlertMemory()

    allowed, reason = memory.can_alert(
        player_id=1630162,
        game_second=100,
    )

    assert allowed is True
    assert reason == "Alert allowed"


def test_global_cooldown_suppresses_early_alert():
    memory = AlertMemory()

    memory.record_alert(
        player_id=1630162,
        game_second=100,
    )

    allowed, reason = memory.can_alert(
        player_id=203497,
        game_second=130,
    )

    assert allowed is False
    assert reason == "Global cooldown has 60 seconds left"


def test_different_player_allowed_after_global_cooldown():
    memory = AlertMemory()

    memory.record_alert(
        player_id=1630162,
        game_second=100,
    )

    allowed, reason = memory.can_alert(
        player_id=203497,
        game_second=190,
    )

    assert allowed is True
    assert reason == "Alert allowed"


def test_same_player_remains_suppressed():
    memory = AlertMemory()

    memory.record_alert(
        player_id=1630162,
        game_second=100,
    )

    allowed, reason = memory.can_alert(
        player_id=1630162,
        game_second=190,
    )

    assert allowed is False
    assert reason == "Player cooldown has 150 seconds left"


def test_same_player_allowed_after_player_cooldown():
    memory = AlertMemory()

    memory.record_alert(
        player_id=1630162,
        game_second=100,
    )

    allowed, reason = memory.can_alert(
        player_id=1630162,
        game_second=340,
    )

    assert allowed is True
    assert reason == "Alert allowed"