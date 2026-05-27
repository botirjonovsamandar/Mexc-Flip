"""Risk manager tests — every circuit breaker must fire."""
from __future__ import annotations

from trading_bot.app.config import RiskConfig
from trading_bot.app.risk_manager import RiskManager


def _healthy_risk() -> RiskManager:
    rm = RiskManager(RiskConfig(
        max_position_usdt=50.0, max_daily_loss_usdt=25.0,
        max_trades_per_day=5, max_consecutive_losses=3,
        max_open_positions=2, pause_after_consecutive_losses_sec=900,
    ))
    rm.set_health(metascalp=True, binance=True, mexc=True)
    return rm


def test_allows_trade_when_healthy():
    rm = _healthy_risk()
    assert rm.can_trade(open_positions=0).allowed


def test_blocks_when_metascalp_down():
    rm = _healthy_risk()
    rm.set_health(metascalp=False)
    d = rm.can_trade(open_positions=0)
    assert not d.allowed
    assert d.reason == "metascalp_unreachable"


def test_blocks_when_daily_loss_hit():
    rm = _healthy_risk()
    rm.register_close(-30.0)
    d = rm.can_trade(open_positions=0)
    assert not d.allowed
    assert d.reason == "max_daily_loss"


def test_blocks_when_trade_count_exceeded():
    rm = _healthy_risk()
    for _ in range(5):
        rm.register_open()
    d = rm.can_trade(open_positions=0)
    assert not d.allowed
    assert d.reason == "max_trades_per_day"


def test_blocks_after_consecutive_losses():
    rm = _healthy_risk()
    for _ in range(3):
        rm.register_close(-1.0)
    # max_consecutive_losses=3 — should block both via streak and via pause
    d = rm.can_trade(open_positions=0)
    assert not d.allowed
    assert d.reason in {"max_consecutive_losses", "paused_after_losses"}


def test_kill_switch_blocks_immediately():
    rm = _healthy_risk()
    rm.trigger_kill_switch("test")
    d = rm.can_trade(open_positions=0)
    assert not d.allowed
    assert d.reason == "emergency_kill_switch"


def test_consecutive_losses_reset_on_win():
    rm = _healthy_risk()
    rm.register_close(-1.0)
    rm.register_close(-1.0)
    rm.register_close(+2.0)
    assert rm.consecutive_losses == 0


def test_position_size_qty():
    rm = _healthy_risk()
    qty = rm.position_size_qty(mark_price=100.0)
    assert abs(qty - 0.5) < 1e-9  # 50 USDT / 100 = 0.5
