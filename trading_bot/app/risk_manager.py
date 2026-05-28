"""Risk manager — pre-trade gates + global circuit breakers.

Holds *all* trading-permission state in one place:
  * daily PnL & trade counter (resets at UTC midnight)
  * consecutive-loss counter + auto-pause window
  * health flags for MetaScalp / Binance / MEXC feed
  * manual emergency kill switch
  * per-trade size sizing

The strategy never opens a position without calling `can_trade()`. Execution
calls `register_fill()` and `register_close()` to keep counters honest.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .config import RiskConfig
from .logger import get_logger

log = get_logger("risk")


@dataclass
class RiskDecision:
    allowed: bool
    reason: str = "ok"


@dataclass
class _DayStats:
    date: str
    pnl_usdt: float = 0.0
    trade_count: int = 0
    consecutive_losses: int = 0
    # Total losing trades today (any negative-PnL close). Used by the
    # circuit breaker — see RiskManager.register_close.
    total_losses: int = 0

    def maybe_roll(self, now: datetime) -> None:
        today = now.strftime("%Y-%m-%d")
        if self.date != today:
            log.info("risk.day_rollover", from_=self.date, to=today,
                     pnl=self.pnl_usdt, trades=self.trade_count)
            self.date = today
            self.pnl_usdt = 0.0
            self.trade_count = 0
            self.consecutive_losses = 0
            self.total_losses = 0


class RiskManager:
    def __init__(self, cfg: RiskConfig) -> None:
        self.cfg = cfg
        now = datetime.now(timezone.utc)
        self._day = _DayStats(date=now.strftime("%Y-%m-%d"))
        self._paused_until: float = 0.0
        self._emergency: bool = cfg.emergency_kill_switch
        # health flags — set by orchestrator each tick
        self.metascalp_ok: bool = False
        self.binance_ok: bool = False
        self.mexc_ok: bool = False
        self._entry_block_reason: str | None = None

    # ---- introspection ------------------------------------------------------

    @property
    def day_pnl(self) -> float:
        return self._day.pnl_usdt

    @property
    def day_trades(self) -> int:
        return self._day.trade_count

    @property
    def consecutive_losses(self) -> int:
        return self._day.consecutive_losses

    @property
    def is_paused(self) -> bool:
        return time.monotonic() < self._paused_until

    @property
    def kill_switch(self) -> bool:
        return self._emergency

    @property
    def entry_block_reason(self) -> str | None:
        return self._entry_block_reason

    # ---- controls -----------------------------------------------------------

    def trigger_kill_switch(self, reason: str = "manual") -> None:
        self._emergency = True
        log.error("risk.kill_switch", reason=reason)

    def clear_kill_switch(self) -> None:
        self._emergency = False
        log.warning("risk.kill_switch_cleared")

    def block_entries(self, reason: str) -> None:
        self._entry_block_reason = reason
        log.error("risk.entries_blocked", reason=reason)

    def clear_entry_block(self, reason: str = "reconciled") -> None:
        if self._entry_block_reason is not None:
            log.warning("risk.entries_unblocked",
                        previous_reason=self._entry_block_reason,
                        reason=reason)
        self._entry_block_reason = None

    def set_health(self, *, metascalp: bool | None = None,
                   binance: bool | None = None, mexc: bool | None = None) -> None:
        if metascalp is not None:
            self.metascalp_ok = metascalp
        if binance is not None:
            self.binance_ok = binance
        if mexc is not None:
            self.mexc_ok = mexc

    # ---- pre-trade gate -----------------------------------------------------

    def can_trade(self, *, open_positions: int,
                  max_allowed_positions: int | None = None) -> RiskDecision:
        self._day.maybe_roll(datetime.now(timezone.utc))

        if self._emergency:
            return RiskDecision(False, "emergency_kill_switch")
        if self._entry_block_reason:
            return RiskDecision(False, self._entry_block_reason)
        if self.is_paused:
            return RiskDecision(False, "paused_after_losses")
        if not self.metascalp_ok:
            return RiskDecision(False, "metascalp_unreachable")
        if not self.binance_ok:
            return RiskDecision(False, "binance_feed_stale")
        if not self.mexc_ok:
            return RiskDecision(False, "mexc_feed_stale")
        if self._day.pnl_usdt <= -self.cfg.max_daily_loss_usdt:
            return RiskDecision(False, "max_daily_loss")
        if self._day.trade_count >= self.cfg.max_trades_per_day:
            return RiskDecision(False, "max_trades_per_day")
        if self._day.consecutive_losses >= self.cfg.max_consecutive_losses:
            return RiskDecision(False, "max_consecutive_losses")
        position_limit = self.cfg.max_open_positions
        if max_allowed_positions is not None:
            position_limit = min(position_limit, max_allowed_positions)
        if open_positions >= position_limit:
            return RiskDecision(False, "max_open_positions")
        return RiskDecision(True, "ok")

    # ---- sizing -------------------------------------------------------------

    def position_size_qty(self, mark_price: float) -> float:
        """Position size in base units for `max_position_usdt` notional."""
        if mark_price <= 0:
            return 0.0
        return self.cfg.max_position_usdt / mark_price

    # ---- lifecycle hooks ----------------------------------------------------

    def register_open(self) -> None:
        self._day.maybe_roll(datetime.now(timezone.utc))
        self._day.trade_count += 1

    def register_close(self, pnl_usdt: float) -> None:
        self._day.maybe_roll(datetime.now(timezone.utc))
        self._day.pnl_usdt += pnl_usdt
        if pnl_usdt < 0:
            self._day.consecutive_losses += 1
            self._day.total_losses += 1
            if self._day.consecutive_losses >= self.cfg.max_consecutive_losses:
                self._paused_until = time.monotonic() + self.cfg.pause_after_consecutive_losses_sec
                log.warning("risk.paused", losses=self._day.consecutive_losses,
                            for_sec=self.cfg.pause_after_consecutive_losses_sec)
            # Hard circuit breaker — after N total losses today, kill
            # trading until a manual restart. Designed to limit a slow
            # bleed when market conditions don't fit the strategy at all.
            limit = getattr(self.cfg, "daily_loss_circuit_breaker_count", 0) or 0
            if limit > 0 and self._day.total_losses >= limit:
                self._emergency = True
                log.error("risk.circuit_breaker_tripped",
                          total_losses=self._day.total_losses,
                          limit=limit,
                          day_pnl=self._day.pnl_usdt,
                          hint="kill switch armed; restart bot to resume")
        else:
            self._day.consecutive_losses = 0
        log.info("risk.day_state",
                 pnl=self._day.pnl_usdt, trades=self._day.trade_count,
                 losses_streak=self._day.consecutive_losses,
                 total_losses=self._day.total_losses)
