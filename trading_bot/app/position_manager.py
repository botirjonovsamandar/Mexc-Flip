"""Position manager — tracks open positions and decides exits.

Exit rules:
  * time stop:        position older than max_position_time_seconds
  * stop loss:        price hits the dynamic stop (initial SL or breakeven)
  * breakeven:        in profit by breakeven_trigger_bps -> stop moves to entry
  * take profit:      bps reached take_profit_bps
  * basis collapse:   |basis_bps| <= basis_collapse_exit_bps while in profit
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable

from .book_cache import BookCache
from .config import RiskConfig
from .logger import get_logger
from .strategy import Side

log = get_logger("positions")


class ExitReason(str, Enum):
    TAKE_PROFIT = "take_profit"
    STOP_LOSS = "stop_loss"
    BASIS_COLLAPSE = "basis_collapse"
    TIME_STOP = "time_stop"
    BREAKEVEN_STOP = "breakeven_stop"
    EMERGENCY = "emergency"
    MANUAL = "manual"


@dataclass
class Position:
    symbol: str
    side: Side
    qty: float
    entry_price: float
    opened_at: float
    entry_snapshot: dict = field(default_factory=dict)
    breakeven_armed: bool = False
    stop_price: float | None = None
    stop_order_id: str | None = None
    stop_confirmed: bool = False
    margin_usdt: float = 0.0
    notional_usdt: float = 0.0
    leverage: float = 1.0
    entry_order_id: str | None = None

    @property
    def age_seconds(self) -> float:
        return time.monotonic() - self.opened_at

    def unrealized_bps(self, mark: float) -> float:
        if self.entry_price <= 0 or mark <= 0:
            return 0.0
        delta = (mark - self.entry_price) / self.entry_price
        if self.side is Side.SHORT:
            delta = -delta
        return delta * 10_000.0

    def unrealized_pnl_usdt(self, mark: float) -> float:
        if self.side is Side.LONG:
            return (mark - self.entry_price) * self.qty
        return (self.entry_price - mark) * self.qty


@dataclass
class ExitDecision:
    should_exit: bool
    reason: ExitReason | None = None
    detail: str = ""


class PositionManager:
    def __init__(self, cfg: RiskConfig, mexc: BookCache) -> None:
        self.cfg = cfg
        self.mexc = mexc
        self._positions: dict[str, Position] = {}

    @property
    def open_symbols(self) -> list[str]:
        return list(self._positions.keys())

    @property
    def count(self) -> int:
        return len(self._positions)

    def get(self, symbol: str) -> Position | None:
        return self._positions.get(symbol.upper())

    def all(self) -> Iterable[Position]:
        return self._positions.values()

    def open(self, symbol: str, side: Side, qty: float, entry_price: float,
             entry_snapshot: dict | None = None, *, margin_usdt: float = 0.0,
             notional_usdt: float = 0.0, leverage: float = 1.0,
             entry_order_id: str | None = None,
             stop_confirmed: bool = False,
             stop_order_id: str | None = None) -> Position:
        sym = symbol.upper()
        if sym in self._positions:
            raise RuntimeError(f"position already open for {sym}")
        sl_mult = self.cfg.stop_loss_percent / 100.0
        stop = entry_price * (1 - sl_mult) if side is Side.LONG else entry_price * (1 + sl_mult)
        pos = Position(
            symbol=sym, side=side, qty=qty, entry_price=entry_price,
            opened_at=time.monotonic(), entry_snapshot=entry_snapshot or {},
            stop_price=stop,
            stop_confirmed=stop_confirmed, stop_order_id=stop_order_id,
            margin_usdt=margin_usdt, notional_usdt=notional_usdt,
            leverage=leverage, entry_order_id=entry_order_id,
        )
        self._positions[sym] = pos
        log.info("position.open", symbol=sym, side=side.value, qty=qty,
                 entry=entry_price, stop=stop)
        return pos

    def close(self, symbol: str) -> Position | None:
        return self._positions.pop(symbol.upper(), None)

    def check_exit(self, symbol: str, binance_mid: float | None) -> ExitDecision:
        pos = self._positions.get(symbol.upper())
        if not pos:
            return ExitDecision(False)
        book = self.mexc.book(symbol)
        mark = book.mid
        if mark is None:
            return ExitDecision(False)

        if pos.age_seconds >= self.cfg.max_position_time_seconds:
            return ExitDecision(True, ExitReason.TIME_STOP, f"age={pos.age_seconds:.1f}s")

        bps = pos.unrealized_bps(mark)

        if pos.stop_price is not None:
            if pos.side is Side.LONG and mark <= pos.stop_price:
                return ExitDecision(True, ExitReason.STOP_LOSS,
                                    f"mark={mark} <= stop={pos.stop_price}")
            if pos.side is Side.SHORT and mark >= pos.stop_price:
                return ExitDecision(True, ExitReason.STOP_LOSS,
                                    f"mark={mark} >= stop={pos.stop_price}")

        if bps >= self.cfg.take_profit_bps:
            return ExitDecision(True, ExitReason.TAKE_PROFIT, f"bps={bps:.2f}")

        if not pos.breakeven_armed and bps >= self.cfg.breakeven_trigger_bps:
            pos.breakeven_armed = True
            pos.stop_price = pos.entry_price
            log.info("position.breakeven_armed", symbol=pos.symbol, bps=bps,
                     new_stop=pos.stop_price)

        if binance_mid and bps > 0:
            basis_bps = (mark - binance_mid) / binance_mid * 10_000.0
            if abs(basis_bps) <= self.cfg.basis_collapse_exit_bps:
                return ExitDecision(True, ExitReason.BASIS_COLLAPSE,
                                    f"basis={basis_bps:.2f}bps bps={bps:.2f}")

        return ExitDecision(False)

    def snapshot(self) -> list[dict]:
        out: list[dict] = []
        for p in self._positions.values():
            book = self.mexc.book(p.symbol)
            mark = book.mid
            out.append({
                "symbol": p.symbol,
                "side": p.side.value,
                "qty": p.qty,
                "entry_price": p.entry_price,
                "stop_price": p.stop_price,
                "stop_confirmed": p.stop_confirmed,
                "margin_usdt": round(p.margin_usdt, 4),
                "notional_usdt": round(p.notional_usdt, 4),
                "leverage": p.leverage,
                "mark": mark,
                "age_sec": round(p.age_seconds, 2),
                "unrealized_bps": round(p.unrealized_bps(mark), 2) if mark else None,
                "unrealized_pnl_usdt": round(p.unrealized_pnl_usdt(mark), 4) if mark else None,
                "breakeven_armed": p.breakeven_armed,
            })
        return out
