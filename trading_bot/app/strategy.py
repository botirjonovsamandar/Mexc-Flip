"""Signal generation: Binance lead -> MEXC lag micro-arbitrage.

Both Binance and MEXC orderbooks arrive through MetaScalp WS and land in
two `BookCache` instances. The strategy is pure: given those two caches,
decide whether a symbol qualifies for LONG / SHORT entry.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Iterable

from .book_cache import BookCache
from .config import StrategyConfig


class Side(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class Decision(str, Enum):
    ENTER = "ENTER"
    REJECT = "REJECT"
    NO_DATA = "NO_DATA"


@dataclass
class SignalSnapshot:
    ts_ms: int
    symbol: str
    binance_mid: float | None
    mexc_mid: float | None
    mexc_best_bid: float | None
    mexc_best_ask: float | None
    binance_impulse_bps: float | None
    basis_bps: float | None
    mexc_spread_bps: float | None
    depth_usdt: float
    btc_impulse_bps: float | None = None
    eth_impulse_bps: float | None = None

    def as_dict(self) -> dict:
        return {
            "ts_ms": self.ts_ms, "symbol": self.symbol,
            "binance_mid": self.binance_mid, "mexc_mid": self.mexc_mid,
            "mexc_best_bid": self.mexc_best_bid, "mexc_best_ask": self.mexc_best_ask,
            "binance_impulse_bps": self.binance_impulse_bps,
            "basis_bps": self.basis_bps,
            "mexc_spread_bps": self.mexc_spread_bps,
            "depth_usdt": self.depth_usdt,
            "btc_impulse_bps": self.btc_impulse_bps,
            "eth_impulse_bps": self.eth_impulse_bps,
        }


@dataclass
class SignalResult:
    decision: Decision
    side: Side | None
    reason: str
    snapshot: SignalSnapshot
    rejects: list[str] = field(default_factory=list)


class CooldownTracker:
    def __init__(self) -> None:
        self._until: dict[str, float] = {}

    def is_active(self, symbol: str) -> bool:
        return time.monotonic() < self._until.get(symbol, 0.0)

    def remaining_sec(self, symbol: str) -> float:
        return max(0.0, self._until.get(symbol, 0.0) - time.monotonic())

    def arm(self, symbol: str, seconds: float) -> None:
        self._until[symbol] = time.monotonic() + seconds


class Strategy:
    BTC = "BTCUSDT"
    ETH = "ETHUSDT"

    def __init__(self, cfg: StrategyConfig, binance: BookCache, mexc: BookCache,
                 cooldown: CooldownTracker | None = None) -> None:
        self.cfg = cfg
        self.binance = binance
        self.mexc = mexc
        self.cooldown = cooldown or CooldownTracker()

    def evaluate(self, symbol: str, open_position_symbols: Iterable[str]) -> SignalResult:
        symbol = symbol.upper()
        snap = self._snapshot(symbol)

        if (snap.binance_mid is None or snap.mexc_mid is None
                or snap.binance_impulse_bps is None):
            return SignalResult(Decision.NO_DATA, None, "no_data", snap)

        rejects: list[str] = []
        open_set = {s.upper() for s in open_position_symbols}

        # Per-symbol freshness: a thin ticker not updating for a few seconds
        # shouldn't block trades on every other ticker — only itself.
        if self.binance.is_stale(symbol):
            rejects.append("stale:binance")
        if self.mexc.is_stale(symbol):
            rejects.append("stale:mexc")

        if self.cooldown.is_active(symbol):
            rejects.append(f"cooldown:{self.cooldown.remaining_sec(symbol):.1f}s")

        if symbol in open_set:
            rejects.append("already_open")

        if snap.mexc_spread_bps is None or snap.mexc_spread_bps > self.cfg.max_mexc_spread_bps:
            rejects.append(f"mexc_spread:{snap.mexc_spread_bps}")

        if snap.depth_usdt < self.cfg.min_depth_usdt:
            rejects.append(f"depth:{snap.depth_usdt:.0f}")

        side: Side | None = None
        impulse = snap.binance_impulse_bps
        if impulse >= self.cfg.min_binance_impulse_bps:
            side = Side.LONG
        elif impulse <= -self.cfg.min_binance_impulse_bps:
            side = Side.SHORT
        else:
            rejects.append(f"impulse:{impulse:.2f}")

        # MEXC must lag Binance in the direction of the impulse.
        if side is Side.LONG:
            if snap.basis_bps is None or snap.basis_bps > -self.cfg.min_basis_lag_bps:
                rejects.append(f"basis_lag:{snap.basis_bps}")
        elif side is Side.SHORT:
            if snap.basis_bps is None or snap.basis_bps < self.cfg.min_basis_lag_bps:
                rejects.append(f"basis_lag:{snap.basis_bps}")

        if self.cfg.btc_eth_filter_enabled and side is not None:
            if self._macro_against(side, snap):
                rejects.append("macro_against")

        if rejects:
            return SignalResult(Decision.REJECT, side, ",".join(rejects), snap, rejects)

        assert side is not None
        return SignalResult(
            Decision.ENTER, side,
            f"impulse={impulse:.2f}bps basis={snap.basis_bps:.2f}bps "
            f"spread={snap.mexc_spread_bps:.2f}bps depth={snap.depth_usdt:.0f}",
            snap,
        )

    def _snapshot(self, symbol: str) -> SignalSnapshot:
        b_book = self.binance.book(symbol)
        m_book = self.mexc.book(symbol)
        b_mid = b_book.mid
        m_mid = m_book.mid
        impulse = self.binance.impulse_bps(symbol, self.cfg.impulse_window_ms)
        basis_bps: float | None = None
        if b_mid and m_mid and b_mid > 0:
            basis_bps = (m_mid - b_mid) / b_mid * 10_000.0
        btc_imp = self.binance.impulse_bps(self.BTC, self.cfg.impulse_window_ms)
        eth_imp = self.binance.impulse_bps(self.ETH, self.cfg.impulse_window_ms)
        return SignalSnapshot(
            ts_ms=int(time.time() * 1000),
            symbol=symbol,
            binance_mid=b_mid,
            mexc_mid=m_mid,
            mexc_best_bid=m_book.best_bid,
            mexc_best_ask=m_book.best_ask,
            binance_impulse_bps=impulse,
            basis_bps=basis_bps,
            mexc_spread_bps=m_book.spread_bps,
            depth_usdt=m_book.top_depth_usdt(),
            btc_impulse_bps=btc_imp,
            eth_impulse_bps=eth_imp,
        )

    def _macro_against(self, side: Side, snap: SignalSnapshot) -> bool:
        thr = self.cfg.btc_eth_filter_threshold_bps
        if side is Side.LONG:
            if snap.btc_impulse_bps is not None and snap.btc_impulse_bps <= -thr:
                return True
            if snap.eth_impulse_bps is not None and snap.eth_impulse_bps <= -thr:
                return True
        else:
            if snap.btc_impulse_bps is not None and snap.btc_impulse_bps >= thr:
                return True
            if snap.eth_impulse_bps is not None and snap.eth_impulse_bps >= thr:
                return True
        return False
