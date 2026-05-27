"""Generic orderbook cache with rolling mid-price history.

One instance per source (Binance via MetaScalp, MEXC via MetaScalp).
Updated either by an orderbook snapshot (full replace) or an incremental
update (price-level merge). Reads expose best bid/ask, mid, spread, depth,
and impulse_bps(window_ms) for the strategy.
"""
from __future__ import annotations

import collections
import time
from dataclasses import dataclass, field


@dataclass
class Book:
    bids: list[tuple[float, float]] = field(default_factory=list)   # sorted desc by price
    asks: list[tuple[float, float]] = field(default_factory=list)   # sorted asc by price
    ts_ms: int = 0

    @property
    def best_bid(self) -> float | None:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0][0] if self.asks else None

    @property
    def mid(self) -> float | None:
        if not self.bids or not self.asks:
            return None
        return (self.bids[0][0] + self.asks[0][0]) / 2.0

    @property
    def spread_bps(self) -> float | None:
        if not self.bids or not self.asks:
            return None
        mid = (self.bids[0][0] + self.asks[0][0]) / 2.0
        if mid <= 0:
            return None
        return (self.asks[0][0] - self.bids[0][0]) / mid * 10_000.0

    def top_depth_usdt(self, levels: int = 5) -> float:
        bid_usd = sum(p * q for p, q in self.bids[:levels])
        ask_usd = sum(p * q for p, q in self.asks[:levels])
        return min(bid_usd, ask_usd)


class BookCache:
    """Per-symbol Book + rolling (ts_ms, mid) deque for impulse calculations."""

    def __init__(self, *, max_staleness_ms: int = 1000,
                 history_seconds: float = 5.0,
                 history_max_len: int = 4096) -> None:
        self._books: dict[str, Book] = {}
        self._mid_history: dict[str, collections.deque[tuple[int, float]]] = {}
        self.max_staleness_ms = max_staleness_ms
        self.history_seconds = history_seconds
        self._history_max_len = history_max_len

    # ---- mutation -----------------------------------------------------------

    def replace_snapshot(self, symbol: str, bids: list[tuple[float, float]],
                          asks: list[tuple[float, float]],
                          ts_ms: int | None = None) -> None:
        sym = symbol.upper()
        b = sorted(bids, key=lambda x: -x[0])
        a = sorted(asks, key=lambda x: x[0])
        ts = ts_ms or _now_ms()
        self._books[sym] = Book(bids=b, asks=a, ts_ms=ts)
        mid = self._books[sym].mid
        if mid is not None:
            self._push_mid(sym, ts, mid)

    def apply_update(self, symbol: str,
                     bid_updates: list[tuple[float, float]],
                     ask_updates: list[tuple[float, float]],
                     ts_ms: int | None = None,
                     best_bid: float | None = None,
                     best_ask: float | None = None) -> None:
        """Merge level updates (qty=0 removes the level)."""
        sym = symbol.upper()
        book = self._books.get(sym) or Book()
        bid_map = {p: q for p, q in book.bids}
        ask_map = {p: q for p, q in book.asks}
        for p, q in bid_updates:
            if q <= 0:
                bid_map.pop(p, None)
                ask_map.pop(p, None)
            else:
                bid_map[p] = q
                ask_map.pop(p, None)
        for p, q in ask_updates:
            if q <= 0:
                ask_map.pop(p, None)
                bid_map.pop(p, None)
            else:
                ask_map[p] = q
                bid_map.pop(p, None)
        if best_bid is not None:
            bid_map = {p: q for p, q in bid_map.items() if p <= best_bid}
            ask_map = {p: q for p, q in ask_map.items() if p > best_bid}
        if best_ask is not None:
            ask_map = {p: q for p, q in ask_map.items() if p >= best_ask}
            bid_map = {p: q for p, q in bid_map.items() if p < best_ask}
        book.bids = sorted(bid_map.items(), key=lambda x: -x[0])
        book.asks = sorted(ask_map.items(), key=lambda x: x[0])
        book.ts_ms = ts_ms or _now_ms()
        self._books[sym] = book
        mid = book.mid
        if mid is not None:
            self._push_mid(sym, book.ts_ms, mid)

    def _push_mid(self, sym: str, ts_ms: int, mid: float) -> None:
        dq = self._mid_history.get(sym)
        if dq is None:
            dq = collections.deque(maxlen=self._history_max_len)
            self._mid_history[sym] = dq
        dq.append((ts_ms, mid))
        # trim old samples older than history_seconds
        cutoff = ts_ms - int(self.history_seconds * 1000)
        while dq and dq[0][0] < cutoff:
            dq.popleft()

    # ---- reads --------------------------------------------------------------

    def book(self, symbol: str) -> Book:
        return self._books.get(symbol.upper(), Book())

    def last_update_ms(self, symbol: str) -> int:
        b = self._books.get(symbol.upper())
        return b.ts_ms if b else 0

    def is_stale(self, symbol: str, max_age_ms: int | None = None) -> bool:
        max_age = max_age_ms if max_age_ms is not None else self.max_staleness_ms
        last = self.last_update_ms(symbol)
        if last == 0:
            return True
        return (_now_ms() - last) > max_age

    def impulse_bps(self, symbol: str, window_ms: int) -> float | None:
        hist = self._mid_history.get(symbol.upper())
        if not hist or len(hist) < 2:
            return None
        now_ms, mid_now = hist[-1]
        target_ts = now_ms - window_ms
        old_mid: float | None = None
        for ts, mid in hist:
            if ts <= target_ts:
                old_mid = mid
            else:
                break
        if old_mid is None or old_mid <= 0:
            return None
        return (mid_now - old_mid) / old_mid * 10_000.0


def _now_ms() -> int:
    return int(time.time() * 1000)
