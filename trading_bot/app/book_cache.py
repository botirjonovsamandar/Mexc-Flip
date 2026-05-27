"""Generic orderbook cache with rolling mid-price history.

One instance per source (Binance via MetaScalp, MEXC via MetaScalp).
Updated either by an orderbook snapshot (full replace) or an incremental
update (price-level merge). Reads expose best bid/ask, mid, spread, depth,
and impulse_bps(window_ms) for the strategy.

Implementation notes:
  * Bid/ask levels are stored in `sortedcontainers.SortedDict` so inserts,
    updates, and removals are O(log n) — the prior dict-then-sort approach
    rebuilt the whole structure on every WS update (200-500 msg/s × full
    sort = up to several hundred ms/sec of pure CPU on busy markets).
  * Mid-price history is kept as parallel arrays (timestamps, mids) with
    `bisect.bisect_right` lookup — the impulse window query is O(log n)
    instead of the prior linear deque scan.
"""
from __future__ import annotations

import array
import bisect
import time
from dataclasses import dataclass, field

from sortedcontainers import SortedDict


@dataclass
class Book:
    # Bids: SortedDict[price -> size], naturally ascending. best_bid = last key.
    # Asks: SortedDict[price -> size], naturally ascending. best_ask = first key.
    bids: SortedDict = field(default_factory=SortedDict)
    asks: SortedDict = field(default_factory=SortedDict)
    ts_ms: int = 0

    @property
    def best_bid(self) -> float | None:
        if not self.bids:
            return None
        # SortedDict keys are sorted ascending; highest bid is the last key.
        return self.bids.keys()[-1]

    @property
    def best_ask(self) -> float | None:
        if not self.asks:
            return None
        return self.asks.keys()[0]

    @property
    def mid(self) -> float | None:
        if not self.bids or not self.asks:
            return None
        return (self.bids.keys()[-1] + self.asks.keys()[0]) / 2.0

    @property
    def spread_bps(self) -> float | None:
        if not self.bids or not self.asks:
            return None
        bb = self.bids.keys()[-1]
        ba = self.asks.keys()[0]
        mid = (bb + ba) / 2.0
        if mid <= 0:
            return None
        return (ba - bb) / mid * 10_000.0

    def top_depth_usdt(self, levels: int = 5) -> float:
        # Top of book — highest bids, lowest asks.
        bid_keys = self.bids.keys()
        ask_keys = self.asks.keys()
        n_b = len(bid_keys)
        bid_usd = 0.0
        for i in range(min(levels, n_b)):
            p = bid_keys[n_b - 1 - i]
            bid_usd += p * self.bids[p]
        ask_usd = 0.0
        for i in range(min(levels, len(ask_keys))):
            p = ask_keys[i]
            ask_usd += p * self.asks[p]
        return min(bid_usd, ask_usd)

    # Compatibility helpers — older callers iterated bids/asks as
    # list[(price, size)]. Provide the same shape on read.
    def bids_list(self, levels: int | None = None) -> list[tuple[float, float]]:
        keys = self.bids.keys()
        if not keys:
            return []
        # Descending by price (highest bid first), matching the old contract.
        rng = range(len(keys) - 1, -1, -1)
        if levels is not None:
            rng = range(len(keys) - 1, max(-1, len(keys) - 1 - levels), -1)
        return [(keys[i], self.bids[keys[i]]) for i in rng]

    def asks_list(self, levels: int | None = None) -> list[tuple[float, float]]:
        keys = self.asks.keys()
        if not keys:
            return []
        rng = range(min(levels, len(keys)) if levels is not None else len(keys))
        return [(keys[i], self.asks[keys[i]]) for i in rng]


class _MidHistory:
    """Parallel arrays + binary search for O(log n) window lookups."""

    __slots__ = ("_ts", "_mids", "_max_len", "_max_age_ms")

    def __init__(self, max_len: int, max_age_ms: int) -> None:
        self._ts: array.array = array.array("q")  # int64 timestamps in ms
        self._mids: array.array = array.array("d")  # float64 mid prices
        self._max_len = max_len
        self._max_age_ms = max_age_ms

    def push(self, ts_ms: int, mid: float) -> None:
        self._ts.append(ts_ms)
        self._mids.append(mid)
        # Trim by age (cutoff in ms).
        cutoff = ts_ms - self._max_age_ms
        # bisect_right returns insertion point — number of samples older than cutoff
        drop = bisect.bisect_right(self._ts, cutoff)
        if drop > 0:
            # array slicing returns new array; for very-frequent pushes the
            # max_len safety net keeps the worst-case cheap.
            self._ts = self._ts[drop:]
            self._mids = self._mids[drop:]
        # Hard cap on length so a stuck consumer can't OOM us.
        if len(self._ts) > self._max_len:
            excess = len(self._ts) - self._max_len
            self._ts = self._ts[excess:]
            self._mids = self._mids[excess:]

    def lookup_before(self, target_ts: int) -> float | None:
        if len(self._ts) < 2:
            return None
        # bisect_right gives the first index with ts > target_ts.
        # We want the last sample with ts <= target_ts → idx - 1.
        idx = bisect.bisect_right(self._ts, target_ts)
        if idx <= 0:
            return None
        return self._mids[idx - 1]

    def latest(self) -> tuple[int, float] | None:
        if not self._ts:
            return None
        return self._ts[-1], self._mids[-1]

    def clear(self) -> None:
        # Backward-compat for tests that manipulate history directly.
        self._ts = array.array("q")
        self._mids = array.array("d")

    def append(self, item: tuple[int, float]) -> None:
        # Backward-compat alias used by older tests that mimicked the deque
        # interface. Equivalent to push(ts, mid) with internal trimming.
        ts_ms, mid = item
        self.push(int(ts_ms), float(mid))

    def __len__(self) -> int:
        return len(self._ts)


class BookCache:
    """Per-symbol Book + rolling (ts_ms, mid) history for impulse calculations."""

    def __init__(self, *, max_staleness_ms: int = 1000,
                 history_seconds: float = 5.0,
                 history_max_len: int = 4096) -> None:
        self._books: dict[str, Book] = {}
        self._mid_history: dict[str, _MidHistory] = {}
        self.max_staleness_ms = max_staleness_ms
        self.history_seconds = history_seconds
        self._history_max_len = history_max_len
        self._history_age_ms = int(history_seconds * 1000)

    # ---- mutation -----------------------------------------------------------

    def replace_snapshot(self, symbol: str, bids: list[tuple[float, float]],
                          asks: list[tuple[float, float]],
                          ts_ms: int | None = None) -> None:
        sym = symbol.upper()
        ts = ts_ms or _now_ms()
        book = Book(ts_ms=ts)
        for p, q in bids:
            if q > 0:
                book.bids[p] = q
        for p, q in asks:
            if q > 0:
                book.asks[p] = q
        self._books[sym] = book
        m = book.mid
        if m is not None:
            self._push_mid(sym, ts, m)

    def apply_update(self, symbol: str,
                     bid_updates: list[tuple[float, float]],
                     ask_updates: list[tuple[float, float]],
                     ts_ms: int | None = None,
                     best_bid: float | None = None,
                     best_ask: float | None = None) -> None:
        """Merge level updates (qty=0 removes the level)."""
        sym = symbol.upper()
        book = self._books.get(sym) or Book()
        for p, q in bid_updates:
            if q <= 0:
                book.bids.pop(p, None)
                book.asks.pop(p, None)
            else:
                book.bids[p] = q
                book.asks.pop(p, None)
        for p, q in ask_updates:
            if q <= 0:
                book.asks.pop(p, None)
                book.bids.pop(p, None)
            else:
                book.asks[p] = q
                book.bids.pop(p, None)
        if best_bid is not None:
            # Drop any bid above best_bid (stale) and any ask at/below it.
            while book.bids and book.bids.keys()[-1] > best_bid:
                book.bids.popitem(-1)
            while book.asks and book.asks.keys()[0] <= best_bid:
                book.asks.popitem(0)
        if best_ask is not None:
            while book.asks and book.asks.keys()[0] < best_ask:
                book.asks.popitem(0)
            while book.bids and book.bids.keys()[-1] >= best_ask:
                book.bids.popitem(-1)
        book.ts_ms = ts_ms or _now_ms()
        self._books[sym] = book
        m = book.mid
        if m is not None:
            self._push_mid(sym, book.ts_ms, m)

    def _push_mid(self, sym: str, ts_ms: int, mid: float) -> None:
        hist = self._mid_history.get(sym)
        if hist is None:
            hist = _MidHistory(self._history_max_len, self._history_age_ms)
            self._mid_history[sym] = hist
        hist.push(ts_ms, mid)

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
        if hist is None or len(hist) < 2:
            return None
        latest = hist.latest()
        if latest is None:
            return None
        now_ms, mid_now = latest
        target_ts = now_ms - window_ms
        old_mid = hist.lookup_before(target_ts)
        if old_mid is None or old_mid <= 0:
            return None
        return (mid_now - old_mid) / old_mid * 10_000.0


def _now_ms() -> int:
    return int(time.time() * 1000)
