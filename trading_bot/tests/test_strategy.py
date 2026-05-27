"""Tests for the strategy decision matrix.

Pure-Python tests with no I/O. We seed two BookCache instances and check
that every reject branch fires and that LONG/SHORT entries trigger when
all conditions are met.
"""
from __future__ import annotations

import time

from trading_bot.app.book_cache import BookCache
from trading_bot.app.config import StrategyConfig
from trading_bot.app.strategy import (CooldownTracker, Decision, Side,
                                       Strategy)


def _binance_cache(books, impulses):
    """Seed best bid/ask, then push old + new mids to fabricate impulse_bps."""
    bc = BookCache(history_seconds=10.0)
    now_ms = int(time.time() * 1000)
    old_ms = now_ms - 600
    for sym, (bid, ask) in books.items():
        mid_now = (bid + ask) / 2
        imp = impulses.get(sym, 0.0)
        mid_old = mid_now / (1 + imp / 10_000.0)
        # push the old mid first so impulse_bps can find it
        bc.replace_snapshot(sym, [(bid, 1000.0)], [(ask, 1000.0)], ts_ms=old_ms)
        # overwrite history entry to fake the old midpoint
        bc._mid_history[sym].clear()  # noqa: SLF001
        bc._mid_history[sym].append((old_ms, mid_old))  # noqa: SLF001
        bc._mid_history[sym].append((now_ms, mid_now))  # noqa: SLF001
        bc._books[sym].ts_ms = now_ms  # noqa: SLF001
    return bc


def _mexc_cache(symbol, bid, ask, depth_each_level: float = 2000.0):
    mc = BookCache(history_seconds=10.0)
    bids = [(bid - 0.0001 * i, depth_each_level / max(bid, 1e-9)) for i in range(5)]
    asks = [(ask + 0.0001 * i, depth_each_level / max(ask, 1e-9)) for i in range(5)]
    mc.replace_snapshot(symbol, bids, asks)
    return mc


def _cfg():
    return StrategyConfig(
        impulse_window_ms=500, min_binance_impulse_bps=6.0,
        min_basis_lag_bps=4.0, max_mexc_spread_bps=4.0,
        min_depth_usdt=2000.0, btc_eth_filter_enabled=False,
        cooldown_per_symbol_sec=30.0,
    )


def test_long_signal_accepted():
    binance_books = {"XLMUSDT": (0.4995, 0.5005), "BTCUSDT": (60000, 60000.1),
                     "ETHUSDT": (3000, 3000.1)}
    bc = _binance_cache(binance_books, {"XLMUSDT": 8.0})
    mc = _mexc_cache("XLMUSDT", 0.49970, 0.49980, depth_each_level=2000.0)
    res = Strategy(_cfg(), bc, mc, CooldownTracker()).evaluate("XLMUSDT", [])
    assert res.decision is Decision.ENTER, res.reason
    assert res.side is Side.LONG


def test_short_signal_accepted():
    binance_books = {"XLMUSDT": (0.4995, 0.5005), "BTCUSDT": (60000, 60000.1),
                     "ETHUSDT": (3000, 3000.1)}
    bc = _binance_cache(binance_books, {"XLMUSDT": -8.0})
    mc = _mexc_cache("XLMUSDT", 0.50020, 0.50030, depth_each_level=2000.0)
    res = Strategy(_cfg(), bc, mc, CooldownTracker()).evaluate("XLMUSDT", [])
    assert res.decision is Decision.ENTER, res.reason
    assert res.side is Side.SHORT


def test_rejects_when_impulse_too_small():
    bc = _binance_cache({"XLMUSDT": (0.4995, 0.5005), "BTCUSDT": (60000, 60000.1),
                          "ETHUSDT": (3000, 3000.1)}, {"XLMUSDT": 3.0})
    mc = _mexc_cache("XLMUSDT", 0.49970, 0.49980)
    res = Strategy(_cfg(), bc, mc).evaluate("XLMUSDT", [])
    assert res.decision is Decision.REJECT
    assert any("impulse" in r for r in res.rejects)


def test_rejects_when_basis_not_lagging():
    bc = _binance_cache({"XLMUSDT": (0.4995, 0.5005), "BTCUSDT": (60000, 60000.1),
                          "ETHUSDT": (3000, 3000.1)}, {"XLMUSDT": 8.0})
    mc = _mexc_cache("XLMUSDT", 0.4995, 0.5005)
    res = Strategy(_cfg(), bc, mc).evaluate("XLMUSDT", [])
    assert res.decision is Decision.REJECT
    assert any("basis_lag" in r for r in res.rejects)


def test_rejects_when_spread_wide():
    bc = _binance_cache({"XLMUSDT": (0.4995, 0.5005), "BTCUSDT": (60000, 60000.1),
                          "ETHUSDT": (3000, 3000.1)}, {"XLMUSDT": 8.0})
    mc = _mexc_cache("XLMUSDT", 0.4990, 0.5000)
    res = Strategy(_cfg(), bc, mc).evaluate("XLMUSDT", [])
    assert res.decision is Decision.REJECT
    assert any("mexc_spread" in r for r in res.rejects)


def test_rejects_when_already_open():
    bc = _binance_cache({"XLMUSDT": (0.4995, 0.5005), "BTCUSDT": (60000, 60000.1),
                          "ETHUSDT": (3000, 3000.1)}, {"XLMUSDT": 8.0})
    mc = _mexc_cache("XLMUSDT", 0.49970, 0.49980)
    res = Strategy(_cfg(), bc, mc).evaluate("XLMUSDT", ["XLMUSDT"])
    assert res.decision is Decision.REJECT
    assert "already_open" in res.rejects


def test_rejects_when_cooldown_active():
    bc = _binance_cache({"XLMUSDT": (0.4995, 0.5005), "BTCUSDT": (60000, 60000.1),
                          "ETHUSDT": (3000, 3000.1)}, {"XLMUSDT": 8.0})
    mc = _mexc_cache("XLMUSDT", 0.49970, 0.49980)
    cd = CooldownTracker()
    cd.arm("XLMUSDT", 30.0)
    res = Strategy(_cfg(), bc, mc, cd).evaluate("XLMUSDT", [])
    assert res.decision is Decision.REJECT
    assert any("cooldown" in r for r in res.rejects)


def test_rejects_when_symbol_data_stale():
    """If MEXC book hasn't updated within max_staleness_ms, reject just this
    symbol — other symbols are unaffected (verified via the reject reason)."""
    bc = _binance_cache({"XLMUSDT": (0.4995, 0.5005), "BTCUSDT": (60000, 60000.1),
                          "ETHUSDT": (3000, 3000.1)}, {"XLMUSDT": 8.0})
    mc = _mexc_cache("XLMUSDT", 0.49970, 0.49980)
    mc.max_staleness_ms = 100
    # Backdate the MEXC book so it's well past the staleness threshold.
    mc._books["XLMUSDT"].ts_ms = int(time.time() * 1000) - 5_000  # noqa: SLF001
    res = Strategy(_cfg(), bc, mc).evaluate("XLMUSDT", [])
    assert res.decision is Decision.REJECT
    assert "stale:mexc" in res.rejects


def test_macro_filter_blocks_long_when_btc_dumping():
    cfg = StrategyConfig(
        impulse_window_ms=500, min_binance_impulse_bps=6.0,
        min_basis_lag_bps=4.0, max_mexc_spread_bps=4.0,
        min_depth_usdt=2000.0, btc_eth_filter_enabled=True,
        btc_eth_filter_threshold_bps=4.0,
    )
    bc = _binance_cache({"XLMUSDT": (0.4995, 0.5005), "BTCUSDT": (60000, 60000.1),
                          "ETHUSDT": (3000, 3000.1)},
                         {"XLMUSDT": 8.0, "BTCUSDT": -10.0})
    mc = _mexc_cache("XLMUSDT", 0.49970, 0.49980)
    res = Strategy(cfg, bc, mc).evaluate("XLMUSDT", [])
    assert res.decision is Decision.REJECT
    assert "macro_against" in res.rejects
