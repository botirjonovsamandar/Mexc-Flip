"""MetaScalp orderbook payload parsing."""
from __future__ import annotations

from trading_bot.app.book_cache import BookCache
from trading_bot.app.main import _parse_orderbook_sides


def test_parse_typed_orderbook_updates():
    data = {
        "connectionId": 2,
        "ticker": "XLMUSDT",
        "updates": [
            {"price": 0.14856, "size": 61.35528, "type": "Bid"},
            {"price": 0.14858, "size": 133.27626, "type": "BestBid"},
            {"price": 0.14859, "size": 3112.36614, "type": "BestAsk"},
            {"price": 0.14861, "size": 2881.99373, "type": "Ask"},
        ],
    }

    bids, asks = _parse_orderbook_sides(data)

    assert bids == [(0.14856, 61.35528), (0.14858, 133.27626)]
    assert asks == [(0.14859, 3112.36614), (0.14861, 2881.99373)]


def test_parse_pascal_case_snapshot_levels_still_works():
    data = {
        "Bids": [{"Price": 1.0, "Size": 2.0}],
        "Asks": [{"Price": 1.1, "Size": 3.0}],
    }

    bids, asks = _parse_orderbook_sides(data)

    assert bids == [(1.0, 2.0)]
    assert asks == [(1.1, 3.0)]


def test_empty_update_does_not_create_fake_levels():
    bids, asks = _parse_orderbook_sides({"updates": []})

    assert bids == []
    assert asks == []


def test_best_markers_prune_crossed_stale_levels():
    cache = BookCache()
    cache.replace_snapshot(
        "HYPEUSDT",
        bids=[(62.815, 10.0)],
        asks=[(62.816, 10.0), (62.818, 10.0)],
    )

    cache.apply_update(
        "HYPEUSDT",
        bid_updates=[(62.826, 20.0)],
        ask_updates=[(62.828, 30.0)],
        best_bid=62.826,
        best_ask=62.828,
    )

    book = cache.book("HYPEUSDT")
    assert book.best_bid == 62.826
    assert book.best_ask == 62.828
    assert book.spread_bps is not None
    assert book.spread_bps > 0
