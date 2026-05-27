"""Execution-engine tests for DRY_RUN + PAPER modes.

Live mode is not exercised — it requires a running MetaScalp instance.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from trading_bot.app.book_cache import BookCache
from trading_bot.app.config import (BotConfig, ExecutionConfig, RiskConfig,
                                     StrategyConfig, TradingMode)
from trading_bot.app.execution import (EntryPlan, ExecutionEngine, FillResult,
                                       TickerRules)
from trading_bot.app.position_manager import ExitReason, PositionManager
from trading_bot.app.risk_manager import RiskManager
from trading_bot.app.strategy import (Decision, Side, SignalResult,
                                       SignalSnapshot)
from trading_bot.app.metascalp_client import (MetaScalpOrderRejected,
                                              OrderRequest, OrderSide,
                                              OrderType)


def _make_bot_cfg(mode: TradingMode, tmp_path: Path) -> BotConfig:
    cfg = BotConfig(
        mode=mode,
        symbols=["XLMUSDT"],
        strategy=StrategyConfig(min_depth_usdt=100.0),
        risk=RiskConfig(max_position_usdt=10.0, max_slippage_bps=50.0,
                        max_spread_bps=20.0, stop_loss_percent=1.0,
                        take_profit_bps=8.0, breakeven_trigger_bps=4.0,
                        max_position_time_seconds=60),
        execution=ExecutionConfig(order_type="LIMIT", min_notional_usdt=1.0,
                                   fill_timeout_ms=500),
    )
    cfg.storage.trades_csv = str(tmp_path / "trades.csv")
    cfg.storage.signals_csv = str(tmp_path / "signals.csv")
    return cfg


def _snap(symbol: str, side: Side, binance_mid: float, mexc_mid: float) -> SignalResult:
    snap = SignalSnapshot(
        ts_ms=int(time.time() * 1000), symbol=symbol,
        binance_mid=binance_mid, mexc_mid=mexc_mid,
        mexc_best_bid=mexc_mid - 0.0005, mexc_best_ask=mexc_mid + 0.0005,
        binance_impulse_bps=8.0, basis_bps=-5.0, mexc_spread_bps=2.0,
        depth_usdt=5000.0,
    )
    return SignalResult(Decision.ENTER, side, "test", snap)


def _populate_mexc(cache: BookCache, symbol: str, bid: float, ask: float,
                    depth_qty: float = 100000.0) -> None:
    cache.replace_snapshot(symbol,
                             [(bid, depth_qty), (bid - 0.001, depth_qty)],
                             [(ask, depth_qty), (ask + 0.001, depth_qty)])


def _populate_tight_mexc(cache: BookCache, symbol: str) -> None:
    cache.replace_snapshot(
        symbol,
        [(0.49995, 100000.0), (0.49990, 100000.0)],
        [(0.50005, 100000.0), (0.50010, 100000.0)],
    )


def _engine_for_plans(tmp_path: Path, *, balance: float) -> ExecutionEngine:
    cfg = _make_bot_cfg(TradingMode.PAPER, tmp_path)
    cfg.symbols = ["XLMUSDT", "DOGEUSDT"]
    cfg.capital.paper_balance_usdt = balance
    cfg.capital.low_balance_margin_pct_range = (0.75, 0.75)
    cfg.capital.high_balance_top_signal_margin_pct_range = (0.60, 0.60)
    cfg.capital.min_net_edge_bps = 0.0
    cfg.capital.min_expected_profit_usdt = 0.0
    cfg.rate_limits.upstream_hourly_limit = 500
    mexc = BookCache()
    _populate_tight_mexc(mexc, "XLMUSDT")
    _populate_tight_mexc(mexc, "DOGEUSDT")
    positions = PositionManager(cfg.risk, mexc)
    risk = RiskManager(cfg.risk)
    risk.set_health(metascalp=True, binance=True, mexc=True)
    return ExecutionEngine(cfg, mexc, None, None, positions, risk, trades_log=None)


def _snap_with_basis(symbol: str, basis_bps: float) -> SignalResult:
    snap = SignalSnapshot(
        ts_ms=int(time.time() * 1000), symbol=symbol,
        binance_mid=0.5005, mexc_mid=0.5000,
        mexc_best_bid=0.49995, mexc_best_ask=0.50005,
        binance_impulse_bps=8.0, basis_bps=basis_bps,
        mexc_spread_bps=2.0, depth_usdt=5000.0,
    )
    return SignalResult(Decision.ENTER, Side.LONG, "test", snap)


@pytest.mark.asyncio
async def test_dry_run_opens_virtual_position(tmp_path):
    cfg = _make_bot_cfg(TradingMode.DRY_RUN, tmp_path)
    mexc = BookCache()
    _populate_mexc(mexc, "XLMUSDT", 0.4995, 0.5005)
    positions = PositionManager(cfg.risk, mexc)
    risk = RiskManager(cfg.risk)
    risk.set_health(metascalp=True, binance=True, mexc=True)
    eng = ExecutionEngine(cfg, mexc, None, None, positions, risk, trades_log=None)

    sig = _snap("XLMUSDT", Side.LONG, 0.5005, 0.5000)
    pos = await eng.try_enter(sig)
    assert pos is not None
    assert positions.count == 1
    assert positions.get("XLMUSDT").side is Side.LONG


@pytest.mark.asyncio
async def test_paper_fill_uses_best_ask_for_long(tmp_path):
    cfg = _make_bot_cfg(TradingMode.PAPER, tmp_path)
    mexc = BookCache()
    _populate_mexc(mexc, "XLMUSDT", 0.4995, 0.5005)
    positions = PositionManager(cfg.risk, mexc)
    risk = RiskManager(cfg.risk)
    risk.set_health(metascalp=True, binance=True, mexc=True)
    eng = ExecutionEngine(cfg, mexc, None, None, positions, risk, trades_log=None)

    sig = _snap("XLMUSDT", Side.LONG, 0.5005, 0.5000)
    pos = await eng.try_enter(sig)
    assert pos is not None
    assert abs(pos.entry_price - 0.5005) < 1e-9


@pytest.mark.asyncio
async def test_close_position_records_pnl(tmp_path):
    cfg = _make_bot_cfg(TradingMode.PAPER, tmp_path)
    mexc = BookCache()
    _populate_mexc(mexc, "XLMUSDT", 0.4995, 0.5005)
    positions = PositionManager(cfg.risk, mexc)
    risk = RiskManager(cfg.risk)
    risk.set_health(metascalp=True, binance=True, mexc=True)
    eng = ExecutionEngine(cfg, mexc, None, None, positions, risk, trades_log=None)

    sig = _snap("XLMUSDT", Side.LONG, 0.5005, 0.5000)
    await eng.try_enter(sig)
    _populate_mexc(mexc, "XLMUSDT", 0.5015, 0.5025)
    await eng.close_position("XLMUSDT", ExitReason.TAKE_PROFIT, "test")
    assert positions.count == 0
    assert risk.day_pnl > 0


@pytest.mark.asyncio
async def test_slippage_estimate_rejects_thin_book(tmp_path):
    cfg = _make_bot_cfg(TradingMode.PAPER, tmp_path)
    cfg.strategy.min_depth_usdt = 0.0       # don't trip the depth check
    cfg.risk.max_position_usdt = 10_000.0
    cfg.risk.max_slippage_bps = 1.0
    mexc = BookCache()
    # only one unit at each level; big size will walk the book far
    mexc.replace_snapshot("XLMUSDT",
                            [(0.4995, 1.0)], [(0.5005, 1.0), (0.5100, 1.0)])
    positions = PositionManager(cfg.risk, mexc)
    risk = RiskManager(cfg.risk)
    risk.set_health(metascalp=True, binance=True, mexc=True)
    eng = ExecutionEngine(cfg, mexc, None, None, positions, risk, trades_log=None)

    sig = _snap("XLMUSDT", Side.LONG, 0.5005, 0.5000)
    pos = await eng.try_enter(sig)
    assert pos is None
    assert positions.count == 0


def test_order_request_payload_shape():
    """OrderRequest -> MetaScalp payload uses PascalCase and numeric enums."""
    from trading_bot.app.metascalp_client import OrderRequest, OrderSide, OrderType
    req = OrderRequest(ticker="XLMUSDT", side=OrderSide.BUY, size=10.0,
                        price=0.5, order_type=OrderType.LIMIT, reduce_only=False)
    p = req.to_payload()
    assert p["Ticker"] == "XLMUSDT"
    assert p["Side"] == 1
    assert p["Size"] == 10.0
    assert p["Type"] == 0
    assert p["Price"] == 0.5
    assert p["ReduceOnly"] is False

    market_req = OrderRequest(ticker="XLMUSDT", side=OrderSide.SELL, size=5.0,
                               order_type=OrderType.MARKET, reduce_only=True)
    mp = market_req.to_payload()
    assert mp["Type"] == 4
    assert mp["Side"] == 2
    assert mp["ReduceOnly"] is True
    assert "Price" not in mp  # market orders must not include Price

    attached_stop_req = OrderRequest(
        ticker="XLMUSDT", side=OrderSide.BUY, size=10.0,
        price=0.5, order_type=OrderType.LIMIT, stop_loss_price=0.495,
    )
    sp = attached_stop_req.to_payload()
    assert sp["StopLossPrice"] == 0.495


def test_small_live_size_respects_min_size_and_increment(tmp_path):
    cfg = _make_bot_cfg(TradingMode.SMALL_LIVE, tmp_path)
    cfg.execution.min_notional_usdt = 5.0
    cfg.risk.max_position_usdt = 50.0
    mexc = BookCache()
    positions = PositionManager(cfg.risk, mexc)
    risk = RiskManager(cfg.risk)
    eng = ExecutionEngine(cfg, mexc, None, None, positions, risk, trades_log=None)
    eng.mexc_ticker_rules["ZECUSDT"] = TickerRules(
        price_increment=0.01,
        size_increment=0.0001,
        min_size=0.01,
        max_size=4100.0,
    )

    qty, notional = eng._size_order("ZECUSDT", 610.0)  # noqa: SLF001

    assert qty == pytest.approx(0.01)
    assert notional == pytest.approx(6.1)


def test_limit_price_rounds_to_tick_in_execution_direction(tmp_path):
    cfg = _make_bot_cfg(TradingMode.SMALL_LIVE, tmp_path)
    mexc = BookCache()
    positions = PositionManager(cfg.risk, mexc)
    risk = RiskManager(cfg.risk)
    eng = ExecutionEngine(cfg, mexc, None, None, positions, risk, trades_log=None)
    eng.mexc_ticker_rules["ZECUSDT"] = TickerRules(price_increment=0.01)

    assert eng._round_price("ZECUSDT", Side.LONG, 610.025) == pytest.approx(610.03)  # noqa: SLF001
    assert eng._round_price("ZECUSDT", Side.SHORT, 610.025) == pytest.approx(610.02)  # noqa: SLF001


def test_tick_rounding_removes_float_precision_noise(tmp_path):
    cfg = _make_bot_cfg(TradingMode.SMALL_LIVE, tmp_path)
    mexc = BookCache()
    positions = PositionManager(cfg.risk, mexc)
    risk = RiskManager(cfg.risk)
    eng = ExecutionEngine(cfg, mexc, None, None, positions, risk, trades_log=None)
    eng.mexc_ticker_rules["ZECUSDT"] = TickerRules(price_increment=0.01)

    price = eng._round_price("ZECUSDT", Side.SHORT, 572.8000000000001)  # noqa: SLF001
    payload = OrderRequest(
        ticker="ZEC_USDT",
        side=OrderSide.SELL,
        size=0.0716,
        price=price,
        order_type=OrderType.LIMIT,
    ).to_payload()

    encoded = json.dumps(payload)
    assert payload["Price"] == pytest.approx(572.8)
    assert "572.8000000000001" not in encoded


def test_native_ticker_mismatch_blocks_btc_mapping(tmp_path):
    eng = _engine_for_plans(tmp_path, balance=50.0)

    gate = eng._validate_live_ticker("XLMUSDT", "BTC_USDT")  # noqa: SLF001

    assert not gate.allowed
    assert "native_ticker_mismatch" in gate.reason


def test_live_position_parser_normalizes_mexc_ticker(tmp_path):
    eng = _engine_for_plans(tmp_path, balance=50.0)

    info = eng._parse_live_position({  # noqa: SLF001
        "Ticker": "HYPE_USDT",
        "Side": "Long",
        "Size": "0.8",
        "EntryPrice": "61.048",
        "UnrealizedPnl": "0.039",
    })

    assert info is not None
    assert info.symbol == "HYPEUSDT"
    assert info.side is Side.LONG
    assert info.qty == pytest.approx(0.8)
    assert info.entry_price == pytest.approx(61.048)


@pytest.mark.asyncio
async def test_low_balance_selects_one_best_signal(tmp_path):
    eng = _engine_for_plans(tmp_path, balance=29.99)

    plans = await eng.build_entry_plans([
        _snap_with_basis("DOGEUSDT", -8.0),
        _snap_with_basis("XLMUSDT", -14.0),
    ])

    assert len(plans) == 1
    assert plans[0].signal.snapshot.symbol == "XLMUSDT"
    assert plans[0].margin_usdt == pytest.approx(29.99 * 0.75)


@pytest.mark.asyncio
async def test_high_balance_best_signal_gets_larger_slot_and_second_gets_rest(tmp_path):
    eng = _engine_for_plans(tmp_path, balance=50.0)

    plans = await eng.build_entry_plans([
        _snap_with_basis("DOGEUSDT", -8.0),
        _snap_with_basis("XLMUSDT", -14.0),
    ])

    assert [p.signal.snapshot.symbol for p in plans] == ["XLMUSDT", "DOGEUSDT"]
    # balance reserve is max(10%, 1 USDT) -> 45 USDT allocatable.
    assert plans[0].margin_usdt == pytest.approx(27.0)
    assert plans[1].margin_usdt == pytest.approx(18.0)


@pytest.mark.asyncio
async def test_weak_second_signal_is_not_opened(tmp_path):
    eng = _engine_for_plans(tmp_path, balance=50.0)
    eng.capital_cfg.min_net_edge_bps = 4.0

    plans = await eng.build_entry_plans([
        _snap_with_basis("DOGEUSDT", -4.0),
        _snap_with_basis("XLMUSDT", -14.0),
    ])

    assert len(plans) == 1
    assert plans[0].signal.snapshot.symbol == "XLMUSDT"


@pytest.mark.asyncio
async def test_pending_entry_counts_as_occupied_slot(tmp_path):
    eng = _engine_for_plans(tmp_path, balance=25.0)
    eng._pending_entries["XLMUSDT"] = 10.0  # noqa: SLF001

    plans = await eng.build_entry_plans([
        _snap_with_basis("DOGEUSDT", -14.0),
    ])

    assert plans == []


@pytest.mark.asyncio
async def test_entry_latency_guard_blocks_new_plans(tmp_path):
    eng = _engine_for_plans(tmp_path, balance=50.0)

    eng._arm_entry_latency_guard(FillResult(  # noqa: SLF001
        ok=False,
        price=None,
        qty=0.0,
        order_id=None,
        latency_send_ms=700.0,
        latency_fill_ms=0.0,
        slippage_bps=0.0,
        reason="test",
    ))

    plans = await eng.build_entry_plans([
        _snap_with_basis("XLMUSDT", -14.0),
    ])

    assert plans == []


class _FakeMetaScalpStopFails:
    def __init__(self) -> None:
        self.calls = []

    async def place_order(self, _connection_id, req):  # noqa: ANN001
        self.calls.append(req)
        if req.order_type == OrderType.STOP_LOSS:
            raise RuntimeError("stop rejected")
        return {"OrderId": f"order-{len(self.calls)}"}

    async def get_orders(self, _connection_id, ticker=None):  # noqa: ANN001
        return [{
            "OrderId": "order-1",
            "Status": "FILLED",
            "FilledSize": 10.0,
            "FilledPrice": 0.50005,
        }]


class _FakeMetaScalpPositionAfterTimeout:
    def __init__(self) -> None:
        self.calls = []

    async def place_order(self, _connection_id, req):  # noqa: ANN001
        self.calls.append(req)
        if req.order_type == OrderType.STOP_LOSS:
            return {"OrderId": "stop-1"}
        return {"OrderId": "entry-1"}

    async def get_orders(self, _connection_id, ticker=None):  # noqa: ANN001
        return []

    async def get_positions(self, _connection_id):  # noqa: ANN001
        return [{
            "Ticker": "XLM_USDT",
            "Side": "Long",
            "Size": "10",
            "EntryPrice": "0.50005",
        }]


class _FakeMetaScalpCancelOrderFails:
    def __init__(self) -> None:
        self.calls = []
        self.cancel_all_called = False

    async def place_order(self, _connection_id, req):  # noqa: ANN001
        self.calls.append(req)
        return {"OrderId": "entry-1"}

    async def get_orders(self, _connection_id, ticker=None):  # noqa: ANN001
        return []

    async def get_positions(self, _connection_id):  # noqa: ANN001
        return []

    async def cancel_order(self, _connection_id, ticker, order_id):  # noqa: ANN001
        raise RuntimeError("cancel_order rejected")

    async def cancel_all(self, _connection_id, ticker=None):  # noqa: ANN001
        self.cancel_all_called = True
        return {"ok": True}


class _FakeMetaScalpPlaceRaisesPositionExists:
    def __init__(self) -> None:
        self.calls = []

    async def place_order(self, _connection_id, req):  # noqa: ANN001
        self.calls.append(req)
        if len(self.calls) == 1:
            raise RuntimeError("transport timeout after exchange accepted order")
        return {"OrderId": "stop-1"}

    async def get_positions(self, _connection_id):  # noqa: ANN001
        return [{
            "Ticker": "XLM_USDT",
            "Side": "Long",
            "Size": "10",
            "EntryPrice": "0.50005",
        }]

    async def cancel_all(self, _connection_id, ticker=None):  # noqa: ANN001
        return {"ok": True}


class _FakeMetaScalpExplicitReject:
    def __init__(self) -> None:
        self.calls = []
        self.cancel_all_called = False

    async def place_order(self, _connection_id, req):  # noqa: ANN001
        self.calls.append(req)
        raise MetaScalpOrderRejected(
            400,
            '{"error":"Price or quantity precision error, please enter again"}',
            req.to_payload(),
        )

    async def get_positions(self, _connection_id):  # noqa: ANN001
        return []

    async def cancel_all(self, _connection_id, ticker=None):  # noqa: ANN001
        self.cancel_all_called = True
        return {"ok": True}


class _FakeMetaScalpExternalPosition:
    def __init__(self) -> None:
        self.calls = []

    async def get_positions(self, _connection_id):  # noqa: ANN001
        return [{
            "Ticker": "XLM_USDT",
            "Side": "Short",
            "Size": "10",
            "EntryPrice": "0.50005",
        }]

    async def place_order(self, _connection_id, req):  # noqa: ANN001
        self.calls.append(req)
        return {"OrderId": "close-1"}

    async def cancel_all(self, _connection_id, ticker=None):  # noqa: ANN001
        return {"ok": True}


@pytest.mark.asyncio
async def test_live_stop_failure_immediately_closes_and_blocks_entries(tmp_path):
    cfg = _make_bot_cfg(TradingMode.SMALL_LIVE, tmp_path)
    cfg.execution.min_notional_usdt = 5.0
    cfg.risk.max_spread_bps = 20.0
    cfg.risk.max_slippage_bps = 50.0
    mexc = BookCache()
    _populate_tight_mexc(mexc, "XLMUSDT")
    positions = PositionManager(cfg.risk, mexc)
    risk = RiskManager(cfg.risk)
    risk.set_health(metascalp=True, binance=True, mexc=True)
    fake = _FakeMetaScalpStopFails()
    eng = ExecutionEngine(cfg, mexc, fake, 5, positions, risk, trades_log=None)
    sig = _snap_with_basis("XLMUSDT", -14.0)
    plan = EntryPlan(sig, margin_usdt=5.0, notional_usdt=5.0,
                     net_edge_bps=5.0, expected_profit_usdt=0.0025,
                     score=1.0)

    pos = await eng.try_enter(plan)

    assert pos is None
    assert positions.count == 0
    assert risk.entry_block_reason == "protective_stop_failed"
    assert [c.order_type for c in fake.calls] == [
        OrderType.LIMIT,
        OrderType.STOP_LOSS,
        OrderType.MARKET,
    ]
    assert fake.calls[-1].reduce_only is True


@pytest.mark.asyncio
async def test_live_timeout_reconciles_position_before_marking_no_fill(tmp_path):
    cfg = _make_bot_cfg(TradingMode.SMALL_LIVE, tmp_path)
    cfg.execution.min_notional_usdt = 5.0
    cfg.execution.fill_timeout_ms = 50
    cfg.risk.max_mexc_requests_per_sec = 100
    cfg.risk.max_mexc_requests_per_min = 100
    cfg.risk.max_spread_bps = 20.0
    cfg.risk.max_slippage_bps = 50.0
    mexc = BookCache()
    _populate_tight_mexc(mexc, "XLMUSDT")
    positions = PositionManager(cfg.risk, mexc)
    risk = RiskManager(cfg.risk)
    risk.set_health(metascalp=True, binance=True, mexc=True)
    fake = _FakeMetaScalpPositionAfterTimeout()
    eng = ExecutionEngine(cfg, mexc, fake, 5, positions, risk, trades_log=None)
    sig = _snap_with_basis("XLMUSDT", -14.0)
    plan = EntryPlan(sig, margin_usdt=5.0, notional_usdt=5.0,
                     net_edge_bps=5.0, expected_profit_usdt=0.0025,
                     score=1.0)

    pos = await eng.try_enter(plan)

    assert pos is not None
    assert positions.count == 1
    assert pos.stop_confirmed is True
    assert [c.order_type for c in fake.calls] == [
        OrderType.LIMIT,
        OrderType.STOP_LOSS,
    ]


@pytest.mark.asyncio
async def test_live_timeout_falls_back_to_cancel_all_when_cancel_order_fails(tmp_path):
    cfg = _make_bot_cfg(TradingMode.SMALL_LIVE, tmp_path)
    cfg.execution.min_notional_usdt = 5.0
    cfg.execution.fill_timeout_ms = 50
    cfg.risk.max_mexc_requests_per_sec = 100
    cfg.risk.max_mexc_requests_per_min = 100
    cfg.risk.max_spread_bps = 20.0
    cfg.risk.max_slippage_bps = 50.0
    mexc = BookCache()
    _populate_tight_mexc(mexc, "XLMUSDT")
    positions = PositionManager(cfg.risk, mexc)
    risk = RiskManager(cfg.risk)
    risk.set_health(metascalp=True, binance=True, mexc=True)
    fake = _FakeMetaScalpCancelOrderFails()
    eng = ExecutionEngine(cfg, mexc, fake, 5, positions, risk, trades_log=None)
    sig = _snap_with_basis("XLMUSDT", -14.0)
    plan = EntryPlan(sig, margin_usdt=5.0, notional_usdt=5.0,
                     net_edge_bps=5.0, expected_profit_usdt=0.0025,
                     score=1.0)

    pos = await eng.try_enter(plan)

    assert pos is None
    assert positions.count == 0
    assert fake.cancel_all_called is True


@pytest.mark.asyncio
async def test_place_error_reconciles_position_before_returning_no_fill(tmp_path):
    cfg = _make_bot_cfg(TradingMode.SMALL_LIVE, tmp_path)
    cfg.execution.min_notional_usdt = 5.0
    cfg.risk.max_mexc_requests_per_sec = 100
    cfg.risk.max_mexc_requests_per_min = 100
    cfg.risk.max_spread_bps = 20.0
    cfg.risk.max_slippage_bps = 50.0
    mexc = BookCache()
    _populate_tight_mexc(mexc, "XLMUSDT")
    positions = PositionManager(cfg.risk, mexc)
    risk = RiskManager(cfg.risk)
    risk.set_health(metascalp=True, binance=True, mexc=True)
    fake = _FakeMetaScalpPlaceRaisesPositionExists()
    eng = ExecutionEngine(cfg, mexc, fake, 5, positions, risk, trades_log=None)
    sig = _snap_with_basis("XLMUSDT", -14.0)
    plan = EntryPlan(sig, margin_usdt=5.0, notional_usdt=5.0,
                     net_edge_bps=5.0, expected_profit_usdt=0.0025,
                     score=1.0)

    pos = await eng.try_enter(plan)

    assert pos is not None
    assert positions.count == 1
    assert [c.order_type for c in fake.calls] == [
        OrderType.LIMIT,
        OrderType.STOP_LOSS,
    ]


@pytest.mark.asyncio
async def test_explicit_order_reject_does_not_cancel_unopened_trade(tmp_path):
    cfg = _make_bot_cfg(TradingMode.SMALL_LIVE, tmp_path)
    cfg.execution.min_notional_usdt = 5.0
    cfg.risk.max_mexc_requests_per_sec = 100
    cfg.risk.max_mexc_requests_per_min = 100
    cfg.risk.max_spread_bps = 20.0
    cfg.risk.max_slippage_bps = 50.0
    mexc = BookCache()
    _populate_tight_mexc(mexc, "XLMUSDT")
    positions = PositionManager(cfg.risk, mexc)
    risk = RiskManager(cfg.risk)
    risk.set_health(metascalp=True, binance=True, mexc=True)
    fake = _FakeMetaScalpExplicitReject()
    eng = ExecutionEngine(cfg, mexc, fake, 5, positions, risk, trades_log=None)
    sig = _snap_with_basis("XLMUSDT", -14.0)
    plan = EntryPlan(sig, margin_usdt=5.0, notional_usdt=5.0,
                     net_edge_bps=5.0, expected_profit_usdt=0.0025,
                     score=1.0)

    pos = await eng.try_enter(plan)

    assert pos is None
    assert positions.count == 0
    assert fake.cancel_all_called is False


@pytest.mark.asyncio
async def test_emergency_close_all_closes_external_allowlist_position(tmp_path):
    cfg = _make_bot_cfg(TradingMode.SMALL_LIVE, tmp_path)
    cfg.risk.max_mexc_requests_per_sec = 100
    cfg.risk.max_mexc_requests_per_min = 100
    mexc = BookCache()
    positions = PositionManager(cfg.risk, mexc)
    risk = RiskManager(cfg.risk)
    fake = _FakeMetaScalpExternalPosition()
    eng = ExecutionEngine(cfg, mexc, fake, 5, positions, risk, trades_log=None)

    await eng.emergency_close_all()

    close_orders = [c for c in fake.calls if c.order_type == OrderType.MARKET]
    assert len(close_orders) == 1
    assert close_orders[0].ticker == "XLM_USDT"
    assert close_orders[0].side == 1
    assert close_orders[0].reduce_only is True


@pytest.mark.asyncio
async def test_external_exchange_position_occupies_entry_slot(tmp_path):
    cfg = _make_bot_cfg(TradingMode.SMALL_LIVE, tmp_path)
    cfg.symbols = ["XLMUSDT"]
    cfg.risk.max_mexc_requests_per_sec = 100
    cfg.risk.max_mexc_requests_per_min = 100
    mexc = BookCache()
    _populate_tight_mexc(mexc, "XLMUSDT")
    positions = PositionManager(cfg.risk, mexc)
    risk = RiskManager(cfg.risk)
    risk.set_health(metascalp=True, binance=True, mexc=True)
    fake = _FakeMetaScalpExternalPosition()
    eng = ExecutionEngine(cfg, mexc, fake, 5, positions, risk, trades_log=None)

    plans = await eng.build_entry_plans([_snap_with_basis("XLMUSDT", -14.0)])

    assert plans == []
    assert eng.occupied_slots == 1
    assert eng.occupied_symbols == {"XLMUSDT"}
