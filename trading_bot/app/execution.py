"""Execution engine — abstracted over modes.

Modes:
  * DRY_RUN    — no order placed; logs the would-be trade only.
  * PAPER      — simulates fill at best ask (LONG) / best bid (SHORT).
  * SMALL_LIVE — real orders via MetaScalp, sized from balance allocation.
  * LIVE       — same path with live config and balance-aware sizing.

LIVE/SMALL_LIVE pre-flight:
  1. re-check the MEXC orderbook (spread + depth) immediately before sending
  2. compute size in base units from selected margin * configured leverage
  3. estimate slippage; reject if > max_slippage_bps
  4. place order via MetaScalpClient (POST /api/connections/{id}/orders)
  5. poll get_orders until terminal status or fill_timeout_ms
  6. cancel if no fill in time
  7. handle partial fills safely
  8. hand position off to PositionManager
  9. on emergency: cancel_all(ticker) and market-close
"""
from __future__ import annotations

import asyncio
import collections
import random
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any, Deque

from .book_cache import Book, BookCache
from .config import (BotConfig, CapitalConfig, ExecutionConfig,
                     LeverageConfig, RateLimitsConfig, RiskConfig,
                     TradingMode)
from .logger import TradesLogger, get_logger
from .metascalp_client import (MetaScalpClient, MetaScalpOrderRejected,
                                OrderRequest, OrderSide, OrderType)
from .position_manager import ExitReason, Position, PositionManager
from .rate_limiter import RateLimiter
from .risk_manager import RiskManager
from .strategy import Side, SignalResult

log = get_logger("execution")


@dataclass
class FillResult:
    ok: bool
    price: float | None
    qty: float
    order_id: str | None
    latency_send_ms: float
    latency_fill_ms: float
    slippage_bps: float
    reason: str = ""


@dataclass(frozen=True)
class AccountSnapshot:
    available_balance_usdt: float
    equity_usdt: float | None
    source: str
    ts_monotonic: float


@dataclass(frozen=True)
class EntryQuote:
    signal: SignalResult
    mark_price: float
    margin_usdt: float
    notional_usdt: float
    gross_edge_bps: float
    net_edge_bps: float
    expected_profit_usdt: float
    expected_slippage_bps: float
    score: float


@dataclass(frozen=True)
class EntryPlan:
    signal: SignalResult
    margin_usdt: float
    notional_usdt: float
    net_edge_bps: float
    expected_profit_usdt: float
    score: float


@dataclass(frozen=True)
class LivePositionInfo:
    symbol: str
    native_ticker: str
    side: Side
    qty: float
    entry_price: float | None
    pnl_usdt: float | None
    raw: dict[str, Any]


@dataclass(frozen=True)
class TickerGate:
    allowed: bool
    reason: str = "ok"


@dataclass(frozen=True)
class TickerRules:
    price_increment: float | None = None
    size_increment: float | None = None
    min_size: float | None = None
    max_size: float | None = None

    @classmethod
    def from_raw(cls, raw: dict[str, Any]) -> "TickerRules":
        def _num(*names: str) -> float | None:
            for name in names:
                value = raw.get(name)
                if value is not None:
                    try:
                        parsed = float(value)
                    except (TypeError, ValueError):
                        continue
                    return parsed if parsed > 0 else None
            return None

        return cls(
            price_increment=_num("PriceIncrement", "priceIncrement"),
            size_increment=_num("SizeIncrement", "sizeIncrement"),
            min_size=_num("MinSize", "minSize"),
            max_size=_num("MaxSize", "maxSize"),
        )


class ExecutionEngine:
    """Decides *how* to trade safely. Whether to trade is up to strategy+risk."""

    def __init__(self, cfg: BotConfig, mexc: BookCache,
                 metascalp: MetaScalpClient | None,
                 mexc_connection_id: int | None,
                 positions: PositionManager, risk: RiskManager,
                 trades_log: TradesLogger | None,
                 mexc_ticker_map: dict[str, str] | None = None,
                 mexc_ticker_rules: dict[str, TickerRules] | None = None) -> None:
        self.cfg = cfg
        self.exec_cfg: ExecutionConfig = cfg.execution
        self.risk_cfg: RiskConfig = cfg.risk
        self.capital_cfg: CapitalConfig = cfg.capital
        self.leverage_cfg: LeverageConfig = cfg.leverage
        self.rate_cfg: RateLimitsConfig = cfg.rate_limits
        self.mode: TradingMode = cfg.mode
        self.mexc = mexc
        self.metascalp = metascalp
        self.mexc_connection_id = mexc_connection_id
        self.positions = positions
        self.risk = risk
        self.trades_log = trades_log
        # canonical 'XLMUSDT' -> exchange-native 'XLM_USDT'; empty until main wires it.
        self.mexc_ticker_map: dict[str, str] = mexc_ticker_map or {}
        self.mexc_ticker_rules: dict[str, TickerRules] = mexc_ticker_rules or {}
        self._pending_entries: dict[str, float] = {}
        # Map of ClientId/OrderId -> Future that _live_fill awaits.
        # Resolved by WS order_update events (see on_ws_order_update).
        self._pending_order_futures: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._account_snapshot: AccountSnapshot | None = None
        self._exchange_positions: list[dict[str, Any]] = []
        self._last_exchange_positions_refresh: float = 0.0
        self._recent_orders: Deque[dict[str, Any]] = collections.deque(maxlen=100)
        self._recent_trades: Deque[dict[str, Any]] = collections.deque(maxlen=100)
        self._entry_latency_block_until: float = 0.0
        self.rate_limiter = RateLimiter(
            max_per_sec=cfg.risk.max_mexc_requests_per_sec,
            max_per_min=cfg.risk.max_mexc_requests_per_min,
            upstream_hourly_limit=cfg.rate_limits.upstream_hourly_limit,
            safety_factor=cfg.rate_limits.safety_factor,
            close_reserve_pct=cfg.rate_limits.close_reserve_pct,
            min_close_reserve_requests=cfg.rate_limits.min_close_reserve_requests,
        )

    def _mexc_ticker(self, canonical: str) -> str:
        """Translate a canonical symbol to MEXC's native ticker; fall back to canonical."""
        return self.mexc_ticker_map.get(canonical.upper(), canonical)

    @property
    def allowed_symbols(self) -> set[str]:
        return {s.upper() for s in self.cfg.symbols}

    @property
    def pending_symbols(self) -> list[str]:
        return list(self._pending_entries.keys())

    @property
    def pending_count(self) -> int:
        return len(self._pending_entries)

    @property
    def occupied_slots(self) -> int:
        return len(self.occupied_symbols)

    @property
    def occupied_symbols(self) -> set[str]:
        symbols = {s.upper() for s in self.positions.open_symbols}
        symbols.update(s.upper() for s in self._pending_entries)
        for pos in self._exchange_positions:
            symbol = str(pos.get("symbol") or "").upper()
            if symbol in self.allowed_symbols:
                symbols.add(symbol)
        return symbols

    @property
    def reserved_pending_margin_usdt(self) -> float:
        return sum(self._pending_entries.values())

    def exchange_positions_snapshot(self) -> list[dict[str, Any]]:
        return list(self._exchange_positions)

    def recent_orders_snapshot(self) -> list[dict[str, Any]]:
        return list(self._recent_orders)

    def recent_trades_snapshot(self) -> list[dict[str, Any]]:
        return list(self._recent_trades)

    # ---- WS event hooks (called from main on_ms_event) ---------------------

    def invalidate_account_snapshot(self) -> None:
        """Force the next account_snapshot() call to bypass the cache.

        Called when MetaScalp WS pushes a balance_update — our cached number
        is stale by definition until we re-fetch.
        """
        self._account_snapshot = None

    def on_ws_order_update(self, data: dict[str, Any]) -> None:
        """Hook for WS-delivered order_update events.

        Resolves any pending entry future keyed by ClientId/OrderId so
        _await_fill can complete without REST polling. Polling fallback
        in _await_fill still runs in case WS misses an event.
        """
        client_id = str(data.get("ClientId") or data.get("clientId") or "")
        order_id = str(data.get("OrderId") or data.get("orderId") or "")
        fut = None
        if client_id and client_id in self._pending_order_futures:
            fut = self._pending_order_futures.get(client_id)
        elif order_id and order_id in self._pending_order_futures:
            fut = self._pending_order_futures.get(order_id)
        if fut is None or fut.done():
            return
        status = str(data.get("Status") or data.get("status") or "").lower()
        terminal_states = ("closed", "filled", "cancelled", "canceled",
                           "rejected", "expired")
        if status in terminal_states:
            try:
                fut.set_result(dict(data))
            except asyncio.InvalidStateError:
                pass

    # ---- public entrypoints -------------------------------------------------

    async def build_entry_plans(self, signals: list[SignalResult]) -> list[EntryPlan]:
        """Rank all valid signals and allocate balance to the best one or two."""
        signals = [s for s in signals if s.side is not None]
        if not signals:
            return []

        latency_wait = self._entry_latency_pause_remaining()
        if latency_wait > 0:
            log.warning("execution.entries_blocked",
                        reason="entry_latency_guard",
                        remaining_sec=round(latency_wait, 1))
            return []

        # WS position_update events force-refresh the cache (see
        # main._on_ms_event). The non-force call here is a no-op when the
        # cache is fresh (TTL = dashboard.exchange_refresh_sec, 30s) but
        # still seeds it on cold start. The forced REST roundtrip that
        # used to sit on this hot path is gone — it added ~80ms per signal.
        await self.refresh_exchange_positions()
        occupied_symbols = self.occupied_symbols
        signals = [
            s for s in signals
            if s.snapshot.symbol.upper() not in occupied_symbols
        ]
        if not signals:
            log.info("execution.entries_blocked",
                     reason="all_signals_already_occupied",
                     occupied_symbols=sorted(occupied_symbols))
            return []

        snapshot = await self.account_snapshot()
        slot_limit = self.dynamic_position_limit(snapshot)
        gate = self.risk.can_trade(
            open_positions=self.occupied_slots,
            max_allowed_positions=slot_limit,
        )
        if not gate.allowed:
            log.info("execution.entries_blocked", reason=gate.reason,
                     occupied_slots=self.occupied_slots, slot_limit=slot_limit)
            return []

        available_slots = max(0, slot_limit - self.occupied_slots)
        if available_slots <= 0:
            return []

        max_new_entries = min(available_slots, len(signals))
        while max_new_entries > 0 and not self._has_entry_budget(max_new_entries):
            max_new_entries -= 1
        if max_new_entries <= 0:
            log.warning("execution.entry_budget_blocked",
                        stats=self.rate_limiter.stats())
            return []

        allocatable = self.allocatable_balance(snapshot)
        if allocatable <= 0:
            log.info("execution.no_allocatable_balance",
                     available=snapshot.available_balance_usdt,
                     equity=snapshot.equity_usdt)
            return []

        leverage = self.effective_leverage()
        if not self._leverage_allowed(leverage):
            log.error("execution.leverage_blocked", leverage=leverage,
                      threshold=self.leverage_cfg.extreme_threshold,
                      allow_extreme=self.leverage_cfg.allow_extreme)
            return []

        plans: list[EntryPlan] = []
        high_balance = self.slot_balance(snapshot) >= self.capital_cfg.two_trade_min_balance_usdt
        can_make_two = high_balance and max_new_entries >= 2 and self.occupied_slots == 0

        if can_make_two:
            top_pct = self._rand_range(self.capital_cfg.high_balance_top_signal_margin_pct_range)
            top_margin = allocatable * top_pct
            best = self._best_quote(signals, top_margin)
            if best is None:
                return []
            plans.append(self._quote_to_plan(best))

            remaining_margin = max(0.0, allocatable - best.margin_usdt)
            remaining_signals = [s for s in signals if s.snapshot.symbol != best.signal.snapshot.symbol]
            second = self._best_quote(remaining_signals, remaining_margin)
            if second is not None:
                plans.append(self._quote_to_plan(second))
        else:
            if high_balance:
                margin = allocatable
            else:
                pct = self._rand_range(self.capital_cfg.low_balance_margin_pct_range)
                margin = min(allocatable, snapshot.available_balance_usdt * pct)
            best = self._best_quote(signals, margin)
            if best is not None:
                plans.append(self._quote_to_plan(best))

        for idx, plan in enumerate(plans, start=1):
            log.info("execution.entry_plan",
                     rank=idx,
                     symbol=plan.signal.snapshot.symbol,
                     side=plan.signal.side.value if plan.signal.side else "-",
                     margin_usdt=round(plan.margin_usdt, 4),
                     notional_usdt=round(plan.notional_usdt, 4),
                     net_edge_bps=round(plan.net_edge_bps, 4),
                     expected_profit_usdt=round(plan.expected_profit_usdt, 6),
                     score=round(plan.score, 6))
        return plans

    async def try_enter(self, entry: SignalResult | EntryPlan) -> Position | None:
        if isinstance(entry, EntryPlan):
            sig = entry.signal
            target_notional_usdt = entry.notional_usdt
            target_margin_usdt = entry.margin_usdt
        else:
            sig = entry
            target_notional_usdt = None
            target_margin_usdt = None
        symbol = sig.snapshot.symbol
        side = sig.side
        if side is None:
            return None
        if symbol.upper() not in self.allowed_symbols:
            log.error("execution.block_not_in_allowlist", symbol=symbol,
                      allowed=sorted(self.allowed_symbols))
            self._record_order_event(symbol=symbol, ticker="",
                                     side=side.value,
                                     order_type="ENTRY",
                                     size=0.0, price=None,
                                     status="blocked",
                                     detail="symbol_not_in_allowlist")
            return None
        if symbol.upper() in self._pending_entries:
            log.info("execution.skip_pending_symbol", symbol=symbol)
            return None

        book = self.mexc.book(symbol)
        mark = self._entry_price(side, book)
        if mark is None:
            log.info("execution.skip_no_book", symbol=symbol)
            return None

        spread = book.spread_bps
        depth = book.top_depth_usdt()
        if spread is None or spread > self.risk_cfg.max_spread_bps:
            log.info("execution.skip_spread", symbol=symbol, spread_bps=spread)
            return None
        if depth < self.cfg.strategy.min_depth_usdt:
            log.info("execution.skip_depth", symbol=symbol, depth=depth)
            return None

        qty, size_usdt = self._size_order(symbol, mark, target_notional_usdt)
        if qty <= 0 or size_usdt < self.exec_cfg.min_notional_usdt:
            log.info("execution.skip_min_notional", symbol=symbol,
                     size_usdt=round(size_usdt, 6), qty=qty)
            return None
        if (target_notional_usdt is None
                and size_usdt > self.risk_cfg.max_position_usdt):
            log.info("execution.skip_size_cap", symbol=symbol,
                     size_usdt=round(size_usdt, 6),
                     max_position_usdt=self.risk_cfg.max_position_usdt)
            return None
        if (self.capital_cfg.max_notional_usdt is not None
                and size_usdt > self.capital_cfg.max_notional_usdt):
            log.info("execution.skip_capital_notional_cap", symbol=symbol,
                     size_usdt=round(size_usdt, 6),
                     max_notional_usdt=self.capital_cfg.max_notional_usdt)
            return None

        liquidity_gate = self._liquidity_gate(book, side, qty, size_usdt)
        if not liquidity_gate.allowed:
            log.info("execution.skip_liquidity", symbol=symbol,
                     reason=liquidity_gate.reason,
                     notional_usdt=round(size_usdt, 4))
            return None

        est_slip_bps = self._estimate_slippage_bps(book, side, qty)
        if est_slip_bps > self.risk_cfg.max_slippage_bps:
            log.info("execution.skip_slippage", symbol=symbol,
                     est_slip_bps=round(est_slip_bps, 2))
            return None

        leverage = self.effective_leverage()
        margin_usdt = target_margin_usdt if target_margin_usdt is not None else size_usdt / leverage
        self._pending_entries[symbol.upper()] = margin_usdt
        try:
            stop_hint = self._initial_stop_price(mark, side)
            if self.mode is TradingMode.DRY_RUN:
                fill = self._dry_run_fill(symbol, side, qty, mark)
            elif self.mode is TradingMode.PAPER:
                fill = self._paper_fill(symbol, side, qty, mark)
            elif self.mode in (TradingMode.SMALL_LIVE, TradingMode.LIVE):
                fill = await self._live_fill(symbol, side, qty, mark, stop_hint)
            else:
                log.error("execution.unknown_mode", mode=self.mode)
                return None
        finally:
            self._pending_entries.pop(symbol.upper(), None)

        if not fill.ok or fill.price is None:
            log.info("execution.no_fill", symbol=symbol, reason=fill.reason)
            return None

        pos = self.positions.open(
            symbol=symbol, side=side, qty=fill.qty, entry_price=fill.price,
            entry_snapshot=sig.snapshot.as_dict(),
            margin_usdt=margin_usdt,
            notional_usdt=fill.qty * fill.price,
            leverage=leverage,
            entry_order_id=fill.order_id,
            stop_confirmed=(self.mode in (TradingMode.DRY_RUN, TradingMode.PAPER)
                            or self.exec_cfg.attached_stop_if_supported),
        )
        self._account_snapshot = None
        self.risk.register_open()
        self._log_open_trade(sig, fill)
        if self.mode in (TradingMode.SMALL_LIVE, TradingMode.LIVE):
            if self._entry_latency_too_high(fill):
                total_ms = fill.latency_send_ms + fill.latency_fill_ms
                self._arm_entry_latency_guard(fill)
                self.risk.block_entries("entry_latency_too_high")
                await self.close_position(
                    pos.symbol,
                    ExitReason.EMERGENCY,
                    f"entry_latency={total_ms:.0f}ms",
                )
                return None
            if not pos.stop_confirmed:
                stop_ok = await self._place_protective_stop(pos)
                if not stop_ok:
                    self.risk.block_entries("protective_stop_failed")
                    await self.close_position(pos.symbol, ExitReason.EMERGENCY,
                                              "protective_stop_failed")
                    return None
        return pos

    async def close_position(self, symbol: str, reason: ExitReason,
                              detail: str = "") -> None:
        pos = self.positions.get(symbol)
        if not pos:
            return
        book = self.mexc.book(symbol)
        exit_price = self._exit_price(pos.side, book)
        if exit_price is None:
            log.warning("execution.close_no_book", symbol=symbol)
            return

        if self.mode in (TradingMode.DRY_RUN, TradingMode.PAPER):
            ok = True
        else:
            ok = await self._live_close(pos)

        if not ok:
            log.error("execution.close_failed", symbol=symbol)
            return

        pnl = (exit_price - pos.entry_price) * pos.qty
        if pos.side is Side.SHORT:
            pnl = -pnl
        self.positions.close(symbol)
        self._account_snapshot = None
        self.risk.register_close(pnl)
        self._log_close_trade(pos, exit_price, pnl, reason, detail)

    async def emergency_close_all(self) -> None:
        log.error("execution.emergency_close_all", count=self.positions.count)
        for sym in list(self.positions.open_symbols):
            await self.close_position(sym, ExitReason.EMERGENCY, "kill_switch")
        if (self.metascalp is not None and self.mexc_connection_id is not None
                and self.mode in (TradingMode.SMALL_LIVE, TradingMode.LIVE)):
            # cancel_all bypasses rate limiter intentionally — emergency action
            # outranks the budget. MEXC's hard cap (10/sec) is still respected
            # because the prior close_position calls already paced themselves.
            await self.refresh_exchange_positions(force=True)
            for raw_pos in list(self._exchange_positions):
                symbol = str(raw_pos.get("symbol") or "").upper()
                if symbol not in self.allowed_symbols:
                    log.warning("execution.skip_external_close_not_allowed",
                                symbol=symbol, pos=raw_pos)
                    continue
                if self.positions.get(symbol) is not None:
                    continue
                side_text = str(raw_pos.get("side") or "")
                side = Side.LONG if side_text == Side.LONG.value else Side.SHORT
                info = LivePositionInfo(
                    symbol=symbol,
                    native_ticker=str(raw_pos.get("native_ticker") or self._mexc_ticker(symbol)),
                    side=side,
                    qty=float(raw_pos.get("qty") or 0.0),
                    entry_price=raw_pos.get("entry_price"),
                    pnl_usdt=raw_pos.get("pnl_usdt"),
                    raw=raw_pos,
                )
                if info.qty > 0:
                    await self._live_close_external(info)
            for sym in sorted(self.allowed_symbols):
                ticker = self._mexc_ticker(sym)
                if not self._validate_live_ticker(sym, ticker).allowed:
                    continue
                try:
                    await self.metascalp.cancel_all(self.mexc_connection_id, ticker=ticker)
                    self._record_order_event(symbol=sym, ticker=ticker,
                                             side="", order_type="CANCEL_ALL",
                                             size=0.0, price=None,
                                             status="sent",
                                             detail="emergency_close_all")
                except Exception as e:  # noqa: BLE001
                    log.exception("execution.cancel_all_failed", symbol=sym, err=str(e))
            await self.refresh_exchange_positions(force=True)

    # ---- exchange state / live safety --------------------------------------

    def _validate_live_ticker(self, canonical: str, native_ticker: str) -> TickerGate:
        canonical = canonical.upper()
        normalized_native = _normalize_symbol(native_ticker)
        if canonical not in self.allowed_symbols:
            return TickerGate(False, "symbol_not_in_allowlist")
        if normalized_native != canonical:
            return TickerGate(False, f"native_ticker_mismatch:{native_ticker}->{normalized_native}")
        if canonical in {"BTCUSDT", "ETHUSDT"}:
            return TickerGate(False, "macro_symbol_not_tradable")
        return TickerGate(True)

    def _record_order_event(self, *, symbol: str, ticker: str, side: str,
                            order_type: str, size: float, price: float | None,
                            status: str, detail: str = "") -> None:
        self._recent_orders.appendleft({
            "ts_ms": int(time.time() * 1000),
            "symbol": symbol,
            "ticker": ticker,
            "side": side,
            "order_type": order_type,
            "size": size,
            "price": price,
            "status": status,
            "detail": detail,
        })

    async def refresh_exchange_positions(self, *, force: bool = False) -> list[dict[str, Any]]:
        now = time.monotonic()
        if (not force and now - self._last_exchange_positions_refresh
                < self.cfg.dashboard.exchange_refresh_sec):
            return self.exchange_positions_snapshot()
        self._last_exchange_positions_refresh = now
        if (self.metascalp is None or self.mexc_connection_id is None
                or self.mode not in (TradingMode.SMALL_LIVE, TradingMode.LIVE)):
            self._exchange_positions = []
            return []
        if not self.rate_limiter.acquire("reconcile"):
            log.warning("execution.positions_rate_limited",
                        stats=self.rate_limiter.stats())
            return self.exchange_positions_snapshot()
        try:
            raw_positions = await self.metascalp.get_positions(self.mexc_connection_id)
        except Exception as e:  # noqa: BLE001
            log.warning("execution.positions_read_failed", err=str(e))
            return self.exchange_positions_snapshot()

        parsed: list[dict[str, Any]] = []
        for raw in raw_positions:
            info = self._parse_live_position(raw)
            if info is None:
                continue
            parsed.append({
                "symbol": info.symbol,
                "native_ticker": info.native_ticker,
                "side": info.side.value,
                "qty": info.qty,
                "entry_price": info.entry_price,
                "pnl_usdt": info.pnl_usdt,
                "allowed": info.symbol in self.allowed_symbols,
                "source": "metascalp",
            })
        self._exchange_positions = parsed
        return self.exchange_positions_snapshot()

    async def _lookup_live_position_fill(self, symbol: str,
                                         side: Side) -> LivePositionInfo | None:
        if self.metascalp is None or self.mexc_connection_id is None:
            return None
        if not self.rate_limiter.acquire("reconcile"):
            return None
        try:
            raw_positions = await self.metascalp.get_positions(self.mexc_connection_id)
        except Exception as e:  # noqa: BLE001
            log.warning("execution.position_lookup_failed", symbol=symbol, err=str(e))
            return None
        for raw in raw_positions:
            info = self._parse_live_position(raw)
            if info and info.symbol == symbol.upper() and info.side is side and info.qty > 0:
                await self.refresh_exchange_positions(force=True)
                return info
        await self.refresh_exchange_positions(force=True)
        return None

    async def _live_close_external(self, info: LivePositionInfo) -> bool:
        assert self.metascalp is not None
        assert self.mexc_connection_id is not None
        ticker_gate = self._validate_live_ticker(info.symbol, info.native_ticker)
        if not ticker_gate.allowed:
            log.warning("execution.external_close_blocked",
                        symbol=info.symbol, native_ticker=info.native_ticker,
                        reason=ticker_gate.reason)
            return False
        req = OrderRequest(
            ticker=info.native_ticker,
            side=OrderSide.SELL if info.side is Side.LONG else OrderSide.BUY,
            size=info.qty,
            order_type=OrderType.MARKET,
            reduce_only=True,
        )
        if not self.rate_limiter.acquire("close"):
            log.error("execution.external_close_rate_limited",
                      symbol=info.symbol, stats=self.rate_limiter.stats())
            return False
        try:
            await self.metascalp.place_order(self.mexc_connection_id, req)
            self._record_order_event(symbol=info.symbol, ticker=info.native_ticker,
                                     side="SELL" if info.side is Side.LONG else "BUY",
                                     order_type="MARKET",
                                     size=info.qty, price=None,
                                     status="external_close_sent",
                                     detail="reduce_only")
            return True
        except Exception as e:  # noqa: BLE001
            log.exception("execution.external_close_failed",
                          symbol=info.symbol, err=str(e))
            self._record_order_event(symbol=info.symbol, ticker=info.native_ticker,
                                     side="SELL" if info.side is Side.LONG else "BUY",
                                     order_type="MARKET",
                                     size=info.qty, price=None,
                                     status="external_close_failed",
                                     detail=str(e)[:180])
            return False

    def _parse_live_position(self, raw: dict[str, Any]) -> LivePositionInfo | None:
        if not isinstance(raw, dict):
            return None
        native = str(
            raw.get("Ticker") or raw.get("ticker")
            or raw.get("Symbol") or raw.get("symbol")
            or raw.get("Name") or raw.get("name")
            or ""
        )
        if not native:
            return None
        symbol = _normalize_symbol(native)
        qty = _first_float(raw, (
            "Size", "size", "Qty", "qty", "Quantity", "quantity",
            "Volume", "volume", "Amount", "amount", "Contracts", "contracts",
            "PositionAmt", "positionAmt",
        ))
        side = _parse_position_side(raw, qty)
        if qty is None or side is None:
            return None
        qty = abs(qty)
        if qty <= 0:
            return None
        entry = _first_float(raw, (
            "EntryPrice", "entryPrice", "OpenPrice", "openPrice",
            "AvgPrice", "avgPrice", "Price", "price",
        ))
        pnl = _first_float(raw, (
            "Pnl", "pnl", "PnL", "UnrealizedPnl", "unrealizedPnl",
            "UnrealizedPnlUsdt", "unrealizedPnlUsdt",
        ))
        return LivePositionInfo(
            symbol=symbol,
            native_ticker=native,
            side=side,
            qty=qty,
            entry_price=entry,
            pnl_usdt=pnl,
            raw=raw,
        )

    # ---- balance, allocation and signal ranking ----------------------------

    async def account_snapshot(self, *, force: bool = False) -> AccountSnapshot:
        now = time.monotonic()
        if (not force and self._account_snapshot is not None
                and now - self._account_snapshot.ts_monotonic < self.capital_cfg.balance_refresh_sec):
            return self._account_snapshot

        if self.mode in (TradingMode.DRY_RUN, TradingMode.PAPER):
            used_margin = sum(p.margin_usdt for p in self.positions.all())
            available = max(0.0, self.capital_cfg.paper_balance_usdt
                            - used_margin - self.reserved_pending_margin_usdt)
            snap = AccountSnapshot(
                available_balance_usdt=available,
                equity_usdt=self.capital_cfg.paper_balance_usdt,
                source=self.mode.value.lower(),
                ts_monotonic=now,
            )
            self._account_snapshot = snap
            return snap

        if self.metascalp is None or self.mexc_connection_id is None:
            snap = AccountSnapshot(0.0, None, "unavailable", now)
            self._account_snapshot = snap
            return snap

        if not self.rate_limiter.acquire("reconcile"):
            log.warning("execution.balance_rate_limited",
                        stats=self.rate_limiter.stats())
            if self._account_snapshot is not None:
                return self._account_snapshot
            return AccountSnapshot(0.0, None, "rate_limited", now)

        try:
            raw = await self.metascalp.get_balance(self.mexc_connection_id)
            available, equity = _extract_usdt_balance(raw)
            available = max(0.0, available - self.reserved_pending_margin_usdt)
            snap = AccountSnapshot(available, equity, "metascalp", now)
            self._account_snapshot = snap
            return snap
        except Exception as e:  # noqa: BLE001
            log.exception("execution.balance_read_failed", err=str(e))
            if self._account_snapshot is not None:
                return self._account_snapshot
            return AccountSnapshot(0.0, None, "balance_error", now)

    def slot_balance(self, snapshot: AccountSnapshot) -> float:
        internal_margin = sum(p.margin_usdt for p in self.positions.all())
        if snapshot.equity_usdt is not None and snapshot.equity_usdt > 0:
            return snapshot.equity_usdt
        return snapshot.available_balance_usdt + internal_margin + self.reserved_pending_margin_usdt

    def dynamic_position_limit(self, snapshot: AccountSnapshot) -> int:
        if self.slot_balance(snapshot) >= self.capital_cfg.two_trade_min_balance_usdt:
            balance_limit = self.capital_cfg.high_balance_max_positions
        else:
            balance_limit = self.capital_cfg.low_balance_max_positions
        return min(
            self.risk_cfg.max_open_positions,
            self.capital_cfg.configured_max_positions,
            balance_limit,
        )

    def allocatable_balance(self, snapshot: AccountSnapshot) -> float:
        reserve = max(
            snapshot.available_balance_usdt * self.capital_cfg.balance_reserve_pct,
            self.capital_cfg.min_balance_reserve_usdt,
        )
        return max(0.0, snapshot.available_balance_usdt - reserve)

    def effective_leverage(self) -> float:
        return self.leverage_cfg.default

    def _leverage_allowed(self, leverage: float) -> bool:
        if leverage > self.leverage_cfg.max_configurable:
            return False
        if leverage >= self.leverage_cfg.extreme_threshold and not self.leverage_cfg.allow_extreme:
            return False
        return True

    def _has_entry_budget(self, entry_count: int) -> bool:
        entry_cost = max(1, self.rate_cfg.entry_request_cost) * max(1, entry_count)
        protective_cost = (
            max(0, self.rate_cfg.stop_request_cost)
            + max(0, self.rate_cfg.emergency_close_request_cost)
        ) * max(1, entry_count)
        return self.rate_limiter.can_start_entry(
            entry_cost=entry_cost,
            protective_cost=protective_cost,
        )

    def _entry_latency_pause_remaining(self) -> float:
        return max(0.0, self._entry_latency_block_until - time.monotonic())

    def _entry_latency_too_high(self, fill: FillResult) -> bool:
        total_ms = fill.latency_send_ms + fill.latency_fill_ms
        return (
            fill.latency_send_ms > self.exec_cfg.max_entry_send_latency_ms
            or total_ms > self.exec_cfg.max_entry_total_latency_ms
        )

    def _arm_entry_latency_guard(self, fill: FillResult) -> None:
        total_ms = fill.latency_send_ms + fill.latency_fill_ms
        self._entry_latency_block_until = max(
            self._entry_latency_block_until,
            time.monotonic() + self.exec_cfg.latency_pause_sec,
        )
        log.error("execution.entry_latency_too_high",
                  send_ms=round(fill.latency_send_ms, 1),
                  fill_ms=round(fill.latency_fill_ms, 1),
                  total_ms=round(total_ms, 1),
                  max_send_ms=self.exec_cfg.max_entry_send_latency_ms,
                  max_total_ms=self.exec_cfg.max_entry_total_latency_ms,
                  pause_sec=self.exec_cfg.latency_pause_sec)

    def _best_quote(self, signals: list[SignalResult],
                    margin_usdt: float) -> EntryQuote | None:
        if margin_usdt <= 0:
            return None
        quotes = [q for q in (self._quote_signal(s, margin_usdt) for s in signals)
                  if q is not None]
        if not quotes:
            return None
        return max(quotes, key=lambda q: (q.score, q.net_edge_bps, q.expected_profit_usdt))

    def _quote_signal(self, sig: SignalResult, margin_usdt: float) -> EntryQuote | None:
        def reject(reason: str, **extra: Any) -> None:
            log.info("execution.quote_rejected",
                     symbol=sig.snapshot.symbol,
                     side=sig.side.value if sig.side else "-",
                     reason=reason,
                     margin_usdt=round(margin_usdt, 4),
                     **extra)

        side = sig.side
        if side is None:
            reject("no_side")
            return None
        symbol = sig.snapshot.symbol
        book = self.mexc.book(symbol)
        mark = self._entry_price(side, book)
        if mark is None or mark <= 0:
            reject("no_mark")
            return None
        spread = book.spread_bps
        if spread is None or spread > self.risk_cfg.max_spread_bps:
            reject("spread",
                   spread_bps=spread,
                   max_spread_bps=self.risk_cfg.max_spread_bps)
            return None
        if book.top_depth_usdt() < self.cfg.strategy.min_depth_usdt:
            reject("depth",
                   depth_usdt=round(book.top_depth_usdt(), 4),
                   min_depth_usdt=self.cfg.strategy.min_depth_usdt)
            return None
        leverage = self.effective_leverage()
        notional = margin_usdt * leverage
        if notional < self.exec_cfg.min_notional_usdt:
            reject("min_notional",
                   notional_usdt=round(notional, 4),
                   min_notional_usdt=self.exec_cfg.min_notional_usdt)
            return None
        qty = notional / mark
        liquidity_gate = self._liquidity_gate(book, side, qty, notional)
        if not liquidity_gate.allowed:
            reject("liquidity",
                   detail=liquidity_gate.reason,
                   notional_usdt=round(notional, 4))
            return None
        slippage_bps = self._estimate_slippage_bps(book, side, qty)
        if slippage_bps > self.risk_cfg.max_slippage_bps:
            reject("slippage",
                   slippage_bps=round(slippage_bps, 4),
                   max_slippage_bps=self.risk_cfg.max_slippage_bps)
            return None
        basis = abs(sig.snapshot.basis_bps or 0.0)
        gross_edge_bps = max(0.0, basis - self.risk_cfg.basis_collapse_exit_bps)
        net_edge_bps = (
            gross_edge_bps
            - spread
            - slippage_bps
            - self.capital_cfg.estimated_fee_bps
            - self.exec_cfg.latency_edge_buffer_bps
        )
        expected_profit = notional * net_edge_bps / 10_000.0
        if net_edge_bps <= self.capital_cfg.min_net_edge_bps:
            reject("net_edge",
                   gross_edge_bps=round(gross_edge_bps, 4),
                   net_edge_bps=round(net_edge_bps, 4),
                   min_net_edge_bps=self.capital_cfg.min_net_edge_bps,
                   spread_bps=round(spread, 4),
                   slippage_bps=round(slippage_bps, 4),
                   latency_buffer_bps=self.exec_cfg.latency_edge_buffer_bps)
            return None
        if expected_profit <= self.capital_cfg.min_expected_profit_usdt:
            reject("expected_profit",
                   expected_profit_usdt=round(expected_profit, 6),
                   min_expected_profit_usdt=self.capital_cfg.min_expected_profit_usdt,
                   net_edge_bps=round(net_edge_bps, 4),
                   notional_usdt=round(notional, 4))
            return None
        impulse = abs(sig.snapshot.binance_impulse_bps or 0.0)
        score = expected_profit + (net_edge_bps * 0.001) + (impulse * 0.0001)
        return EntryQuote(
            signal=sig,
            mark_price=mark,
            margin_usdt=margin_usdt,
            notional_usdt=notional,
            gross_edge_bps=gross_edge_bps,
            net_edge_bps=net_edge_bps,
            expected_profit_usdt=expected_profit,
            expected_slippage_bps=slippage_bps,
            score=score,
        )

    def _quote_to_plan(self, quote: EntryQuote) -> EntryPlan:
        return EntryPlan(
            signal=quote.signal,
            margin_usdt=quote.margin_usdt,
            notional_usdt=quote.notional_usdt,
            net_edge_bps=quote.net_edge_bps,
            expected_profit_usdt=quote.expected_profit_usdt,
            score=quote.score,
        )

    def _rand_range(self, bounds: tuple[float, float]) -> float:
        lo, hi = bounds
        return random.uniform(lo, hi)

    # ---- mode-specific fills ------------------------------------------------

    def _dry_run_fill(self, symbol: str, side: Side, qty: float, mark: float) -> FillResult:
        log.info("execution.dry_run_fill", symbol=symbol, side=side.value,
                 qty=qty, price=mark)
        return FillResult(True, mark, qty, None, 0.0, 0.0, 0.0, "dry_run")

    def _paper_fill(self, symbol: str, side: Side, qty: float, mark: float) -> FillResult:
        book = self.mexc.book(symbol)
        if side is Side.LONG and book.best_ask:
            price = book.best_ask
        elif side is Side.SHORT and book.best_bid:
            price = book.best_bid
        else:
            price = mark
        slip = abs(price - mark) / mark * 10_000.0 if mark > 0 else 0.0
        log.info("execution.paper_fill", symbol=symbol, side=side.value,
                 qty=qty, price=price, slip_bps=round(slip, 2))
        return FillResult(True, price, qty, f"paper-{int(time.time()*1000)}",
                          0.0, 0.0, slip, "paper")

    async def _live_fill(self, symbol: str, side: Side, qty: float,
                          mark: float, stop_price: float | None = None) -> FillResult:
        assert self.metascalp is not None
        assert self.mexc_connection_id is not None
        book = self.mexc.book(symbol)
        limit_offset = self.exec_cfg.limit_offset_bps / 10_000.0
        if side is Side.LONG:
            limit_price = self._round_price(
                symbol, side, (book.best_ask or mark) * (1 + limit_offset),
            )
        else:
            limit_price = self._round_price(
                symbol, side, (book.best_bid or mark) * (1 - limit_offset),
            )

        mexc_ticker = self._mexc_ticker(symbol)
        ticker_gate = self._validate_live_ticker(symbol, mexc_ticker)
        if not ticker_gate.allowed:
            log.error("execution.native_ticker_blocked",
                      symbol=symbol, native_ticker=mexc_ticker,
                      reason=ticker_gate.reason)
            self._record_order_event(symbol=symbol, ticker=mexc_ticker,
                                     side=side.value, order_type="ENTRY",
                                     size=qty, price=limit_price,
                                     status="blocked",
                                     detail=ticker_gate.reason)
            return FillResult(False, None, 0.0, None, 0.0, 0.0, 0.0,
                              ticker_gate.reason)
        attached_stop = None
        if self.exec_cfg.attached_stop_if_supported and stop_price is not None:
            stop_side = Side.SHORT if side is Side.LONG else Side.LONG
            attached_stop = self._round_price(symbol, stop_side, stop_price)

        req = OrderRequest(
            ticker=mexc_ticker,
            side=OrderSide.BUY if side is Side.LONG else OrderSide.SELL,
            size=qty,
            price=limit_price,
            order_type=(OrderType.LIMIT if self.exec_cfg.order_type.upper() == "LIMIT"
                         else OrderType.MARKET),
            reduce_only=False,
            stop_loss_price=attached_stop,
        )

        if not self.rate_limiter.acquire("entry"):
            log.warning("execution.rate_limited", symbol=symbol,
                         stats=self.rate_limiter.stats())
            return FillResult(False, None, 0.0, None, 0.0, 0.0, 0.0, "rate_limited")

        t_send = time.monotonic()
        try:
            resp = await self.metascalp.place_order(self.mexc_connection_id, req)
        except Exception as e:  # noqa: BLE001
            log.exception("execution.live_place_failed", err=str(e))
            self._record_order_event(symbol=symbol, ticker=mexc_ticker,
                                     side=side.value,
                                     order_type="LIMIT" if req.order_type == OrderType.LIMIT else "MARKET",
                                     size=qty, price=limit_price,
                                     status="rejected",
                                     detail=str(e)[:180])
            live_fill = await self._lookup_live_position_fill(symbol, side)
            if live_fill is not None:
                fill_price = live_fill.entry_price or mark
                self._record_order_event(symbol=symbol, ticker=mexc_ticker,
                                         side=side.value,
                                         order_type="POSITION",
                                         size=live_fill.qty,
                                         price=fill_price,
                                         status="filled_reconciled",
                                         detail="detected_in_positions_after_place_error")
                return FillResult(True, fill_price, live_fill.qty, None,
                                  0.0, 0.0, 0.0, "live_reconciled_after_place_error")
            if isinstance(e, MetaScalpOrderRejected):
                log.info("execution.place_rejected_no_cleanup",
                         symbol=symbol,
                         status_code=e.status_code,
                         detail=e.detail[:180])
                return FillResult(False, None, 0.0, None, 0.0, 0.0, 0.0,
                                  f"place_rejected:{e}")
            with self._suppress_log():
                try:
                    if self.rate_limiter.acquire("protective"):
                        await self.metascalp.cancel_all(self.mexc_connection_id,
                                                        ticker=mexc_ticker)
                        self._record_order_event(symbol=symbol, ticker=mexc_ticker,
                                                 side=side.value,
                                                 order_type="CANCEL_ALL",
                                                 size=0.0, price=None,
                                                 status="sent",
                                                 detail="place_error_cleanup")
                except Exception as cleanup_err:  # noqa: BLE001
                    log.warning("execution.place_error_cleanup_failed",
                                symbol=symbol, err=str(cleanup_err))
            return FillResult(False, None, 0.0, None, 0.0, 0.0, 0.0,
                              f"place_error:{e}")
        send_ms = (time.monotonic() - t_send) * 1000
        if send_ms > self.exec_cfg.max_entry_send_latency_ms:
            self._arm_entry_latency_guard(
                FillResult(False, None, 0.0, None, send_ms, 0.0, 0.0,
                           "entry_send_latency_too_high")
            )
        self._record_order_event(symbol=symbol, ticker=mexc_ticker,
                                 side=side.value,
                                 order_type="LIMIT" if req.order_type == OrderType.LIMIT else "MARKET",
                                 size=qty, price=limit_price,
                                 status="placed",
                                 detail=str(resp.get("OrderId") or resp.get("ClientId")
                                            or resp.get("orderId") or resp.get("clientId") or ""))

        order_id = str(resp.get("OrderId") or resp.get("ClientId")
                        or resp.get("orderId") or resp.get("clientId") or "")
        client_id = str(resp.get("ClientId") or resp.get("clientId") or "")

        # Register a Future under BOTH ids (whichever the WS event references).
        # The on_ws_order_update hook resolves the Future as soon as MetaScalp
        # pushes a terminal status — eliminating the 150ms polling cadence.
        loop = asyncio.get_event_loop()
        fut: asyncio.Future[dict[str, Any]] = loop.create_future()
        future_keys: list[str] = []
        for key in (client_id, order_id):
            if key and key not in self._pending_order_futures:
                self._pending_order_futures[key] = fut
                future_keys.append(key)

        try:
            fill_price, filled_qty, filled = await self._await_fill(
                order_id, mexc_ticker,
                timeout_ms=self.exec_cfg.fill_timeout_ms,
                ws_future=fut,
            )
        finally:
            for key in future_keys:
                self._pending_order_futures.pop(key, None)
        fill_ms = (time.monotonic() - t_send) * 1000 - send_ms

        if not filled or fill_price is None:
            live_fill = await self._lookup_live_position_fill(symbol, side)
            if live_fill is not None:
                log.warning("execution.fill_detected_from_positions",
                            symbol=symbol, qty=live_fill.qty,
                            entry_price=live_fill.entry_price)
                fill_price = live_fill.entry_price or mark
                filled_qty = live_fill.qty
                self._record_order_event(symbol=symbol, ticker=mexc_ticker,
                                         side=side.value,
                                         order_type="POSITION",
                                         size=filled_qty,
                                         price=fill_price,
                                         status="filled_reconciled",
                                         detail="detected_in_positions_after_timeout")
                return FillResult(True, fill_price, filled_qty, order_id,
                                  send_ms, fill_ms, 0.0, "live_reconciled")
            with self._suppress_log():
                try:
                    if not self.rate_limiter.acquire("protective"):
                        log.warning("execution.cancel_budget_limited", symbol=symbol,
                                    stats=self.rate_limiter.stats())
                    if order_id:
                        try:
                            await self.metascalp.cancel_order(self.mexc_connection_id,
                                                               ticker=mexc_ticker,
                                                               order_id=order_id)
                        except Exception as e:  # noqa: BLE001
                            log.warning("execution.cancel_order_failed_fallback_all",
                                        symbol=symbol, order_id=order_id, err=str(e))
                            await self.metascalp.cancel_all(self.mexc_connection_id,
                                                            ticker=mexc_ticker)
                    else:
                        await self.metascalp.cancel_all(self.mexc_connection_id,
                                                        ticker=mexc_ticker)
                    self._record_order_event(symbol=symbol, ticker=mexc_ticker,
                                             side=side.value,
                                             order_type="CANCEL",
                                             size=qty, price=limit_price,
                                             status="sent",
                                             detail="fill_timeout")
                except Exception as e:  # noqa: BLE001
                    log.warning("execution.cancel_after_timeout_failed", err=str(e))
            live_fill = await self._lookup_live_position_fill(symbol, side)
            if live_fill is not None:
                fill_price = live_fill.entry_price or mark
                filled_qty = live_fill.qty
                self._record_order_event(symbol=symbol, ticker=mexc_ticker,
                                         side=side.value,
                                         order_type="POSITION",
                                         size=filled_qty,
                                         price=fill_price,
                                         status="filled_reconciled",
                                         detail="detected_in_positions_after_cancel")
                return FillResult(True, fill_price, filled_qty, order_id,
                                  send_ms, fill_ms, 0.0, "live_reconciled")
            return FillResult(False, None, 0.0, order_id, send_ms, fill_ms, 0.0,
                              "fill_timeout")

        slip_bps = abs(fill_price - mark) / mark * 10_000.0 if mark > 0 else 0.0
        return FillResult(True, fill_price, filled_qty, order_id,
                          send_ms, fill_ms, slip_bps, "live")

    async def _live_close(self, pos: Position) -> bool:
        assert self.metascalp is not None
        assert self.mexc_connection_id is not None
        req = OrderRequest(
            ticker=self._mexc_ticker(pos.symbol),
            side=OrderSide.SELL if pos.side is Side.LONG else OrderSide.BUY,
            size=pos.qty,
            order_type=OrderType.MARKET,
            reduce_only=True,
        )
        if not self.rate_limiter.acquire("close"):
            log.error("execution.close_rate_limited", symbol=pos.symbol,
                       stats=self.rate_limiter.stats(),
                       hint="position stays open; will retry on next tick")
            return False
        try:
            await self.metascalp.place_order(self.mexc_connection_id, req)
            self._record_order_event(symbol=pos.symbol, ticker=req.ticker,
                                     side="SELL" if pos.side is Side.LONG else "BUY",
                                     order_type="MARKET",
                                     size=pos.qty, price=None,
                                     status="close_sent",
                                     detail="reduce_only")
            return True
        except Exception as e:  # noqa: BLE001
            log.exception("execution.live_close_failed", err=str(e))
            return False

    async def _place_protective_stop(self, pos: Position) -> bool:
        """Place the exchange/terminal-side stop after a live fill."""
        assert self.metascalp is not None
        assert self.mexc_connection_id is not None
        if pos.stop_price is None:
            return False
        stop_side = Side.SHORT if pos.side is Side.LONG else Side.LONG
        req = OrderRequest(
            ticker=self._mexc_ticker(pos.symbol),
            side=OrderSide.SELL if pos.side is Side.LONG else OrderSide.BUY,
            size=pos.qty,
            price=self._round_price(pos.symbol, stop_side, pos.stop_price),
            order_type=OrderType.STOP_LOSS,
            reduce_only=True,
        )
        if not self.rate_limiter.acquire("protective"):
            log.error("execution.stop_rate_limited", symbol=pos.symbol,
                      stats=self.rate_limiter.stats())
            return False
        try:
            resp = await asyncio.wait_for(
                self.metascalp.place_order(self.mexc_connection_id, req),
                timeout=max(0.1, self.exec_cfg.protective_stop_timeout_ms / 1000.0),
            )
        except Exception as e:  # noqa: BLE001
            log.exception("execution.stop_place_failed", symbol=pos.symbol,
                          err=str(e))
            self._record_order_event(symbol=pos.symbol, ticker=req.ticker,
                                     side="SELL" if pos.side is Side.LONG else "BUY",
                                     order_type="STOP_LOSS",
                                     size=pos.qty, price=req.price,
                                     status="rejected",
                                     detail=str(e)[:180])
            return False
        pos.stop_order_id = str(resp.get("OrderId") or resp.get("ClientId")
                                or resp.get("orderId") or resp.get("clientId") or "")
        pos.stop_confirmed = True
        self._record_order_event(symbol=pos.symbol, ticker=req.ticker,
                                 side="SELL" if pos.side is Side.LONG else "BUY",
                                 order_type="STOP_LOSS",
                                 size=pos.qty, price=req.price,
                                 status="placed",
                                 detail=pos.stop_order_id)
        log.info("execution.stop_confirmed", symbol=pos.symbol,
                 stop_price=pos.stop_price, order_id=pos.stop_order_id)
        return True

    async def _await_fill(self, order_id: str, ticker: str, *, timeout_ms: int,
                          ws_future: asyncio.Future[dict[str, Any]] | None = None,
                          ) -> tuple[float | None, float, bool]:
        """Wait for an order's terminal state.

        Primary path: a Future resolved by `on_ws_order_update` when MetaScalp
        pushes the order_update WS event (typically 10-50ms after the fill).

        Fallback path: poll get_orders every 500ms — catches the rare case where
        the WS event was missed (disconnect, parse error). 500ms is 3x the old
        150ms cadence which was on the hot path; now polling is only a safety
        net so it can be much slower.
        """
        assert self.metascalp is not None
        assert self.mexc_connection_id is not None
        deadline = time.monotonic() + timeout_ms / 1000.0
        last_filled_qty = 0.0
        last_price: float | None = None
        poll_interval = 0.5  # was 0.15 — WS-driven now, poll is fallback only

        def _parse_ws_payload(data: dict[str, Any]) -> tuple[float | None, float, str]:
            status = str(data.get("Status") or data.get("status") or "").lower()
            fq = (data.get("FilledSize") or data.get("filledSize")
                  or data.get("FilledQty") or 0.0)
            fp = (data.get("FilledPrice") or data.get("filledPrice")
                  or data.get("AvgPrice") or data.get("Price") or data.get("price"))
            qty = float(fq) if fq else 0.0
            price = float(fp) if fp is not None else None
            return price, qty, status

        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break

            # First: race the WS future against a poll interval.
            if ws_future is not None and not ws_future.done():
                try:
                    payload = await asyncio.wait_for(
                        asyncio.shield(ws_future),
                        timeout=min(poll_interval, remaining),
                    )
                except asyncio.TimeoutError:
                    payload = None
                else:
                    price, qty, status = _parse_ws_payload(payload)
                    if price is not None:
                        last_price = price
                    if qty > 0:
                        last_filled_qty = qty
                    if status in ("closed", "filled", "done"):
                        return last_price, last_filled_qty, True
                    if status in ("cancelled", "canceled", "rejected", "expired"):
                        return last_price, last_filled_qty, last_filled_qty > 0

            # Fallback poll. Only acquires budget if WS path didn't resolve.
            if ws_future is None or not ws_future.done():
                if not self.rate_limiter.acquire("entry"):
                    await asyncio.sleep(poll_interval)
                    continue
                try:
                    orders = await self.metascalp.get_orders(
                        self.mexc_connection_id, ticker=ticker,
                    )
                except Exception:  # noqa: BLE001
                    await asyncio.sleep(poll_interval)
                    continue
                for o in orders:
                    oid = str(o.get("OrderId") or o.get("ClientId")
                              or o.get("orderId") or o.get("clientId") or "")
                    if order_id and oid != order_id:
                        continue
                    status = str(o.get("Status") or o.get("status") or "").upper()
                    fq = (o.get("FilledSize") or o.get("filledSize")
                          or o.get("FilledQty") or 0.0)
                    last_filled_qty = float(fq) if fq else last_filled_qty
                    fp = (o.get("FilledPrice") or o.get("filledPrice")
                          or o.get("AvgPrice") or o.get("Price") or o.get("price"))
                    if fp is not None:
                        last_price = float(fp)
                    if status in ("FILLED", "CLOSED", "DONE"):
                        return last_price, last_filled_qty, True
                    if status in ("CANCELLED", "CANCELED", "REJECTED", "EXPIRED"):
                        return last_price, last_filled_qty, last_filled_qty > 0
        return last_price, last_filled_qty, last_filled_qty > 0

    # ---- helpers ------------------------------------------------------------

    def _size_usdt(self) -> float:
        if self.mode is TradingMode.SMALL_LIVE:
            return self.exec_cfg.min_notional_usdt
        return self.risk_cfg.max_position_usdt

    def _size_order(self, symbol: str, mark: float,
                    target_notional_usdt: float | None = None) -> tuple[float, float]:
        size_usdt = target_notional_usdt if target_notional_usdt is not None else self._size_usdt()
        qty = size_usdt / mark
        rules = self.mexc_ticker_rules.get(symbol.upper())
        if rules is not None:
            if (self.mode is TradingMode.SMALL_LIVE or target_notional_usdt is not None) \
                    and rules.min_size is not None:
                qty = max(qty, rules.min_size)
            if rules.size_increment is not None:
                if self.mode is TradingMode.SMALL_LIVE or target_notional_usdt is not None:
                    qty = _round_up_to_step(qty, rules.size_increment)
                else:
                    qty = _round_down_to_step(qty, rules.size_increment)
            if rules.min_size is not None and qty < rules.min_size:
                return 0.0, 0.0
            if rules.max_size is not None and qty > rules.max_size:
                qty = rules.max_size
        return qty, qty * mark

    def _round_price(self, symbol: str, side: Side, price: float) -> float:
        rules = self.mexc_ticker_rules.get(symbol.upper())
        if rules is None or rules.price_increment is None:
            return price
        if side is Side.LONG:
            return _round_up_to_step(price, rules.price_increment)
        return _round_down_to_step(price, rules.price_increment)

    def _entry_price(self, side: Side, book: Book) -> float | None:
        return book.best_ask if side is Side.LONG else book.best_bid

    def _exit_price(self, side: Side, book: Book) -> float | None:
        return book.best_bid if side is Side.LONG else book.best_ask

    def _initial_stop_price(self, entry_price: float, side: Side) -> float:
        sl_mult = self.risk_cfg.stop_loss_percent / 100.0
        if side is Side.LONG:
            return entry_price * (1 - sl_mult)
        return entry_price * (1 + sl_mult)

    def _estimate_slippage_bps(self, book: Book, side: Side, qty: float) -> float:
        levels = book.asks if side is Side.LONG else book.bids
        if not levels:
            return 1e9
        mid = book.mid or levels[0][0]
        remaining = qty
        notional = 0.0
        filled = 0.0
        for price, q in levels:
            take = min(remaining, q)
            notional += take * price
            filled += take
            remaining -= take
            if remaining <= 0:
                break
        if filled <= 0:
            return 1e9
        avg = notional / filled
        return abs(avg - mid) / mid * 10_000.0 if mid > 0 else 1e9

    def _liquidity_gate(self, book: Book, side: Side, qty: float,
                        notional_usdt: float) -> TickerGate:
        if qty <= 0 or notional_usdt <= 0:
            return TickerGate(False, "empty_order")
        levels = book.asks if side is Side.LONG else book.bids
        if not levels:
            return TickerGate(False, "empty_entry_side_book")
        best_notional = max(0.0, levels[0][0] * levels[0][1])
        side_depth = sum(max(0.0, price * level_qty)
                         for price, level_qty in levels[:5])
        best_ratio = best_notional / notional_usdt
        side_ratio = side_depth / notional_usdt
        if best_ratio < self.exec_cfg.min_best_level_to_notional:
            return TickerGate(
                False,
                f"best_level_ratio:{best_ratio:.2f}<"
                f"{self.exec_cfg.min_best_level_to_notional:.2f}",
            )
        if side_ratio < self.exec_cfg.min_entry_side_depth_to_notional:
            return TickerGate(
                False,
                f"side_depth_ratio:{side_ratio:.2f}<"
                f"{self.exec_cfg.min_entry_side_depth_to_notional:.2f}",
            )
        return TickerGate(True)

    def _suppress_log(self):  # type: ignore[no-untyped-def]
        # Tiny context manager that exists so the cancel-on-timeout block
        # stays readable; the real log call inside emits its own warning.
        from contextlib import nullcontext
        return nullcontext()

    # ---- logging ------------------------------------------------------------

    def _log_open_trade(self, sig: SignalResult, fill: FillResult) -> None:
        s = sig.snapshot
        row = {
            "time": int(time.time() * 1000),
            "symbol": s.symbol,
            "side": sig.side.value if sig.side else "",
            "entry_price": fill.price,
            "exit_price": "",
            "size": fill.qty,
            "pnl": "",
            "fee": "",
            "slippage": round(fill.slippage_bps, 4),
            "binance_mid": s.binance_mid,
            "mexc_mid": s.mexc_mid,
            "basis_bps": s.basis_bps,
            "binance_impulse_bps": s.binance_impulse_bps,
            "mexc_spread_bps": s.mexc_spread_bps,
            "depth_usdt": s.depth_usdt,
            "reason_for_entry": sig.reason,
            "reason_for_exit": "",
            "latency_order_send": round(fill.latency_send_ms, 2),
            "latency_fill": round(fill.latency_fill_ms, 2),
            "mode": self.mode.value,
        }
        self._recent_trades.appendleft(row)
        if self.trades_log:
            self.trades_log.write(row)

    def _log_close_trade(self, pos: Position, exit_price: float, pnl: float,
                          reason: ExitReason, detail: str) -> None:
        snap = pos.entry_snapshot or {}
        row = {
            "time": int(time.time() * 1000),
            "symbol": pos.symbol,
            "side": pos.side.value,
            "entry_price": pos.entry_price,
            "exit_price": exit_price,
            "size": pos.qty,
            "pnl": round(pnl, 6),
            "fee": "",
            "slippage": "",
            "binance_mid": snap.get("binance_mid"),
            "mexc_mid": snap.get("mexc_mid"),
            "basis_bps": snap.get("basis_bps"),
            "binance_impulse_bps": snap.get("binance_impulse_bps"),
            "mexc_spread_bps": snap.get("mexc_spread_bps"),
            "depth_usdt": snap.get("depth_usdt"),
            "reason_for_entry": "",
            "reason_for_exit": f"{reason.value}:{detail}",
            "latency_order_send": "",
            "latency_fill": "",
            "mode": self.mode.value,
        }
        self._recent_trades.appendleft(row)
        if self.trades_log:
            self.trades_log.write(row)
        log.info("execution.closed", symbol=pos.symbol, side=pos.side.value,
                 pnl=round(pnl, 6), reason=reason.value, detail=detail)


def _round_up_to_step(value: float, step: float) -> float:
    return _round_to_step(value, step, ROUND_CEILING)


def _round_down_to_step(value: float, step: float) -> float:
    return _round_to_step(value, step, ROUND_FLOOR)


def _round_to_step(value: float, step: float, rounding: str) -> float:
    if step <= 0:
        return value
    step_dec = Decimal(str(step))
    value_dec = Decimal(str(value))
    units = (value_dec / step_dec).to_integral_value(rounding=rounding)
    rounded = units * step_dec
    decimals = max(0, -step_dec.normalize().as_tuple().exponent)
    return round(float(rounded), decimals)


def _normalize_symbol(value: Any) -> str:
    return str(value or "").upper().replace("_", "").replace("-", "").replace("/", "")


def _first_float(raw: dict[str, Any], names: tuple[str, ...]) -> float | None:
    for name in names:
        value = raw.get(name)
        if value is None:
            continue
        if isinstance(value, str):
            value = value.replace("(", "").replace(")", "")
        try:
            parsed = float(value)
        except (TypeError, ValueError):
            continue
        return parsed
    return None


def _parse_position_side(raw: dict[str, Any], qty: float | None) -> Side | None:
    for name in ("Side", "side", "Direction", "direction", "PositionSide",
                 "positionSide", "Type", "type"):
        value = raw.get(name)
        if value is None:
            continue
        if isinstance(value, (int, float)):
            if int(value) == 1:
                return Side.LONG
            if int(value) == 2:
                return Side.SHORT
        text = str(value).lower()
        if "long" in text or text in {"buy", "b"}:
            return Side.LONG
        if "short" in text or text in {"sell", "s"}:
            return Side.SHORT
    if qty is not None:
        if qty > 0:
            return Side.LONG
        if qty < 0:
            return Side.SHORT
    return None


def _extract_usdt_balance(raw: Any) -> tuple[float, float | None]:
    """Best-effort parser for MetaScalp balance payload variants."""
    candidates: list[dict[str, Any]] = []

    def collect(value: Any) -> None:
        if isinstance(value, dict):
            currency = str(
                value.get("Currency")
                or value.get("currency")
                or value.get("Coin")
                or value.get("coin")
                or value.get("Asset")
                or value.get("asset")
                or value.get("QuoteAsset")
                or value.get("quoteAsset")
                or ""
            ).upper()
            if currency in {"USDT", ""}:
                candidates.append(value)
            for nested_key in ("Items", "items", "Balances", "balances",
                               "Assets", "assets", "data"):
                nested = value.get(nested_key)
                if isinstance(nested, (list, dict)):
                    collect(nested)
        elif isinstance(value, list):
            for item in value:
                collect(item)

    collect(raw)

    available_names = (
        "availableOpen", "AvailableOpen",
        "availableBalance", "AvailableBalance",
        "availableCash", "AvailableCash",
        "Available", "available",
        "Free", "free",
        "Balance", "balance",
        "cashBalance", "CashBalance",
    )
    equity_names = (
        "equity", "Equity",
        "totalEquity", "TotalEquity",
        "Total", "total",
        "cashBalance", "CashBalance",
        "Balance", "balance",
    )

    def num(item: dict[str, Any], names: tuple[str, ...]) -> float | None:
        for name in names:
            value = item.get(name)
            if value is None:
                continue
            try:
                parsed = float(value)
            except (TypeError, ValueError):
                continue
            if parsed >= 0:
                return parsed
        return None

    for item in candidates:
        currency = str(item.get("Currency") or item.get("currency")
                       or item.get("Coin") or item.get("coin")
                       or item.get("Asset") or item.get("asset") or "").upper()
        if currency and currency != "USDT":
            continue
        available = num(item, available_names)
        if available is None:
            continue
        equity = num(item, equity_names)
        return available, equity
    return 0.0, None
