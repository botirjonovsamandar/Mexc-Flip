"""Orchestrator — wires MetaScalp, market data caches, strategy, risk and
execution together.

Architecture:
  * ONE MetaScalp HTTP client, ONE MetaScalp WS subscriber.
  * TWO BookCache instances: `binance_cache` (signal source),
    `mexc_cache` (execution venue).
  * The WS subscriber gets orderbook for every traded symbol on BOTH the
    Binance and MEXC connections, plus account-level events on the MEXC
    connection so we see fills as they happen.

Run:
    python -m app.main --config configs\\config.json
"""
from __future__ import annotations

import argparse
import asyncio
import signal
import time
from contextlib import suppress
from pathlib import Path
from typing import Any

import uvicorn

from .book_cache import BookCache
from .config import BotConfig, TradingMode, load_config
from .dashboard import DashboardState, build_app
from .execution import ExecutionEngine, TickerRules
from .logger import (LatencyLogger, SignalsLogger, TradesLogger, get_logger,
                     setup_logging)
from .metascalp_client import (ConnectionInfo, MetaScalpClient,
                                MetaScalpUnavailable, MetaScalpWS)
from .position_manager import PositionManager
from .risk_manager import RiskManager
from .strategy import CooldownTracker, Decision, Strategy


log = get_logger("main")


class TradingBot:
    def __init__(self, cfg: BotConfig) -> None:
        self.cfg = cfg
        self.binance_cache = BookCache(
            max_staleness_ms=cfg.market_data.max_staleness_ms,
            history_seconds=cfg.market_data.mid_history_seconds,
        )
        self.mexc_cache = BookCache(
            max_staleness_ms=cfg.market_data.max_staleness_ms,
            history_seconds=cfg.market_data.mid_history_seconds,
        )
        self.cooldown = CooldownTracker()
        self.strategy = Strategy(cfg.strategy, self.binance_cache, self.mexc_cache,
                                  self.cooldown)
        self.risk = RiskManager(cfg.risk)
        self.positions = PositionManager(cfg.risk, self.mexc_cache)
        self.trades_log = TradesLogger(cfg.storage.trades_csv)
        self.signals_log = SignalsLogger(cfg.storage.signals_csv)
        self.latency_log = LatencyLogger(cfg.storage.latency_log)

        self.metascalp: MetaScalpClient | None = None
        self.metascalp_ws: MetaScalpWS | None = None
        self.binance_conn: ConnectionInfo | None = None
        self.mexc_conn: ConnectionInfo | None = None
        # ConnectionId -> source-name resolver used by the WS event handler.
        self._cid_to_source: dict[int, str] = {}
        # Canonical symbol ('XLMUSDT') -> exchange-native ticker ('XLM_USDT' on MEXC).
        self.binance_tickers: dict[str, str] = {}
        self.mexc_tickers: dict[str, str] = {}
        # Reverse maps for normalising incoming WS payloads back to canonical.
        self._binance_reverse: dict[str, str] = {}
        self._mexc_reverse: dict[str, str] = {}

        self.execution = ExecutionEngine(
            cfg=cfg, mexc=self.mexc_cache, metascalp=None,
            mexc_connection_id=None,
            positions=self.positions, risk=self.risk,
            trades_log=self.trades_log,
        )
        self.dashboard = DashboardState()
        self.dashboard.mode = cfg.mode.value
        self._stop = asyncio.Event()
        # Event-driven hot path: WS handler sets `_dirty_event` and records the
        # symbol in `_dirty_symbols` when a Binance update arrives. The signal
        # and position-exit loops wake up immediately and evaluate ONLY those
        # symbols — no fixed 50ms / 100ms polling latency.
        self._dirty_symbols: set[str] = set()
        self._dirty_event: asyncio.Event = asyncio.Event()

    # ---- lifecycle ----------------------------------------------------------

    async def run(self) -> None:
        log.info("bot.starting", mode=self.cfg.mode.value, symbols=self.cfg.symbols)
        tasks: list[asyncio.Task[Any]] = [
            asyncio.create_task(self._metascalp_loop(), name="metascalp_lifecycle"),
            asyncio.create_task(self._signal_loop(), name="signal_loop"),
            asyncio.create_task(self._position_loop(), name="position_loop"),
            asyncio.create_task(self._health_loop(), name="health_loop"),
        ]
        if self.cfg.dashboard.enabled:
            tasks.append(asyncio.create_task(self._serve_dashboard(),
                                              name="dashboard"))

        await self._stop.wait()
        log.info("bot.stopping")
        for t in tasks:
            t.cancel()
        with suppress(asyncio.CancelledError):
            await asyncio.gather(*tasks, return_exceptions=True)
        if self.metascalp:
            await self.metascalp.close()

    def request_stop(self) -> None:
        self._stop.set()

    def request_dashboard_stop(self) -> None:
        self.risk.trigger_kill_switch("dashboard_stop")
        self.request_stop()

    # ---- metascalp loop ----------------------------------------------------

    async def _metascalp_loop(self) -> None:
        """Connect to MetaScalp, find both connections, subscribe via WS.
        On any error: tear down and retry with exponential backoff.
        """
        delay = self.cfg.metascalp.ws_reconnect_delay_sec
        while not self._stop.is_set():
            try:
                await self._metascalp_session()
                delay = self.cfg.metascalp.ws_reconnect_delay_sec
            except MetaScalpUnavailable as e:
                log.error("metascalp.unavailable", err=str(e),
                          hint="Is MetaScalp running on 127.0.0.1?")
                self.risk.set_health(metascalp=False, binance=False, mexc=False)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                log.exception("metascalp.session_error", err=str(e))
                self.risk.set_health(metascalp=False, binance=False, mexc=False)
            with suppress(asyncio.TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            delay = min(delay * 2, self.cfg.metascalp.ws_max_reconnect_delay_sec)

    async def _metascalp_session(self) -> None:
        client = MetaScalpClient(self.cfg.metascalp)
        await client.connect()
        self.metascalp = client
        # resolve both connections
        bh = self.cfg.metascalp.binance_connection
        mh = self.cfg.metascalp.mexc_connection
        self.binance_conn = await client.find_connection(
            connection_id=bh.connection_id, exchange=bh.exchange,
            market_types=bh.market_types, require_can_trade=bh.require_can_trade,
        )
        self.mexc_conn = await client.find_connection(
            connection_id=mh.connection_id, exchange=mh.exchange,
            market_types=mh.market_types, require_can_trade=mh.require_can_trade,
        )
        self._cid_to_source = {
            self.binance_conn.id: "binance",
            self.mexc_conn.id: "mexc",
        }
        log.info("metascalp.connections_ready",
                 binance=self.binance_conn.id, mexc=self.mexc_conn.id,
                 mexc_can_trade=self.mexc_conn.can_trade)

        # Build per-exchange ticker maps so we can talk Binance's 'XLMUSDT'
        # and MEXC's 'XLM_USDT' without leaking the separator into strategy.
        binance_syms = list(self.cfg.symbols)
        for m in ("BTCUSDT", "ETHUSDT"):
            if m not in binance_syms:
                binance_syms.append(m)
        self.binance_tickers = await client.build_ticker_map(
            self.binance_conn.id, binance_syms,
        )
        self.mexc_tickers = await client.build_ticker_map(
            self.mexc_conn.id, self.cfg.symbols,
        )
        missing_binance = [s for s in binance_syms if s not in self.binance_tickers]
        missing_mexc = [s for s in self.cfg.symbols if s not in self.mexc_tickers]
        if missing_binance or missing_mexc:
            log.error("symbols.missing_on_exchange",
                      binance=missing_binance, mexc=missing_mexc,
                      hint="rename or drop these in configs/config.json -> symbols")
        self._binance_reverse = {v.upper(): k for k, v in self.binance_tickers.items()}
        self._mexc_reverse = {v.upper(): k for k, v in self.mexc_tickers.items()}
        log.info("metascalp.ticker_maps_ready",
                 binance_count=len(self.binance_tickers),
                 mexc_count=len(self.mexc_tickers))

        self.execution.metascalp = client
        self.execution.mexc_connection_id = self.mexc_conn.id
        self.execution.mexc_ticker_map = self.mexc_tickers
        self.execution.invalidate_entry_templates()
        self.execution.mexc_ticker_rules = await _build_ticker_rules(
            client, self.mexc_conn.id, self.mexc_tickers,
        )

        # subscribe via WS
        ws = MetaScalpWS(client, on_event=self._on_ms_event)
        # account-level events on the MEXC connection
        ws.add_connection_subscribe(self.mexc_conn.id)
        depth = self.cfg.market_data.orderbook_depth_levels
        fetch = self.cfg.market_data.fetch_snapshot_on_subscribe
        depth_pct = self.cfg.market_data.orderbook_depth_percent
        for canonical in binance_syms:
            ex_ticker = self.binance_tickers.get(canonical)
            if ex_ticker:
                ws.add_orderbook(self.binance_conn.id, ticker=ex_ticker,
                                  depth_levels=depth, fetch_snapshot=fetch,
                                  depth_percent=depth_pct)
        for canonical in self.cfg.symbols:
            ex_ticker = self.mexc_tickers.get(canonical)
            if ex_ticker:
                ws.add_orderbook(self.mexc_conn.id, ticker=ex_ticker,
                                  depth_levels=depth, fetch_snapshot=fetch,
                                  depth_percent=depth_pct)
        self.metascalp_ws = ws
        self.risk.set_health(metascalp=True)
        await ws.run()

    async def _on_ms_event(self, msg: dict[str, Any]) -> None:
        """Parse MetaScalp WS messages.

        Orderbook payloads route to the right BookCache by ConnectionId.
        Order / position / balance events are logged; the execution engine
        polls REST for fill confirmation in this MVP.
        """
        mtype = str(msg.get("Type") or msg.get("type") or "")
        data = msg.get("Data") or msg.get("data") or {}

        if mtype in ("orderbook_snapshot", "orderbook_update"):
            cid = int(data.get("ConnectionId") or data.get("connectionId") or 0)
            ticker_native = str(data.get("Ticker") or data.get("ticker") or "").upper()
            if not ticker_native:
                return
            source = self._cid_to_source.get(cid)
            if source is None:
                return
            if source == "binance":
                cache = self.binance_cache
                canonical = self._binance_reverse.get(ticker_native, ticker_native)
            else:
                cache = self.mexc_cache
                canonical = self._mexc_reverse.get(ticker_native, ticker_native.replace("_", ""))
            bids, asks, best_bid, best_ask = _parse_orderbook_payload(data)
            if not bids and not asks:
                return
            ts_ms = int(data.get("Time") or data.get("ts") or time.time() * 1000)
            if mtype == "orderbook_snapshot":
                cache.replace_snapshot(canonical, bids, asks, ts_ms=ts_ms)
            else:
                cache.apply_update(canonical, bids, asks, ts_ms=ts_ms,
                                   best_bid=best_bid, best_ask=best_ask)
            # Wake the signal/exit loops on any Binance touch — strategy reads
            # Binance impulse; MEXC updates change the book but not the trigger.
            if source == "binance":
                self._dirty_symbols.add(canonical.upper())
                self._dirty_event.set()
        elif mtype in ("order_update", "position_update", "balance_update",
                        "finres_update"):
            log.debug("metascalp.event", type=mtype, data=data)
            if mtype == "order_update":
                self.dashboard.push_order(_compact_order_event(data))
                self.execution.on_ws_order_update(data)
            elif mtype == "position_update":
                self.dashboard.exchange_positions = await self.execution.refresh_exchange_positions(
                    force=True,
                )
            elif mtype == "balance_update":
                self.execution.invalidate_account_snapshot()
        elif mtype == "subscribed":
            log.info("metascalp.subscribed", data=data)
        elif mtype == "error":
            log.error("metascalp.ws_server_error",
                      err=data.get("Error") or data.get("error"))

    # ---- loops --------------------------------------------------------------

    async def _signal_loop(self) -> None:
        """Event-driven: wakes when Binance WS pushes an update.

        Evaluates only the symbols that actually changed (`_dirty_symbols`).
        A 250ms fallback timeout still re-runs the full sweep in case the
        WS feed is silent — protects time-based decisions (cooldowns, exits).
        """
        fallback_sec = max(0.05,
                           self.cfg.strategy.signal_eval_interval_ms / 1000.0 * 5)
        allowed = {s.upper() for s in self.cfg.symbols}
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._dirty_event.wait(),
                                       timeout=fallback_sec)
            except asyncio.TimeoutError:
                # Periodic sweep — re-check all symbols even when WS is quiet.
                syms_to_eval: list[str] = list(self.cfg.symbols)
            else:
                # Drain the dirty set, intersected with our traded universe.
                dirty = self._dirty_symbols.copy()
                self._dirty_symbols.clear()
                self._dirty_event.clear()
                syms_to_eval = [s for s in dirty if s in allowed]
                if not syms_to_eval:
                    continue

            # First: check exits on any dirty symbol that already has a
            # position. This is the fast-path for price-driven exits — no
            # 100ms polling lag anymore.
            if self.cfg.mode is not TradingMode.DRY_RUN:
                open_set = set(self.positions.open_symbols)
                for sym in syms_to_eval:
                    if sym in open_set:
                        b_mid = self.binance_cache.book(sym).mid
                        decision = self.positions.check_exit(sym, b_mid)
                        if decision.should_exit and decision.reason:
                            await self.execution.close_position(
                                sym, decision.reason, decision.detail,
                            )

            occupied_symbols = set(self.execution.occupied_symbols)
            enter_signals = []
            for sym in syms_to_eval:
                res = self.strategy.evaluate(sym, occupied_symbols)
                self._record_signal(res)
                if res.decision is Decision.ENTER:
                    enter_signals.append(res)

            if not enter_signals:
                continue

            plans = await self.execution.build_entry_plans(enter_signals)
            if not plans:
                continue

            if self.cfg.mode is TradingMode.DRY_RUN:
                for plan in plans:
                    sig = plan.signal
                    log.info("signal.would_enter_selected",
                             symbol=sig.snapshot.symbol,
                             side=sig.side.value if sig.side else "-",
                             margin_usdt=round(plan.margin_usdt, 4),
                             notional_usdt=round(plan.notional_usdt, 4),
                             expected_profit_usdt=round(plan.expected_profit_usdt, 6),
                             reason=sig.reason)
                    self.cooldown.arm(sig.snapshot.symbol,
                                      self.cfg.strategy.cooldown_per_symbol_sec)
                continue

            for plan in plans:
                pos = await self.execution.try_enter(plan)
                if pos:
                    self.cooldown.arm(plan.signal.snapshot.symbol,
                                      self.cfg.strategy.cooldown_per_symbol_sec)

    async def _position_loop(self) -> None:
        """Event-driven exit check.

        Wakes on the same Binance dirty event as _signal_loop, but only acts
        on symbols where we have open positions. 1s fallback covers the
        time-based exits (`max_position_time_seconds`).
        """
        while not self._stop.is_set():
            await asyncio.sleep(1.0)
            if self.cfg.mode is TradingMode.DRY_RUN:
                continue
            # Time-based exits (max_position_time_seconds, breakeven trigger)
            # don't depend on a fresh Binance tick, so the periodic sweep
            # iterates ALL open symbols here. The Binance-tick-driven path is
            # below (we don't have a separate event for exits — strategy reads
            # Binance impulse and position_manager reads Binance mid for stops,
            # both share the same dirty signal).
            for sym in list(self.positions.open_symbols):
                b_mid = self.binance_cache.book(sym).mid
                decision = self.positions.check_exit(sym, b_mid)
                if decision.should_exit and decision.reason:
                    await self.execution.close_position(sym, decision.reason,
                                                          decision.detail)

    async def _health_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(0.5)
            # Global flags now mean "WS transport alive" + "at least one symbol fresh".
            # Per-symbol staleness is enforced inside Strategy.evaluate(); a thin
            # ticker can be stale without taking the whole feed down.
            ws_alive = self.metascalp_ws is not None and self.metascalp_ws.is_connected
            binance_ok = ws_alive and any(
                not self.binance_cache.is_stale(s) for s in self.cfg.symbols
            )
            mexc_ok = ws_alive and any(
                not self.mexc_cache.is_stale(s) for s in self.cfg.symbols
            )
            self.risk.set_health(binance=binance_ok, mexc=mexc_ok)
            self.dashboard.kill_switch = self.risk.kill_switch
            self.dashboard.paused = self.risk.is_paused
            self.dashboard.metascalp_ok = self.risk.metascalp_ok
            self.dashboard.binance_ok = binance_ok
            self.dashboard.mexc_ok = mexc_ok
            self.dashboard.day_pnl = self.risk.day_pnl
            self.dashboard.day_trades = self.risk.day_trades
            self.dashboard.consecutive_losses = self.risk.consecutive_losses
            self.dashboard.positions = self.positions.snapshot()
            self.dashboard.exchange_positions = await self.execution.refresh_exchange_positions()
            for order in reversed(self.execution.recent_orders_snapshot()):
                if order not in self.dashboard.recent_orders:
                    self.dashboard.push_order(order)
            for trade in reversed(self.execution.recent_trades_snapshot()):
                if trade not in self.dashboard.recent_trades:
                    self.dashboard.push_trade(trade)
            if self.execution.occupied_slots == 0:
                self.risk.clear_entry_block("flat_reconcile")
            now_ms = int(time.time() * 1000)
            b_last = max((self.binance_cache.last_update_ms(s)
                           for s in self.cfg.symbols), default=0)
            m_last = max((self.mexc_cache.last_update_ms(s)
                           for s in self.cfg.symbols), default=0)
            self.dashboard.latency["binance_ms"] = (now_ms - b_last) if b_last else "-"
            self.dashboard.latency["mexc_ms"] = (now_ms - m_last) if m_last else "-"
            rl = self.execution.rate_limiter.stats()
            self.dashboard.rate_limit = {
                "used_sec": rl.used_last_sec,
                "used_min": rl.used_last_min,
                "used_hour": rl.used_last_hour,
                "safe_hourly_cap": rl.safe_hourly_cap,
                "entry_budget_remaining": rl.entry_budget_remaining,
                "close_reserve": rl.close_reserve,
                "total_hourly_remaining": rl.total_hourly_remaining,
                "rejected_total": rl.rejected_total,
            }
            if isinstance(self.dashboard.latency["binance_ms"], int) and \
                    self.dashboard.latency["binance_ms"] > self.cfg.market_data.max_staleness_ms:
                self.latency_log.write({
                    "ts": now_ms, "kind": "binance_stale",
                    "age_ms": self.dashboard.latency["binance_ms"],
                })

    # ---- helpers ------------------------------------------------------------

    def _record_signal(self, res) -> None:  # type: ignore[no-untyped-def]
        s = res.snapshot
        if res.decision is Decision.NO_DATA:
            return
        row = {
            "time": s.ts_ms,
            "symbol": s.symbol,
            "side": res.side.value if res.side else "",
            "binance_mid": s.binance_mid,
            "mexc_mid": s.mexc_mid,
            "basis_bps": s.basis_bps,
            "binance_impulse_bps": s.binance_impulse_bps,
            "mexc_spread_bps": s.mexc_spread_bps,
            "depth_usdt": s.depth_usdt,
            "decision": res.decision.value,
            "reject_reason": res.reason if res.decision is not Decision.ENTER else "",
            "mode": self.cfg.mode.value,
        }
        self.signals_log.write(row)
        self.dashboard.push_signal({
            "ts_ms": s.ts_ms, "symbol": s.symbol,
            "side": res.side.value if res.side else "-",
            "decision": res.decision.value, "reason": res.reason,
            "binance_impulse_bps": s.binance_impulse_bps,
            "basis_bps": s.basis_bps,
            "mexc_spread_bps": s.mexc_spread_bps,
            "depth_usdt": s.depth_usdt,
        })

    async def _serve_dashboard(self) -> None:
        app = build_app(
            self.dashboard,
            on_stop=self.request_dashboard_stop,
            on_resume=lambda: self.risk.clear_kill_switch(),
            on_close_all=self.execution.emergency_close_all,
        )
        cfg = uvicorn.Config(
            app, host=self.cfg.dashboard.host, port=self.cfg.dashboard.port,
            log_level="warning", access_log=False, loop="asyncio",
        )
        server = uvicorn.Server(cfg)
        await server.serve()


def _parse_levels(rows: Any) -> list[tuple[float, float]]:
    """Accept both [{Price,Size}, ...] and [[price, size], ...] payloads."""
    out: list[tuple[float, float]] = []
    if not isinstance(rows, list):
        return out
    for r in rows:
        if isinstance(r, dict):
            p = r.get("Price") or r.get("price")
            q = r.get("Size") or r.get("size") or r.get("Volume") or r.get("volume")
            if p is None or q is None:
                continue
            out.append((float(p), float(q)))
        elif isinstance(r, (list, tuple)) and len(r) >= 2:
            out.append((float(r[0]), float(r[1])))
    return out


async def _build_ticker_rules(
    client: MetaScalpClient,
    connection_id: int,
    ticker_map: dict[str, str],
) -> dict[str, TickerRules]:
    raw_tickers = await client.get_tickers(connection_id)
    by_name = {
        str(t.get("Name") or t.get("name") or "").upper(): t
        for t in raw_tickers
        if isinstance(t, dict)
    }
    out: dict[str, TickerRules] = {}
    for canonical, native in ticker_map.items():
        raw = by_name.get(native.upper())
        if raw is not None:
            out[canonical.upper()] = TickerRules.from_raw(raw)
    return out


def _parse_orderbook_sides(data: dict[str, Any]) -> tuple[list[tuple[float, float]],
                                                          list[tuple[float, float]]]:
    bids, asks, _, _ = _parse_orderbook_payload(data)
    return bids, asks


def _compact_order_event(data: dict[str, Any]) -> dict[str, Any]:
    ticker = str(data.get("Ticker") or data.get("ticker")
                 or data.get("Symbol") or data.get("symbol") or "")
    symbol = ticker.upper().replace("_", "").replace("-", "").replace("/", "")
    return {
        "ts_ms": int(time.time() * 1000),
        "symbol": symbol,
        "ticker": ticker,
        "side": data.get("Side") or data.get("side") or data.get("Direction") or "",
        "order_type": data.get("Type") or data.get("type") or "",
        "size": data.get("Size") or data.get("size") or data.get("Qty") or data.get("qty") or "",
        "price": data.get("Price") or data.get("price") or "",
        "status": data.get("Status") or data.get("status") or "update",
        "detail": str(data.get("OrderId") or data.get("orderId")
                      or data.get("ClientId") or data.get("clientId") or ""),
    }


def _parse_orderbook_payload(
    data: dict[str, Any],
) -> tuple[list[tuple[float, float]], list[tuple[float, float]],
           float | None, float | None]:
    bids = _parse_levels(data.get("Bids") or data.get("bids") or [])
    asks = _parse_levels(data.get("Asks") or data.get("asks") or [])
    if bids or asks:
        return bids, asks, None, None

    # MetaScalp live WS updates use one mixed `updates` list:
    # {"price": 0.14858, "size": 133.27, "type": "BestBid"}.
    return _parse_typed_updates(data.get("Updates") or data.get("updates") or [])


def _parse_typed_updates(
    rows: Any,
) -> tuple[list[tuple[float, float]], list[tuple[float, float]],
           float | None, float | None]:
    bids: list[tuple[float, float]] = []
    asks: list[tuple[float, float]] = []
    best_bid: float | None = None
    best_ask: float | None = None
    if not isinstance(rows, list):
        return bids, asks, best_bid, best_ask
    for r in rows:
        if not isinstance(r, dict):
            continue
        p = r.get("Price") or r.get("price")
        q = r.get("Size") or r.get("size") or r.get("Volume") or r.get("volume")
        if p is None or q is None:
            continue
        side = str(r.get("Type") or r.get("type") or r.get("Side") or r.get("side") or "")
        side = side.lower()
        level = (float(p), float(q))
        if side == "bestbid":
            best_bid = level[0]
        elif side == "bestask":
            best_ask = level[0]
        if "bid" in side or side in {"buy", "b"}:
            bids.append(level)
        elif "ask" in side or side in {"sell", "a"}:
            asks.append(level)
    return bids, asks, best_bid, best_ask


def _install_signal_handlers(bot: TradingBot) -> None:
    loop = asyncio.get_event_loop()

    def _stop(*_: Any) -> None:
        log.warning("signal_received_stopping")
        bot.request_stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except NotImplementedError:
            signal.signal(sig, _stop)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MEXC/Binance basis bot")
    p.add_argument("--config", default="configs/config.json",
                    help="path to config json")
    return p.parse_args()


async def _amain() -> None:
    args = parse_args()
    cfg_path = Path(args.config)
    cfg = load_config(cfg_path)
    setup_logging(level=cfg.logging.level, json_output=cfg.logging.json_output,
                  redact=cfg.logging.redact_secrets,
                  errors_log_path=cfg.storage.errors_log)
    bot = TradingBot(cfg)
    _install_signal_handlers(bot)
    log.info("config.loaded", path=str(cfg_path), mode=cfg.mode.value,
             symbol_count=len(cfg.symbols))
    if cfg.mode is TradingMode.LIVE:
        log.error("LIVE_mode_active",
                  hint="Make sure you ran DRY_RUN / PAPER / SMALL_LIVE first.")
    await bot.run()


def main() -> None:
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
