"""MetaScalp local API client (REST + WebSocket).

Mirrors the public MetaScalp SDK reference at
https://metascalp.github.io/metascalp-sdk/ .

Key properties of the real API (do not change without re-reading the docs):
  * Host is always 127.0.0.1, port auto-selected in 17845..17855.
  * NO authentication — the local API is unauthenticated by design.
  * REST is under /api/* (not /api/v1/*). Discovery is GET /ping.
  * All payloads use PascalCase (Ticker, Side, Size, Price, Type, ReduceOnly).
  * Side: 1=Buy, 2=Sell. Type: 0=Limit, 1=Stop, 2=StopLoss, 3=TakeProfit, 4=Market.
  * WS is ws://127.0.0.1:{port}/  (same port as HTTP, no /ws suffix).
  * WS messages have shape {"Type": "...", "Data": {...}}.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Iterable

import httpx
import msgspec
import websockets

# msgspec decoder — ~10x faster than stdlib json.loads on the hot WS path.
# We decode to plain dict (not typed Struct) because MetaScalp uses 16+
# message types with optional fields; the dispatcher in main._on_ms_event
# branches on Type anyway. The win is in the decode itself.
_WS_DECODER = msgspec.json.Decoder()

from .config import MetaScalpConfig
from .logger import get_logger

log = get_logger("metascalp")


# ---- enums (mirroring the API) --------------------------------------------

class OrderSide:
    BUY = 1
    SELL = 2


class OrderType:
    LIMIT = 0
    STOP = 1
    STOP_LOSS = 2
    TAKE_PROFIT = 3
    MARKET = 4


class MarketType:
    SPOT = 0
    FUTURES = 1
    USDT_FUTURES = 2
    COIN_FUTURES = 3
    INVERSE_FUTURES = 4
    USDT_PERPETUAL = 5
    USDC_PERPETUAL = 6
    MARGIN = 7
    OPTIONS = 8
    STOCK = 9


class ConnectionState:
    DISCONNECTED = 0
    CONNECTING = 1
    CONNECTED = 2
    RECONNECTING = 3
    RESETTING = 4


class MetaScalpError(Exception):
    """Generic MetaScalp client error."""


class MetaScalpOrderRejected(MetaScalpError):
    """Raised when MetaScalp explicitly rejects an order request."""

    def __init__(self, status_code: int | None, detail: str,
                 payload: dict[str, Any]) -> None:
        self.status_code = status_code
        self.detail = detail
        self.payload = payload
        super().__init__(
            f"order rejected by MetaScalp: {status_code or 'unknown'} {detail}"
        )


class MetaScalpUnavailable(MetaScalpError):
    """Raised when no MetaScalp instance can be reached."""


@dataclass
class ConnectionInfo:
    id: int
    name: str
    exchange: str
    exchange_id: str
    market: str
    market_type: int
    state: int
    view_mode: bool
    demo_mode: bool
    raw: dict[str, Any]

    @property
    def is_connected(self) -> bool:
        return self.state == ConnectionState.CONNECTED

    @property
    def can_trade(self) -> bool:
        """ViewMode=true means the connection is read-only — orders rejected."""
        return not self.view_mode

    @classmethod
    def from_raw(cls, raw: dict[str, Any]) -> "ConnectionInfo":
        return cls(
            id=int(raw.get("Id") or raw.get("id") or 0),
            name=str(raw.get("Name") or raw.get("name") or ""),
            exchange=str(raw.get("Exchange") or raw.get("exchange") or ""),
            exchange_id=str(raw.get("ExchangeId") or raw.get("exchangeId") or ""),
            market=str(raw.get("Market") or raw.get("market") or ""),
            market_type=int(raw.get("MarketType") or raw.get("marketType") or 0),
            state=int(raw.get("State") or raw.get("state") or 0),
            view_mode=bool(raw.get("ViewMode") or raw.get("viewMode") or False),
            demo_mode=bool(raw.get("DemoMode") or raw.get("demoMode") or False),
            raw=raw,
        )


@dataclass
class OrderRequest:
    ticker: str
    side: int                       # OrderSide.BUY / SELL
    size: float
    price: float | None = None
    order_type: int = OrderType.LIMIT
    reduce_only: bool = False
    stop_loss_price: float | None = None

    def to_payload(self) -> dict[str, Any]:
        p: dict[str, Any] = {
            "Ticker": self.ticker,
            "Side": int(self.side),
            "Size": float(self.size),
            "Type": int(self.order_type),
            "ReduceOnly": bool(self.reduce_only),
        }
        if self.price is not None and self.order_type != OrderType.MARKET:
            p["Price"] = float(self.price)
        if self.stop_loss_price is not None:
            p["StopLossPrice"] = float(self.stop_loss_price)
        return p


# ---------------------------------------------------------------------------

class MetaScalpClient:
    def __init__(self, cfg: MetaScalpConfig) -> None:
        self.cfg = cfg
        self._port: int | None = cfg.port_override
        self._client: httpx.AsyncClient | None = None
        self.app_name: str | None = None
        self.app_version: str | None = None

    async def __aenter__(self) -> "MetaScalpClient":
        await self.connect()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    # ---- lifecycle ----------------------------------------------------------

    async def connect(self) -> None:
        if self._port is None:
            self._port = await self.scan_ports()
            if self._port is None:
                raise MetaScalpUnavailable(
                    f"No MetaScalp API found on {self.cfg.host}:"
                    f"{self.cfg.port_scan_start}-{self.cfg.port_scan_end}. "
                    "Is MetaScalp running?"
                )
        # Explicit pool config so a stray reconnect can't blip the order path.
        limits = httpx.Limits(
            max_keepalive_connections=20,
            max_connections=20,
            keepalive_expiry=300.0,
        )
        self._client = httpx.AsyncClient(
            base_url=f"http://{self.cfg.host}:{self._port}",
            timeout=self.cfg.request_timeout_sec,
            limits=limits,
        )
        # Cache app metadata from /ping for the dashboard.
        try:
            r = await self._client.get("/ping")
            r.raise_for_status()
            data = r.json() if r.content else {}
            self.app_name = str(data.get("AppName") or data.get("Name") or "")
            self.app_version = str(data.get("Version") or "")
        except Exception:  # noqa: BLE001
            pass
        log.info("metascalp.connected", host=self.cfg.host, port=self._port,
                 app=self.app_name, version=self.app_version)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    @property
    def port(self) -> int:
        if self._port is None:
            raise MetaScalpError("Not connected yet")
        return self._port

    # ---- discovery ----------------------------------------------------------

    async def scan_ports(self) -> int | None:
        for port in range(self.cfg.port_scan_start, self.cfg.port_scan_end + 1):
            if await self._probe(port):
                log.info("metascalp.found", port=port)
                return port
        return None

    async def _probe(self, port: int) -> bool:
        url = f"http://{self.cfg.host}:{port}/ping"
        try:
            async with httpx.AsyncClient(timeout=0.5) as c:
                r = await c.get(url)
                return r.status_code == 200
        except (httpx.ConnectError, httpx.ReadTimeout, httpx.ReadError,
                httpx.HTTPError):
            return False
        except Exception as e:  # noqa: BLE001
            log.debug("metascalp.probe_error", port=port, err=str(e))
            return False

    async def ping(self) -> bool:
        try:
            r = await self._get("/ping")
            return r.status_code == 200
        except Exception:  # noqa: BLE001
            return False

    # ---- low-level helpers --------------------------------------------------

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> httpx.Response:
        assert self._client is not None
        r = await self._client.get(path, params=params)
        r.raise_for_status()
        return r

    async def _post(self, path: str, body: dict[str, Any]) -> httpx.Response:
        assert self._client is not None
        r = await self._client.post(path, json=body)
        r.raise_for_status()
        return r

    async def _put(self, path: str, body: dict[str, Any],
                    params: dict[str, Any] | None = None) -> httpx.Response:
        assert self._client is not None
        r = await self._client.put(path, params=params, json=body)
        r.raise_for_status()
        return r

    async def _delete(self, path: str, params: dict[str, Any] | None = None) -> httpx.Response:
        assert self._client is not None
        r = await self._client.delete(path, params=params)
        r.raise_for_status()
        return r

    # ---- connections --------------------------------------------------------

    async def get_connections(self) -> list[ConnectionInfo]:
        r = await self._get("/api/connections")
        data = r.json()
        items = data if isinstance(data, list) else data.get("connections") or data.get("Items") or []
        return [ConnectionInfo.from_raw(x) for x in items]

    async def get_connection(self, connection_id: int) -> ConnectionInfo:
        r = await self._get(f"/api/connections/{connection_id}")
        return ConnectionInfo.from_raw(r.json())

    async def find_connection(self, *, exchange: str,
                              market_types: Iterable[int] | None = None,
                              connection_id: int | None = None,
                              require_can_trade: bool = False
                              ) -> ConnectionInfo:
        """Look up a connection by exchange name and market type(s).

        If `connection_id` is given, fetch it directly and skip discovery.
        `exchange` matching is case-insensitive substring against `Exchange`
        and `ExchangeId` fields.
        """
        if connection_id is not None:
            conn = await self.get_connection(connection_id)
            if require_can_trade and not conn.can_trade:
                raise MetaScalpError(
                    f"Connection {connection_id} is in ViewMode (read-only). "
                    "Disable ViewMode in MetaScalp UI to trade via API."
                )
            return conn

        ex = exchange.lower()
        mt_set = set(market_types) if market_types else None
        conns = await self.get_connections()
        for c in conns:
            if ex not in c.exchange.lower() and ex not in c.exchange_id.lower():
                continue
            if mt_set is not None and c.market_type not in mt_set:
                continue
            if require_can_trade and not c.can_trade:
                continue
            return c
        raise MetaScalpError(
            f"No {exchange} connection found "
            f"(market_types={list(market_types) if market_types else 'any'}, "
            f"require_can_trade={require_can_trade}). "
            "Add the connection in MetaScalp first."
        )

    async def get_tickers(self, connection_id: int) -> list[dict[str, Any]]:
        """Return the raw ticker list (each entry has Name/BaseAsset/QuoteAsset/...)."""
        r = await self._get(f"/api/connections/{connection_id}/tickers")
        data = r.json()
        items = data if isinstance(data, list) else data.get("tickers") or data.get("Tickers") or []
        return list(items)

    async def build_ticker_map(self, connection_id: int,
                                canonical_symbols: Iterable[str]
                                ) -> dict[str, str]:
        """Map canonical symbol ('XLMUSDT') -> exchange-native Name ('XLM_USDT').

        Pivots on BaseAsset+QuoteAsset. When multiple tickers share the same
        base+quote (perpetual + several dated quarterlies on Binance), we pick
        the one with the shortest Name — the perpetual is always 'BTCUSDT'
        while quarterlies have a date suffix like 'BTCUSDT_260925'.
        """
        items = await self.get_tickers(connection_id)
        candidates: dict[str, list[str]] = {}
        for it in items:
            if not isinstance(it, dict):
                continue
            base = str(it.get("BaseAsset") or it.get("baseAsset") or "").upper()
            quote = str(it.get("QuoteAsset") or it.get("quoteAsset") or "").upper()
            name = str(it.get("Name") or it.get("name") or "")
            tradable = bool(it.get("IsTradingAllowed", True))
            if base and quote and name and tradable:
                candidates.setdefault(f"{base}{quote}", []).append(name)
        out: dict[str, str] = {}
        for s in canonical_symbols:
            names = candidates.get(s.upper())
            if names:
                # Prefer the shortest name -> perpetual ('BTCUSDT') over
                # dated quarterlies ('BTCUSDT_260925').
                out[s.upper()] = min(names, key=len)
        return out

    # ---- account & trading wrappers -----------------------------------------

    async def get_balance(self, connection_id: int) -> dict[str, Any]:
        r = await self._get(f"/api/connections/{connection_id}/balance")
        return r.json()

    async def get_positions(self, connection_id: int) -> list[dict[str, Any]]:
        r = await self._get(f"/api/connections/{connection_id}/positions")
        data = r.json()
        return data if isinstance(data, list) else data.get("positions") or data.get("Items") or []

    async def get_orders(self, connection_id: int, ticker: str | None = None
                          ) -> list[dict[str, Any]]:
        params = {"Ticker": ticker} if ticker else None
        r = await self._get(f"/api/connections/{connection_id}/orders", params=params)
        data = r.json()
        return data if isinstance(data, list) else data.get("orders") or data.get("Items") or []

    async def place_order(self, connection_id: int, req: OrderRequest) -> dict[str, Any]:
        payload = req.to_payload()
        t0 = time.monotonic()
        try:
            r = await self._post(f"/api/connections/{connection_id}/orders", payload)
        except httpx.HTTPStatusError as e:
            detail = e.response.text[:500] if e.response is not None else ""
            status_code = e.response.status_code if e.response else None
            log.error("metascalp.order_rejected",
                      status_code=status_code,
                      detail=detail,
                      ticker=req.ticker, side=req.side, size=req.size,
                      order_type=req.order_type, price=req.price)
            raise MetaScalpOrderRejected(status_code, detail, payload) from e
        elapsed_ms = (time.monotonic() - t0) * 1000
        body = r.json() if r.content else {}
        log.info("metascalp.order_placed",
                 ticker=req.ticker, side=req.side, size=req.size,
                 order_type=req.order_type, price=req.price,
                 latency_ms=round(elapsed_ms, 2),
                 server_exec_ms=body.get("ExecutionTimeMs"))
        return body

    async def cancel_order(self, connection_id: int, ticker: str, order_id: str,
                            order_type: int | None = None) -> dict[str, Any]:
        body: dict[str, Any] = {"Ticker": ticker, "OrderId": str(order_id)}
        if order_type is not None:
            body["Type"] = int(order_type)
        r = await self._post(f"/api/connections/{connection_id}/orders/cancel", body)
        return r.json() if r.content else {"ok": True}

    async def cancel_all(self, connection_id: int, ticker: str | None = None
                          ) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if ticker:
            body["Ticker"] = ticker
        r = await self._post(f"/api/connections/{connection_id}/orders/cancel-all", body)
        return r.json() if r.content else {"ok": True}

    # ---- WebSocket ----------------------------------------------------------

    def ws_url(self) -> str:
        return f"ws://{self.cfg.host}:{self.port}/"


EventCallback = Callable[[dict[str, Any]], Awaitable[None]]


@dataclass
class OrderbookSubscription:
    connection_id: int
    ticker: str
    depth_levels: int = 50
    fetch_snapshot: bool = True
    zoom_index: int = 0
    # >0 enables MetaScalp's server-side band-pass filter around best bid/ask.
    # Applies to both snapshot and updates. Cuts payload massively.
    depth_percent: float = 0.0


class MetaScalpWS:
    """Resilient MetaScalp WebSocket subscriber.

    Usage:
        ws = MetaScalpWS(client, on_event=handler)
        ws.add_connection_subscribe(conn_id)             # account events
        ws.add_orderbook(conn_id, ticker="BTCUSDT")
        ws.add_trade(conn_id, ticker="BTCUSDT")          # optional
        await ws.run()
    """

    def __init__(self, client: MetaScalpClient, on_event: EventCallback) -> None:
        self._client = client
        self._on_event = on_event
        self._conn_subs: set[int] = set()
        self._orderbook_subs: list[OrderbookSubscription] = []
        self._trade_subs: list[tuple[int, str]] = []
        self._mark_subs: list[tuple[int, str]] = []
        self._stop = asyncio.Event()
        self._connected = asyncio.Event()

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    # ---- subscription registry ---------------------------------------------

    def add_connection_subscribe(self, connection_id: int) -> None:
        self._conn_subs.add(connection_id)

    def add_orderbook(self, connection_id: int, ticker: str, *,
                      depth_levels: int = 50, zoom_index: int = 0,
                      fetch_snapshot: bool = True,
                      depth_percent: float = 0.0) -> None:
        self._orderbook_subs.append(OrderbookSubscription(
            connection_id=connection_id, ticker=ticker,
            depth_levels=depth_levels, fetch_snapshot=fetch_snapshot,
            zoom_index=zoom_index, depth_percent=depth_percent,
        ))

    def add_trade(self, connection_id: int, ticker: str, zoom_index: int = 1) -> None:
        self._trade_subs.append((connection_id, ticker))

    def add_mark_price(self, connection_id: int, ticker: str) -> None:
        self._mark_subs.append((connection_id, ticker))

    def stop(self) -> None:
        self._stop.set()

    # ---- main loop ----------------------------------------------------------

    async def run(self) -> None:
        delay = self._client.cfg.ws_reconnect_delay_sec
        while not self._stop.is_set():
            try:
                await self._run_once()
                delay = self._client.cfg.ws_reconnect_delay_sec
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                self._connected.clear()
                log.warning("metascalp.ws_error", err=str(e),
                            reconnect_in_sec=round(delay, 1))
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                delay = min(delay * 2, self._client.cfg.ws_max_reconnect_delay_sec)

    async def _run_once(self) -> None:
        url = self._client.ws_url()
        async with websockets.connect(url, ping_interval=20, ping_timeout=10,
                                      max_size=2**21) as ws:
            self._connected.set()
            log.info("metascalp.ws_connected", url=url)
            await self._send_all_subscriptions(ws)
            async for raw in ws:
                if self._stop.is_set():
                    return
                try:
                    msg = _WS_DECODER.decode(raw)
                except Exception:  # noqa: BLE001
                    continue
                try:
                    await self._on_event(msg)
                except Exception as e:  # noqa: BLE001
                    log.exception("metascalp.ws_handler_error", err=str(e))
        self._connected.clear()

    async def _send_all_subscriptions(self, ws: Any) -> None:
        for cid in self._conn_subs:
            await ws.send(json.dumps({"Type": "subscribe",
                                       "Data": {"ConnectionId": cid}}))
        for s in self._orderbook_subs:
            payload: dict[str, Any] = {
                "ConnectionId": s.connection_id,
                "Ticker": s.ticker,
                "ZoomIndex": s.zoom_index,
                "DepthLevels": s.depth_levels,
                "FetchSnapshot": s.fetch_snapshot,
            }
            if s.depth_percent and s.depth_percent > 0:
                payload["DepthPercent"] = s.depth_percent
            await ws.send(json.dumps({"Type": "orderbook_subscribe",
                                       "Data": payload}))
        for cid, ticker in self._trade_subs:
            await ws.send(json.dumps({"Type": "trade_subscribe", "Data": {
                "ConnectionId": cid, "Ticker": ticker, "ZoomIndex": 1,
            }}))
        for cid, ticker in self._mark_subs:
            await ws.send(json.dumps({"Type": "mark_price_subscribe", "Data": {
                "ConnectionId": cid, "Ticker": ticker,
            }}))


@contextlib.asynccontextmanager
async def open_metascalp(cfg: MetaScalpConfig) -> AsyncIterator[MetaScalpClient]:
    client = MetaScalpClient(cfg)
    await client.connect()
    try:
        yield client
    finally:
        await client.close()
