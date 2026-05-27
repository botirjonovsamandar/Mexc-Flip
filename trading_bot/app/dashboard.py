"""Minimal localhost dashboard.

Endpoints:
  GET  /                — single-page status view (HTML)
  GET  /api/status      — JSON snapshot of everything below
  POST /api/stop        — stop the bot process; localhost dashboard exits too
  POST /api/close_all   — close every open position immediately
  POST /api/resume      — clear kill switch (only if no auto-pause)
"""
from __future__ import annotations

import collections
from typing import Any, Awaitable, Callable, Deque

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse


_INDEX_HTML = """<!doctype html>
<html><head><title>Trading bot</title>
<style>
body{font-family:ui-monospace,Consolas,monospace;background:#0e1116;color:#e6edf3;margin:1rem}
h1{font-size:1.1rem;margin:.2rem 0 1rem}
table{border-collapse:collapse;width:100%;margin:.5rem 0 1rem}
th,td{padding:.25rem .5rem;border-bottom:1px solid #30363d;text-align:right}
th:first-child,td:first-child{text-align:left}
.bad{color:#ff7b72} .good{color:#7ee787} .warn{color:#d29922}
.row{display:flex;gap:1rem;flex-wrap:wrap}
.card{background:#161b22;padding:.6rem 1rem;border-radius:6px;min-width:200px}
button{margin-right:.5rem;padding:.4rem .8rem;background:#21262d;color:#e6edf3;border:1px solid #30363d;border-radius:6px;cursor:pointer}
button.danger{background:#5a1f1f;border-color:#7c2626}
</style></head><body>
<h1>flip — MEXC/Binance basis bot</h1>
<div id="head"></div>
<div>
  <button onclick="post('/api/stop')">STOP BOT (kill switch)</button>
  <button class="danger" onclick="post('/api/close_all')">CLOSE ALL</button>
  <button onclick="post('/api/resume')">resume</button>
</div>
<h2>positions</h2><div id="pos"></div>
<h2>exchange positions</h2><div id="expos"></div>
<h2>last orders</h2><div id="ord"></div>
<h2>last signals</h2><div id="sig"></div>
<h2>last trades</h2><div id="trd"></div>
<script>
async function load(){
  let s;
  try {
    const r = await fetch('/api/status'); s = await r.json();
  } catch(e) {
    document.getElementById('head').innerHTML = '<div class="card bad">dashboard stopped</div>';
    return;
  }
  document.getElementById('head').innerHTML = render_head(s);
  document.getElementById('pos').innerHTML = table(s.positions, ['symbol','side','qty','entry_price','mark','unrealized_bps','unrealized_pnl_usdt','age_sec','stop_price','breakeven_armed']);
  document.getElementById('expos').innerHTML = table(s.exchange_positions, ['symbol','native_ticker','side','qty','entry_price','pnl_usdt','allowed','source']);
  document.getElementById('ord').innerHTML = table(s.recent_orders, ['ts_ms','symbol','ticker','side','order_type','size','price','status','detail']);
  document.getElementById('sig').innerHTML = table(s.recent_signals, ['ts_ms','symbol','side','decision','reason','binance_impulse_bps','basis_bps','mexc_spread_bps','depth_usdt']);
  document.getElementById('trd').innerHTML = table(s.recent_trades, ['time','symbol','side','entry_price','exit_price','pnl','reason_for_exit','mode']);
}
function table(rows, cols){
  if(!rows||!rows.length) return '<i>(empty)</i>';
  let h='<table><tr>'+cols.map(c=>'<th>'+c+'</th>').join('')+'</tr>';
  for(const r of rows) h+='<tr>'+cols.map(c=>'<td>'+fmt(r[c])+'</td>').join('')+'</tr>';
  return h+'</table>';
}
function fmt(v){ if(v===null||v===undefined) return '-';
  if(typeof v==='number') return Number.isInteger(v)?v:v.toFixed(4);
  return String(v); }
function render_head(s){
  const cls = b => b ? 'good' : 'bad';
  return `<div class="row">
    <div class="card">mode: <b>${s.mode}</b></div>
    <div class="card">kill switch: <b class="${s.kill_switch?'bad':'good'}">${s.kill_switch}</b></div>
    <div class="card">paused: <b class="${s.paused?'warn':'good'}">${s.paused}</b></div>
    <div class="card">MetaScalp: <b class="${cls(s.metascalp_ok)}">${s.metascalp_ok}</b></div>
    <div class="card">Binance: <b class="${cls(s.binance_ok)}">${s.binance_ok}</b></div>
    <div class="card">MEXC feed: <b class="${cls(s.mexc_ok)}">${s.mexc_ok}</b></div>
    <div class="card">day PnL: <b class="${s.day_pnl>=0?'good':'bad'}">${s.day_pnl.toFixed(4)}</b></div>
    <div class="card">trades today: <b>${s.day_trades}</b></div>
    <div class="card">losses streak: <b class="${s.consecutive_losses>=2?'warn':''}">${s.consecutive_losses}</b></div>
    <div class="card">latency Binance (ms): <b>${s.latency.binance_ms}</b></div>
    <div class="card">latency MEXC (ms): <b>${s.latency.mexc_ms}</b></div>
    <div class="card">MEXC req/s: <b>${s.rate_limit.used_sec}</b> · req/min: <b>${s.rate_limit.used_min}</b> · req/hour: <b>${s.rate_limit.used_hour}</b> · entry left: <b>${s.rate_limit.entry_budget_remaining}</b> · close reserve: <b>${s.rate_limit.close_reserve}</b> · throttled: <b class="${s.rate_limit.rejected_total>0?'warn':''}">${s.rate_limit.rejected_total}</b></div>
  </div>`;
}
async function post(u){ await fetch(u,{method:'POST'}); await load(); }
load(); setInterval(load, 2000);
</script>
</body></html>
"""


StatusProvider = Callable[[], dict[str, Any]]


class DashboardState:
    """Live snapshot pushed in by the orchestrator each tick."""

    def __init__(self, max_recent: int = 50) -> None:
        self.mode: str = "DRY_RUN"
        self.kill_switch: bool = False
        self.paused: bool = False
        self.metascalp_ok: bool = False
        self.binance_ok: bool = False
        self.mexc_ok: bool = False
        self.day_pnl: float = 0.0
        self.day_trades: int = 0
        self.consecutive_losses: int = 0
        self.positions: list[dict[str, Any]] = []
        self.exchange_positions: list[dict[str, Any]] = []
        self.recent_signals: Deque[dict[str, Any]] = collections.deque(maxlen=max_recent)
        self.recent_orders: Deque[dict[str, Any]] = collections.deque(maxlen=max_recent)
        self.recent_trades: Deque[dict[str, Any]] = collections.deque(maxlen=max_recent)
        self.latency: dict[str, Any] = {"binance_ms": "-", "mexc_ms": "-"}
        self.rate_limit: dict[str, Any] = {
            "used_sec": 0,
            "used_min": 0,
            "used_hour": 0,
            "safe_hourly_cap": None,
            "entry_budget_remaining": None,
            "close_reserve": 0,
            "total_hourly_remaining": None,
            "rejected_total": 0,
        }

    def push_signal(self, row: dict[str, Any]) -> None:
        self.recent_signals.appendleft(row)

    def push_order(self, row: dict[str, Any]) -> None:
        self.recent_orders.appendleft(row)

    def push_trade(self, row: dict[str, Any]) -> None:
        self.recent_trades.appendleft(row)

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "kill_switch": self.kill_switch,
            "paused": self.paused,
            "metascalp_ok": self.metascalp_ok,
            "binance_ok": self.binance_ok,
            "mexc_ok": self.mexc_ok,
            "day_pnl": self.day_pnl,
            "day_trades": self.day_trades,
            "consecutive_losses": self.consecutive_losses,
            "positions": self.positions,
            "exchange_positions": self.exchange_positions,
            "recent_signals": list(self.recent_signals),
            "recent_orders": list(self.recent_orders),
            "recent_trades": list(self.recent_trades),
            "latency": self.latency,
            "rate_limit": self.rate_limit,
        }


def build_app(state: DashboardState,
              on_stop: Callable[[], None],
              on_resume: Callable[[], None],
              on_close_all: Callable[[], Awaitable[None]]) -> FastAPI:
    app = FastAPI(title="trading_bot")

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:  # noqa: D401
        return _INDEX_HTML

    @app.get("/api/status")
    async def status() -> JSONResponse:
        return JSONResponse(state.as_dict())

    @app.post("/api/stop")
    async def stop() -> JSONResponse:
        on_stop()
        return JSONResponse({"ok": True})

    @app.post("/api/resume")
    async def resume() -> JSONResponse:
        on_resume()
        return JSONResponse({"ok": True})

    @app.post("/api/close_all")
    async def close_all() -> JSONResponse:
        await on_close_all()
        return JSONResponse({"ok": True})

    return app
