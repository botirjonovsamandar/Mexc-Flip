# flip — MEXC Futures basis bot

Auto-trades the Binance → MEXC micro-basis on MEXC Futures.
**Every exchange goes through a locally running MetaScalp**; the bot itself
never touches Binance/MEXC directly, never opens a browser, never reads
cookies. You configure both exchanges inside MetaScalp once (Binance via
API key+secret, MEXC via U_ID) and the bot talks to MetaScalp on
`127.0.0.1`.

API reference used: <https://metascalp.github.io/metascalp-sdk/>

## What it does

* Subscribes via the MetaScalp WebSocket to **two** connections:
  * Binance USDT-Perpetual (signal source) — orderbook for every traded
    symbol + BTCUSDT / ETHUSDT for the macro filter.
  * MEXC USDT-Perpetual (execution venue) — orderbook + account-level
    events (`order_update`, `position_update`, `balance_update`).
* Each evaluation tick computes:
  * `binance_mid`, `mexc_mid`, `basis_bps = (mexc - binance) / binance * 1e4`
  * `binance_impulse_bps` over a rolling 500 ms window
  * `mexc_spread_bps`, `depth_usdt` (top 5 levels, worst side)
* When Binance impulses ≥ +6 bps and MEXC lags by ≥ 4 bps with a tight
  spread and enough depth — and BTC/ETH isn't moving against it —
  ranks all valid signals by estimated net edge/profit and only sends the best
  one or two entries allowed by balance and slot limits.
* Entries are balance-aware: below 31 USDT only one slot is allowed; at 31 USDT
  and above the best signal gets the larger configured margin slice and the
  second signal gets the remaining allocatable balance only if it is still
  profitable.
* Exits on take-profit (bps), stop-loss (%), basis collapse, breakeven trail,
  or time stop. Live fills must receive a protective StopLoss order; if that
  fails, the bot immediately closes the position and blocks new entries until
  it reconciles flat.

## Architecture

```
                  ┌─────────────────────────────┐
                  │      MetaScalp (local)      │
                  │  ┌───────────┐ ┌──────────┐ │
   Binance ───────┼──┤  Binance  │ │   MEXC   ├─┼──── MEXC
   (API key)      │  │ connection│ │connection│ │     (U_ID)
                  │  └─────┬─────┘ └────┬─────┘ │
                  │        │            │       │
                  │   /api/connections, /api/.../orders, WS
                  └────────┼────────────┼───────┘
                           │            │
                  ┌────────▼────────────▼───────┐
                  │      trading_bot (Python)   │
                  │  binance_cache  mexc_cache  │
                  │       Strategy → Execution  │
                  │       Risk     ↔ Positions  │
                  │           Dashboard         │
                  └─────────────────────────────┘
```

## Trading modes

Run them in this order — never start at LIVE.

1. `DRY_RUN`   — only signals, no orders. MetaScalp may be off; the bot
   complains in logs but the dashboard still runs.
2. `PAPER`     — virtual fills at top of the MEXC book; PnL is tracked locally.
3. `SMALL_LIVE` — real orders via MetaScalp using balance-aware allocation,
   while still enforcing `execution.min_notional_usdt`.
4. `LIVE`      — same execution path with live-size config; use only after
   DRY_RUN, PAPER, and SMALL_LIVE are clean.

Set the mode in `configs/config.json` (copy from `config.example.json`).
In current builds, both `SMALL_LIVE` and `LIVE` use the balance-aware
allocation rules from the `capital` and `leverage` config sections; they do
not blindly spend `max_position_usdt`.

## Manual setup — what you have to do by hand

### 1. Install and launch MetaScalp
Run the MetaScalp app. It binds the local HTTP+WS server to the first free
port in `17845..17855`. Verify in PowerShell:
```powershell
curl http://127.0.0.1:17845/ping
```
If port 17845 is busy, walk up: 17846, 17847, …

### 2. Add the Binance connection (with API keys)
Inside MetaScalp UI: **Add connection → Binance → USDT-M Futures**.

* Paste your Binance **API key + API secret** (these stay inside MetaScalp;
  the bot never sees them).
* On Binance side, the key needs permissions:
  * `Enable Reading` — required.
  * `Enable Futures` — required only if you plan to trade Binance from
    the bot. We don't here, but enable it if you might later.
  * `Enable Withdrawals` — **NEVER** enable.
  * Whitelist your IP under `Restrict access to trusted IPs only`.
* Wait until `State` becomes Connected. Confirm with:
  ```powershell
  curl http://127.0.0.1:17845/api/connections
  ```
  Look for the entry where `Exchange: "Binance"`, `MarketType: 5`,
  `State: 2`. Note its `Id`.

### 3. Add the MEXC connection (via U_ID)
Inside MetaScalp UI: **Add connection → MEXC → USDT-Perpetual** (or
whatever label MetaScalp uses for MEXC perpetuals). Connect via U_ID per
MetaScalp's MEXC instructions.

* After it connects, in the response of `GET /api/connections` you should
  see `Exchange: "MEXC"`, `MarketType: 5`, `State: 2`,
  **and `ViewMode: false`**. `ViewMode: true` would make the connection
  read-only — bot cannot place orders. If MetaScalp puts the U_ID connection
  in ViewMode for some reason, switch it off in the UI.
* Note this `Id` too.

### 4. (Optional but recommended) Pin the connection ids in the config
Open `configs/config.json` and fill in the `connection_id` fields:
```json
"binance_connection": { "connection_id": 12, "exchange": "Binance", ... },
"mexc_connection":    { "connection_id": 47, "exchange": "MEXC",    ... }
```
If you leave them `null`, the bot picks the first connection that matches
`exchange` + `market_types`. Pinning is safer if you have multiple MEXC
or Binance connections (e.g. spot + futures).

### 5. Verify the tickers exist on both sides
```powershell
curl "http://127.0.0.1:17845/api/connections/<binance_id>/tickers"
curl "http://127.0.0.1:17845/api/connections/<mexc_id>/tickers"
```
Confirm every symbol you listed in `symbols` is present in BOTH lists,
spelled exactly as MetaScalp returns it (e.g. `XLMUSDT`, not `XLM/USDT`).
If MEXC uses a different spelling, rename the entries in `symbols`.

### 6. Install the bot
```powershell
cd "c:\Users\User\Desktop\Soft\Mexc flip\trading_bot"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy configs\config.example.json configs\config.json
```

Open `configs\config.json`. Check `mode: "DRY_RUN"` (do not start higher).

### 7. Run the bot in DRY_RUN
```powershell
python -m app.main --config configs\config.json
```
Open the dashboard at <http://127.0.0.1:8765/>. You should see:
* `MetaScalp: true`, `Binance: true`, `MEXC feed: true` after a few seconds.
* Signals flowing in the "last signals" table — **most should be REJECT**.
  That is the goal of DRY_RUN: confirm the filters reject bad trades.
* `signals.csv` populated under `data/trades/`.

### 8. Move to PAPER, then SMALL_LIVE, then LIVE
After at least a few hours of DRY_RUN looks healthy, switch `mode` to
`PAPER`, restart. After PAPER, switch to `SMALL_LIVE`, then `LIVE`.

## Balance, leverage and entry slots

The main live-risk knobs are in `capital`, `leverage`, and `rate_limits`.

* `capital.two_trade_min_balance_usdt = 31.0`: below this, the bot can open
  only one position.
* `capital.low_balance_margin_pct_range = [0.75, 0.80]`: one low-balance trade
  uses 75-80% of available balance, capped by the reserve.
* `capital.high_balance_top_signal_margin_pct_range = [0.50, 0.60]`: when two
  trades are allowed, the strongest signal gets this share of allocatable
  balance and the second gets the remainder.
* `capital.configured_max_positions` is validated in the `1..10` range, but the
  default is 2 and the balance tier can reduce it to 1.
* `leverage.default = 5.0`: used for sizing. The bot does not change leverage
  directly on MEXC in this version; set/check it in MetaScalp.

## Dashboard

* **STOP BOT** — sets the kill switch; no new entries.
* **CLOSE ALL** — closes every open position immediately (cancels MEXC orders too).
* **resume** — clears the kill switch.

## Tests

```powershell
python -m pytest -q
```
Covers: full strategy decision matrix (LONG / SHORT / each reject branch),
every risk-manager circuit breaker, DRY_RUN + PAPER execution paths,
balance-aware entry planning, hourly request reserves, protective-stop failure
handling, and the exact PascalCase order payload shape MetaScalp expects.

## MEXC rate limits


Current MEXC Futures docs publish per-endpoint limits such as create order
`4 requests / 2 seconds`, TP/SL `5 requests / 2 seconds`, and close-all
`4 requests / 2 seconds`. MetaScalp/U_ID can have a different practical
ceiling, so the bot adds its own configurable hourly budget on top.

Defence in depth in the code:

| Layer | Mechanism | Where |
|---|---|---|
| Static cap | `risk.max_trades_per_day = 40` | [risk_manager.py](app/risk_manager.py) |
| Short windows | `max_mexc_requests_per_sec = 6`, `max_mexc_requests_per_min = 180` | [rate_limiter.py](app/rate_limiter.py) |
| Hourly safe cap | `floor(rate_limits.upstream_hourly_limit * safety_factor)` | [rate_limiter.py](app/rate_limiter.py) |
| Close reserve | Entries stop before spending the close/stop/cancel reserve | [execution.py](app/execution.py) |

Dashboard shows live `req/s`, `req/min`, `req/hour`, entry budget remaining,
close reserve, and throttled calls. If `throttled` starts going up, the bot
will stop new entries before it spends the reserved close budget.

## Per-symbol freshness (vs. global staleness)

Health flags (`Binance: true`, `MEXC feed: true`) now mean only "WS
transport alive and at least one symbol fresh". Per-symbol staleness is
checked inside [strategy.py](app/strategy.py) — if MEXC hasn't ticked
XLMUSDT in the last `market_data.max_staleness_ms` (default 2000 ms),
only XLMUSDT is rejected with `stale:mexc`; other symbols carry on.
This avoids the previous behaviour where one thin ticker took the whole
feed down.

## Safety promises

* No browser automation, no cookie scraping, no antibot/antidetect bypass.
* MetaScalp's local API is unauthenticated — the bot ALSO stores no
  Binance/MEXC secrets. They live only inside MetaScalp.
* Secrets that *could* leak through nested data structures (e.g. raw
  connection payloads) are auto-redacted by the logger.
* `emergency_kill_switch` in the config (and the dashboard button) blocks
  every new entry immediately.
* Risk caps (`max_daily_loss_usdt`, `max_consecutive_losses`,
  `max_trades_per_day`) are checked on every tick; tripping any of them
  pauses the bot until UTC midnight or a manual resume.

## Troubleshooting

| Symptom on dashboard            | What to check |
| ------------------------------- | ------------- |
| `MetaScalp: false`              | MetaScalp app not running, or port outside 17845–17855. |
| `Binance: false` (MetaScalp ok) | Binance connection in MetaScalp shows `State != 2`, or no ticker subscription confirmed. |
| `MEXC feed: false`              | MEXC connection in MetaScalp not yet `State=2`; or `Ticker` mismatch (rename in `symbols`). |
| `BLOCKED` signals only          | Risk manager paused. Check `consecutive_losses`, `day_pnl`, or `kill_switch`. |
| Orders never fill in SMALL_LIVE | `ViewMode: true` on MEXC connection; or `Ticker` rejected by MetaScalp (verify with `GET /api/connections/{id}/tickers`). |
