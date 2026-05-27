"""Configuration loader with pydantic validation.

NOTE — secrets:
  * MetaScalp local API has NO authentication, so the bot stores no tokens.
  * Binance API key/secret and the MEXC U_ID live inside the MetaScalp app,
    not in this config. The bot only references connections by id/exchange.
"""
from __future__ import annotations

import json
from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class TradingMode(str, Enum):
    DRY_RUN = "DRY_RUN"
    PAPER = "PAPER"
    SMALL_LIVE = "SMALL_LIVE"
    LIVE = "LIVE"


class ConnectionHint(BaseModel):
    """How to find a connection inside MetaScalp.

    If `connection_id` is provided, it wins. Otherwise we look up by
    `exchange` (case-insensitive substring against Exchange/ExchangeId)
    filtered by `market_types` (see metascalp_client.MarketType — for
    USDT perpetuals on Binance and MEXC the canonical value is 5).
    """
    connection_id: Optional[int] = None
    exchange: str = ""
    # 5=UsdtPerpetual, 2=UsdtFutures, 1=Futures (generic) — match all by default.
    market_types: list[int] = Field(default_factory=lambda: [5, 2, 1])
    require_can_trade: bool = False                  # set True for the venue we trade on


class MetaScalpConfig(BaseModel):
    host: str = "127.0.0.1"
    port_scan_start: int = 17845
    port_scan_end: int = 17855
    port_override: Optional[int] = None
    request_timeout_sec: float = 3.0
    ws_reconnect_delay_sec: float = 1.0
    ws_max_reconnect_delay_sec: float = 30.0
    binance_connection: ConnectionHint = Field(
        default_factory=lambda: ConnectionHint(exchange="Binance", require_can_trade=False)
    )
    mexc_connection: ConnectionHint = Field(
        default_factory=lambda: ConnectionHint(exchange="MEXC", require_can_trade=True)
    )

    @field_validator("host")
    @classmethod
    def host_must_be_local(cls, v: str) -> str:
        if v not in {"127.0.0.1", "localhost", "::1"}:
            raise ValueError("MetaScalp host must be local.")
        return v


class MarketDataConfig(BaseModel):
    """Settings that apply to both Binance and MEXC orderbook subscriptions."""
    orderbook_depth_levels: int = 10
    # MetaScalp server-side band-pass filter: keeps only levels within this
    # percent of best bid/ask, applied to both snapshot and updates. Saves
    # ~70-80% of WS payload for shallow strategies. 0 disables.
    orderbook_depth_percent: float = 0.5
    fetch_snapshot_on_subscribe: bool = True
    # 2s is enough headroom for thin tickers; the strategy applies this per-symbol.
    max_staleness_ms: int = 2000
    mid_history_seconds: float = 5.0


class StrategyConfig(BaseModel):
    impulse_window_ms: int = 500
    min_binance_impulse_bps: float = 6.0
    min_basis_lag_bps: float = 4.0
    max_mexc_spread_bps: float = 4.0
    min_depth_usdt: float = 5000.0
    btc_eth_filter_enabled: bool = True
    btc_eth_filter_threshold_bps: float = 4.0
    cooldown_per_symbol_sec: float = 30.0
    signal_eval_interval_ms: int = 50


class RiskConfig(BaseModel):
    max_position_usdt: float = 50.0
    max_daily_loss_usdt: float = 25.0
    max_trades_per_day: int = 40
    max_consecutive_losses: int = 3
    max_open_positions: int = 2
    stop_loss_percent: float = 1.0
    take_profit_bps: float = 8.0
    breakeven_trigger_bps: float = 4.0
    max_slippage_bps: float = 3.0
    max_spread_bps: float = 5.0
    max_position_time_seconds: int = 60
    basis_collapse_exit_bps: float = 1.0
    emergency_kill_switch: bool = False
    pause_after_consecutive_losses_sec: int = 900
    # MEXC Contract API allows 20 req / 2 sec = 10/sec on the order path.
    # WS-driven fills (no polling) keep us well below this; raised defaults
    # to avoid throttling real entry/close bursts.
    max_mexc_requests_per_sec: int = 15
    max_mexc_requests_per_min: int = 400


class CapitalConfig(BaseModel):
    """Balance-aware position allocation.

    The bot uses margin percentages from the free balance, then multiplies the
    selected margin by configured leverage to get order notional.
    """

    two_trade_min_balance_usdt: float = 31.0
    low_balance_max_positions: int = 1
    high_balance_max_positions: int = 2
    configured_max_positions: int = 2
    low_balance_margin_pct_range: tuple[float, float] = (0.75, 0.80)
    high_balance_top_signal_margin_pct_range: tuple[float, float] = (0.50, 0.60)
    balance_reserve_pct: float = 0.10
    min_balance_reserve_usdt: float = 1.0
    paper_balance_usdt: float = 25.0
    balance_refresh_sec: float = 30.0
    estimated_fee_bps: float = 0.0
    min_net_edge_bps: float = 4.0
    min_expected_profit_usdt: float = 0.02
    max_notional_usdt: Optional[float] = None

    @field_validator("configured_max_positions", "low_balance_max_positions",
                     "high_balance_max_positions")
    @classmethod
    def max_positions_range(cls, v: int) -> int:
        if v < 1 or v > 10:
            raise ValueError("position limits must be in range 1..10")
        return v

    @field_validator("low_balance_margin_pct_range",
                     "high_balance_top_signal_margin_pct_range")
    @classmethod
    def pct_range_valid(cls, v: tuple[float, float]) -> tuple[float, float]:
        lo, hi = v
        if lo <= 0 or hi <= 0 or lo > hi or hi > 1:
            raise ValueError("percentage range must be 0 < low <= high <= 1")
        return v


class LeverageConfig(BaseModel):
    default: float = 5.0
    max_configurable: float = 450.0
    extreme_threshold: float = 50.0
    allow_extreme: bool = False

    @field_validator("default", "max_configurable", "extreme_threshold")
    @classmethod
    def leverage_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("leverage values must be positive")
        return v


class RateLimitsConfig(BaseModel):
    """Internal request budget layered above exchange/MetaScalp limits."""

    upstream_hourly_limit: Optional[int] = 5000
    safety_factor: float = 0.88
    close_reserve_pct: float = 0.10
    min_close_reserve_requests: int = 10
    entry_request_cost: int = 3
    stop_request_cost: int = 1
    emergency_close_request_cost: int = 1

    @field_validator("safety_factor", "close_reserve_pct")
    @classmethod
    def pct_valid(cls, v: float) -> float:
        if v <= 0 or v > 1:
            raise ValueError("rate-limit percentages must be 0 < value <= 1")
        return v


class ExecutionConfig(BaseModel):
    order_type: str = "LIMIT"                 # "LIMIT" or "MARKET"
    limit_offset_bps: float = 0.5
    fill_timeout_ms: int = 1500
    cancel_on_partial_after_ms: int = 2000
    min_notional_usdt: float = 5.0
    attached_stop_if_supported: bool = False
    protective_stop_timeout_ms: int = 1000
    latency_edge_buffer_bps: float = 3.0
    min_entry_side_depth_to_notional: float = 20.0
    min_best_level_to_notional: float = 2.0
    max_entry_send_latency_ms: float = 600.0
    max_entry_total_latency_ms: float = 1200.0
    latency_pause_sec: float = 300.0


class DashboardConfig(BaseModel):
    enabled: bool = True
    host: str = "127.0.0.1"
    port: int = 8765
    exchange_refresh_sec: float = 30.0


class StorageConfig(BaseModel):
    sqlite_path: str = "data/trading_bot.sqlite"
    trades_csv: str = "data/trades/trades.csv"
    signals_csv: str = "data/trades/signals.csv"
    errors_log: str = "data/logs/errors.log"
    latency_log: str = "data/logs/latency.log"
    positions_snapshot: str = "data/trades/positions_snapshot.json"


class LoggingConfig(BaseModel):
    level: str = "INFO"
    json_output: bool = True
    redact_secrets: bool = True


class BotConfig(BaseModel):
    mode: TradingMode = TradingMode.DRY_RUN
    metascalp: MetaScalpConfig = Field(default_factory=MetaScalpConfig)
    market_data: MarketDataConfig = Field(default_factory=MarketDataConfig)
    symbols: list[str] = Field(default_factory=list)
    strategy: StrategyConfig = Field(default_factory=StrategyConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    capital: CapitalConfig = Field(default_factory=CapitalConfig)
    leverage: LeverageConfig = Field(default_factory=LeverageConfig)
    rate_limits: RateLimitsConfig = Field(default_factory=RateLimitsConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    dashboard: DashboardConfig = Field(default_factory=DashboardConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    @field_validator("symbols")
    @classmethod
    def symbols_must_be_upper(cls, v: list[str]) -> list[str]:
        return [s.upper() for s in v]


def load_config(path: str | Path) -> BotConfig:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(
            f"Config not found: {p}. "
            "Copy configs/config.example.json -> configs/config.json"
        )
    with p.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    raw = {k: v for k, v in raw.items() if not k.startswith("_")}
    return BotConfig.model_validate(raw)
