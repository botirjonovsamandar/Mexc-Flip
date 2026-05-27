"""Structured logging + CSV sinks for trades, signals, latency.

Secrets are redacted: any key containing 'token', 'secret', 'api_key', 'u_id',
'cookie', 'authorization' is replaced with '***'. Never log raw config values.
"""
from __future__ import annotations

import csv
import json
import logging
import sys
import threading
from pathlib import Path
from typing import Any, Iterable

import structlog


_SECRET_KEYS = (
    "token", "secret", "api_key", "apikey", "u_id", "uid",
    "cookie", "authorization", "password", "passwd",
)

_TRADES_HEADER = [
    "time", "symbol", "side", "entry_price", "exit_price", "size",
    "pnl", "fee", "slippage", "binance_mid", "mexc_mid", "basis_bps",
    "binance_impulse_bps", "mexc_spread_bps", "depth_usdt",
    "reason_for_entry", "reason_for_exit", "latency_order_send", "latency_fill",
    "mode",
]

_SIGNALS_HEADER = [
    "time", "symbol", "side", "binance_mid", "mexc_mid", "basis_bps",
    "binance_impulse_bps", "mexc_spread_bps", "depth_usdt",
    "decision", "reject_reason", "mode",
]


def _redact(event_dict: dict[str, Any]) -> dict[str, Any]:
    def _walk(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {
                k: ("***" if any(s in k.lower() for s in _SECRET_KEYS) else _walk(v))
                for k, v in obj.items()
            }
        if isinstance(obj, list):
            return [_walk(x) for x in obj]
        return obj
    return _walk(event_dict)


def _redact_processor(_: Any, __: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    return _redact(event_dict)


def setup_logging(level: str = "INFO", json_output: bool = True,
                  redact: bool = True, errors_log_path: str | None = None) -> None:
    """Initialize structlog. Call once on startup."""
    processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]
    if redact:
        processors.append(_redact_processor)

    if json_output:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer(colors=True))

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )

    if errors_log_path:
        # Also mirror ERROR+ to a file via stdlib logging.
        Path(errors_log_path).parent.mkdir(parents=True, exist_ok=True)
        root = logging.getLogger()
        if not any(isinstance(h, logging.FileHandler)
                   and getattr(h, "baseFilename", "") == str(Path(errors_log_path).resolve())
                   for h in root.handlers):
            fh = logging.FileHandler(errors_log_path, encoding="utf-8")
            fh.setLevel(logging.ERROR)
            fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            root.addHandler(fh)
        root.setLevel(logging.INFO)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name) if name else structlog.get_logger()


class CsvSink:
    """Thread-safe append-only CSV writer with a fixed header."""

    def __init__(self, path: str | Path, header: Iterable[str]) -> None:
        self.path = Path(path)
        self.header = list(header)
        self._lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists() or self.path.stat().st_size == 0:
            with self.path.open("w", encoding="utf-8", newline="") as f:
                csv.writer(f).writerow(self.header)

    def write(self, row: dict[str, Any]) -> None:
        with self._lock, self.path.open("a", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow([row.get(k, "") for k in self.header])


class TradesLogger(CsvSink):
    def __init__(self, path: str | Path) -> None:
        super().__init__(path, _TRADES_HEADER)


class SignalsLogger(CsvSink):
    def __init__(self, path: str | Path) -> None:
        super().__init__(path, _SIGNALS_HEADER)


class LatencyLogger:
    """One JSON object per line — easier to grep + parse than CSV here."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def write(self, payload: dict[str, Any]) -> None:
        with self._lock, self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, separators=(",", ":")) + "\n")
