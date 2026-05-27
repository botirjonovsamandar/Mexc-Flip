"""Structured logging + CSV sinks for trades, signals, latency.

Secrets are redacted: any key containing 'token', 'secret', 'api_key', 'u_id',
'cookie', 'authorization' is replaced with '***'. Never log raw config values.

CSV / JSON sinks dispatch writes to a background asyncio task. The hot path
(signal evaluation, fill recording) does `queue.put_nowait` and returns
immediately — no disk I/O blocks the event loop. A small bounded queue plus
an overflow drop policy keeps memory predictable if the writer falls behind.
"""
from __future__ import annotations

import asyncio
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


_log = get_logger("logger")


class _AsyncWriterMixin:
    """Shared bounded-queue + background-task plumbing for the CSV/JSON sinks.

    Each sink lazily binds to the running event loop on the first `write()`
    call from inside the loop. If we're called from a thread without a loop
    (rare — only direct invocations from non-async code), we fall back to a
    synchronous write under the lock.

    The 4096-slot queue plus drop-newest policy means that under sustained
    write storms the hot path stays non-blocking, at the cost of dropping
    a tail signal row. Trades and latency events are far less frequent so
    they almost never see the overflow path.
    """

    _MAX_QUEUE = 4096

    def __init__(self) -> None:
        self._queue: asyncio.Queue[dict[str, Any]] | None = None
        self._task: asyncio.Task[None] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._sync_lock = threading.Lock()
        self._dropped = 0

    def _ensure_writer(self) -> bool:
        """Bind to the running loop if we're inside one; otherwise return False."""
        if self._queue is not None and self._task is not None and not self._task.done():
            return True
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return False
        # We're inside an event loop — lazy-bind the queue + writer task.
        self._loop = loop
        self._queue = asyncio.Queue(maxsize=self._MAX_QUEUE)
        self._task = loop.create_task(self._writer_loop())
        return True

    async def _writer_loop(self) -> None:
        assert self._queue is not None
        while True:
            row = await self._queue.get()
            try:
                # Disk I/O off the hot path. The lock here is only contended
                # with the sync-fallback path; that path is rare.
                with self._sync_lock:
                    self._sync_write(row)
            except Exception as e:  # noqa: BLE001
                _log.warning("logger.async_write_failed",
                             sink=type(self).__name__, err=str(e))
            finally:
                self._queue.task_done()

    def _sync_write(self, row: dict[str, Any]) -> None:  # pragma: no cover
        raise NotImplementedError

    def write(self, row: dict[str, Any]) -> None:
        if self._ensure_writer():
            assert self._queue is not None
            try:
                self._queue.put_nowait(row)
            except asyncio.QueueFull:
                self._dropped += 1
                if self._dropped % 100 == 1:
                    _log.warning("logger.queue_full_drop",
                                 sink=type(self).__name__,
                                 dropped_total=self._dropped)
            return
        # No running loop (e.g., tests / startup-time write): sync fallback.
        with self._sync_lock:
            self._sync_write(row)


class CsvSink(_AsyncWriterMixin):
    """Append-only CSV writer with a fixed header. Writes happen off-loop."""

    def __init__(self, path: str | Path, header: Iterable[str]) -> None:
        super().__init__()
        self.path = Path(path)
        self.header = list(header)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists() or self.path.stat().st_size == 0:
            with self.path.open("w", encoding="utf-8", newline="") as f:
                csv.writer(f).writerow(self.header)

    def _sync_write(self, row: dict[str, Any]) -> None:
        with self.path.open("a", encoding="utf-8", newline="") as f:
            csv.writer(f).writerow([row.get(k, "") for k in self.header])


class TradesLogger(CsvSink):
    def __init__(self, path: str | Path) -> None:
        super().__init__(path, _TRADES_HEADER)


class SignalsLogger(CsvSink):
    def __init__(self, path: str | Path) -> None:
        super().__init__(path, _SIGNALS_HEADER)


class LatencyLogger(_AsyncWriterMixin):
    """One JSON object per line — easier to grep + parse than CSV here."""

    def __init__(self, path: str | Path) -> None:
        super().__init__()
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _sync_write(self, payload: dict[str, Any]) -> None:
        with self.path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, separators=(",", ":")) + "\n")
