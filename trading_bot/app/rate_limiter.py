"""Sliding-window rate limiter for outbound MEXC/MetaScalp requests.

MEXC Contract API allows 20 requests / 2 seconds = 10/sec on standard
endpoints. We connect via MetaScalp (which forwards to MEXC over a U_ID
session), so the ceiling is at most that. We stay well below — default 6/sec,
180/min — to leave room for MetaScalp's own bookkeeping calls.

Three windows are checked: per-second, per-minute, and optional per-hour.
Entry requests must leave a configured hourly reserve for protective stop,
cancel, close, and reconcile calls.
"""
from __future__ import annotations

import collections
import threading
import time
from dataclasses import dataclass
from typing import Literal


RequestPurpose = Literal["entry", "protective", "close", "reconcile"]


@dataclass
class RateLimiterStats:
    used_last_sec: int
    used_last_min: int
    used_last_hour: int = 0
    safe_hourly_cap: int | None = None
    entry_budget_cap: int | None = None
    entry_budget_remaining: int | None = None
    close_reserve: int = 0
    total_hourly_remaining: int | None = None
    rejected_total: int = 0


class RateLimiter:
    """Sliding-window counter, thread-safe, sync-friendly (no asyncio needed)."""

    def __init__(self, *, max_per_sec: int = 6, max_per_min: int = 180,
                 upstream_hourly_limit: int | None = None,
                 safety_factor: float = 0.88,
                 close_reserve_pct: float = 0.10,
                 min_close_reserve_requests: int = 10) -> None:
        self.max_per_sec = max_per_sec
        self.max_per_min = max_per_min
        self.upstream_hourly_limit = upstream_hourly_limit
        self.safety_factor = safety_factor
        self.close_reserve_pct = close_reserve_pct
        self.min_close_reserve_requests = min_close_reserve_requests
        self._events: collections.deque[float] = collections.deque()
        self._rejected = 0
        self._lock = threading.Lock()

    def _prune(self, now: float) -> None:
        cutoff = now - 3600.0
        while self._events and self._events[0] < cutoff:
            self._events.popleft()

    @property
    def safe_hourly_cap(self) -> int | None:
        if self.upstream_hourly_limit is None:
            return None
        return max(0, int(self.upstream_hourly_limit * self.safety_factor))

    @property
    def close_reserve(self) -> int:
        safe_cap = self.safe_hourly_cap
        if safe_cap is None:
            return 0
        pct_reserve = int(safe_cap * self.close_reserve_pct)
        return min(safe_cap, max(self.min_close_reserve_requests, pct_reserve))

    @property
    def entry_budget_cap(self) -> int | None:
        safe_cap = self.safe_hourly_cap
        if safe_cap is None:
            return None
        return max(0, safe_cap - self.close_reserve)

    def _used(self, now: float) -> tuple[int, int, int]:
        sec_cutoff = now - 1.0
        min_cutoff = now - 60.0
        used_sec = sum(1 for t in self._events if t >= sec_cutoff)
        used_min = sum(1 for t in self._events if t >= min_cutoff)
        used_hour = len(self._events)
        return used_sec, used_min, used_hour

    def can_start_entry(self, *, entry_cost: int = 1,
                        protective_cost: int = 0) -> bool:
        """Return whether a new entry can start without spending close reserve."""
        now = time.monotonic()
        with self._lock:
            self._prune(now)
            _, _, used_hour = self._used(now)
            entry_cap = self.entry_budget_cap
            safe_cap = self.safe_hourly_cap
            if entry_cap is None or safe_cap is None:
                return True
            if used_hour + entry_cost > entry_cap:
                return False
            return used_hour + entry_cost + protective_cost <= safe_cap

    def acquire(self, purpose: RequestPurpose = "entry", *, cost: int = 1) -> bool:
        """Try to consume one token. Returns False if either window is full."""
        now = time.monotonic()
        with self._lock:
            self._prune(now)
            used_sec, used_min, used_hour = self._used(now)
            safe_cap = self.safe_hourly_cap
            entry_cap = self.entry_budget_cap
            if (
                used_sec + cost > self.max_per_sec
                or used_min + cost > self.max_per_min
            ):
                self._rejected += 1
                return False
            if safe_cap is not None and used_hour + cost > safe_cap:
                self._rejected += 1
                return False
            if (purpose == "entry" and entry_cap is not None
                    and used_hour + cost > entry_cap):
                self._rejected += 1
                return False
            for _ in range(cost):
                self._events.append(now)
            return True

    def stats(self) -> RateLimiterStats:
        now = time.monotonic()
        with self._lock:
            self._prune(now)
            used_sec, used_min, used_hour = self._used(now)
            safe_cap = self.safe_hourly_cap
            entry_cap = self.entry_budget_cap
            entry_remaining = None if entry_cap is None else max(0, entry_cap - used_hour)
            total_remaining = None if safe_cap is None else max(0, safe_cap - used_hour)
            return RateLimiterStats(
                used_last_sec=used_sec,
                used_last_min=used_min,
                used_last_hour=used_hour,
                safe_hourly_cap=safe_cap,
                entry_budget_cap=entry_cap,
                entry_budget_remaining=entry_remaining,
                close_reserve=self.close_reserve,
                total_hourly_remaining=total_remaining,
                rejected_total=self._rejected,
            )
