"""Rate limiter unit tests — second and minute windows, rejection counter."""
from __future__ import annotations

import time

from trading_bot.app.rate_limiter import RateLimiter


def test_under_limit_all_accepted():
    rl = RateLimiter(max_per_sec=5, max_per_min=300)
    for _ in range(5):
        assert rl.acquire() is True
    s = rl.stats()
    assert s.used_last_sec == 5
    assert s.rejected_total == 0


def test_burst_rejected_at_sec_cap():
    rl = RateLimiter(max_per_sec=3, max_per_min=300)
    assert rl.acquire() and rl.acquire() and rl.acquire()
    assert rl.acquire() is False     # 4th in same second blocked
    assert rl.acquire() is False
    assert rl.stats().rejected_total == 2


def test_window_slides_after_one_second():
    rl = RateLimiter(max_per_sec=2, max_per_min=300)
    assert rl.acquire() and rl.acquire()
    assert rl.acquire() is False
    time.sleep(1.05)
    # Both old events have aged out of the per-sec window.
    assert rl.acquire() is True


def test_minute_cap_independent_of_second():
    rl = RateLimiter(max_per_sec=100, max_per_min=3)
    assert rl.acquire() and rl.acquire() and rl.acquire()
    # Minute window is full even though per-sec is fine.
    assert rl.acquire() is False


def test_hourly_limit_keeps_close_reserve_for_non_entries():
    rl = RateLimiter(
        max_per_sec=1000,
        max_per_min=1000,
        upstream_hourly_limit=500,
        safety_factor=0.88,
        close_reserve_pct=0.10,
        min_close_reserve_requests=10,
    )

    stats = rl.stats()
    assert stats.safe_hourly_cap == 440
    assert stats.close_reserve == 44
    assert stats.entry_budget_cap == 396

    for _ in range(396):
        assert rl.acquire("entry") is True

    assert rl.acquire("entry") is False
    assert rl.acquire("close") is True


def test_entry_start_requires_protective_budget_too():
    rl = RateLimiter(
        max_per_sec=1000,
        max_per_min=1000,
        upstream_hourly_limit=500,
        safety_factor=0.88,
        close_reserve_pct=0.10,
        min_close_reserve_requests=10,
    )
    for _ in range(395):
        assert rl.acquire("entry") is True

    assert rl.can_start_entry(entry_cost=1, protective_cost=1) is True
    assert rl.can_start_entry(entry_cost=3, protective_cost=2) is False
