"""
tests/test_fv_engine_bugs.py
────────────────────────────
Regression tests for the two bugs found from the first live run.

Bug 1 — Static strike K caused P(UP)=1.0 all window:
    If K is set below the running BTC price, P(UP) locks at 1.0 even as BTC
    is actually falling toward a DOWN resolution.  K must be snapped at the
    window boundary, not hardcoded.

Bug 2 — σ=0 on flat markets collapses BS to a binary comparator:
    When BTC barely moves, all log returns ≈ 0, stdev = 0, and BS returns
    exactly 1.0 or 0.0 — no probability signal.  A minimum σ floor must keep
    probabilities meaningful.

Run:
    python tests/test_fv_engine_bugs.py
    python -m pytest tests/test_fv_engine_bugs.py -v
"""

from __future__ import annotations

import math
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.math_utils import annualize_vol, black_scholes_prob

# Match the engine's floor
MIN_SIGMA_FLOOR = 0.20


# ═══════════════════════════════════════════════════════════════════════════════
# Bug 1: Static K causes P(UP) to lock at 1.0
# ═══════════════════════════════════════════════════════════════════════════════

def test_bug1_static_k_below_price_locks_prob_at_1():
    """
    The original bug: K=76000 hardcoded, BTC running at 76025–76070.
    S > K always → P(UP) = 1.0 even as BTC falls.
    This test confirms that behaviour and documents why it is wrong.
    """
    K_wrong    = 76_000.0   # old hardcoded value
    sigma      = 1e-12     # σ≈0 as in production (flat market + wrong K together)
    T_4min     = 4 * 60 / (365.25 * 24 * 3600)

    # BTC above K the whole time — P(UP) stuck at 1.0
    for btc_price in [76_025.0, 76_045.0, 76_070.0, 76_068.0, 76_042.0]:
        p = black_scholes_prob(btc_price, K_wrong, T_4min, sigma)
        assert p > 0.98, (
            f"With K=76000 < BTC={btc_price}, P(UP)={p:.4f} — was stuck at 1.0 in prod"
        )

def test_bug1_correct_k_gives_meaningful_probability():
    """
    With K = BTC price at window open (76025), a price of 76042 with 4min
    left should give a meaningful, non-binary probability — not 1.0.
    """
    K_correct  = 76_025.0   # BTC at window open — the real benchmark
    btc_now    = 76_042.0   # slightly above K — moderate edge for UP
    sigma      = 0.25
    T_4min     = 4 * 60 / (365.25 * 24 * 3600)

    p = black_scholes_prob(btc_now, K_correct, T_4min, sigma)
    assert 0.50 < p < 0.95, (
        f"With correct K, P(UP)={p:.4f} should be between 0.50–0.95, not locked at 1.0"
    )

def test_bug1_btc_below_correct_k_gives_down_edge():
    """
    If BTC is below K (window-open price), P(UP) < 0.5 — DOWN has edge.
    This is exactly the scenario that played out in the first live run:
    BTC fell below its window-open price, but the engine reported P(UP)=1.0.
    """
    K_correct  = 76_025.0
    btc_falling = 76_000.0   # BTC fell below K
    sigma       = 0.25
    T_2min      = 2 * 60 / (365.25 * 24 * 3600)

    p = black_scholes_prob(btc_falling, K_correct, T_2min, sigma)
    assert p < 0.50, (
        f"BTC={btc_falling} < K={K_correct} → P(UP)={p:.4f} should be < 0.5"
    )

def test_bug1_strike_snap_detects_boundary():
    """
    The boundary detection logic must fire exactly when the 300s window rolls.
    Simulate two consecutive timestamps: one before and one after a boundary.
    """
    WINDOW = 300
    # Pick a known boundary
    base = 1_700_000_000   # arbitrary epoch
    boundary = base - (base % WINDOW)

    before_boundary = boundary - 1
    at_boundary     = boundary
    after_boundary  = boundary + 1

    def get_window(ts):
        return int(ts) - (int(ts) % WINDOW)

    assert get_window(before_boundary) != get_window(at_boundary + 1), \
        "Boundary crossing must produce different window IDs"
    assert get_window(at_boundary)     == get_window(after_boundary), \
        "Same-window timestamps must map to the same boundary"


# ═══════════════════════════════════════════════════════════════════════════════
# Bug 2: σ = 0 on flat markets — BS becomes a comparator
# ═══════════════════════════════════════════════════════════════════════════════

def test_bug2_flat_prices_give_zero_sigma():
    """
    Confirm that flat/barely-moving prices produce σ ≈ 0 — the root cause
    of the output being P(UP)=1.0000 / P(DN)=0.0000 with no gradient.
    """
    flat_prices  = [76_025.45] * 30   # identical prices — all log returns = 0
    sigma = annualize_vol(flat_prices, interval_s=0.15)
    assert sigma == 0.0, f"Flat prices must give σ=0, got {sigma}"

def test_bug2_sigma_floor_prevents_binary_output():
    """
    With the σ floor applied, even a flat market should produce
    a non-binary probability (not exactly 0 or 1).
    """
    K      = 76_025.0
    S_atm  = 76_025.0   # exactly at-the-money — worst case for binary collapse
    T_3min = 3 * 60 / (365.25 * 24 * 3600)

    # Without floor: σ=0 → binary
    p_no_floor = black_scholes_prob(S_atm, K, T_3min, sigma=1e-12)
    # ATM with σ→0 should be very close to 0.5 but the floor prevents collapse
    # Actually with σ→0, the formula uses binary settlement
    # Let's just check that applying the floor works correctly
    
    p_with_floor = black_scholes_prob(S_atm, K, T_3min, sigma=MIN_SIGMA_FLOOR)
    assert 0.40 < p_with_floor < 0.60, (
        f"ATM with σ floor should give ~0.5, got {p_with_floor:.4f}"
    )

def test_bug2_sigma_floor_keeps_probability_meaningful():
    """
    Even during a completely flat BTC window, the floored σ should produce
    probabilities that reflect real uncertainty — not binary 0/1 values.
    """
    K = 76_025.0
    T = 4 * 60 / (365.25 * 24 * 3600)

    cases = [
        (76_025.0, "ATM",                 0.40, 0.60),
        (76_050.0, "25pts above K",       0.50, 0.80),
        (76_000.0, "25pts below K",       0.20, 0.50),
        (76_100.0, "75pts above K",       0.60, 0.99),
        (75_950.0, "75pts below K",       0.01, 0.40),
    ]
    for S, label, lo, hi in cases:
        raw_sigma = 0.0   # flat market
        sigma     = max(raw_sigma, MIN_SIGMA_FLOOR)
        p         = black_scholes_prob(S, K, T, sigma)
        assert lo < p < hi, (
            f"[{label}] S={S}, K={K}: P(UP)={p:.4f} not in ({lo}, {hi})"
        )

def test_bug2_measured_vs_assumed_tick_interval():
    """
    Binance bookTicker fires ~5–15 ticks/second, not 1/second.
    Using interval_s=1.0 when actual is 0.15s underestimates σ by sqrt(1/0.15) ≈ 2.6×.
    Verify that using the correct interval gives a higher (more accurate) σ.
    """
    import random
    random.seed(42)
    sigma_per_tick_true = 0.60 / math.sqrt(365.25 * 24 * 3600 / 0.15)
    price = 100_000.0
    prices = [price]
    for _ in range(299):
        price *= math.exp(random.gauss(0, sigma_per_tick_true))
        prices.append(price)

    sigma_wrong   = annualize_vol(prices, interval_s=1.00)   # assumes 1s ticks
    sigma_correct = annualize_vol(prices, interval_s=0.15)   # actual ~150ms ticks

    assert sigma_correct > sigma_wrong * 1.5, (
        f"Correct interval should give σ={sigma_correct:.3f} >> "
        f"wrong interval σ={sigma_wrong:.3f}"
    )

def test_bug2_floor_does_not_suppress_real_vol():
    """
    When BTC is actually volatile, measured σ should exceed the floor
    and the floor should have no effect.
    """
    import random
    random.seed(99)
    # Simulate 80% annualized vol with 0.15s ticks
    sigma_per_tick = 0.80 / math.sqrt(365.25 * 24 * 3600 / 0.15)
    price = 100_000.0
    prices = [price]
    for _ in range(299):
        price *= math.exp(random.gauss(0, sigma_per_tick))
        prices.append(price)

    raw_sigma     = annualize_vol(prices, interval_s=0.15)
    floored_sigma = max(raw_sigma, MIN_SIGMA_FLOOR)

    assert raw_sigma > MIN_SIGMA_FLOOR, (
        f"High-vol market raw σ={raw_sigma:.3f} should exceed floor={MIN_SIGMA_FLOOR}"
    )
    assert floored_sigma == raw_sigma, "Floor must not suppress genuinely high vol"


# ═══════════════════════════════════════════════════════════════════════════════
# Combined: reproduce the actual first-run scenario
# ═══════════════════════════════════════════════════════════════════════════════

def test_first_run_scenario_fixed():
    """
    Reproduce the exact conditions of the first live run and confirm the
    fixes produce the correct output.

    Scenario:
        - BTC at window open: ~76025 (this is now K)
        - BTC during window: 76025 → 76070 (slightly above K, barely moving)
        - BTC at expiry: 75968 (below K → DOWN wins)
        - σ measured: ≈ 0 (flat market)

    Old behaviour: P(UP)=1.0000 all window (K=76000 < S always, σ=0)
    New behaviour: P(UP) reflects actual uncertainty with σ floor applied
    """
    K     = 76_025.0   # snapped at window open (was hardcoded 76000 before)
    sigma = MIN_SIGMA_FLOOR   # floor applied (was ~0.0 before)

    # Mid-window: BTC at 76042, 2min left
    T_2min = 2 * 60 / (365.25 * 24 * 3600)
    p_mid  = black_scholes_prob(76_042.0, K, T_2min, sigma)
    assert 0.50 < p_mid < 0.80, (
        f"Mid-window P(UP)={p_mid:.4f}: should show mild UP edge, not 1.0"
    )

    # Late window: BTC at 75980 (falling below K), 30s left
    T_30s  = 30 / (365.25 * 24 * 3600)
    p_late = black_scholes_prob(75_980.0, K, T_30s, sigma)
    assert p_late < 0.45, (
        f"Late-window P(UP)={p_late:.4f}: BTC below K with 30s left should favor DOWN"
    )

    # At expiry: BTC=75968 < K=76025 → DOWN wins, P(UP) should be very low
    T_tiny = 1 / (365.25 * 24 * 3600)
    p_exp  = black_scholes_prob(75_968.0, K, T_tiny, sigma)
    assert p_exp < 0.30, (
        f"At expiry P(UP)={p_exp:.4f}: BTC=75968 < K=76025 → DOWN, should be < 0.30"
    )


# ── Self-running ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_bug1_static_k_below_price_locks_prob_at_1,
        test_bug1_correct_k_gives_meaningful_probability,
        test_bug1_btc_below_correct_k_gives_down_edge,
        test_bug1_strike_snap_detects_boundary,
        test_bug2_flat_prices_give_zero_sigma,
        test_bug2_sigma_floor_prevents_binary_output,
        test_bug2_sigma_floor_keeps_probability_meaningful,
        test_bug2_measured_vs_assumed_tick_interval,
        test_bug2_floor_does_not_suppress_real_vol,
        test_first_run_scenario_fixed,
    ]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {t.__name__}  →  {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)


# ═══════════════════════════════════════════════════════════════════════════════
# Task 1.1: intra_window_prices buffer correctness
# ═══════════════════════════════════════════════════════════════════════════════

def _make_engine_no_zmq():
    """
    Construct a FVEngine with ZMQ sockets replaced by nulls so we can
    exercise internal state without requiring a running ZMQ infrastructure.
    """
    import unittest.mock as mock
    from core.fv_engine import FVEngine
    with mock.patch("core.fv_engine.get_subscriber"), \
         mock.patch("core.fv_engine.get_publisher"):
        engine = FVEngine()
    return engine


def test_task11_intra_window_buffer_exists():
    """FVEngine must have _intra_window_prices deque after Task 1.1."""
    from collections import deque
    engine = _make_engine_no_zmq()
    assert hasattr(engine, "_intra_window_prices"), \
        "_intra_window_prices buffer missing from FVEngine"
    assert isinstance(engine._intra_window_prices, deque), \
        "_intra_window_prices must be a deque"


def test_task11_intra_window_buffer_appends_on_tick():
    """
    Driving _snap_strike() + manual appends should grow _intra_window_prices.
    We simulate what _on_tick does: snap strike, then append.
    """
    engine = _make_engine_no_zmq()

    WINDOW = 300
    base_ts = 1_700_000_000.0
    boundary = base_ts - (base_ts % WINDOW)  # aligned boundary
    mid = 95_000.0

    # Simulate 5 ticks inside the same window
    for i in range(5):
        ts = boundary + 1.0 + i * 0.1   # all within the same window
        engine._snap_strike(mid, ts)
        engine._intra_window_prices.append(mid)

    assert len(engine._intra_window_prices) == 5, \
        f"Expected 5 intra-window ticks, got {len(engine._intra_window_prices)}"


def test_task11_intra_window_buffer_cleared_on_boundary():
    """
    After boundary snap, _intra_window_prices must be empty.
    _prices (cross-window) must NOT be cleared.
    """
    engine = _make_engine_no_zmq()

    WINDOW = 300
    base_ts = 1_700_000_000.0
    boundary1 = base_ts - (base_ts % WINDOW)
    boundary2 = boundary1 + WINDOW

    mid = 95_000.0

    # Fill buffer in window 1
    for i in range(10):
        ts = boundary1 + 1.0 + i * 0.1
        engine._snap_strike(mid, ts)
        engine._prices.append(mid)
        engine._intra_window_prices.append(mid)

    assert len(engine._intra_window_prices) == 10, "Should have 10 ticks pre-boundary"
    assert len(engine._prices) == 10, "_prices should also have 10 ticks"

    # Cross into window 2 — boundary snap must clear intra buffer
    engine._snap_strike(mid, boundary2 + 0.5)

    assert len(engine._intra_window_prices) == 0, \
        f"_intra_window_prices must be empty after boundary snap, got {len(engine._intra_window_prices)}"
    assert len(engine._prices) == 10, \
        "_prices (cross-window) must NOT be cleared on boundary snap"


def test_task11_intra_window_contains_only_current_window_ticks():
    """
    After a boundary crossing, ticks appended in the NEW window must be
    separate from those in the old window (which were cleared).
    """
    engine = _make_engine_no_zmq()

    WINDOW = 300
    base_ts = 1_700_000_000.0
    boundary1 = base_ts - (base_ts % WINDOW)
    boundary2 = boundary1 + WINDOW

    mid_w1 = 95_000.0
    mid_w2 = 96_000.0  # different price in window 2

    # Window 1: 7 ticks
    for i in range(7):
        engine._snap_strike(mid_w1, boundary1 + 1.0 + i * 0.1)
        engine._intra_window_prices.append(mid_w1)

    # Boundary crossing + 3 ticks in window 2
    engine._snap_strike(mid_w2, boundary2 + 0.5)
    for i in range(3):
        engine._intra_window_prices.append(mid_w2)

    # Only the 3 new ticks should be present
    assert len(engine._intra_window_prices) == 3, \
        f"After boundary, only 3 new ticks should be in intra_window_prices, got {len(engine._intra_window_prices)}"
    for p in engine._intra_window_prices:
        assert p == mid_w2, \
            f"Stale window-1 price {mid_w1} found in intra_window_prices after boundary reset"
