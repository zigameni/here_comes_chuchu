"""
tests/test_math_utils.py
────────────────────────
Unit tests for shared/math_utils.py.

Run:
    python tests/test_math_utils.py
    python -m pytest tests/test_math_utils.py -v
"""

from __future__ import annotations

import math
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.math_utils import (
    annualize_vol,
    black_scholes_prob,
    ewma_vol,
    norm_cdf,
    prob_from_z,
    time_to_expiry_years,
    z_score,
)


# ── norm_cdf ──────────────────────────────────────────────────────────────────

def test_norm_cdf_at_zero():
    assert abs(norm_cdf(0.0) - 0.5) < 1e-12, "Φ(0) must be exactly 0.5"

def test_norm_cdf_symmetry():
    for x in [0.5, 1.0, 1.96, 3.0]:
        assert abs(norm_cdf(x) + norm_cdf(-x) - 1.0) < 1e-12, \
            f"Φ(x) + Φ(-x) = 1 failed at x={x}"

def test_norm_cdf_known_values():
    assert abs(norm_cdf(1.96) - 0.9750) < 0.0001
    assert abs(norm_cdf(-1.96) - 0.0250) < 0.0001
    assert abs(norm_cdf(1.0) - 0.8413) < 0.0001

def test_norm_cdf_bounds():
    assert norm_cdf(10.0) > 0.9999
    assert norm_cdf(-10.0) < 0.0001


# ── black_scholes_prob ────────────────────────────────────────────────────────

def test_bs_atm_is_near_half():
    p = black_scholes_prob(S=100_000, K=100_000, T=5/525_960, sigma=0.80)
    assert 0.40 < p < 0.60, f"ATM prob should be ~0.5, got {p:.4f}"

def test_bs_deep_itm_near_expiry():
    p = black_scholes_prob(S=110_000, K=100_000, T=1e-7, sigma=0.80)
    assert p > 0.99, f"Deep ITM near expiry should be > 0.99, got {p:.6f}"

def test_bs_deep_otm_near_expiry():
    p = black_scholes_prob(S=90_000, K=100_000, T=1e-7, sigma=0.80)
    assert p < 0.01, f"Deep OTM near expiry should be < 0.01, got {p:.6f}"

def test_bs_prob_up_plus_down_equals_one():
    cases = [
        (100_000, 100_000, 1/365, 0.80),
        (105_000, 100_000, 5/525_960, 0.60),
        (95_000, 100_000, 0.001, 1.20),
    ]
    for S, K, T, sigma in cases:
        p_up = black_scholes_prob(S, K, T, sigma)
        p_dn = 1.0 - p_up
        assert abs(p_up + p_dn - 1.0) < 1e-12

def test_bs_invalid_prices_return_zero():
    assert black_scholes_prob(S=0, K=100_000, T=0.01, sigma=0.5) == 0.0
    assert black_scholes_prob(S=100_000, K=0, T=0.01, sigma=0.5) == 0.0

def test_bs_zero_time_binary_settlement():
    assert black_scholes_prob(S=100_001, K=100_000, T=0.0, sigma=0.5) == 1.0
    assert black_scholes_prob(S=99_999, K=100_000, T=0.0, sigma=0.5) == 0.0

def test_bs_higher_vol_flattens_probability():
    p_low_vol  = black_scholes_prob(105_000, 100_000, 1/365, sigma=0.20)
    p_high_vol = black_scholes_prob(105_000, 100_000, 1/365, sigma=2.00)
    assert p_low_vol > p_high_vol, \
        "Lower vol ITM should give higher probability than high vol"

def test_bs_more_time_flattens_probability():
    p_near  = black_scholes_prob(105_000, 100_000, T=1e-5,  sigma=0.80)
    p_far   = black_scholes_prob(105_000, 100_000, T=1/12,  sigma=0.80)
    assert p_near > p_far, \
        "Near-expiry ITM should have higher prob than far-expiry"


# ── annualize_vol ─────────────────────────────────────────────────────────────

def test_annualize_vol_too_few_samples():
    assert annualize_vol([100.0, 101.0]) == 0.0, "< 3 samples should return 0"

def test_annualize_vol_flat_prices():
    prices = [100_000.0] * 100
    sigma = annualize_vol(prices)
    assert sigma == 0.0 or sigma < 1e-10, f"Flat prices should give σ≈0, got {sigma}"

def test_annualize_vol_realistic_btc():
    import random
    random.seed(42)
    price = 100_000.0
    prices = [price]
    for _ in range(299):
        price *= math.exp(random.gauss(0, 0.001))
        prices.append(price)
    sigma = annualize_vol(prices, interval_s=1.0)
    assert sigma > 0.0, "Non-flat prices should give σ > 0"
    assert sigma < 100.0, f"σ={sigma:.2f} seems unrealistically large"

def test_annualize_vol_reasonable_range():
    import random
    random.seed(7)
    sigma_per_tick = 0.60 / math.sqrt(365.25 * 24 * 3600)
    price = 100_000.0
    prices = [price]
    for _ in range(299):
        price *= math.exp(random.gauss(0, sigma_per_tick))
        prices.append(price)
    sigma = annualize_vol(prices, interval_s=1.0)
    assert 0.10 < sigma < 5.0, f"Expected σ in [0.10, 5.0], got {sigma:.4f}"


# ── time_to_expiry_years ──────────────────────────────────────────────────────

def test_tte_past_expiry():
    past = time.time() - 60
    assert time_to_expiry_years(past) == 0.0

def test_tte_future():
    future = time.time() + 300
    T = time_to_expiry_years(future)
    assert T > 0.0
    expected = 300 / (365.25 * 24 * 3600)
    assert abs(T - expected) < 1e-6

# Task 1.2: now_ts parameter (backward-compatible refactor)

def test_tte_explicit_now_ts():
    """Passing now_ts explicitly must be fully deterministic."""
    now = 1_700_000_000.0
    end = now + 300.0
    T = time_to_expiry_years(end, now_ts=now)
    expected = 300.0 / (365.25 * 24 * 3600)
    assert abs(T - expected) < 1e-12, f"Expected {expected}, got {T}"

def test_tte_explicit_now_ts_expired():
    now = 1_700_000_000.0
    end = now - 1.0
    assert time_to_expiry_years(end, now_ts=now) == 0.0

def test_tte_none_uses_wall_clock():
    future = time.time() + 300
    T = time_to_expiry_years(future, now_ts=None)
    assert T > 0.0

def test_tte_backward_compat_single_arg():
    """Original single-argument call must still work."""
    future = time.time() + 60
    T = time_to_expiry_years(future)
    assert T > 0.0


# ── ewma_vol (Task 1.2) ───────────────────────────────────────────────────────

def test_ewma_vol_too_few_samples():
    assert ewma_vol([100.0], alpha=0.94) == 0.0, "< 2 samples must return 0"

def test_ewma_vol_flat_prices():
    prices = [100_000.0] * 50
    assert ewma_vol(prices, alpha=0.94) == 0.0, "Flat prices must give ewma_vol=0"

def test_ewma_vol_positive_for_moving_prices():
    import random
    random.seed(1)
    price = 100_000.0
    prices = [price]
    for _ in range(49):
        price *= math.exp(random.gauss(0, 0.0001))
        prices.append(price)
    sigma = ewma_vol(prices, alpha=0.94, interval_s=0.1)
    assert sigma > 0.0, "Moving prices must give ewma_vol > 0"

def test_ewma_vol_weights_recent_returns_more_than_old():
    """
    EWMA reacts faster to a late vol spike than simple stdev.
    40 quiet ticks then 10 high-vol ticks: ewma > annualize_vol.
    """
    import random
    random.seed(42)
    QUIET_SIGMA = 0.60 / math.sqrt(365.25 * 24 * 3600 / 0.1)
    SPIKE_SIGMA = QUIET_SIGMA * 10

    price = 100_000.0
    prices = [price]
    for _ in range(40):
        price *= math.exp(random.gauss(0, QUIET_SIGMA))
        prices.append(price)
    for _ in range(10):
        price *= math.exp(random.gauss(0, SPIKE_SIGMA))
        prices.append(price)

    sigma_ewma   = ewma_vol(prices, alpha=0.94, interval_s=0.1)
    sigma_simple = annualize_vol(prices, interval_s=0.1)

    assert sigma_ewma > sigma_simple, (
        f"EWMA (alpha=0.94) should weight recent spike more: "
        f"ewma={sigma_ewma:.3f}  simple={sigma_simple:.3f}"
    )

def test_ewma_vol_higher_alpha_reacts_more_slowly():
    """Higher alpha = slower reaction to new vol spike."""
    import random
    random.seed(7)
    QUIET = 0.0001
    SPIKE = 0.01
    price = 100_000.0
    prices = [price]
    for _ in range(30):
        price *= math.exp(random.gauss(0, QUIET))
        prices.append(price)
    for _ in range(5):
        price *= math.exp(random.gauss(0, SPIKE))
        prices.append(price)

    sigma_fast = ewma_vol(prices, alpha=0.50, interval_s=0.1)
    sigma_slow = ewma_vol(prices, alpha=0.99, interval_s=0.1)
    assert sigma_fast > sigma_slow, (
        f"Lower alpha should react faster: alpha=0.50→{sigma_fast:.3f}  alpha=0.99→{sigma_slow:.3f}"
    )

def test_ewma_vol_reasonable_range_for_btc():
    import random
    random.seed(99)
    sigma_per_tick = 0.60 / math.sqrt(365.25 * 24 * 3600 / 0.1)
    price = 100_000.0
    prices = [price]
    for _ in range(200):
        price *= math.exp(random.gauss(0, sigma_per_tick))
        prices.append(price)
    sigma = ewma_vol(prices, alpha=0.94, interval_s=0.1)
    assert 0.05 < sigma < 10.0, f"Expected plausible ewma_vol, got {sigma:.4f}"

def test_ewma_vol_deque_input():
    """ewma_vol must accept a deque, not just a list."""
    from collections import deque as Deque
    import random
    random.seed(3)
    price = 100_000.0
    prices = Deque(maxlen=100)
    prices.append(price)
    for _ in range(49):
        price *= math.exp(random.gauss(0, 0.0001))
        prices.append(price)
    sigma = ewma_vol(prices, alpha=0.94, interval_s=0.1)
    assert sigma > 0.0, "ewma_vol must work with deque input"


# ── z_score (Task 1.2) ────────────────────────────────────────────────────────

def test_z_score_at_strike():
    assert z_score(0.0, 0.80, 60) == 0.0, "At strike, z must be 0"

def test_z_score_zero_sigma_returns_zero():
    assert z_score(0.005, 0.0, 60) == 0.0

def test_z_score_zero_time_returns_zero():
    assert z_score(0.005, 0.80, 0.0) == 0.0

def test_z_score_known_value():
    """
    BTC 0.5% above K, sigma=80% annual, 60s remaining.
    sigma_remaining = 0.80 * sqrt(60 / 31_557_600) ≈ 0.003477
    z = 0.005 / 0.003477 ≈ 1.438
    """
    SECONDS_PER_YEAR = 365.25 * 24 * 3600
    sigma_remaining = 0.80 * math.sqrt(60 / SECONDS_PER_YEAR)
    expected_z = 0.005 / sigma_remaining
    got_z = z_score(0.005, 0.80, 60)
    assert abs(got_z - expected_z) < 1e-10, f"Expected z≈{expected_z:.4f}, got {got_z:.4f}"

def test_z_score_negative_for_btc_below_k():
    assert z_score(-0.005, 0.80, 60) < 0.0

def test_z_score_symmetry():
    z_up   = z_score(+0.005, 0.80, 60)
    z_down = z_score(-0.005, 0.80, 60)
    assert abs(z_up + z_down) < 1e-12, "z_score must be antisymmetric"

def test_z_score_larger_at_shorter_time():
    """Same displacement gives larger |z| with less time remaining."""
    z_early = z_score(0.005, 0.80, 250)
    z_late  = z_score(0.005, 0.80, 30)
    assert z_late > z_early, "z must grow as t_remaining shrinks"


# ── prob_from_z (Task 1.2) ────────────────────────────────────────────────────

def test_prob_from_z_at_zero():
    assert abs(prob_from_z(0.0) - 0.5) < 1e-12

def test_prob_from_z_positive_z():
    assert prob_from_z(1.28) > 0.899

def test_prob_from_z_negative_z():
    assert prob_from_z(-1.28) < 0.101

def test_prob_from_z_symmetry():
    for z in [0.5, 1.0, 1.96, 3.0]:
        assert abs(prob_from_z(z) + prob_from_z(-z) - 1.0) < 1e-12

def test_prob_from_z_matches_norm_cdf():
    for z in [-2.0, -1.0, 0.0, 1.0, 2.0]:
        assert prob_from_z(z) == norm_cdf(z), f"Mismatch at z={z}"


# ── Self-running without pytest ───────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        # norm_cdf
        test_norm_cdf_at_zero,
        test_norm_cdf_symmetry,
        test_norm_cdf_known_values,
        test_norm_cdf_bounds,
        # black_scholes_prob
        test_bs_atm_is_near_half,
        test_bs_deep_itm_near_expiry,
        test_bs_deep_otm_near_expiry,
        test_bs_prob_up_plus_down_equals_one,
        test_bs_invalid_prices_return_zero,
        test_bs_zero_time_binary_settlement,
        test_bs_higher_vol_flattens_probability,
        test_bs_more_time_flattens_probability,
        # annualize_vol
        test_annualize_vol_too_few_samples,
        test_annualize_vol_flat_prices,
        test_annualize_vol_realistic_btc,
        test_annualize_vol_reasonable_range,
        # time_to_expiry_years (original + Task 1.2 refactor)
        test_tte_past_expiry,
        test_tte_future,
        test_tte_explicit_now_ts,
        test_tte_explicit_now_ts_expired,
        test_tte_none_uses_wall_clock,
        test_tte_backward_compat_single_arg,
        # ewma_vol (Task 1.2)
        test_ewma_vol_too_few_samples,
        test_ewma_vol_flat_prices,
        test_ewma_vol_positive_for_moving_prices,
        test_ewma_vol_weights_recent_returns_more_than_old,
        test_ewma_vol_higher_alpha_reacts_more_slowly,
        test_ewma_vol_reasonable_range_for_btc,
        test_ewma_vol_deque_input,
        # z_score (Task 1.2)
        test_z_score_at_strike,
        test_z_score_zero_sigma_returns_zero,
        test_z_score_zero_time_returns_zero,
        test_z_score_known_value,
        test_z_score_negative_for_btc_below_k,
        test_z_score_symmetry,
        test_z_score_larger_at_shorter_time,
        # prob_from_z (Task 1.2)
        test_prob_from_z_at_zero,
        test_prob_from_z_positive_z,
        test_prob_from_z_negative_z,
        test_prob_from_z_symmetry,
        test_prob_from_z_matches_norm_cdf,
    ]

    passed = 0
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {t.__name__}  ->  {e}")
            failed += 1

    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)
