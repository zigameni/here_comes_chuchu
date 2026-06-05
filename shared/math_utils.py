"""
shared/math_utils.py
────────────────────
Pure-stdlib Black-Scholes binary probability math.

No scipy dependency — uses Python's math.erf for the normal CDF,
which is accurate to machine precision and has zero import overhead.

Public API
----------
    norm_cdf(x)                                             → float
    black_scholes_prob(S, K, T, sigma)                      → float  [0.0 – 1.0]
    annualize_vol(prices, interval_s)                       → float  (annualized σ, simple stdev)
    ewma_vol(prices, alpha, interval_s)                     → float  (annualized σ, EWMA-weighted)
    time_to_expiry_years(end_ts_s, now_ts=None)             → float
    z_score(btc_delta_pct, sigma_annualized, t_remaining_s) → float
    prob_from_z(z)                                          → float  [0.0 – 1.0]
    sign(x)                                                 → int
"""

from __future__ import annotations

import math
import statistics
from collections import deque
from typing import Sequence

# ── Constants ──────────────────────────────────────────────────────────────────
_SQRT2     = math.sqrt(2.0)
_SECONDS_PER_YEAR = 365.25 * 24 * 3600  # seconds in a trading year
_MIN_SIGMA = 1e-8   # floor to avoid division-by-zero near expiry
_MIN_T     = 1e-10  # floor for time-to-expiry


# ── Normal CDF ─────────────────────────────────────────────────────────────────

def norm_cdf(x: float) -> float:
    """
    Standard normal cumulative distribution function Φ(x).

    Uses the identity:  Φ(x) = 0.5 * (1 + erf(x / √2))
    Accurate to machine precision (~1e-15). No external deps.

    Examples
    --------
    >>> abs(norm_cdf(0.0) - 0.5) < 1e-12
    True
    >>> norm_cdf(10.0) > 0.9999
    True
    >>> norm_cdf(-10.0) < 0.0001
    True
    """
    return 0.5 * (1.0 + math.erf(x / _SQRT2))


# ── Black-Scholes binary call probability ─────────────────────────────────────

def black_scholes_prob(
    S: float,
    K: float,
    T: float,
    sigma: float,
) -> float:
    """
    P(Yes) for a binary call option — probability that S > K at expiry.

    Formula:  P(Yes) = Φ(d₂)
              d₂ = [ ln(S/K) / (σ√T) ] - σ√T/2

    Parameters
    ----------
    S     : float  Current underlying price (e.g. BTC/USDT mid)
    K     : float  Strike price (the market's BTC level to beat)
    T     : float  Time to expiry in *years*  (use time_to_expiry_years())
    sigma : float  Annualized realized volatility (use annualize_vol())

    Returns
    -------
    float in [0.0, 1.0]
        Probability the market resolves YES (S > K at expiry).

    Edge cases
    ----------
    * T ≤ 0          → binary settlement: 1.0 if S > K else 0.0
    * sigma ≤ 0      → same binary settlement
    * S ≤ 0 or K ≤ 0 → returns 0.0 (invalid prices)

    Examples
    --------
    >>> # At the money, lots of time left → ~50%
    >>> abs(black_scholes_prob(100, 100, 1/365, 0.80) - 0.5) < 0.02
    True
    >>> # Far in-the-money near expiry → approaches 1.0
    >>> black_scholes_prob(105, 100, 1e-6, 0.80) > 0.99
    True
    >>> # Far out-of-the-money near expiry → approaches 0.0
    >>> black_scholes_prob(95, 100, 1e-6, 0.80) < 0.01
    True
    """
    if S <= 0.0 or K <= 0.0:
        return 0.0

    T = max(T, _MIN_T)
    sigma = max(sigma, _MIN_SIGMA)

    if T <= _MIN_T or sigma <= _MIN_SIGMA:
        # Binary settlement at expiry
        return 1.0 if S > K else 0.0

    sqrt_T = math.sqrt(T)
    try:
        d2 = (math.log(S / K) / (sigma * sqrt_T)) - (sigma * sqrt_T / 2.0)
    except (ValueError, ZeroDivisionError):
        return 1.0 if S > K else 0.0

    return norm_cdf(d2)


# ── Realized volatility ────────────────────────────────────────────────────────

def annualize_vol(
    prices: Sequence[float] | deque,
    interval_s: float = 1.0,
) -> float:
    """
    Annualized realized volatility from a sequence of recent prices.

    Method
    ------
    1. Compute log-returns:  r_i = ln(p_i / p_{i-1})
    2. Compute std dev of returns over the window
    3. Scale to annual:  σ_annual = σ_interval × √(seconds_per_year / interval_s)

    Parameters
    ----------
    prices     : Sequence of recent mid-prices (oldest first)
    interval_s : Average spacing between samples in seconds (default: 1.0)

    Returns
    -------
    float  Annualized σ.  Returns 0.0 if fewer than 3 samples.

    Notes
    -----
    * Do NOT apply a moving average to σ output — smooth σ *input* (prices)
      using EWMA if needed (see Phases_analysis.md recommendation).
    * With a 300-sample buffer at ~1 tick/s this gives a ~5-minute σ window,
      which is appropriate for 5-minute binary markets.
    """
    prices = list(prices)  # works for both list and deque
    n = len(prices)
    if n < 3:
        return 0.0

    log_returns = [
        math.log(prices[i] / prices[i - 1])
        for i in range(1, n)
        if prices[i - 1] > 0 and prices[i] > 0
    ]

    if len(log_returns) < 2:
        return 0.0

    std_interval = statistics.stdev(log_returns)
    periods_per_year = _SECONDS_PER_YEAR / interval_s
    return std_interval * math.sqrt(periods_per_year)


# ── Time to expiry ─────────────────────────────────────────────────────────────

def time_to_expiry_years(end_ts_s: float, now_ts: float | None = None) -> float:
    """
    Compute T = time remaining until market expiry, in years.

    Parameters
    ----------
    end_ts_s : float        Unix timestamp (seconds) of market close / expiry
    now_ts   : float | None Current time as Unix timestamp (seconds).
                            Defaults to ``time.time()`` when None.
                            Pass an explicit value for replay / unit-test scenarios
                            where wall-clock time must be controlled.

    Returns
    -------
    float  Seconds remaining / seconds_per_year.
           Returns 0.0 if the market has already expired.

    Example
    -------
    A 5-minute market with 3 minutes left:
        T = (3 * 60) / (365.25 * 24 * 3600)  ≈  5.71e-6 years
    """
    import time as _time
    t_now = now_ts if now_ts is not None else _time.time()
    remaining_s = end_ts_s - t_now
    if remaining_s <= 0.0:
        return 0.0
    return remaining_s / _SECONDS_PER_YEAR


# ── EWMA realized volatility ───────────────────────────────────────────────────

def ewma_vol(
    prices: Sequence[float] | deque,
    alpha: float,
    interval_s: float = 1.0,
) -> float:
    """
    Annualized realized volatility using exponentially-weighted moving average.

    EWMA gives recent returns more weight than the simple stdev used in
    ``annualize_vol()``.  This makes it more responsive to intra-window vol
    regime changes — essential when the price buffer is limited to the current
    5-minute window and early ticks should not dominate late-window estimates.

    Method
    ------
    1. Compute log-returns:  r_i = ln(p_i / p_{i-1})
    2. Initialise EWMA variance with the first squared return: v = r_1²
    3. Update recursively:   v_i = alpha * v_{i-1} + (1 - alpha) * r_i²
    4. Annualise:            σ = sqrt(v) * sqrt(seconds_per_year / interval_s)

    Parameters
    ----------
    prices     : Sequence of recent mid-prices (oldest first, len ≥ 2)
    alpha      : EWMA decay factor in (0, 1).  Higher = more weight on history.
                 Typical values: 0.94 (RiskMetrics), 0.97–0.99 for fast ticks.
    interval_s : Average time between samples in seconds (default: 1.0).
                 Use the measured tick interval, not an assumed value.

    Returns
    -------
    float  Annualized σ ≥ 0.  Returns 0.0 if fewer than 2 samples or if
           all returns are zero (flat prices).

    Notes
    -----
    * Compared to ``annualize_vol()`` (simple stdev), EWMA reacts faster to
      vol spikes because it down-weights stale returns exponentially.
    * The alpha parameter does NOT need to be tuned per-window-length — it
      controls the half-life of the decay, independent of buffer size.
    * Half-life in ticks: t½ = ln(0.5) / ln(alpha).  At alpha=0.94 and
      0.1s ticks: t½ ≈ 11 ticks ≈ 1.1 seconds.

    Examples
    --------
    >>> # Flat prices → zero vol
    >>> ewma_vol([100.0, 100.0, 100.0, 100.0], alpha=0.94) == 0.0
    True
    >>> # Non-flat prices → positive vol
    >>> ewma_vol([100.0, 100.1, 100.05, 100.2], alpha=0.94, interval_s=0.1) > 0
    True
    """
    prices = list(prices)
    n = len(prices)
    if n < 2:
        return 0.0

    # Compute log-returns, skipping any zero/negative prices
    returns = [
        math.log(prices[i] / prices[i - 1])
        for i in range(1, n)
        if prices[i - 1] > 0 and prices[i] > 0
    ]

    if not returns:
        return 0.0

    # Initialise EWMA variance with first squared return
    ewma_variance = returns[0] ** 2

    # Recursive EWMA update
    for r in returns[1:]:
        ewma_variance = alpha * ewma_variance + (1.0 - alpha) * r * r

    if ewma_variance <= 0.0:
        return 0.0

    periods_per_year = _SECONDS_PER_YEAR / interval_s
    return math.sqrt(ewma_variance * periods_per_year)


# ── Z-score and probability helpers ───────────────────────────────────────────

def z_score(
    btc_delta_pct: float,
    sigma_annualized: float,
    t_remaining_s: float,
) -> float:
    """
    Standardised displacement of BTC from its window-open strike K.

    This is the key Architecture A (Terminal Oracle Sniper) signal: how many
    standard deviations is BTC above or below K, given the time remaining?
    A large positive z means strong UP signal; large negative means strong DOWN.

    Formula
    -------
        sigma_remaining = sigma_annualized * sqrt(t_remaining_s / seconds_per_year)
        z = btc_delta_pct / sigma_remaining

    Where ``btc_delta_pct = (btc_now - K) / K``.

    Parameters
    ----------
    btc_delta_pct    : float  Fractional displacement from strike: (S - K) / K.
                              Positive = BTC above K (UP favoured).
    sigma_annualized : float  Annualized realized volatility (from ewma_vol or
                              annualize_vol).  Must be > 0.
    t_remaining_s    : float  Seconds remaining until window close.  Must be > 0.

    Returns
    -------
    float  Z-score.  Returns 0.0 if sigma or t_remaining are non-positive
           (degenerate / at-expiry cases).

    Examples
    --------
    >>> # BTC 0.5% above K with 60s left and 80% annual vol
    >>> # sigma_remaining = 0.80 * sqrt(60 / 31557600) ≈ 0.00347
    >>> # z = 0.005 / 0.00347 ≈ 1.44
    >>> abs(z_score(0.005, 0.80, 60) - 1.44) < 0.01
    True
    >>> # At strike → z = 0
    >>> z_score(0.0, 0.80, 60) == 0.0
    True
    """
    if sigma_annualized <= 0.0 or t_remaining_s <= 0.0:
        return 0.0

    sigma_remaining = sigma_annualized * math.sqrt(t_remaining_s / _SECONDS_PER_YEAR)
    if sigma_remaining <= 0.0:
        return 0.0

    return btc_delta_pct / sigma_remaining


def prob_from_z(z: float) -> float:
    """
    Convert a z-score to a probability via the standard normal CDF.

    This is just ``norm_cdf(z)``, named explicitly for Architecture A callers
    where the intent is to map a displacement signal to a win probability.

    Parameters
    ----------
    z : float  Z-score (output of z_score())

    Returns
    -------
    float in [0.0, 1.0]
        Probability that BTC ends above K, given z standard deviations of
        current displacement.

    Examples
    --------
    >>> abs(prob_from_z(0.0) - 0.5) < 1e-12   # at-the-money → 50%
    True
    >>> prob_from_z(1.28) > 0.899              # z=1.28 → ~90%
    True
    >>> prob_from_z(-1.28) < 0.101             # symmetric
    True
    """
    return norm_cdf(z)


# ── Signal helpers ─────────────────────────────────────────────────────────────

def sign(x: float) -> int:
    """Return 1 if x > 0, -1 if x < 0, else 0."""
    if x > 0.0:
        return 1
    if x < 0.0:
        return -1
    return 0
