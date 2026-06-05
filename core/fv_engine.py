"""
core/fv_engine.py
─────────────────
Phase 2 — Fair Value Engine.

Subscribes to the Binance BBO ZMQ feed, computes a continuous Black-Scholes
P(Yes) probability on every tick, and publishes the result to fv_stream.ipc.

Published schema
----------------
    [timestamp_ms, boundary_ts, prob_up, prob_down, sigma, btc_price, intra_vol, is_sigma_real, strike]

Fixes applied (vs original)
----------------------------
Bug 1 — Wrong strike K:
    K was hardcoded to a static value (e.g. 76000) regardless of where BTC
    was when the 5-minute window opened.  If BTC opens at 76050 and K=76000,
    P(UP) stays at 1.0 even as BTC falls to 75960 — because S > K is always
    true.  The correct K is the BTC mid-price at the exact 5-minute window
    boundary (the moment the Polymarket market opens).

    Fix: _snap_strike() is called on every tick and detects when the
    300-second boundary rolls over.  It snapshots the current BTC mid as the
    new K at that moment.  K auto-updates each market window with zero manual
    config.

Bug 2 — σ = 0 on flat markets (BS degenerates to comparator):
    When BTC doesn't move for several ticks, all log returns = 0, stdev = 0,
    and Black-Scholes collapses to a binary: P = 1.0 if S > K else 0.0.
    No probability signal — just a price comparator.

    Fix A: MIN_SIGMA_FLOOR = 0.50 (50% annualized).  BTC vol is never truly 0.
    The floor is applied AFTER the rolling calculation, so measured vol still
    dominates when the market is actually moving.  The floor was raised from
    0.20 to 0.50 after testing showed 98% of fills hitting the floor — 20%
    was too conservative relative to BTC's empirical 50–120% realized vol range.

    Fix B: Tick interval is measured from actual inter-arrival times, not
    assumed to be 1.0s.  Binance bookTicker fires 5–15x/second; assuming 1s
    causes a ~3–4× under-estimate of annualized σ.

Run
---
    python -m core.fv_engine          # from repo root
    python core/fv_engine.py          # direct

Config (env or .env file)
-------------------------
    MARKET_WINDOW_SECONDS  — window length in seconds (default: 300)
    MARKET_ID              — Polymarket condition ID (informational only in Phase 2)
    PRICE_BUFFER           — ticks to keep for σ calculation (default: 3000)
"""

from __future__ import annotations

import asyncio
import logging
from shared.log_setup import setup_logging
from shared.metrics import emit as emit_metric
import math
import os
import signal
import statistics
import sys
import time
from collections import deque
from pathlib import Path
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from shared.ipc import Channel, get_publisher, get_subscriber, pack, unpack
from shared.math_utils import annualize_vol, ewma_vol, black_scholes_prob, time_to_expiry_years

log = setup_logging("fv_engine")

# ── Config ─────────────────────────────────────────────────────────────────────

# Market window length.  Must match Polymarket market cadence (300s = 5 min).
MARKET_WINDOW_SECONDS: int = int(os.getenv("MARKET_WINDOW_SECONDS", "300"))

# Polymarket condition ID — informational only in Phase 2.
# Phase 3 will drive this from the pm_daemon market discovery stream.
MARKET_ID: str = os.getenv("MARKET_ID", "phase2-dynamic-K")

# Rolling price buffer depth.
# Binance bookTicker arrives ~5–15 ticks/s → 3000 ticks ≈ 3–10 minutes.
# This covers a full 5-minute window so annualize_vol() has enough price
# history to produce a reliable sigma instead of nearly always hitting the floor.
# The old default (300 ≈ 20–60 seconds) was too shallow for flat-BTC sessions.
PRICE_BUFFER: int = int(os.getenv("PRICE_BUFFER", "3000"))

# Ticks to collect before publishing (avoids garbage σ from cold start)
MIN_BUFFER_FILL: int = 30

# ── EWMA / intra-window vol config (Task 1.3) ──────────────────────────────────
# EWMA decay factor.
#
# WHY 0.999 instead of the RiskMetrics default of 0.94:
#   Binance bookTicker fires on every bid/ask change — at quiet moments several
#   consecutive ticks carry the SAME price (log-return = 0).  alpha=0.94 gives a
#   half-life of ln(0.5)/ln(0.94) ≈ 11 ticks ≈ 1.1 s at 100ms ticks.  When BTC
#   is even briefly flat the EWMA variance decays toward zero, ewma_vol collapses
#   to near-zero, and is_sigma_real flips to False — blocking entries correctly,
#   but also causing ewma_vol to be wrong whenever it IS True (the 1-second memory
#   is too short to capture the window's actual vol regime).
#
#   alpha=0.999 gives half-life ≈ 693 ticks ≈ 69 s at 100ms ticks.  The EWMA
#   retains ~50% weight on the last 69 s and ~90% on the last 230 s — enough to
#   smooth over flat-tick clusters while still reflecting genuine intra-window vol.
#   This matches the engineering plan recommendation: "Use alpha=0.999 for ~100s
#   half-life or higher."
EWMA_ALPHA: float = float(os.getenv("EWMA_ALPHA", "0.999"))

# Minimum intra-window ticks before EWMA is trusted.  Below this threshold the
# engine falls back to the cross-window path to avoid noisy warm-start estimates.
MIN_INTRA_TICKS: int = int(os.getenv("MIN_INTRA_TICKS", "30"))

# Dynamic floor fraction: floor = SIGMA_FLOOR_FRACTION × cross_window_vol × sqrt(T/300)
# 0.30 means the floor is 30% of recent cross-window vol, scaled by time remaining.
# This replaces the old fixed MIN_SIGMA_FLOOR=0.50 which caused 261/261 floor hits.
SIGMA_FLOOR_FRACTION: float = float(os.getenv("SIGMA_FLOOR_FRACTION", "0.30"))

# Hard minimum floor (annualized) — applies even if cross-window vol is near zero.
SIGMA_FLOOR_MIN: float = float(os.getenv("SIGMA_FLOOR_MIN", "0.10"))

# Rollback escape hatch: set USE_INTRA_WINDOW_VOL=0 to revert to cross-window path.
USE_INTRA_WINDOW_VOL: bool = os.getenv("USE_INTRA_WINDOW_VOL", "1") != "0"

# Tick-interval smoothing window (for inter-arrival time estimation)
TICK_INTERVAL_WINDOW: int = 50

# Logging thresholds
LOG_THRESHOLD: float    = 0.005   # Only log when FV moves by ≥ 0.5%
STATUS_INTERVAL_S: float = 5.0    # Force a status line every N seconds


class FVEngine:
    """
    Stateful fair-value calculator with dynamic strike and σ floor.

    Key state:
        _prices          — rolling BTC mid-price buffer (for σ)
        _strike          — current K, updated at each window boundary
        _window_end_ts   — Unix ts when the current window closes
        _tick_intervals  — inter-arrival times for accurate σ scaling
    """

    def __init__(self) -> None:
        # Cross-window buffer: retains ticks across boundaries.
        # Used by the legacy annualize_vol() path (Tasks 1.1–1.2 only).
        # Task 1.3 will switch vol computation to _intra_window_prices.
        self._prices: deque[float]         = deque(maxlen=PRICE_BUFFER)

        # Intra-window buffer: cleared at every 5-minute boundary.
        # Only contains ticks from the CURRENT window.
        # Task 1.3: primary vol source — fed into ewma_vol() via _compute_sigma().
        self._intra_window_prices: deque[float] = deque(maxlen=PRICE_BUFFER)

        self._tick_intervals: deque[float] = deque(maxlen=TICK_INTERVAL_WINDOW)
        self._last_tick_t: float           = 0.0
        self._tick_count: int              = 0

        # ── Strike & window (Bug 1 fix) ────────────────────────────────────────
        # Both are set on the first tick via _snap_strike().
        # _strike is snapped at each 300s boundary; _window_end_ts is the
        # next boundary after that snap.
        self._strike: float        = 0.0   # K — set on first tick
        self._window_end_ts: float = 0.0   # expiry ts — set on first tick
        self._current_boundary: int = 0    # tracks which window we are in

        # ── Sigma quality flag (Task 1.3) ─────────────────────────────────────────
        # True when sigma is derived from ≥ MIN_INTRA_TICKS intra-window prices.
        # False during warmup (first ~3s of each window) or if cross-window fallback
        # is active.  Not yet published in FV_STREAM message (that is Task 1.4).
        self._is_sigma_real: bool    = False

        # Logging state
        self._last_fv: float         = 0.5
        self._last_log_fv: float     = -1.0
        self._last_status_t: float   = 0.0

        # ZMQ sockets
        self._sub = get_subscriber(Channel.BINANCE_BBO)
        self._pub = get_publisher(Channel.FV_STREAM)

        log.info(
            "FVEngine ready — window=%ds  buffer=%d  intra_vol=%s  "            "ewma_alpha=%.2f  min_intra_ticks=%d  floor_frac=%.2f",
            MARKET_WINDOW_SECONDS, PRICE_BUFFER,
            "ENABLED" if USE_INTRA_WINDOW_VOL else "DISABLED",
            EWMA_ALPHA, MIN_INTRA_TICKS, SIGMA_FLOOR_FRACTION,
        )

    # ── Main loop ──────────────────────────────────────────────────────────────

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                raw = await asyncio.get_event_loop().run_in_executor(
                    None, self._recv_with_timeout, 0.05
                )
            except Exception:
                continue
            if raw is not None:
                self._on_tick(raw)
        log.info("FVEngine stopped.")

    def _recv_with_timeout(self, timeout_s: float) -> bytes | None:
        if self._sub.poll(int(timeout_s * 1000)):
            return self._sub.recv()
        return None

    # ── Strike snapping (Bug 1 fix) ────────────────────────────────────────────

    def _snap_strike(self, mid: float, now_ts: float) -> None:
        """
        Detect 5-minute boundary crossings and snapshot BTC mid as the new K.

        Called on every tick BEFORE computing BS probability.
        On the very first tick, K is initialised to the current mid so the
        engine is immediately usable (avoids a 5-min wait for the first snap).
        On subsequent boundary crossings, K resets to the BTC price at that
        exact moment — matching how Polymarket's oracle anchors UP/DOWN.
        """
        boundary = int(now_ts) - (int(now_ts) % MARKET_WINDOW_SECONDS)

        if boundary != self._current_boundary:
            old_k = self._strike
            self._strike           = mid
            self._window_end_ts    = boundary + MARKET_WINDOW_SECONDS
            self._current_boundary = boundary

            # Reset the intra-window buffer so vol computation for the new
            # window starts clean — no contamination from previous window prices.
            prev_intra_len = len(self._intra_window_prices)
            self._intra_window_prices.clear()

            if old_k == 0.0:
                log.info(
                    "Strike initialised: K=%.2f  window_end=%s",
                    self._strike,
                    time.strftime("%H:%M:%S", time.gmtime(self._window_end_ts)),
                )
            else:
                log.info(
                    "New window — K reset: %.2f → %.2f  (prev window end %s)  "
                    "intra_buf cleared (%d ticks dropped)",
                    old_k, self._strike,
                    time.strftime("%H:%M:%S", time.gmtime(self._window_end_ts)),
                    prev_intra_len,
                )

    # ── Tick handler ──────────────────────────────────────────────────────────

    def _on_tick(self, raw: bytes) -> None:
        payload   = unpack(raw)
        _ts_ms, bid, ask = payload
        mid       = (bid + ask) / 2.0
        now_ts    = time.time()

        # ── Measure tick interval for accurate σ scaling ──────────────────────
        if self._last_tick_t > 0:
            interval = now_ts - self._last_tick_t
            if 0.001 < interval < 5.0:   # ignore outliers (e.g. after reconnect)
                self._tick_intervals.append(interval)
        self._last_tick_t = now_ts

        # ── Snap strike at window boundary ────────────────────────────────────
        self._snap_strike(mid, now_ts)

        self._prices.append(mid)
        self._intra_window_prices.append(mid)   # Task 1.1: intra-window buffer
        self._tick_count += 1

        # ── Warmup guard ──────────────────────────────────────────────────────
        if len(self._prices) < MIN_BUFFER_FILL:
            if self._tick_count % 10 == 0:
                log.info("Warming up: %d/%d ticks  mid=%.2f", len(self._prices), MIN_BUFFER_FILL, mid)
            return

        # ── σ calculation (Task 1.3 — intra-window EWMA) ────────────────────────
        # Use measured average tick interval so annualization is accurate.
        avg_interval = (
            statistics.mean(self._tick_intervals)
            if len(self._tick_intervals) >= 5
            else 1.0
        )
        T_remaining_s = max(0.0, self._window_end_ts - now_ts)
        sigma, intra_raw, self._is_sigma_real = self._compute_sigma(avg_interval, T_remaining_s)

        # ── Time to expiry ────────────────────────────────────────────────────
        T = time_to_expiry_years(self._window_end_ts)

        # ── Fair value ────────────────────────────────────────────────────────
        K        = self._strike
        prob_up  = black_scholes_prob(mid, K, T, sigma)
        prob_down = 1.0 - prob_up
        self._last_fv = prob_up

        # ── Publish ───────────────────────────────────────────────────────────
        timestamp_ms = int(time.time() * 1000)
        # Publish boundary_ts (int) instead of static MARKET_ID so consumers
        # can match this FV exactly to the PM market with the same market_ts.
        # Schema (Task 4.1): 9-element list.
        # Fields 0-7 are unchanged — old consumers degrade gracefully.
        # [ts_ms, boundary_ts, prob_up, prob_down, sigma, btc_price, intra_vol, is_sigma_real, strike]
        msg = pack([
            timestamp_ms,
            self._current_boundary,
            prob_up,
            prob_down,
            sigma,
            mid,
            intra_raw,
            int(self._is_sigma_real),
            self._strike,
        ])
        self._pub.send(msg)

        # ── Terminal output ───────────────────────────────────────────────────
        now_mono   = time.monotonic()
        fv_moved   = abs(prob_up - self._last_log_fv) >= LOG_THRESHOLD
        status_due = (now_mono - self._last_status_t) >= STATUS_INTERVAL_S

        if fv_moved or status_due:
            mins_left  = max(0.0, (self._window_end_ts - now_ts) / 60.0)
            self._print_tick(mid, K, prob_up, prob_down, sigma, intra_raw,
                             self._is_sigma_real, T, mins_left, avg_interval)
            self._last_log_fv    = prob_up
            self._last_status_t  = now_mono

            # O2: emit fv_status metric every STATUS_INTERVAL_S seconds
            # (or when FV moves > LOG_THRESHOLD) for the dashboard.
            btc_delta_pct = ((mid - K) / K * 100) if K > 0 else 0.0
            emit_metric(
                "fv_status",
                ts_ms         = timestamp_ms,
                sigma         = round(sigma, 4),
                intra_vol     = round(intra_raw, 4),
                is_sigma_real = self._is_sigma_real,
                prob_up       = round(prob_up, 4),
                btc_price     = round(mid, 2),
                strike        = round(K, 2),
                btc_delta_pct = round(btc_delta_pct, 4),
                t_remaining_s = round(max(0.0, self._window_end_ts - now_ts), 1),
                boundary_ts   = self._current_boundary,
            )

    # ── Sigma computation (Task 1.3) ──────────────────────────────────────────

    def _dynamic_floor(self, avg_interval: float, T_remaining_s: float) -> float:
        """
        Dynamic sigma floor based on recent cross-window vol.

        floor = max(SIGMA_FLOOR_FRACTION × cross_window_vol, SIGMA_FLOOR_MIN)
                × sqrt(T_remaining / MARKET_WINDOW_SECONDS)

        This replaces the fixed MIN_SIGMA_FLOOR=0.50 that caused 261/261 floor
        hits.  The floor is now proportional to actual market vol and collapses
        to zero as expiry approaches — same as the old scaled-floor behaviour,
        but the base is empirical rather than hardcoded.
        """
        cross_sigma = annualize_vol(self._prices, interval_s=avg_interval)
        # If cross-window buffer is still warming up, cross_sigma may be 0.
        # SIGMA_FLOOR_MIN (default 0.10) provides a hard lower bound.
        floor_base = max(cross_sigma * SIGMA_FLOOR_FRACTION, SIGMA_FLOOR_MIN)
        scale = math.sqrt(T_remaining_s / MARKET_WINDOW_SECONDS)
        return floor_base * scale

    def _compute_sigma(
        self,
        avg_interval: float,
        T_remaining_s: float,
    ) -> tuple[float, float, bool]:
        """
        Compute sigma for the Black-Scholes probability calculation.

        Returns
        -------
        (sigma, intra_raw, is_sigma_real)
            sigma         — value to pass to black_scholes_prob()
            intra_raw     — raw EWMA estimate before floor application (0.0 if
                            cross-window fallback was used), for logging
            is_sigma_real — True when sigma comes from ≥ MIN_INTRA_TICKS
                            intra-window prices; False during warmup or if
                            USE_INTRA_WINDOW_VOL=0

        Paths
        -----
        Primary (USE_INTRA_WINDOW_VOL=1, buffer ≥ MIN_INTRA_TICKS):
            intra_sigma = ewma_vol(_intra_window_prices, EWMA_ALPHA, avg_interval)
            is_sigma_real = intra_sigma > 0.001
            sigma = max(intra_sigma, _dynamic_floor())

        Fallback (warmup or USE_INTRA_WINDOW_VOL=0):
            cross_sigma = annualize_vol(_prices, avg_interval)
            is_sigma_real = False
            sigma = max(cross_sigma, _dynamic_floor())
        """
        floor = self._dynamic_floor(avg_interval, T_remaining_s)

        if USE_INTRA_WINDOW_VOL and len(self._intra_window_prices) >= MIN_INTRA_TICKS:
            intra_raw = ewma_vol(
                self._intra_window_prices,
                alpha=EWMA_ALPHA,
                interval_s=avg_interval,
            )
            is_sigma_real = intra_raw > 0.001
            sigma = max(intra_raw, floor)
            return sigma, intra_raw, is_sigma_real

        # Fallback: cross-window path (warmup or disabled)
        cross_sigma = annualize_vol(self._prices, interval_s=avg_interval)
        sigma = max(cross_sigma, floor)
        return sigma, 0.0, False

    @staticmethod
    def _print_tick(
        mid:           float,
        K:             float,
        prob_up:       float,
        prob_down:     float,
        sigma:         float,
        intra_raw:     float,
        is_sigma_real: bool,
        T:             float,
        mins_left:     float,
        tick_int:      float,
    ) -> None:
        """Colour-coded terminal output for validation."""
        def _col(p: float) -> str:
            if p > 0.70 or p < 0.30:
                return f"\033[32m{p:.4f}\033[0m"   # green — strong signal
            elif p > 0.55 or p < 0.45:
                return f"\033[33m{p:.4f}\033[0m"   # yellow — mild edge
            else:
                return f"\033[37m{p:.4f}\033[0m"   # white — near 50/50

        # σ source indicator: [EWMA] when using real intra-window vol,
        # [XWIN] when falling back to cross-window buffer.
        sigma_src = "\033[32m[EWMA]\033[0m" if is_sigma_real else "\033[33m[XWIN]\033[0m"
        raw_str = f" raw={intra_raw:.3f}" if is_sigma_real and intra_raw > 0 else ""

        print(
            f"  BTC={mid:>10.2f}  K={K:>10.2f}  Δ={mid-K:>+7.2f}  "
            f"P(UP)={_col(prob_up)}  P(DN)={_col(prob_down)}  "
            f"σ={sigma:.3f}{raw_str} {sigma_src}  "
            f"T={T*365.25*24*60:>5.1f}m  left={mins_left:.1f}m  "
            f"tick={tick_int*1000:.0f}ms",
            flush=True,
        )


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    try:
        import uvloop
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        log.info("Event loop: uvloop")
    except ImportError:
        log.info("Event loop: asyncio (uvloop not available)")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stop_event = asyncio.Event()

    def _handle_signal(sig_num, _frame):
        log.info("Signal %s — shutting down.", sig_num)
        loop.call_soon_threadsafe(stop_event.set)

    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    engine = FVEngine()
    log.info(
        "Subscribing to %s  →  publishing to %s",
        Channel.BINANCE_BBO, Channel.FV_STREAM,
    )
    print(
        f"\n  {'BTC':>10}  {'K':>10}  {'Δ':>7}  "
        f"{'P(UP)':>8}  {'P(DN)':>8}  {'sigma':>7}  "
        f"{'T(m)':>6}  {'left(m)':>7}  tick"
    )
    print("─" * 95)

    try:
        loop.run_until_complete(engine.run(stop_event))
    finally:
        loop.close()
        log.info("ZMQ sockets closed.")


if __name__ == "__main__":
    main()
