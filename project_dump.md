# Project Snapshot

Root: `/home/ziga/workspace/btc-bot-status`

## Included Files

- `cmd/smart_paper_trader.py`
- `find_best_params.sh`
- `optimization_results/optimization_results.csv`
- `strategies/base.py`
- `strategies/tos_signal/__init__.py`
- `strategies/tos_signal/config.py`
- `strategies/tos_signal/signal_stack.py`
- `strategies/tos_signal/stats.py`
- `strategies/tos_signal/strategy.py`
- `tools/optimizer.py`
- `tools/parameter_registry.py`

---

## cmd/smart_paper_trader.py

```py
"""
cmd/smart_paper_trader.py
─────────────────────────
Phase 3.5 — Smart Paper Trader (Real Validator).

The Phase 3 paper_trader.py proved the pipeline works end-to-end.
This replaces it with proper position management so P&L is meaningful:

  • Position cap per side per market — stops unlimited stacking
  • Take-profit exit — sell when unrealised gain ≥ TAKE_PROFIT_PCT
  • Stop-loss exit  — sell when unrealised loss ≥ STOP_LOSS_PCT
  • Uses PM best *bid* as the exit price (realistic taker-sell price)
  • Tracks exit reasons so you can see what is actually driving P&L

Subscribes to:
    Channel.FV_STREAM  — [ts_ms, market_id, prob_up, prob_down, sigma, btc_price]
    Channel.PM_BOOK    — [ts_ms, market_id, bid_up, ask_up, bid_down, ask_down]
                         (requires pm_daemon.py patch — see NOTES below)

NOTES
-----
pm_daemon.py publishes [ts_ms, market_id, ask_up, ask_dn] today (4 fields).
Phase 3.5 needs bids too.  Two options:

  Option A (recommended): patch pm_daemon._publish_book() to publish
    [ts_ms, market_id, bid_up, ask_up, bid_dn, ask_dn]
  and update Channel.PM_BOOK consumers accordingly.

  Option B (fallback, used when BIDS_IN_PM_BOOK=0 env var):
  This file falls back to treating ask as bid proxy
  (i.e. exit at the ask, which OVERSTATES exit proceeds — conservative).
  Set BIDS_IN_PM_BOOK=0 in .env to run without patching pm_daemon first.

Run
---
    python -m cmd.smart_paper_trader     # from repo root
    python cmd/smart_paper_trader.py     # direct

Validation gate (Phase 3.5 → Phase 4)
--------------------------------------
Run ≥ 24h across multiple markets.  All gates must pass:

    Net P&L > 0 USDC
    Win rate (settled markets) > 52%
    Mean FV age at fill < 500ms
    Stop-loss exits exist AND are smaller avg loss than full settlement loss
    Sigma NOT stuck at floor for > 90% of fills (if it is: ring buffer too short)
"""

from __future__ import annotations

import asyncio
import json
import logging
from shared.log_setup import setup_logging
from shared.metrics import emit as emit_metric
import os
import signal
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from pathlib import Path
from statistics import NormalDist
from typing import Optional

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

import zmq
import zmq.asyncio as azmq
import aiohttp

from shared.ipc import Channel, unpack
from shared.math_utils import sign
from core.risk import RiskManagerV2
from strategies.base import BaseStrategy, EntrySignal
from strategies.tos.strategy import TOSStrategy
from strategies.tos_signal.strategy import TOSSignalStrategy
from strategies.tos_signal.signal_stack import SignalStack

from strategies.base import FVState, PMState, Position, EntrySignal, BaseStrategy

log = setup_logging("smart_paper_trader")

# ── Config ─────────────────────────────────────────────────────────────────────

# Whether pm_daemon publishes bids (6-field schema).
# Set BIDS_IN_PM_BOOK=0 in .env to run in fallback mode (ask as exit proxy).
BIDS_IN_PM_BOOK: bool = os.getenv("BIDS_IN_PM_BOOK", "1") != "0"

# Minimum edge (FV − PM ask) to simulate an entry fill.
MIN_EDGE_THRESHOLD: float = float(os.getenv("MIN_EDGE_THRESHOLD", "0.03"))

# Minimum ask price required to enter a position.
# Guards against near-expired markets where PM has priced a side to 0.01–0.02
# but the sigma floor keeps FV near 0.50, producing a fake 50% edge.
# Any ask below this threshold means the market has already priced in the
# outcome — our FV simply hasn't caught up yet.
MIN_ENTRY_ASK: float = float(os.getenv("MIN_ENTRY_ASK", "0.05"))

# FV extreme-value gate (Task 1.5 fix).
# When FV ≥ FV_ENTRY_MAX or FV ≤ FV_ENTRY_MIN the market is essentially decided:
# BTC is far above/below K with little time left.  Black-Scholes collapses toward
# 1.0 / 0.0, creating a fake "edge" against a PM price of 0.88–0.97.  Entering
# here means buying into a nearly-settled market for a slim profit that evaporates
# if BTC mean-reverts even $50.
# Root cause of 24/45 fills at FV=1.000 in the Task 1.4 paper run.
FV_ENTRY_MAX: float = float(os.getenv("FV_ENTRY_MAX", "0.97"))
FV_ENTRY_MIN: float = float(os.getenv("FV_ENTRY_MIN", "0.03"))

# Out-of-band kill switch. If this file exists, trading halts.
KILL_SWITCH_FILE: str = os.getenv("KILL_SWITCH_FILE", "/tmp/btcbot_halt")

# Simulated shares per entry fill.
PAPER_TRADE_SHARES: float = float(os.getenv("PAPER_TRADE_SHARES", "5.0"))

# Maximum shares held per side per market at any time.
# Stops unlimited stacking on a single binary outcome.
# Implementation plan specifies 10 (2 fills of 5 shares each).
MAX_SHARES_PER_SIDE: float = float(os.getenv("MAX_SHARES_PER_SIDE", "10.0"))  # 2 fills

# ── Legacy flat TP/SL (kept for reference — not used when USE_TIME_AWARE_EXITS=1) ──
# These were the original Phase 3 thresholds. They fired mid-window regardless of
# time remaining, causing the bot to cut positions that were still viable bets and
# then immediately re-enter lower — a buy-and-sell cascade that destroys value.
TAKE_PROFIT_PCT: float = float(os.getenv("TAKE_PROFIT_PCT", "0.35"))
STOP_LOSS_PCT: float   = float(os.getenv("STOP_LOSS_PCT",   "0.25"))

# ── Time-Aware Exit System ─────────────────────────────────────────────────────
# Binary markets settle at $1 or $0 — that IS the target exit.
# Mid-window prices are noise around the eventual outcome.
# This system only exits mid-window when we are TRULY convinced the bet is wrong.
#
# The window is split into three zones:
#   EARLY (more than LATE_WINDOW_SECONDS remaining)
#     → Hold unconditionally. Do not SL. The only exit is a very high bid price
#       indicating the market has already confirmed the thesis.
#   LATE  (between EMERGENCY_SECONDS and LATE_WINDOW_SECONDS remaining)
#     → Light exits: cut only if bid has collapsed to near zero (nearly dead).
#       Take profit if the bid is high enough that most of the value is captured.
#   EMERGENCY (fewer than EMERGENCY_SECONDS remaining)
#     → Double-confirmation cut: bid is very low AND our FV model also says we
#       are losing. Both signals must agree — market price alone is not enough.
#     → Also lock in high-confidence wins near settlement.

USE_TIME_AWARE_EXITS: bool = os.getenv("USE_TIME_AWARE_EXITS", "1") != "0"

# EARLY zone: only exit if bid has risen so high that holding adds little extra value
# e.g. bid=0.88 with 3 min left → already captured ~88% of max payout → lock in
EARLY_HIGH_CONFIDENCE_BID: float = float(os.getenv("EARLY_HIGH_CONFIDENCE_BID", "0.88"))

# LATE zone: last N seconds before this window starts light exits
LATE_WINDOW_SECONDS: float = float(os.getenv("LATE_WINDOW_SECONDS", "120.0"))
# Cut if bid collapses to this level in the late zone (market says ~8% chance of winning)
LATE_SL_FLOOR: float       = float(os.getenv("LATE_SL_FLOOR",       "0.08"))
# Take profit in late zone if bid exceeds this (locked-in majority of value)
LATE_TP_BID: float         = float(os.getenv("LATE_TP_BID",         "0.82"))

# EMERGENCY zone: last N seconds — near-certainty exits only
EMERGENCY_SECONDS: float   = float(os.getenv("EMERGENCY_SECONDS",   "60.0"))
# Cut if bid is THIS low (market saying ~12% chance) AND FV also turned against us
EMERGENCY_CUT_PRICE: float = float(os.getenv("EMERGENCY_CUT_PRICE", "0.12"))
# FV must also be below this to trigger emergency cut (double-confirmation)
EMERGENCY_FV_CONFIRM: float = float(os.getenv("EMERGENCY_FV_CONFIRM", "0.30"))
# Lock in near-certain win in emergency zone
EMERGENCY_TP_BID: float    = float(os.getenv("EMERGENCY_TP_BID",    "0.88"))

# FV stale threshold (ms) — skip PM tick if FV is older than this.
FV_STALE_MS: float = float(os.getenv("FV_STALE_MS", "500"))  # tightened from Phase 3's 10s

# Must match MARKET_WINDOW_SECONDS in fv_engine.py (both read from .env).
MARKET_WINDOW_SECONDS: int = int(os.getenv("MARKET_WINDOW_SECONDS", "300"))

# Sigma floor — must match MIN_SIGMA_FLOOR in core/fv_engine.py (both read from .env).
# Used to detect when the FV engine is running on the floor rather than real vol.
MIN_SIGMA_FLOOR: float = float(os.getenv("MIN_SIGMA_FLOOR", "0.50"))

# Fill cooldown per side per market (ms).
FILL_COOLDOWN_MS: float = float(os.getenv("FILL_COOLDOWN_MS", "5000"))

# Minimum seconds into the current 5-min window before entries are allowed.
# Blocks the noisy early-window period when K has just snapped and sigma is
# still reflecting carry-over data from the previous window.
# 100s matches observed behaviour of profitable PM bots. Set to 0 to disable.
MIN_WINDOW_AGE_S: float = float(os.getenv("MIN_WINDOW_AGE_S", "100"))

# Architecture A: Terminal Oracle Sniper entry policy.
ENTRY_POLICY: str = os.getenv("ENTRY_POLICY", "legacy").strip().upper()
TOS_ENTRY_START_S: float = float(os.getenv("TOS_ENTRY_START_S", "210"))
TOS_ENTRY_END_S: float = float(os.getenv("TOS_ENTRY_END_S", "270"))
TOS_MIN_PROB: float = float(os.getenv("TOS_MIN_PROB", "0.70"))
TOS_MIN_EDGE: float = float(os.getenv("TOS_MIN_EDGE", "0.05"))
TOS_MIN_LIQUIDITY: float = float(os.getenv("TOS_MIN_LIQUIDITY", "20.0"))
_TOS_DEFAULT_Z_THRESHOLD: float = NormalDist().inv_cdf(
    min(max(TOS_MIN_PROB, 1e-9), 1.0 - 1e-9)
)
TOS_Z_THRESHOLD: float = float(os.getenv("TOS_Z_THRESHOLD", str(_TOS_DEFAULT_Z_THRESHOLD)))

# Architecture A: TOS holds positions to settlement — no mid-window exits.
# Set EXIT_POLICY=TOS when running TOS paper-trade experiments so that
# mid-window TP/SL events do not contaminate win-rate and expectancy numbers.
# Default "legacy" preserves the existing time-aware exit behaviour.
EXIT_POLICY: str = os.getenv("EXIT_POLICY", "legacy").strip().upper()

# Task 2.5: Gamma API resolution for settlement.
# GAMMA_HOST must match config.py (both read the same env var).
GAMMA_HOST: str = os.getenv("GAMMA_HOST", "https://gamma-api.polymarket.com")
# Timeout (seconds) for a single one-shot resolution query.
# 2s is enough for a REST GET; the market already closed so latency is low-priority.
RESOLUTION_TIMEOUT_S: float = float(os.getenv("RESOLUTION_TIMEOUT_S", "2.0"))

# Task R3: enable position reconciliation on startup (live trading path only).
# Paper trading default is False — reconciliation is a no-op in paper mode.
LIVE_TRADING: bool = os.getenv("LIVE_TRADING", "0").lower() in ("1", "true", "yes")


# Where to write fill + exit records.
FILLS_PATH: Path   = Path(os.getenv("FILLS_PATH",   "smart_fills.jsonl"))
EXITS_PATH: Path   = Path(os.getenv("EXITS_PATH",   "smart_exits.jsonl"))

# Status print interval (seconds).
STATUS_INTERVAL_S: float = float(os.getenv("STATUS_INTERVAL_S", "15.0"))

# ── Architecture B — Dual-Leg Arb Scanner (Phase 3, Task 3.2) ────────────────
# Disabled by default. Set ARB_ENABLED=1 to activate the paper arb scanner.
# The scanner loop always runs but is a no-op when ARB_ENABLED=0, so enabling
# it never requires a restart of the entire trader — just flip the env var.
ARB_ENABLED: bool = os.getenv("ARB_ENABLED", "0") != "0"

# Fire an arb when ask_up + ask_dn < this value.
# At 0.96 the guaranteed edge = 1.00 − 0.96 = 0.04 per share (4%).
# After Polymarket taker fees (~2% per leg), net edge ≈ 0%→2% — use with care.
# Lower this threshold to be more selective; 0.90 is very conservative.
ARB_TARGET_COMBINED: float = float(os.getenv("ARB_TARGET_COMBINED", "0.96"))

# Maximum USDC committed per arb opportunity (both legs combined).
ARB_MAX_USDC: float = float(os.getenv("ARB_MAX_USDC", "4.0"))

# Minimum shares per leg to bother entering (Polymarket exchange minimum is 5).
ARB_MIN_SHARES: float = float(os.getenv("ARB_MIN_SHARES", "5.0"))

# Arb scanner poll interval in milliseconds (how often to check PM state).
ARB_SCAN_INTERVAL_MS: float = float(os.getenv("ARB_SCAN_INTERVAL_MS", "50.0"))

_STD_NORMAL = NormalDist()


def probability_to_z(prob_up: float) -> float:
    """Convert UP probability to signed model-implied standard-normal z-score."""
    p = min(max(float(prob_up), 1e-9), 1.0 - 1e-9)
    return _STD_NORMAL.inv_cdf(p)


# ── Data classes ───────────────────────────────────────────────────────────────



@dataclass
class EntryRecord:
    """Persisted when a simulated entry fill happens."""
    event:          str   = "ENTRY"
    ts_ms:          int   = 0
    market_id:      str   = ""
    side:           str   = ""
    ask:            float = 0.0
    shares:         float = 0.0
    cost:           float = 0.0
    fv:             float = 0.0
    edge:           float = 0.0
    btc_price:      float = 0.0
    sigma:          float = 0.0
    fv_age_ms:      int   = 0
    # Task 1.5: sigma quality fields — essential for post-run diagnosis.
    # Without these we cannot tell from smart_fills.jsonl whether the
    # is_sigma_real gate was True or False at fill time.
    is_sigma_real:  bool  = False   # True = sigma from intra-window EWMA
    intra_vol:      float = 0.0     # raw EWMA vol before floor (0 during warmup)
    # Task 2.6: TOS analysis fields — slice win-rate by z-score bucket,
    # time-in-window, and window identity after a paper run.
    z_score:        float = 0.0     # model-implied z at fill time (FVState.z_score)
    elapsed_s:      float = 0.0     # seconds since window start at fill time
    window_start_ts: int  = 0       # unix epoch of window open (= market_ts)
    window_end_ts:  int   = 0       # unix epoch of window resolution (= end_ts)


@dataclass
class ExitRecord:
    """Persisted when a position is exited (take-profit, stop-loss, or settlement)."""
    event:          str   = "EXIT"
    ts_ms:          int   = 0
    market_id:      str   = ""
    side:           str   = ""
    shares:         float = 0.0
    avg_entry:      float = 0.0
    exit_price:     float = 0.0
    cost:           float = 0.0
    proceeds:       float = 0.0
    pnl:            float = 0.0
    pnl_pct:        float = 0.0
    exit_reason:    str   = ""   # TAKE_PROFIT | STOP_LOSS | SETTLEMENT


@dataclass
class ArbPosition:
    """
    Task 3.2 — Dual-leg structural arb position (UP + DOWN in the same window).

    Buying N shares of UP and N shares of DOWN costs N × combined.
    At settlement exactly one leg pays $1.00/share, the other $0.00.
    Total proceeds = N × $1.00, guaranteed regardless of outcome.
    Edge = N × (1.0 − combined) — always positive when combined < 1.0.
    """
    market_id: str
    combined:  float   # ask_up + ask_dn at entry time
    shares:    float   # shares per leg (identical for both legs)
    cost:      float   # combined × shares
    ts_ms:     int     # entry timestamp ms

    @property
    def guaranteed_proceeds(self) -> float:
        """Exactly one leg pays $1.00/share at settlement."""
        return self.shares * 1.0

    @property
    def expected_pnl(self) -> float:
        """Risk-free P&L locked in at entry (pre-fees)."""
        return self.guaranteed_proceeds - self.cost


@dataclass
class Stats:
    """Running totals for status display."""
    entries:            int   = 0
    total_cost:         float = 0.0
    total_proceeds:     float = 0.0
    exits_take_profit:  int   = 0
    exits_stop_loss:    int   = 0
    exits_settlement:   int   = 0
    exits_emergency:    int   = 0   # emergency cuts near expiry
    settled_markets:    int   = 0
    settlement_wins:    int   = 0   # positions held to settlement and WON ($1)
    settlement_losses:  int   = 0   # positions held to settlement and LOST ($0)
    cap_blocks:         int   = 0   # times entry was blocked by position cap
    stale_skips:        int   = 0
    window_mismatches:  int   = 0
    # Backward-compatible field name; now counts entries whose FV sigma was not
    # sourced from real intra-window EWMA volatility.
    sigma_at_floor:     int   = 0
    sigma_total:        int   = 0
    # Task 3.2 — Architecture B arb-specific counters (separate from TOS).
    # arb_cost / arb_proceeds are NOT included in total_cost / total_proceeds
    # so TOS and arb P&L remain independently auditable.
    arb_entries:        int   = 0
    arb_settled:        int   = 0
    arb_cost:           float = 0.0
    arb_proceeds:       float = 0.0   # passed all signal gates → entry attempted

    def reset_for_market(self) -> None:
        """Reset all per-market counters on window transition.

        Intentionally NOT reset (session-level lifetime totals):
          settled_markets, settlement_wins, settlement_losses
        """
        # Core trade stats
        self.entries           = 0
        self.total_cost        = 0.0
        self.total_proceeds    = 0.0
        # Exit breakdown
        self.exits_take_profit = 0
        self.exits_stop_loss   = 0
        self.exits_settlement  = 0
        self.exits_emergency   = 0
        # Diagnostic counters
        self.cap_blocks        = 0
        self.stale_skips       = 0
        self.window_mismatches = 0
        self.sigma_at_floor    = 0
        self.sigma_total       = 0
        # Arb stats (separate P&L track, also per-market)
        self.arb_entries       = 0
        self.arb_settled       = 0
        self.arb_cost          = 0.0
        self.arb_proceeds      = 0.0

    @property
    def net_pnl(self) -> float:
        return self.total_proceeds - self.total_cost

    @property
    def arb_net_pnl(self) -> float:
        """Net P&L from arb trades (always ≥ 0 in paper mode if combined < 1.0)."""
        return self.arb_proceeds - self.arb_cost

    @property
    def arb_roi_pct(self) -> float:
        return (self.arb_net_pnl / self.arb_cost * 100) if self.arb_cost > 0 else 0.0

    @property
    def roi_pct(self) -> float:
        return (self.net_pnl / self.total_cost * 100) if self.total_cost > 0 else 0.0

    @property
    def total_exits(self) -> int:
        return (self.exits_take_profit + self.exits_stop_loss
                + self.exits_settlement + self.exits_emergency)

    @property
    def sigma_not_real_pct(self) -> float:
        """Percent of entries where sigma came from warmup/fallback, not EWMA."""
        return (
            self.sigma_at_floor / self.sigma_total * 100
            if self.sigma_total > 0 else 0.0
        )

    @property
    def win_rate(self) -> float:
        """
        True win rate for binary markets.

        WINNING = position closed profitably:
          - Mid-window take-profit (we captured the edge early)
          - Settlement win (market resolved in our favour at $1)

        LOSING = position closed at a loss:
          - Mid-window stop-loss / emergency cut
          - Settlement loss (market resolved against us at $0)

        This is the correct metric for this strategy. The old win_rate
        only counted mid-window TP vs SL and excluded settlements entirely,
        making it useless for evaluating a hold-to-settlement approach.
        """
        wins   = self.exits_take_profit + self.settlement_wins
        losses = self.exits_stop_loss + self.exits_emergency + self.settlement_losses
        decided = wins + losses
        return (wins / decided * 100) if decided > 0 else 0.0

    @property
    def settlement_win_rate(self) -> float:
        """Win rate on positions that were held all the way to settlement."""
        decided = self.settlement_wins + self.settlement_losses
        return (self.settlement_wins / decided * 100) if decided > 0 else 0.0


# ── Smart Paper Trader ─────────────────────────────────────────────────────────

class SmartPaperTrader:
    """
    Phase 3.5 paper trading engine with position management.

    Entry logic is identical to paper_trader.py.
    Adds:
      - MAX_SHARES_PER_SIDE cap to prevent unlimited stacking
      - Take-profit exit when unrealised gain >= TAKE_PROFIT_PCT
      - Stop-loss exit when unrealised loss >= STOP_LOSS_PCT
      - Exit evaluation on every PM book tick (not just at settlement)
      - Separate fills and exits logs for analysis
    """

    def __init__(self) -> None:
        ctx = azmq.Context.instance()

        from shared.ipc import _resolve_addr, Channel
        
        self._is_replay = os.getenv("REPLAY_MODE") == "1"
        # CRITICAL: In replay mode, start at 0. Let the first message set the clock.
        # Using wall-clock here breaks fv_age_ms and RiskManager hourly windows.
        self._current_ts_ms = 0 if self._is_replay else int(time.time() * 1000)
        
        
        self.replay_stats = {"fv_received": 0, "pm_received": 0}
        
        if self._is_replay:
            replay_addr = _resolve_addr(Channel.REPLAY_STREAM)
            self._replay_sub = ctx.socket(zmq.PULL)
            # Use backpressure instead of infinite queue
            self._replay_sub.set_hwm(10000)
            self._replay_sub.connect(replay_addr)
            self._ready_sock = ctx.socket(zmq.PUSH)
            self._ready_sock.connect(_resolve_addr(Channel.REPLAY_READY))
        else:
            fv_addr = _resolve_addr(Channel.FV_STREAM)
            pm_addr = _resolve_addr(Channel.PM_BOOK)

            self._fv_sub = ctx.socket(zmq.SUB)
            self._fv_sub.set_hwm(1000)
            self._fv_sub.connect(fv_addr)
            self._fv_sub.setsockopt(zmq.SUBSCRIBE, b"")

            self._pm_sub = ctx.socket(zmq.SUB)
            self._pm_sub.set_hwm(1000)
            self._pm_sub.connect(pm_addr)
            self._pm_sub.setsockopt(zmq.SUBSCRIBE, b"")

        self._fv  = FVState()
        self._pm  = PMState()
        self._stats = Stats()
        self._entry_policy = ENTRY_POLICY
        self._exit_policy  = EXIT_POLICY   #  Task 2.4: TOS holds to settlement
        self._risk = RiskManagerV2()
        # Initialize isolated strategy based on entry policy
        if self._entry_policy == "TOS_SIGNAL":
            from strategies.tos_signal.strategy import TOSSignalStrategy
            self._strategy = TOSSignalStrategy()
        elif self._entry_policy == "TOS":
            from strategies.tos.strategy import TOSStrategy
            self._strategy = TOSStrategy()
        else:
            self._strategy = None


        # Task 2.5: cache market resolution outcomes to avoid repeated Gamma queries.
        # Key: market_id (str)  Value: "UP", "DOWN", or None (unknown)
        self._resolve_cache: dict[str, Optional[str]] = {}
        # market_id -> last matched-window FV probability. Used only as the
        # fallback settlement proxy when Gamma has not resolved yet.
        self._settlement_proxy_prob_up: dict[str, float] = {}

        # market_id -> {side -> Position}
        self._positions: dict[str, dict[str, Position]] = defaultdict(dict)

        # market_id -> {side -> last_fill_ts_ms}
        self._last_fill_ts: dict[str, dict[str, float]] = defaultdict(dict)

        self._last_pm_market: str = ""

        # Task 3.2 — Architecture B: arb positions keyed by market_id.
        # Separate from _positions (TOS) so P&L is independently auditable.
        # Cleared at settlement; at most one entry per market window.
        self._arb_positions: dict[str, ArbPosition] = {}

                
        # Task R3: Exchange handle for startup position reconciliation.
        # None in paper trading mode; set to an Exchange instance before run()
        # when LIVE_TRADING=True.
        self._exchange: Optional[object] = None

        self._fills_file = FILLS_PATH.open("a", buffering=1)
        self._exits_file = EXITS_PATH.open("a", buffering=1)

        log.info(
            "SmartPaperTrader ready -- "
            "edge=%.0f%%  max_shares=%.0f  tp=%.0f%%  sl=%.0f%%  stale=%.0fms  bids=%s  "
            "fv_gate=[%.2f,%.2f]  entry_policy=%s  exit_policy=%s",
            MIN_EDGE_THRESHOLD * 100,
            MAX_SHARES_PER_SIDE,
            TAKE_PROFIT_PCT * 100,
            STOP_LOSS_PCT * 100,
            FV_STALE_MS,
            "YES" if BIDS_IN_PM_BOOK else "NO (ask proxy)",
            FV_ENTRY_MIN, FV_ENTRY_MAX,
            self._entry_policy,
            self._exit_policy,
        )
    def _strategy(self, value):
        self.__dict__['_strategy_inst'] = value

    @property
    def _btc_history(self):
        if self._strategy and hasattr(self._strategy, '_btc_history'):
            return self._strategy._btc_history
        return self.__dict__.get('_fallback_btc_history', deque(maxlen=600))

    @_btc_history.setter
    def _btc_history(self, value):
        if self._strategy and hasattr(self._strategy, '_btc_history'):
            self._strategy._btc_history = value
        else:
            self.__dict__['_fallback_btc_history'] = value

    @property
    def _signal_stack(self):
        if self._strategy and hasattr(self._strategy, '_signal_stack'):
            return self._strategy._signal_stack
        return self.__dict__.get('_fallback_signal_stack', SignalStack())

    @_signal_stack.setter
    def _signal_stack(self, value):
        if self._strategy and hasattr(self._strategy, '_signal_stack'):
            self._strategy._signal_stack = value
        else:
            self.__dict__['_fallback_signal_stack'] = value

    # ── Main loop ──────────────────────────────────────────────────────────────

    async def _reconcile_positions(self) -> None:
        """
        Task R3: On startup (live mode only), query the exchange for existing
        positions and load them into self._positions.

        This prevents double-entering a market after a crash-restart where we
        already hold shares from the previous session.

        Paper mode (LIVE_TRADING=False, the current default) is a deliberate
        no-op — there are no real positions to reconcile.
        """
        if not LIVE_TRADING:
            return
        if self._exchange is None:
            log.warning(
                "LIVE_TRADING=True but no exchange configured — "
                "startup reconciliation skipped"
            )
            return
        open_positions = await self._exchange.get_open_positions()  # type: ignore[union-attr]
        for pos in open_positions:
            self._positions[pos.market_id][pos.side] = Position(
                market_id = pos.market_id,
                side      = pos.side,
                shares    = pos.shares,
                cost      = pos.avg_entry * pos.shares,
            )
        log.info("Reconciled %d open position(s) from exchange", len(open_positions))

    def _update_clock(self, ts_ms: int):
        if ts_ms > self._current_ts_ms:
            self._current_ts_ms = ts_ms
        self._risk.update_time(self._current_ts_ms)

    async def run(self, stop_event: asyncio.Event) -> None:

        async def drain_fv():
            while not stop_event.is_set():
                try:
                    raw = await self._fv_sub.recv()
                    self._on_fv(raw)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    log.debug("FV recv error: %s", e)

        async def drain_pm():
            while not stop_event.is_set():
                try:
                    raw = await self._pm_sub.recv()
                    self._on_pm(raw)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    log.debug("PM recv error: %s", e)
                    
        async def drain_unified():
            """Live processing: keep FV current while processing PM book ticks."""
            poller = azmq.Poller()
            poller.register(self._fv_sub, zmq.POLLIN)
            poller.register(self._pm_sub, zmq.POLLIN)
            
            while not stop_event.is_set():
                await poller.poll(10)

                # FV can arrive 5-15x faster than PM.  Drain any backlog and
                # keep only the newest FV so fresh PM ticks are not evaluated
                # against seconds-old fair value.
                latest_fv = None
                while True:
                    try:
                        latest_fv = await self._fv_sub.recv(flags=zmq.NOBLOCK)
                    except zmq.Again:
                        break
                if latest_fv is not None:
                    self._on_fv(latest_fv)

                # PM is the trigger for entries/exits.  Process all pending PM
                # ticks in arrival order, using the freshest FV available above.
                while True:
                    try:
                        raw_pm = await self._pm_sub.recv(flags=zmq.NOBLOCK)
                    except zmq.Again:
                        break
                    self._on_pm(raw_pm)

        async def drain_replay():
            from shared.ipc import Channel
            while not stop_event.is_set():
                try:
                    parts = await self._replay_sub.recv_multipart()
                    if len(parts) != 2: continue
                    channel, raw = parts
                    ch = channel.decode("utf-8")
                    if ch == "__REPLAY_EOF__":
                        log.info(
                            "Replay EOF received after FV=%s PM=%s",
                            self.replay_stats["fv_received"],
                            self.replay_stats["pm_received"],
                        )
                        await self._ready_sock.send(b"EOF_ACK")
                        stop_event.set()
                        break
                    elif ch == Channel.FV_STREAM:
                        self._on_fv(raw)
                    elif ch == Channel.PM_BOOK:
                        self._on_pm(raw)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    log.debug("Replay recv error: %s", e)

        async def status_ticker():
            while not stop_event.is_set():
                await asyncio.sleep(STATUS_INTERVAL_S)
                if not stop_event.is_set():
                    self._print_status()

        # Task R3: reconcile any positions left open from a prior run before
        # the feed loops start.  No-op in paper mode (LIVE_TRADING=False).
        await self._reconcile_positions()

        tasks = []
        if not self._is_replay:
            tasks.extend([
                asyncio.create_task(status_ticker()),
                asyncio.create_task(self._arb_scanner_loop(stop_event)),
            ])
        
        if self._is_replay:
            tasks.append(asyncio.create_task(drain_replay()))
            await self._ready_sock.send(b"READY")
        else:
            # CRITICAL: Use unified drain to eliminate async race conditions
            tasks.append(asyncio.create_task(drain_unified()))

        await stop_event.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        if self._is_replay:
            log.info(
                "Replay stats: FV=%s PM=%s",
                self.replay_stats["fv_received"],
                self.replay_stats["pm_received"],
            )
        self._fills_file.close()
        self._exits_file.close()
        
        if self._is_replay:
            self._replay_sub.close(linger=0)
            self._ready_sock.close(linger=0)
        else:
            self._fv_sub.close(linger=0)
            self._pm_sub.close(linger=0)
            
        self._ctx.destroy(linger=0)
            
        log.info("SmartPaperTrader stopped.")
        self._print_status(final=True)

    # ── Message handlers ───────────────────────────────────────────────────────

    def _on_fv(self, raw: bytes) -> None:
        self.replay_stats["fv_received"] += 1
        try:
            parsed = unpack(raw)
            if parsed and len(parsed) > 0:
                ts_ms = int(parsed[0])
                if self._fv.ts_ms and ts_ms < self._fv.ts_ms:
                    log.debug("Dropping out-of-order FV tick ts=%s current=%s", ts_ms, self._fv.ts_ms)
                    return
                self._update_clock(ts_ms)
        except Exception as e:
            log.debug("FV parse error: %s", e)
            return

        # Task 1.4: 8-field schema [ts_ms, boundary_ts, prob_up, prob_down,
        #           sigma, btc_price, intra_vol, is_sigma_real].
        # Guard >= 8 for forward-compat; fall back gracefully on old 6-field engine.
        if len(parsed) < 6:
            log.debug("FV message too short: %d fields", len(parsed))
            return

        ts_ms, market_id, prob_up, prob_down, sigma, btc_price = parsed[:6]
        intra_vol     = float(parsed[6]) if len(parsed) >= 7 else 0.0
        is_sigma_real = bool(parsed[7])  if len(parsed) >= 8 else False
        strike        = float(parsed[8]) if len(parsed) >= 9 else 0.0
        boundary_ts = int(market_id) if isinstance(market_id, (int, float)) else 0
        z_score = probability_to_z(float(prob_up))

        self._fv = FVState(
            ts_ms=ts_ms,
            market_id=market_id,
            boundary_ts=boundary_ts,
            prob_up=prob_up,
            prob_down=prob_down,
            sigma=sigma,
            btc_price=btc_price,
            intra_vol=intra_vol,
            is_sigma_real=is_sigma_real,
            z_score=z_score,
            strike=strike,
        )
        if self._strategy:
            self._strategy.on_fv_update(self._fv)

    def _on_pm(self, raw: bytes) -> None:
        self.replay_stats["pm_received"] += 1
        try:
            parsed = unpack(raw)
            if parsed and len(parsed) > 0:
                self._update_clock(int(parsed[0]))
        except Exception as e:
            log.debug("PM parse error: %s", e)
            return

        # Parse schema: 11-field current (Task 3.3), 10-field (pre-3.3),
        # 8-field, 6-field with bids, or 4-field legacy fallback.
        market_ts = 0
        end_ts = 0
        liq_up = 0.0
        liq_dn = 0.0
        combined_ask = None
        if BIDS_IN_PM_BOOK and len(parsed) >= 11:
            (
                ts_ms, market_id, bid_up, ask_up, bid_dn, ask_dn,
                market_ts, end_ts, liq_up, liq_dn, combined_ask,
            ) = parsed[:11]
        elif BIDS_IN_PM_BOOK and len(parsed) >= 10:
            (
                ts_ms, market_id, bid_up, ask_up, bid_dn, ask_dn,
                market_ts, end_ts, liq_up, liq_dn,
            ) = parsed[:10]
        elif BIDS_IN_PM_BOOK and len(parsed) >= 8:
            ts_ms, market_id, bid_up, ask_up, bid_dn, ask_dn, market_ts, end_ts = parsed[:8]
        elif BIDS_IN_PM_BOOK and len(parsed) >= 6:
            ts_ms, market_id, bid_up, ask_up, bid_dn, ask_dn = parsed[:6]
        elif len(parsed) >= 4:
            ts_ms, market_id, ask_up, ask_dn = parsed[:4]
            bid_up, bid_dn = ask_up, ask_dn   # fallback: use ask as exit proxy
            if BIDS_IN_PM_BOOK:
                log.warning(
                    "BIDS_IN_PM_BOOK=1 but only 4 fields received — "
                    "patch pm_daemon or set BIDS_IN_PM_BOOK=0"
                )
        else:
            log.debug("PM message too short: %d fields", len(parsed))
            return

        # Market transition: settle old positions, then update tracking.
        # Task 2.5: use _schedule_settlement() so production runs get async
        # Gamma resolution while tests (no event loop) get FV proxy directly.
        if market_id != self._last_pm_market and self._last_pm_market:
            #self._schedule_settlement(self._last_pm_market)
           
            # CRITICAL: In replay mode, settle synchronously to prevent the async task 
            # from executing after the new market's ticks have overwritten self._fv/self._pm
            if self._is_replay:
                self._settle_market(self._last_pm_market)
            else:
                self._schedule_settlement(self._last_pm_market)
            
            
            
            # Reset ALL per-market status counters so the STATUS display
            # shows per-market performance, not lifetime accumulations.
            # entries_this_window and current_spent are reset separately via
            # record_window_boundary() below (risk layer).
            # session-level totals (settled_markets, settlement_wins/losses)
            # are intentionally preserved across markets — see Stats.reset_for_market().
            self._stats.reset_for_market()
            if self._strategy:
                self._strategy.reset_for_market()
        self._last_pm_market = market_id

        self._pm = PMState(
            ts_ms=ts_ms,
            market_id=market_id,
            bid_up=bid_up,
            ask_up=ask_up,
            bid_down=bid_dn,
            ask_down=ask_dn,
            market_ts=int(market_ts or 0),
            end_ts=int(end_ts or 0),
            liq_up=float(liq_up or 0.0),
            liq_down=float(liq_dn or 0.0),
            _combined_ask=combined_ask,
        )

        if Path(KILL_SWITCH_FILE).exists():
            log.critical("Kill switch file detected — halting all entries")
            self._risk.halt("kill switch file")
            return

        if self._pm.market_ts:
            self._risk.record_window_boundary(self._pm.market_ts)

        if self._fv.boundary_ts and self._pm.market_ts and self._fv.boundary_ts != self._pm.market_ts:
            self._stats.window_mismatches += 1
            log.debug(
                "Window mismatch: FV boundary=%d PM market_ts=%d - skipping",
                self._fv.boundary_ts, self._pm.market_ts,
            )
            return

        if self._fv.ts_ms and self._pm.market_ts:
            self._settlement_proxy_prob_up[market_id] = float(self._fv.prob_up)

        # Stale FV guard (tightened to 500ms vs Phase 3's 10s)
        now_ms = self._current_ts_ms
        if not self._fv.ts_ms:
            self._stats.stale_skips += 1
            log.debug("Skipping - no FV tick received yet")
            return
        fv_age_ms = now_ms - self._fv.ts_ms
        pm_age_ms = now_ms - ts_ms
        
        if not self._risk.check_data_freshness(fv_age_ms, pm_age_ms):
            self._stats.stale_skips += 1
            return

        if fv_age_ms > FV_STALE_MS:
            self._stats.stale_skips += 1
            log.debug("Skipping — FV stale %.0fms", fv_age_ms)
            return

        # 1. Check exits on open positions FIRST (always before new entries)
        self._check_exits(market_id, bid_up, bid_dn, ts_ms)

        # 2. Then check for new entries
        self._check_entries(market_id, ask_up, ask_dn, ts_ms, fv_age_ms)

    # ── Exit evaluation ────────────────────────────────────────────────────────

    def _check_exits(
        self,
        market_id: str,
        bid_up:    Optional[float],
        bid_dn:    Optional[float],
        ts_ms:     int,
    ) -> None:
        """
        Evaluate exits on every open position.

        Time-Aware Strategy (USE_TIME_AWARE_EXITS=1, default):
        ─────────────────────────────────────────────────────
        Binary markets settle at $1 or $0. Mid-window prices oscillate around
        the eventual outcome. The correct primary exit is SETTLEMENT.

        We split the 5-minute window into three zones:

          EARLY (>120s remaining)
            Hold. The position was entered because FV says the market is
            underpriced. A mid-window dip is not a reason to cut — BTC can
            reverse in the final seconds. The only exit is if the bid has
            risen so high (>0.88) that holding adds almost no remaining value
            over selling now.

          LATE (120s–60s remaining)
            Light exits. Cut if the bid has collapsed to near zero (≤0.08) —
            the market is pricing us as nearly dead. Take profit if bid ≥ 0.82
            (captured most of the $1 payout already).

          EMERGENCY (<60s remaining)
            Double-confirmation cut: BOTH the bid AND our FV must agree we are
            losing. Bid ≤ 0.12 alone is not enough — maybe the market is wrong.
            FV ≤ 0.30 AND bid ≤ 0.12 together = strong signal the bet is bad.
            Also lock in near-certain wins (bid ≥ 0.88).

        Why absolute bid prices instead of % unrealised PnL:
          On a binary market, the bid price IS the implied probability of winning.
          bid=0.08 means the market says 8% chance of paying $1 regardless of
          our entry price. % PnL depends on entry, which is irrelevant to the
          probability of settlement.

        Legacy flat TP/SL (USE_TIME_AWARE_EXITS=0):
          Original Phase 3 behaviour — exit at +35% / -25% unrealised PnL
          at any point during the window. Kept for A/B comparison.

        TOS exit policy (EXIT_POLICY=TOS):
          Architecture A holds every position to settlement. There are no
          mid-window stop-losses or take-profits. This is intentional: the
          entry signal is designed to be right at settlement, not mid-window.
          Mixing mid-window exits with TOS entries would corrupt win-rate and
          expectancy measurements. Settlement is handled by _settle_market().
        """
        # Task 2.4: TOS settlement-only gate — no mid-window exits.
        if self._exit_policy == "TOS":
            return

        positions = self._positions.get(market_id, {})
        if not positions:
            return

        bids   = {"UP": bid_up, "DOWN": bid_dn}
        fv_map = {"UP": self._fv.prob_up, "DOWN": self._fv.prob_down}

        # Derive window timing from the unix timestamp.
        # Windows start at multiples of MARKET_WINDOW_SECONDS (300s) since epoch.
        elapsed_s   = (ts_ms / 1000) % MARKET_WINDOW_SECONDS
        remaining_s = MARKET_WINDOW_SECONDS - elapsed_s

        for side, pos in list(positions.items()):
            bid = bids.get(side)
            if bid is None or pos.shares <= 0:
                continue

            fv        = fv_map.get(side, 0.5)
            upnl_pct  = pos.unrealised_pct(bid)

            # ── Legacy flat TP/SL ─────────────────────────────────────────────
            if not USE_TIME_AWARE_EXITS:
                if upnl_pct >= TAKE_PROFIT_PCT:
                    self._exit_position(pos, bid, ts_ms, "TAKE_PROFIT")
                elif upnl_pct <= -STOP_LOSS_PCT:
                    self._exit_position(pos, bid, ts_ms, "STOP_LOSS")
                continue

            # ── Time-aware binary market exits ────────────────────────────────

            if remaining_s <= EMERGENCY_SECONDS:
                # ── EMERGENCY ZONE: last 60 seconds ───────────────────────
                # Double-confirmation cut: market AND model must both say we lose.
                if bid <= EMERGENCY_CUT_PRICE and fv <= EMERGENCY_FV_CONFIRM:
                    log.warning(
                        "🚨 EMERGENCY CUT %s %s │ bid=%.4f≤%.2f  fv=%.4f≤%.2f  "
                        "entry=%.4f  upnl=%+.1f%%  remaining=%.0fs",
                        market_id[:8], side,
                        bid, EMERGENCY_CUT_PRICE,
                        fv, EMERGENCY_FV_CONFIRM,
                        pos.avg_entry, upnl_pct * 100, remaining_s,
                    )
                    self._exit_position(pos, bid, ts_ms, "EMERGENCY_CUT")

                elif bid >= EMERGENCY_TP_BID:
                    # Near settlement, position is highly likely to pay $1.
                    # Lock in the gain rather than risk a last-second reversal.
                    log.info(
                        "✅ EMERGENCY TP %s %s │ bid=%.4f≥%.2f  remaining=%.0fs",
                        market_id[:8], side, bid, EMERGENCY_TP_BID, remaining_s,
                    )
                    self._exit_position(pos, bid, ts_ms, "TAKE_PROFIT")

            elif remaining_s <= LATE_WINDOW_SECONDS:
                # ── LATE ZONE: 60–120 seconds remaining ───────────────────
                if bid <= LATE_SL_FLOOR:
                    # Bid has collapsed to near zero. The market is very confident
                    # this side loses. We're in the late window — not much time for
                    # recovery. Cut the loss and free up capital.
                    log.warning(
                        "📉 LATE FLOOR CUT %s %s │ bid=%.4f≤%.2f  "
                        "fv=%.4f  entry=%.4f  remaining=%.0fs",
                        market_id[:8], side,
                        bid, LATE_SL_FLOOR,
                        fv, pos.avg_entry, remaining_s,
                    )
                    self._exit_position(pos, bid, ts_ms, "STOP_LOSS")

                elif bid >= LATE_TP_BID:
                    # High bid in late window — position has already captured
                    # most of the available value. Take it rather than hold for
                    # the last few cents of upside at binary risk.
                    log.info(
                        "💰 LATE TP %s %s │ bid=%.4f≥%.2f  remaining=%.0fs",
                        market_id[:8], side, bid, LATE_TP_BID, remaining_s,
                    )
                    self._exit_position(pos, bid, ts_ms, "TAKE_PROFIT")

            else:
                # ── EARLY ZONE: more than 120 seconds remaining ────────────
                # HOLD. The only exit is a high-conviction signal that the position
                # is essentially already won. We do NOT cut losses here — a down-move
                # in BTC mid-window is noise, not a bet outcome signal.
                if bid >= EARLY_HIGH_CONFIDENCE_BID:
                    # Bid is above 88¢ early in the window. The market has already
                    # priced this as near-certain. Capture the gain now rather than
                    # hold for the final 12¢ at binary risk.
                    log.info(
                        "🏆 EARLY HIGH-CONFIDENCE TP %s %s │ bid=%.4f≥%.2f  "
                        "entry=%.4f  remaining=%.0fs",
                        market_id[:8], side, bid, EARLY_HIGH_CONFIDENCE_BID,
                        pos.avg_entry, remaining_s,
                    )
                    self._exit_position(pos, bid, ts_ms, "TAKE_PROFIT")
                # else: hold — do nothing

    def _exit_position(
        self,
        pos:        Position,
        exit_price: float,
        ts_ms:      int,
        reason:     str,
    ) -> None:
        """Simulate selling a position and record the exit."""
        proceeds  = exit_price * pos.shares
        pnl       = proceeds - pos.cost
        pnl_pct   = pnl / pos.cost if pos.cost > 0 else 0.0

        rec = ExitRecord(
            ts_ms=ts_ms,
            market_id=pos.market_id,
            side=pos.side,
            shares=pos.shares,
            avg_entry=pos.avg_entry,
            exit_price=exit_price,
            cost=pos.cost,
            proceeds=proceeds,
            pnl=pnl,
            pnl_pct=pnl_pct,
            exit_reason=reason,
        )

        self._stats.total_proceeds += proceeds
        self._risk.record_settlement(cost_usdc=pos.cost, gross_return=proceeds)
        if reason == "TAKE_PROFIT":
            self._stats.exits_take_profit += 1
        elif reason == "STOP_LOSS":
            self._stats.exits_stop_loss += 1
        elif reason == "EMERGENCY_CUT":
            self._stats.exits_emergency += 1
        elif reason == "SETTLEMENT":
            self._stats.exits_settlement += 1
            # Track whether this was a settlement win ($1) or loss ($0)
            # Binary markets pay exactly $1 or $0; draw at $0.5 is counted separately
            if exit_price >= 0.99:
                self._stats.settlement_wins += 1
            elif exit_price <= 0.01:
                self._stats.settlement_losses += 1
            # else: draw (exit_price ≈ 0.5) — not counted in either

        # Remove position
        del self._positions[pos.market_id][pos.side]
        if not self._positions[pos.market_id]:
            del self._positions[pos.market_id]

        self._exits_file.write(json.dumps(asdict(rec)) + "\n")
        self._exits_file.flush()

        # O2: emit exit metric for dashboard + Phase 1b R4 alerting.
        emit_metric(
            "exit",
            ts_ms       = ts_ms,
            market_id   = pos.market_id[:8],
            side        = pos.side,
            reason      = reason,
            pnl         = round(pnl, 4),
            pnl_pct     = round(pnl_pct, 4),
            cost        = round(pos.cost, 4),
            proceeds    = round(proceeds, 4),
            exit_price  = round(exit_price, 4),
            avg_entry   = round(pos.avg_entry, 4),
            shares      = pos.shares,
        )

        _WIN  = "\033[32m"
        _LOSS = "\033[31m"
        _RST  = "\033[0m"
        colour = _WIN if pnl >= 0 else _LOSS
        reason_tag = {
            "TAKE_PROFIT":   "TP  ",
            "STOP_LOSS":     "SL  ",
            "EMERGENCY_CUT": "EMRG",
            "SETTLEMENT":    "SETL",
        }.get(reason, reason[:4])

        print(
            f"  EXIT  {reason_tag}  {pos.side:<4}  "
            f"entry={pos.avg_entry:.4f}  exit={exit_price:.4f}  "
            f"pnl={colour}{pnl:+.4f}{_RST} ({pnl_pct:+.1%})  "
            f"shares={pos.shares:.0f}  mkt={pos.market_id[:8]}…",
            flush=True,
        )

    # ── Settlement ─────────────────────────────────────────────────────────────

    def _schedule_settlement(self, market_id: str) -> None:
        """
        Dispatch settlement to the async (Gamma) or sync (FV proxy) path.

        In the production async context (event loop running):
            Creates an asyncio task for _settle_market_async(), which queries
            Gamma for the actual outcome and falls back to FV proxy on failure.

        In test / non-async contexts (no running loop):
            Calls _settle_market() directly (FV proxy only, no network I/O).
        """
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self._settle_market_async(market_id))
        except RuntimeError:
            # No running event loop — test context or direct sync call.
            self._settle_market(market_id)

    async def _resolve_market_settlement(self, market_id: str) -> Optional[str]:
        """
        One-shot Gamma API query for actual settlement outcome.

        Returns 'UP', 'DOWN', or None if the outcome cannot be determined
        (network failure, timeout, API error, or market not yet resolved).

        Never raises — all exceptions are caught and logged so the async
        settlement caller can safely fall back to the FV proxy.
        """
        url = f"{GAMMA_HOST}/markets/{market_id}"
        try:
            timeout = aiohttp.ClientTimeout(total=RESOLUTION_TIMEOUT_S)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        log.debug(
                            "Gamma resolution HTTP %d for %s",
                            resp.status, market_id[:8],
                        )
                        return None
                    data = await resp.json()
        except Exception as e:
            log.debug("Gamma resolution error for %s: %s", market_id[:8], e)
            return None

        # Gamma returns winner in 'outcome' or 'winner' field depending on state.
        winner = (data.get("outcome") or data.get("winner") or "").strip().upper()
        if "UP" in winner:
            return "UP"
        if "DOWN" in winner:
            return "DOWN"

        if data.get("resolved") or data.get("closed"):
            # Market is closed but winner field is ambiguous — log and fall back.
            log.warning(
                "Market %s closed but winner field unclear: %r — using FV proxy",
                market_id[:8], winner,
            )
        else:
            log.debug("Market %s not yet resolved by Gamma", market_id[:8])
        return None

    async def _settle_market_async(self, market_id: str) -> None:
        """
        Async settlement path: query Gamma for the actual outcome, fall back
        to FV proxy if the API is unavailable or returns no result.

        Positions are settled at 1.0 (winner) or 0.0 (loser). The cache
        prevents repeated Gamma calls for the same market_id across retries.
        """
        positions = self._positions.get(market_id, {})
        if not positions:
            return

        # Check cache first — avoid redundant API calls.
        outcome = self._resolve_cache.get(market_id)
        if outcome is None:
            # CRITICAL: Never hit the network during deterministic replay.
            if self._is_replay:
                outcome = None 
            else:
                outcome = await self._resolve_market_settlement(market_id)
                if outcome is not None:
                    self._resolve_cache[market_id] = outcome

        if outcome == "UP":
            settlement = {"UP": 1.0, "DOWN": 0.0}
            source = "Gamma"
        elif outcome == "DOWN":
            settlement = {"UP": 0.0, "DOWN": 1.0}
            source = "Gamma"
        else:
            # Gamma unavailable or unresolved — fall back to FV proxy.
            prob_up = self._settlement_proxy_prob_up.get(market_id, self._fv.prob_up)
            log.warning(
                "Could not resolve %s from Gamma — using FV proxy (prob_up=%.4f)",
                market_id[:8], prob_up,
            )
            if prob_up > 0.5:
                settlement = {"UP": 1.0, "DOWN": 0.0}
            elif prob_up < 0.5:
                settlement = {"UP": 0.0, "DOWN": 1.0}
            else:
                settlement = {"UP": 0.5, "DOWN": 0.5}
            source = "FV-proxy"

        for side, pos in list(positions.items()):
            if pos.shares <= 0:
                continue
            settle_price = settlement[side]
            self._exit_position(pos, settle_price, self._current_ts_ms, "SETTLEMENT")

        self._stats.settled_markets += 1
        log.info(
            "Market %s settled via %s  outcome=%s  cumulative_net=%+.4f USDC",
            market_id[:8], source, outcome or "proxy", self._stats.net_pnl,
        )
        # Task 3.2: settle any arb position in this market window.
        self._settle_arb_position(market_id)

    def _settle_market(self, market_id: str) -> None:
        """
        Synchronous FV proxy settlement — used as a fallback when Gamma is
        unavailable and as the direct path in non-async contexts (tests).

        Uses FV probability at the last known tick:
            prob_up > 0.5  →  UP wins $1, DOWN loses
            prob_up < 0.5  →  DOWN wins $1, UP loses
            prob_up = 0.5  →  draw; both settle at $0.50
        """
        positions = self._positions.get(market_id, {})
        if not positions:
            return

        prob_up = self._settlement_proxy_prob_up.get(market_id, self._fv.prob_up)
        if prob_up > 0.5:
            settlement = {"UP": 1.0, "DOWN": 0.0}
        elif prob_up < 0.5:
            settlement = {"UP": 0.0, "DOWN": 1.0}
        else:
            settlement = {"UP": 0.5, "DOWN": 0.5}

        for side, pos in list(positions.items()):
            if pos.shares <= 0:
                continue
            settle_price = settlement[side]
            self._exit_position(pos, settle_price, self._current_ts_ms, "SETTLEMENT")

        self._stats.settled_markets += 1
        log.info(
            "Market %s settled via FV-proxy  prob_up=%.4f  cumulative_net=%+.4f USDC",
            market_id[:8], prob_up, self._stats.net_pnl,
        )
        # Task 3.2: settle any arb position in this market window.
        self._settle_arb_position(market_id)


    # ── Architecture B — Arb Scanner (Phase 3, Task 3.2) ──────────────────────

    async def _arb_scanner_loop(self, stop_event: asyncio.Event) -> None:
        """
        Task 3.2: Run alongside TOS as an independent asyncio task.

        Polls PM state every ARB_SCAN_INTERVAL_MS for structural arb:
            combined_ask = ask_up + ask_dn < ARB_TARGET_COMBINED

        When found, calls _scan_arb() which validates liquidity and size
        constraints, then calls _simulate_arb_entry() to record the fill.

        The loop is always created in run() but is a no-op when ARB_ENABLED=0,
        so toggling arb requires only an env var change, not a process restart.
        """
        interval_s = ARB_SCAN_INTERVAL_MS / 1000.0
        while not stop_event.is_set():
            await asyncio.sleep(interval_s)
            if not ARB_ENABLED:
                continue
            try:
                self._scan_arb()
            except Exception as e:
                log.debug("ArbScanner error: %s", e)

    def _scan_arb(self) -> None:
        """
        Single arb evaluation tick. Called from _arb_scanner_loop every 50ms.

        Checks:
          1. PM data is fresh (market_id set)
          2. No existing arb position in this window (one arb per window)
          3. Risk manager not halted
          4. Both asks present; combined < ARB_TARGET_COMBINED
          5. Sufficient liquidity on both legs for ARB_MIN_SHARES
          6. Budget does not exceed ARB_MAX_USDC
        """
        pm = self._pm
        if not pm.market_id:
            return  # no PM data yet

        # One arb position per market window — don't compound.
        if pm.market_id in self._arb_positions:
            return

        # Risk gate: respect circuit-breaker halts.
        if self._risk.trading_halted:
            return

        combined = pm.combined_ask()
        if combined is None or combined >= ARB_TARGET_COMBINED:
            return

        # Both liquidity fields must be non-zero (pm_daemon populates from book depth).
        if not pm.liq_up or not pm.liq_down:
            return

        # Size: minimum of liquidity on each leg and budget.
        max_shares_liq    = min(pm.liq_up, pm.liq_down)
        max_shares_budget = ARB_MAX_USDC / combined if combined > 0 else 0.0
        shares = min(max_shares_liq, max_shares_budget)

        if shares < ARB_MIN_SHARES:
            log.debug(
                "ARB SKIP  %s — combined=%.4f shares=%.1f below min=%.0f",
                pm.market_id[:8], combined, shares, ARB_MIN_SHARES,
            )
            return

        self._simulate_arb_entry(
            market_id=pm.market_id,
            ask_up=pm.ask_up,    # type: ignore[arg-type]  (None excluded above)
            ask_dn=pm.ask_down,  # type: ignore[arg-type]
            combined=combined,
            shares=shares,
            ts_ms=pm.ts_ms,
        )

    def _simulate_arb_entry(
        self,
        market_id: str,
        ask_up:    float,
        ask_dn:    float,
        combined:  float,
        shares:    float,
        ts_ms:     int,
    ) -> None:
        """
        Record a paper dual-leg arb entry. Does NOT place real orders.

        Persists an "arb_entry" record to fills JSONL. Settlement is handled
        by _settle_arb_position() when the market window transitions.
        """
        cost = combined * shares
        arb_pos = ArbPosition(
            market_id=market_id,
            combined=combined,
            shares=shares,
            cost=cost,
            ts_ms=ts_ms,
        )
        self._arb_positions[market_id] = arb_pos
        self._stats.arb_entries += 1
        self._stats.arb_cost += cost

        record = {
            "type":         "arb_entry",
            "ts_ms":        ts_ms,
            "market_id":    market_id,
            "ask_up":       ask_up,
            "ask_dn":       ask_dn,
            "combined":     combined,
            "shares":       shares,
            "cost":         cost,
            "expected_pnl": arb_pos.expected_pnl,
        }
        self._fills_file.write(json.dumps(record) + "\n")
        self._fills_file.flush()

        log.info(
            "ARB ENTRY  %s — combined=%.4f  shares=%.1f  cost=$%.4f  "
            "expected_pnl=$%.4f",
            market_id[:8], combined, shares, cost, arb_pos.expected_pnl,
        )

    def _settle_arb_position(self, market_id: str) -> None:
        """
        Settle an arb position when the market window closes.

        Arb proceeds are always exactly shares × $1.00 — one leg pays $1,
        the other pays $0, total = $1 regardless of outcome. This is the
        guarantee that makes the trade risk-free when combined < 1.0.

        Called from both _settle_market_async and _settle_market so that
        arb positions are always cleaned up at window transition, even when
        the async Gamma path is used.
        """
        arb_pos = self._arb_positions.pop(market_id, None)
        if arb_pos is None:
            return

        proceeds = arb_pos.guaranteed_proceeds
        pnl      = arb_pos.expected_pnl  # = proceeds - cost

        self._stats.arb_settled   += 1
        self._stats.arb_proceeds  += proceeds

        record = {
            "type":     "arb_exit",
            "ts_ms":    self._current_ts_ms,
            "market_id": market_id,
            "combined":  arb_pos.combined,
            "shares":    arb_pos.shares,
            "cost":      arb_pos.cost,
            "proceeds":  proceeds,
            "pnl":       pnl,
        }
        self._exits_file.write(json.dumps(record) + "\n")
        self._exits_file.flush()

        log.info(
            "ARB SETTLE  %s — cost=$%.4f  proceeds=$%.4f  pnl=$%+.4f",
            market_id[:8], arb_pos.cost, proceeds, pnl,
        )

    # ── Entry evaluation ───────────────────────────────────────────────────────

# ── Entry evaluation ───────────────────────────────────────────────────────

    def _check_entries(
        self,
        market_id: str,
        ask_up:    Optional[float],
        ask_dn:    Optional[float],
        ts_ms:     int,
        fv_age_ms: int,
    ) -> None:
        allowed, reason = self._risk.check_entry_allowed(self._fv.is_sigma_real)
        if not allowed:
            log.debug("SKIP  %s — risk gate: %s", market_id[:8], reason)
            return

        if self._entry_policy in ("TOS", "TOS_SIGNAL"):
            self._check_strategy_entries(market_id, ts_ms, fv_age_ms)
            return

        # Window age guard — skip the noisy early-window period.
        # K has just snapped and sigma is still carry-over from the previous
        # window, so any edge seen before MIN_WINDOW_AGE_S is unreliable.
        window_age_s = (ts_ms / 1000) % MARKET_WINDOW_SECONDS
        if window_age_s < MIN_WINDOW_AGE_S:
            log.debug(
                "SKIP  %s — window only %.0fs old (min %ds)",
                market_id[:8], window_age_s, MIN_WINDOW_AGE_S,
            )
            return

        # ── Sigma-real gate (Task 1.5) ────────────────────────────────────────
        # Block all entries while the intra-window EWMA buffer is still warming
        # up (is_sigma_real=False).  During warmup the engine falls back to the
        # cross-window buffer with a dynamic floor, which means sigma doesn't
        # reflect actual current-window volatility.  FV probabilities computed
        # from a floor-capped sigma can be materially wrong — particularly in
        # the collapse toward FV≈1.0 / FV≈0.0 that caused 261/261 floor fills.
        #
        # This gate fires for the first ~MIN_INTRA_TICKS ticks (~3s at 10t/s)
        # of each new window.  The window-age guard above already blocks the
        # first MIN_WINDOW_AGE_S seconds, so in practice both guards overlap
        # and this is a belt-and-suspenders safety net for the remaining gap.
        if not self._fv.is_sigma_real:
            log.debug(
                "SKIP  %s — sigma not real (intra-buffer warming up, "
                "intra_vol=%.4f)",
                market_id[:8], self._fv.intra_vol,
            )
            return

        sides = [
            ("UP",   ask_up, self._fv.prob_up),
            ("DOWN", ask_dn, self._fv.prob_down),
        ]
        for side, ask, fv in sides:
            if ask is None:
                continue

            # Near-expired market guard — PM has priced this side to near zero
            # but sigma floor keeps FV near 0.50, producing a fake edge.
            if ask < MIN_ENTRY_ASK:
                log.debug(
                    "SKIP  %s %s — ask %.4f below MIN_ENTRY_ASK %.2f (near-expired)",
                    market_id[:8], side, ask, MIN_ENTRY_ASK,
                )
                continue

            # FV extreme-value guard (Task 1.5 fix).
            # When FV approaches 1.0 or 0.0, Black-Scholes is acting as a binary
            # comparator — BTC is already far above/below K with little time left.
            # Any "edge" here is the difference between PM's 0.88–0.97 and our
            # model's 1.0, which collapses if BTC moves $50 back toward K.
            # This blocked all 24 FV=1.000 fills from the Task 1.4 paper run.
            if fv >= FV_ENTRY_MAX or fv <= FV_ENTRY_MIN:
                log.debug(
                    "SKIP  %s %s — FV=%.4f extreme [%.2f,%.2f] (market essentially decided)",
                    market_id[:8], side, fv, FV_ENTRY_MIN, FV_ENTRY_MAX,
                )
                continue

            edge = fv - ask
            if edge <= MIN_EDGE_THRESHOLD:
                continue

            # Position cap check
            pos = self._positions.get(market_id, {}).get(side)
            current_shares = pos.shares if pos else 0.0
            if current_shares >= MAX_SHARES_PER_SIDE:
                self._stats.cap_blocks += 1
                log.debug(
                    "CAP  %s %s — already holding %.0f/%.0f shares",
                    market_id[:8], side, current_shares, MAX_SHARES_PER_SIDE,
                )
                continue

            # Cooldown check
            last = self._last_fill_ts.get(market_id, {}).get(side, 0.0)
            if ts_ms - last < FILL_COOLDOWN_MS:
                continue

            self._simulate_entry(market_id, side, ask, fv, edge, ts_ms, fv_age_ms)

    def _check_strategy_entries(self, market_id: str, ts_ms: int, fv_age_ms: int) -> None:
        """Routes to the isolated strategy interface."""
        if self._strategy is None:
            return
        
        signals = self._strategy.evaluate_entry(self._fv, self._pm)
        for sig in signals:
            pos = self._positions.get(market_id, {}).get(sig.side)
            current_shares = pos.shares if pos else 0.0
            if current_shares >= MAX_SHARES_PER_SIDE:
                self._stats.cap_blocks += 1
                log.debug(
                    "STRATEGY CAP  %s %s - already holding %.0f/%.0f shares ",
                    market_id[:8], sig.side, current_shares, MAX_SHARES_PER_SIDE,
                )
                continue

            last = self._last_fill_ts.get(market_id, {}).get(sig.side, 0.0)
            if ts_ms - last < FILL_COOLDOWN_MS:
                continue

            self._simulate_entry(
                market_id, sig.side, sig.ask, sig.fv, sig.edge, ts_ms, fv_age_ms,
            )


    def _simulate_entry(
        self,
        market_id: str,
        side:      str,
        ask:       float,
        fv:        float,
        edge:      float,
        ts_ms:     int,
        fv_age_ms: int,
    ) -> None:
        cost = ask * PAPER_TRADE_SHARES

        # Task 2.6: derive window-timing fields for the fill record.
        # Prefer PM market_ts (exact window-open epoch published by pm_daemon).
        # Fall back to modulo computation when PM hasn't published yet (legacy mode).
        fill_s = ts_ms / 1000.0
        pm_market_ts = self._pm.market_ts if hasattr(self, "_pm") else 0
        if pm_market_ts:
            # CRITICAL: Use virtual clock for window calculations. 
            # Using time.time() here breaks determinism and shifts window boundaries per run.
            fill_s = self._current_ts_ms / 1000.0

            elapsed_s = fill_s - pm_market_ts
            window_start_ts = int(pm_market_ts)
            # Fallback only if pm_market_ts is missing
            if pm_market_ts == 0:
                elapsed_s = fill_s % MARKET_WINDOW_SECONDS
                window_start_ts = int(fill_s - elapsed_s)

        pm_end_ts = self._pm.end_ts if hasattr(self, "_pm") else 0

        rec = EntryRecord(
            ts_ms=ts_ms,
            market_id=market_id,
            side=side,
            ask=ask,
            shares=PAPER_TRADE_SHARES,
            cost=cost,
            fv=fv,
            edge=edge,
            btc_price=self._fv.btc_price,
            sigma=self._fv.sigma,
            fv_age_ms=fv_age_ms,
            is_sigma_real=self._fv.is_sigma_real,   # Task 1.5: diagnostic field
            intra_vol=self._fv.intra_vol,            # Task 1.5: raw EWMA before floor
            # Task 2.6: TOS analysis fields
            z_score=self._fv.z_score,
            elapsed_s=round(elapsed_s, 3),
            window_start_ts=window_start_ts,
            window_end_ts=int(pm_end_ts),
        )

        # Update position
        pos = self._positions[market_id].get(side)
        if pos is None:
            pos = Position(market_id=market_id, side=side)
            self._positions[market_id][side] = pos
        pos.shares += PAPER_TRADE_SHARES
        pos.cost   += cost

        # Stats
        self._stats.entries     += 1
        self._stats.total_cost  += cost
        self._stats.sigma_total += 1
        self._risk.entries_this_window += 1
        self._risk.record_entry(cost_usdc=cost)
        # Task 1.4: use is_sigma_real flag instead of comparing against the
        # now-removed fixed MIN_SIGMA_FLOOR constant.  sigma_at_floor now counts
        # entries where sigma came from the floor/warmup path, not real intra-window vol.
        if not self._fv.is_sigma_real:
            self._stats.sigma_at_floor += 1

        self._last_fill_ts[market_id][side] = float(ts_ms)

        self._fills_file.write(json.dumps(asdict(rec)) + "\n")
        self._fills_file.flush()

        # O2: structured metrics emission for dashboard + alerting (Phase 1c).
        emit_metric(
            "entry",
            ts_ms         = ts_ms,
            market_id     = market_id[:8],
            side          = side,
            ask           = round(ask, 4),
            fv            = round(fv, 4),
            edge          = round(edge, 4),
            cost          = round(cost, 4),
            sigma         = round(self._fv.sigma, 4),
            intra_vol     = round(self._fv.intra_vol, 4),
            is_sigma_real = self._fv.is_sigma_real,
            z_score       = round(self._fv.z_score, 4),
            elapsed_s     = round(elapsed_s, 1),
            fv_age_ms     = fv_age_ms,
        )

        _C_HIGH = "\033[32m"
        _C_MED  = "\033[33m"
        _C_RST  = "\033[0m"
        colour = _C_HIGH if edge > 0.06 else _C_MED

        pos_str = f"pos={pos.shares:.0f}/{MAX_SHARES_PER_SIDE:.0f}sh"
        print(
            f"  ENTRY {side:<4}  ask={ask:.4f}  fv={fv:.4f}  "
            f"edge={colour}{edge:+.4f}{_C_RST}  "
            f"σ={self._fv.sigma:.3f}  BTC={self._fv.btc_price:.2f}  "
            f"age={fv_age_ms}ms  {pos_str}  mkt={market_id[:8]}…",
            flush=True,
        )

    # ── Status display ─────────────────────────────────────────────────────────

    def _print_status(self, final: bool = False) -> None:
        s   = self._stats
        label = "FINAL" if final else "STATUS"

        fv_age = self._current_ts_ms - self._fv.ts_ms if self._fv.ts_ms else -1
        pm_age = self._current_ts_ms - self._pm.ts_ms if self._pm.ts_ms else -1

        open_pos_count = sum(len(v) for v in self._positions.values())

        # Sigma quality warning. The counter name is kept for compatibility,
        # but it now means "not real intra-window sigma" rather than fixed floor.
        sigma_not_real_pct = s.sigma_not_real_pct
        sigma_warn = "  SIGMA NOT REAL (warmup/fallback)" if sigma_not_real_pct > 80 else ""

        # Win rate across all exits
        winning_exits = s.exits_take_profit   # TP exits are always profitable
        # settlement wins need to be tracked separately in a real impl
        total_exits = s.total_exits

        print(
            f"\n  [{label}]  "
            f"entries={s.entries}  "
            f"cost=${s.total_cost:.2f}  "
            f"proceeds=${s.total_proceeds:.2f}  "
            f"net={s.net_pnl:+.4f} USDC  "
            f"ROI={s.roi_pct:+.1f}%",
            flush=True,
        )
        print(
            f"  Exits: tp={s.exits_take_profit}  sl={s.exits_stop_loss}  "
            f"settled={s.exits_settlement}  "
            f"| open_pos={open_pos_count}  "
            f"cap_blocks={s.cap_blocks}  "
            f"stale_skips={s.stale_skips}  "
            f"window_mismatches={s.window_mismatches}",
            flush=True,
        )
        print(
            f"  FV: p_up={self._fv.prob_up:.4f}  BTC={self._fv.btc_price:.2f}  "
            f"σ={self._fv.sigma:.4f}  age={fv_age}ms  "
            f"[sigma_not_real={sigma_not_real_pct:.0f}%{sigma_warn}]",
            flush=True,
        )
        print(
            f"  PM: ask_up={self._pm.ask_up}  bid_up={self._pm.bid_up}  "
            f"ask_dn={self._pm.ask_down}  bid_dn={self._pm.bid_down}  "
            f"liq_up={self._pm.liq_up:.0f}  liq_dn={self._pm.liq_down:.0f}  "
            f"age={pm_age}ms",
            flush=True,
        )
        # Task 3.2: show arb stats only when arb is enabled or has fired.
        if ARB_ENABLED or s.arb_entries > 0:
            combined = self._pm.combined_ask()
            combined_str = f"{combined:.4f}" if combined is not None else "n/a"
            print(
                f"  ARB: entries={s.arb_entries}  settled={s.arb_settled}  "
                f"cost=${s.arb_cost:.4f}  proceeds=${s.arb_proceeds:.4f}  "
                f"net={s.arb_net_pnl:+.4f}  "
                f"combined_now={combined_str}  "
                f"target=<{ARB_TARGET_COMBINED}",
                flush=True,
            )

        # Dynamic strategy diagnostics
        if self._strategy and hasattr(self._strategy, 'get_diagnostics'):
            diag = self._strategy.get_diagnostics()
            if diag.get("strategy") == "TOS_SIGNAL":
                sig_gate = diag.get("signal_gate", {})
                if sig_gate.get("tos_candidates", 0) > 0:
                    total_rejected = (
                        sig_gate.get('rejected_btc_history', 0) + sig_gate.get('rejected_consensus', 0)
                        + sig_gate.get('rejected_disagreement', 0)
                    )
                    print(
                        f"  SIGNAL GATE  candidates={sig_gate.get('tos_candidates', 0)}   "
                        f"accepted={sig_gate.get('accepted_signal', 0)}   "
                        f"rejected={total_rejected} ",
                        flush=True,
                    )
                    print(
                        f"    REJECT:btc_history_not_ready={sig_gate.get('rejected_btc_history', 0)}   "
                        f"REJECT:no_consensus(both_None)={sig_gate.get('rejected_consensus', 0)}   "
                        f"REJECT:disagreement={sig_gate.get('rejected_disagreement', 0)} ",
                        flush=True,
                    )
                    print(
                        f"    [per-signal None counts]   "
                        f"momentum_None={sig_gate.get('rejected_momentum', 0)}   "
                        f"imbalance_None={sig_gate.get('rejected_imbalance', 0)} ",
                        flush=True,
                    )
        print("", flush=True)

        # O2: emit heartbeat snapshot to metrics.jsonl for the dashboard
        # and Phase 1b R4 alerting.  Fires on every _print_status() call
        # (default: every STATUS_INTERVAL_S seconds).
        open_pos_count = sum(len(v) for v in self._positions.values())
        emit_metric(
            "heartbeat",
            entries             = s.entries,
            exits_tp            = s.exits_take_profit,
            exits_sl            = s.exits_stop_loss,
            settlement_wins     = s.settlement_wins,
            settlement_losses   = s.settlement_losses,
            net_pnl             = round(s.net_pnl, 4),
            total_cost          = round(s.total_cost, 4),
            roi_pct             = round(s.roi_pct, 2),
            sigma_not_real_pct  = round(s.sigma_not_real_pct, 1),
            fv_age_ms           = fv_age,
            pm_age_ms           = pm_age,
            open_pos            = open_pos_count,
            stale_skips         = s.stale_skips,
            window_mismatches   = s.window_mismatches,
            exit_policy         = self._exit_policy,
            entry_policy        = self._entry_policy,
            hourly_pnl          = round(self._risk.hourly_pnl, 4),
            halted              = self._risk.trading_halted,
        )


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    try:
        import uvloop, warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        log.info("Event loop: uvloop")
    except ImportError:
        log.info("Event loop: asyncio (uvloop not available)")

    loop      = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stop_event = asyncio.Event()

    def _signal(sig, _frame):
        log.info("Signal %s — shutting down.", sig)
        loop.call_soon_threadsafe(stop_event.set)

    signal.signal(signal.SIGINT,  _signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _signal)

    print(f"\n  Phase 3.5 — Smart Paper Trader")
    print(f"  Edge: ≥{MIN_EDGE_THRESHOLD:.1%}   "
          f"Max shares/side: {MAX_SHARES_PER_SIDE:.0f}   "
          f"TP: {TAKE_PROFIT_PCT:.0%}   SL: {STOP_LOSS_PCT:.0%}")
    print(f"  FV stale cutoff: {FV_STALE_MS:.0f}ms   "
          f"Bids in PM schema: {'YES' if BIDS_IN_PM_BOOK else 'NO (ask proxy)'}")
    print(f"  Fills → {FILLS_PATH.resolve()}")
    print(f"  Exits → {EXITS_PATH.resolve()}")
    print("─" * 75)

    trader = SmartPaperTrader()

    try:
        loop.run_until_complete(trader.run(stop_event))
    finally:
        loop.close()
        from shared.metrics import close as close_metrics
        close_metrics()  # flush final metrics.jsonl line before exit


if __name__ == "__main__":
    main()
```

## config.py

```py
import os
from dotenv import load_dotenv

load_dotenv()

# ── Credentials ────────────────────────────────────────────────────────────────
PRIVATE_KEY       = os.environ["PRIVATE_KEY"]
CLOB_API_KEY      = os.environ["CLOB_API_KEY"]
CLOB_SECRET       = os.environ["CLOB_SECRET"]
CLOB_PASSPHRASE   = os.environ["CLOB_PASSPHRASE"]
WALLET_ADDRESS    = os.environ["WALLET_ADDRESS"]

# ── Trading mode ──────────────────────────────────────────────────────────────
# TRADING_MODE is the explicit operational mode. LIVE_TRADING is kept for
# backward compatibility with older scripts, but live startup must require both.
TRADING_MODE: str = os.getenv("TRADING_MODE", "paper").strip().lower()
if TRADING_MODE not in ("paper", "live"):
    raise ValueError("TRADING_MODE must be 'paper' or 'live'")

# Set LIVE_TRADING=1 to enable real order placement and startup reconciliation.
# Default is False (paper trading). Required before live deployment.
LIVE_TRADING: bool = os.getenv("LIVE_TRADING", "0").lower() in ("1", "true", "yes")
if TRADING_MODE == "live" and not LIVE_TRADING:
    raise ValueError("TRADING_MODE=live requires LIVE_TRADING=1")
if TRADING_MODE == "paper" and LIVE_TRADING:
    raise ValueError("LIVE_TRADING=1 requires TRADING_MODE=live")

# ── Polymarket endpoints ───────────────────────────────────────────────────────
CLOB_HOST         = "https://clob.polymarket.com"
GAMMA_HOST        = "https://gamma-api.polymarket.com"
WS_HOST           = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# ── Strategy parameters ────────────────────────────────────────────────────────

# Limit how many consecutive markets to trade before stopping (0 = run forever)
MAX_MARKETS_TO_TRADE = int(os.getenv("MAX_MARKETS_TO_TRADE", "0"))


# Full level set for reference — uncomment and widen once wallet > $100.
# At that point the Phase 3 vol filter will clip this list dynamically so
# you never post the full width during a spike.
#
# PRICE_LEVELS = [0.40, 0.45, 0.48, 0.52, 0.55, 0.60]   # $100+ wallet
# PRICE_LEVELS = [0.45, 0.48, 0.52, 0.55]                # $50+ wallet

# $10 wallet: single tight band around fair value.
# Both levels are exactly 0.50, so one-sided exposure is minimal.
# UP orders: 0.50  |  DOWN orders: 0.50
# Max one-sided exposure: 0.50 × 5 shares = $2.50
PRICE_LEVELS = [0.50]

# Shares per ladder level.
# At 5 (exchange minimum), one full ladder = 4 orders × ~$2.50 = $10 locked.
# Leaves $10 free for FOK arb and unmatched-fill buffer.
LADDER_SIZE_PER_LEVEL   = float(os.getenv("LADDER_SIZE_PER_LEVEL",   5))

# Hard cap on total USDC spent per market window.
# Set just below wallet size so one bad window can't drain everything.
MAX_SPEND_PER_MARKET    = float(os.getenv("MAX_SPEND_PER_MARKET",   8))

# Hard cap on the maximum number of shares held on either side.
MAX_POSITION_SHARES     = float(os.getenv("MAX_POSITION_SHARES",    10))

# Limit max entries per window to avoid runaway entries on a single market
MAX_ENTRIES_PER_WINDOW  = int(os.getenv("MAX_ENTRIES_PER_WINDOW",   5))

# Minimum arb edge required before firing a FOK trade (2% = 0.02).
# Do not lower this — at $20 you cannot absorb a marginal arb that goes wrong.
TARGET_EDGE             = float(os.getenv("TARGET_EDGE",           0.02))

# Maximum USDC committed to a single FOK arb attempt.
# $4 at combined ≈ 0.95 → ~8 shares per leg, above MIN_ARB_SHARES.
MAX_TAKER_FILL_USDC     = float(os.getenv("MAX_TAKER_FILL_USDC",    4))

# risk.py last-resort halt: imbalance > 2× this value triggers a circuit break.
# Kept proportional to wallet size (was 10 at $20 scale).
MAX_INVENTORY_IMBALANCE = float(os.getenv("MAX_INVENTORY_IMBALANCE", 5))

# Merge matched UP+DOWN pairs once we have this many USDC worth.
# MUST be ≤ LADDER_SIZE_PER_LEVEL (5) so pairs are recycled within the window
# 2.5 USDC corresponds to 2.5 shares matched.
MERGE_THRESHOLD_USDC    = 2.5

# Out-of-band kill switch. If this file exists, trading halts.
KILL_SWITCH_FILE        = os.getenv("KILL_SWITCH_FILE", "/tmp/btcbot_halt")

MARKET_INTERVAL_SECONDS  = int(os.getenv("MARKET_INTERVAL_SECONDS", "300"))
MARKET_WINDOW_SECONDS    = MARKET_INTERVAL_SECONDS
STOP_BUYING_BEFORE_CLOSE = 15      # Stop new buys N seconds before close
ORACLE_WAIT_SECONDS      = 320     # Wait after close before attempting redeem
COMBINED_ASK_STOP        = 1.02    # Circuit breaker on mispriced book
MAX_PRICE_GAP            = float(os.getenv("MAX_PRICE_GAP", 0.30))  # Max difference between UP and DOWN prices

# Below this ask price, a side is considered a strong loser by the market.
# The bot will stop posting NEW limit buys on that side to avoid accumulating
# worthless shares filled by informed sellers.
# e.g. if DOWN ask = 0.20, market thinks DOWN has only 20% chance — skip DOWN posts.
# Set higher (e.g. 0.40) to be more conservative; 0.0 disables this guard.
LOSING_SIDE_THRESHOLD    = float(os.getenv("LOSING_SIDE_THRESHOLD", 0.35))

# Kill switch: halt trading after losing this much in any 60-minute period.
# $2.5 = 25% of a $10 wallet — aggressive but appropriate; a single bad hour
# should not wipe more than a quarter of capital before the bot stops itself.
MAX_LOSS_PER_HOUR_USDC  = float(os.getenv("MAX_LOSS_PER_HOUR_USDC",  9999.5))

POLL_INTERVAL_SECONDS   = 1.5     # Fallback book polling interval
WINDOW_ENTRY_BUFFER     = 1       # Seconds to wait after window open
WINDOW_ENTRY_TIMEOUT    = 10      # Stop entering if more than N seconds late

# Lowered from 10 to 5 (exchange minimum) so FOK arb can fire at small sizes.
# At $6 max taker fill and ask ≈ 0.50, max shares ≈ 12 — well above this floor.
# Without this change the arb check almost never triggers at $20 scale.
MIN_ARB_SHARES          = 5

# ── Phase 1: inventory control & ladder refresh ────────────────────────────────
# Thresholds scaled to $10 wallet / 5-share orders.
# Soft → begin skewing (Phase 2).  Hard → cancel heavy-side orders immediately.
# Stack must satisfy: SOFT < HARD < MAX_INVENTORY_IMBALANCE < IMBALANCE × 2
#   2 < 4 < 5 < 10  ✓
MAX_INVENTORY_SOFT      = float(os.getenv("MAX_INVENTORY_SOFT",   2))
MAX_INVENTORY_HARD      = float(os.getenv("MAX_INVENTORY_HARD",   LADDER_SIZE_PER_LEVEL))
LADDER_REFRESH_SECS     = int(os.getenv("LADDER_REFRESH_SECS",   75))

# ── Phase 2: inventory skewing ─────────────────────────────────────────────────
# SKEW_FACTOR unchanged: 0.001 × 3-share imbalance = 0.003 price shift.
# MAX_SKEW_OFFSET tightened to 0.02: with PRICE_LEVELS = [0.48, 0.52] a
# 0.05 offset would push bids outside the tight band — counterproductive.
SKEW_FACTOR         = float(os.getenv("SKEW_FACTOR",      0.001))
MAX_SKEW_OFFSET     = float(os.getenv("MAX_SKEW_OFFSET",  0.02))

# ── Phase 3: volatility detection ──────────────────────────────────────────────
# VOL_BUFFER_SIZE            — short-term ring buffer depth (~15s at 1.5s/tick).
# VOL_BASELINE_SIZE          — longer-horizon baseline (240 × 15s ≈ 1 hour).
# VOL_BASELINE_INTERVAL_SECS — how often a snapshot is pushed to the baseline.
# VOL_PAUSE_MULTIPLIER       — spike threshold vs hourly average.
#                              Lowered to 2.5 (vs default 3.0): at $20 the cost
#                              of adverse selection during a spike is proportionally
#                              much larger, so we exit the book earlier.
# VOL_PAUSE_SECONDS          — how long to stay out after a spike (unchanged).
# VOL_SPREAD_TIGHT / WIDE    — adaptive ladder band endpoints.
#                              With PRICE_LEVELS = [0.48, 0.52] the tight band
#                              (0.48–0.52) matches exactly, so at low vol the
#                              filter passes both levels cleanly.  At high vol
#                              the wide band (0.44–0.56) would accept wider
#                              levels if PRICE_LEVELS is expanded later.
VOL_BUFFER_SIZE            = int(os.getenv("VOL_BUFFER_SIZE",             10))
VOL_BASELINE_SIZE          = int(os.getenv("VOL_BASELINE_SIZE",          240))
VOL_BASELINE_INTERVAL_SECS = int(os.getenv("VOL_BASELINE_INTERVAL_SECS",  15))
VOL_PAUSE_MULTIPLIER       = float(os.getenv("VOL_PAUSE_MULTIPLIER",     2.5))
VOL_PAUSE_SECONDS          = int(os.getenv("VOL_PAUSE_SECONDS",           30))
VOL_SPREAD_TIGHT: tuple[float, float] = (0.48, 0.52)
VOL_SPREAD_WIDE:  tuple[float, float] = (0.44, 0.56)
```

## find_best_params.sh

```sh
#!/bin/bash
source venv/bin/activate
mkdir -p optimization_results

nohup python -m tools.optimizer \
  --stage all \
  --mode bayesian \
  --n-iter 150 \
  --filter-start 2026-06-06 \
  --filter-end 2026-06-06 \
  --train-start 2026-06-05 \
  --train-end 2026-06-07 \
  --val-start 2026-06-08 \
  --val-end 2026-06-09 \
  --captures-dir captures/ \
  --output-dir optimization_results/ \
  > optimization_results/optimizer.log 2>&1 &
```


## optimization_results/optimization_results.csv

```csv
run_id,timestamp,mode,seed,TOS_ENTRY_START_S,TOS_ENTRY_END_S,TOS_MIN_PROB,TOS_MIN_EDGE,TOS_MIN_LIQUIDITY,TOS_Z_THRESHOLD,TOS_MAX_STRIKE_CROSSES,TOS_MIN_EFFICIENCY_RATIO,MIN_EDGE_THRESHOLD,MIN_ENTRY_ASK,FV_ENTRY_MAX,FV_ENTRY_MIN,FV_STALE_MS,MIN_WINDOW_AGE_S,FILL_COOLDOWN_MS,PAPER_TRADE_SHARES,MAX_SHARES_PER_SIDE,MAX_SPEND_PER_MARKET,MAX_ENTRIES_PER_WINDOW,EARLY_HIGH_CONFIDENCE_BID,LATE_WINDOW_SECONDS,LATE_SL_FLOOR,LATE_TP_BID,EMERGENCY_SECONDS,EMERGENCY_CUT_PRICE,EMERGENCY_FV_CONFIRM,EMERGENCY_TP_BID,BTC_MOMENTUM_GATE,ORDERBOOK_IMBALANCE_GATE,SIGNAL_MIN_LIQUIDITY,filter_net_pnl,filter_roi,filter_win_rate,filter_sharpe,filter_drawdown,filter_trades,train_skipped,train_net_pnl,train_roi,train_win_rate,train_sharpe,train_drawdown,train_trades,val_net_pnl,val_roi,val_win_rate,val_sharpe,val_drawdown,val_trades,val_skipped,passed_validation,error
7fde296d,2026-06-11T19:34:24.741289+00:00,bayesian,,190.0,270.0,0.8,0.12000000000000001,60.0,1.2000000000000002,20,0.1,0.060000000000000005,0.03,0.89,0.03,300.0,160.0,1000.0,5.0,10.0,8.0,5,0.8,160.0,0.04,0.8799999999999999,70.0,0.18,0.2,0.8,0.0017000000000000001,0.55,25.0,13.2,0.3158,1.0,2.33,0.0,9,False,1.5,0.008,0.7568,0.0135,-12.65,37,-15.9,-0.1372,0.7308,-0.2055,-16.0,26,False,False,
ba35ed21,2026-06-11T19:39:56.668183+00:00,bayesian,,160.0,260.0,0.85,0.09000000000000001,100.0,0.8500000000000001,20,0.45,0.09,0.03,0.9299999999999999,0.01,900.0,140.0,7000.0,5.0,10.0,8.0,5,0.85,140.0,0.1,0.7999999999999999,70.0,0.25,0.45,0.95,0.0013000000000000002,0.35000000000000003,30.0,1.8,0.0638,0.8333,0.1331,-2.9,6,False,-2.55,-0.1453,0.75,-0.2646,-2.55,4,,,,,,,True,False,
a3c97118,2026-06-11T19:44:25.922520+00:00,bayesian,,190.0,270.0,0.7,0.12000000000000001,50.0,0.55,50,0.0,0.02,0.12000000000000001,0.97,0.13,400.0,20.0,20000.0,5.0,10.0,8.0,5,0.9,80.0,0.04,0.86,90.0,0.22000000000000003,0.4,0.8,0.0007000000000000001,0.30000000000000004,45.0,31.15,0.1117,0.8621,0.2199,0.0,58,False,8.0,0.005,0.7337,0.008,-13.75,323,-14.3,-0.0178,0.716,-0.0266,-32.55,169,False,False,
5a409a31,2026-06-11T19:51:53.351574+00:00,bayesian,,180.0,280.0,0.65,0.06,10.0,0.8,15,0.05,0.03,0.07,0.89,0.13,900.0,180.0,27000.0,5.0,10.0,8.0,5,0.85,100.0,0.08,0.72,80.0,0.14,0.15,0.75,0.0019000000000000002,0.4,50.0,14.3,0.0194,0.8235,0.0466,-6.7,136,False,-84.7,-0.0414,0.7727,-0.0796,-86.8,374,,,,,,,True,False,
93423fef,2026-06-11T19:57:52.354355+00:00,bayesian,,190.0,260.0,0.85,0.06,60.0,0.3,15,0.4,0.06999999999999999,0.13,0.9099999999999999,0.13,400.0,80.0,23000.0,5.0,10.0,8.0,5,0.85,60.0,0.17,0.7799999999999999,60.0,0.07,0.4,0.8,0.0007000000000000001,0.2,45.0,1.65,0.1236,1.0,2.4004,0.0,3,False,1.65,0.1236,1.0,2.4004,0.0,3,-7.55,-0.3348,0.75,-0.3924,-8.15,4,False,False,
2fdad146,2026-06-11T20:05:37.212831+00:00,bayesian,,240.0,260.0,0.8,0.04,50.0,0.45,20,0.2,0.03,0.02,0.9299999999999999,0.01,600.0,20.0,11000.0,5.0,10.0,8.0,5,0.95,180.0,0.04,0.7799999999999999,70.0,0.12000000000000001,0.30000000000000004,0.9,0.0017000000000000001,0.30000000000000004,15.0,0,0.0,0.0,0.0,0.0,0,True,,,,,,,,,,,,,True,False,
078ac1c7,2026-06-11T20:06:57.705369+00:00,bayesian,,240.0,260.0,0.7,0.06,40.0,1.2000000000000002,45,0.35000000000000003,0.09999999999999999,0.04,0.9299999999999999,0.13,700.0,40.0,17000.0,5.0,10.0,8.0,5,0.75,60.0,0.14,0.76,70.0,0.11,0.45,0.85,0.0008,0.4,10.0,0,0.0,0.0,0.0,0.0,0,True,,,,,,,,,,,,,True,False,
6fd12ffb,2026-06-11T20:08:20.333590+00:00,bayesian,,240.0,290.0,0.7,0.04,30.0,0.8,50,0.0,0.04,0.09000000000000001,0.87,0.13,700.0,60.0,13000.0,5.0,10.0,8.0,5,0.9,160.0,0.05,0.82,80.0,0.22999999999999998,0.15,0.8,0.0017000000000000001,0.55,40.0,-13.6,-0.0355,0.7821,-0.0689,-23.55,78,True,,,,,,,,,,,,,True,False,
92eb5ce4,2026-06-11T20:09:41.284815+00:00,bayesian,,160.0,250.0,0.8,0.05,90.0,0.9500000000000001,25,0.45,0.060000000000000005,0.15,0.9299999999999999,0.06999999999999999,300.0,80.0,14000.0,5.0,10.0,8.0,5,0.8,80.0,0.17,0.8999999999999999,70.0,0.09,0.2,0.9,0.0004,0.30000000000000004,30.0,-6.2,-0.2366,0.6667,-0.4035,-6.7,6,True,,,,,,,,,,,,,True,False,
779ecac1,2026-06-11T20:11:01.116788+00:00,bayesian,,210.0,290.0,0.8,0.05,80.0,0.45,25,0.35000000000000003,0.03,0.04,0.99,0.13,200.0,80.0,28000.0,5.0,10.0,8.0,5,0.75,140.0,0.19,0.7,20.0,0.18,0.2,0.85,0.0001,0.2,50.0,0.35,0.0753,1.0,0.0,0.0,1,False,0.35,0.0753,1.0,0.0,0.0,1,-2.85,-0.1023,0.8333,-0.229,-3.15,6,False,False,
d4da2139,2026-06-11T20:18:42.068103+00:00,bayesian,,210.0,240.0,0.6,0.15,70.0,0.25,5,0.2,0.08,0.15,0.85,0.06999999999999999,500.0,120.0,22000.0,5.0,10.0,8.0,5,0.95,120.0,0.14,0.9199999999999999,40.0,0.05,0.35,0.75,0.0011,0.2,20.0,-4.1,-1.0,0.0,0.0,-4.1,1,True,,,,,,,,,,,,,True,False,
5cbf0ab0,2026-06-11T20:20:01.502612+00:00,bayesian,,190.0,270.0,0.7,0.1,30.0,0.5,40,0.2,0.060000000000000005,0.12000000000000001,0.99,0.09,400.0,0.0,21000.0,5.0,10.0,8.0,5,0.9,60.0,0.1,0.84,50.0,0.05,0.35,0.8,0.0007000000000000001,0.25,40.0,-4.5,-0.1525,0.7143,-0.276,-5.2,7,True,,,,,,,,,,,,,True,False,
6d6847a4,2026-06-11T20:21:22.899145+00:00,bayesian,,210.0,270.0,0.85,0.02,60.0,0.25,35,0.30000000000000004,0.01,0.12000000000000001,0.97,0.15,500.0,100.0,22000.0,5.0,10.0,8.0,5,0.9,80.0,0.14,0.86,90.0,0.2,0.4,0.85,0.0007000000000000001,0.25,40.0,-4.0,-0.4444,0.5,-0.6018,-4.35,2,True,,,,,,,,,,,,,True,False,
d84449cd,2026-06-11T20:22:44.231245+00:00,bayesian,,180.0,250.0,0.75,0.15,50.0,0.6000000000000001,5,0.5,0.01,0.12000000000000001,0.95,0.09,200.0,0.0,30000.0,5.0,10.0,8.0,5,0.85,80.0,0.2,0.74,50.0,0.15000000000000002,0.35,0.8,0.0011,0.4,45.0,0,0.0,0.0,0.0,0.0,0,True,,,,,,,,,,,,,True,False,
82b5aa2a,2026-06-11T20:24:04.261174+00:00,bayesian,,200.0,280.0,0.6,0.09000000000000001,10.0,0.6000000000000001,35,0.1,0.08,0.1,0.89,0.15,400.0,40.0,19000.0,5.0,10.0,8.0,5,0.9,100.0,0.07,0.82,90.0,0.08,0.4,0.75,0.00030000000000000003,0.30000000000000004,35.0,1.75,0.0136,0.8276,0.0337,-10.0,29,False,4.25,0.01,0.7865,0.0193,-2.65,89,3.45,0.0095,0.8228,0.0199,-9.2,79,False,False,
aee27075,2026-06-11T20:31:47.133034+00:00,bayesian,,220.0,280.0,0.6,0.08,10.0,0.35,35,0.1,0.08,0.09000000000000001,0.89,0.15,500.0,60.0,25000.0,5.0,10.0,8.0,5,0.8,100.0,0.09,0.7999999999999999,30.0,0.08,0.30000000000000004,0.75,0.0001,0.2,35.0,-6.0,-0.0698,0.75,-0.1568,-11.85,20,True,,,,,,,,,,,,,True,False,
f7bf124c,2026-06-11T20:33:08.200553+00:00,bayesian,,150.0,280.0,0.65,0.08,20.0,0.65,10,0.30000000000000004,0.06999999999999999,0.1,0.9099999999999999,0.11,700.0,100.0,16000.0,5.0,10.0,8.0,5,0.85,100.0,0.07,0.76,50.0,0.08,0.4,0.75,0.0004,0.45,5.0,-6.25,-0.0877,0.8462,-0.133,-8.15,13,True,,,,,,,,,,,,,True,False,
9efab204,2026-06-11T20:34:28.062755+00:00,bayesian,,200.0,240.0,0.75,0.11,80.0,0.65,30,0.15000000000000002,0.09999999999999999,0.07,0.85,0.15,400.0,40.0,18000.0,5.0,10.0,8.0,5,0.95,60.0,0.12,0.82,60.0,0.1,0.30000000000000004,0.75,0.0004,0.25,30.0,0.75,0.0256,0.8571,0.0572,-1.3,7,False,-2.05,-0.025,0.7895,-0.0495,-2.05,19,,,,,,,True,False,
59a4f2e3,2026-06-11T20:40:24.109382+00:00,bayesian,,170.0,250.0,0.65,0.07,30.0,0.35,40,0.4,0.05,0.13999999999999999,0.9099999999999999,0.11,300.0,60.0,24000.0,5.0,10.0,8.0,5,0.9,120.0,0.17,0.7799999999999999,90.0,0.07,0.45,0.85,0.001,0.5,35.0,0,0.0,0.0,0.0,0.0,0,True,,,,,,,,,,,,,True,False,
49a399e8,2026-06-11T20:41:44.425778+00:00,bayesian,,220.0,290.0,0.75,0.13,100.0,1.05,10,0.25,0.08,0.1,0.87,0.11,1000.0,120.0,11000.0,5.0,10.0,8.0,5,0.85,120.0,0.12,0.84,40.0,0.14,0.4,0.8,0.00030000000000000003,0.35000000000000003,45.0,0,0.0,0.0,0.0,0.0,0,True,,,,,,,,,,,,,True,False,
f6dc42e6,2026-06-11T20:43:04.717957+00:00,bayesian,,180.0,280.0,0.6,0.09000000000000001,20.0,0.7,30,0.5,0.06999999999999999,0.07,0.9099999999999999,0.15,600.0,40.0,5000.0,5.0,10.0,8.0,5,0.8,60.0,0.06,0.74,60.0,0.060000000000000005,0.35,0.75,0.0014000000000000002,0.6,25.0,0.7,0.1628,1.0,0.0,0.0,1,False,0.7,0.1628,1.0,0.0,0.0,1,0.65,0.1494,1.0,0.0,0.0,1,False,False,
MANUAL04,2026-06-11T21:10:06.660412+00:00,manual,,200.0,280.0,0.6,0.09,10.0,0.6,35,0.1,0.08,0.1,0.89,0.15,400.0,40.0,8000.0,5.0,10.0,8.0,5,0.9,100.0,0.07,0.82,90.0,0.08,0.4,0.75,0.0003,0.3,35.0,,,,,,,,,,,,,,,,,,,,,,
MANUAL05,2026-06-11T21:10:06.660412+00:00,manual,,200.0,280.0,0.6,0.09,10.0,0.6,35,0.1,0.08,0.1,0.89,0.15,400.0,40.0,19000.0,5.0,10.0,8.0,5,0.9,100.0,0.07,0.79,90.0,0.08,0.4,0.73,0.0003,0.3,35.0,,,,,,,,,,,,,,,,,,,,,,
MANUAL06,2026-06-11T21:10:06.660412+00:00,manual,,200.0,280.0,0.6,0.09,10.0,0.6,35,0.1,0.08,0.1,0.89,0.15,400.0,40.0,8000.0,5.0,10.0,8.0,5,0.9,100.0,0.07,0.79,90.0,0.08,0.4,0.73,0.0003,0.3,35.0,,,,,,,,,,,,,,,,,,,,,,
8a518273,2026-06-11T21:58:00.015908+00:00,bayesian,,190.0,270.0,0.65,0.13,50.0,0.55,50,0.0,0.02,0.13,0.97,0.13,400.0,20.0,19000.0,5.0,10.0,8.0,5,0.9,80.0,0.03,0.86,90.0,0.22000000000000003,0.4,0.8,0.0006000000000000001,0.30000000000000004,45.0,15.45,0.0351,0.7717,0.0614,-1.65,92,False,14.65,0.0095,0.7357,0.0149,-7.7,314,-6.9,-0.009,0.7108,-0.0135,-25.1,166,False,False,
1254d9fd,2026-06-11T22:05:42.996186+00:00,bayesian,,200.0,260.0,0.6,0.13,70.0,0.4,45,0.05,0.09,0.13,0.95,0.11,400.0,20.0,18000.0,5.0,10.0,8.0,5,0.9,100.0,0.03,0.82,80.0,0.16,0.4,0.8,0.0005,0.35000000000000003,50.0,9.15,0.0424,0.8043,0.0868,-1.4,46,False,-0.85,-0.0014,0.7537,-0.0024,-15.9,134,,,,,,,True,False,
9c255c0c,2026-06-11T22:11:39.872162+00:00,bayesian,,200.0,270.0,0.65,0.02,40.0,0.55,45,0.05,0.05,0.1,0.97,0.15,300.0,80.0,24000.0,5.0,10.0,8.0,5,0.95,80.0,0.06,0.8799999999999999,90.0,0.12000000000000001,0.45,0.75,0.0009000000000000001,0.25,35.0,6.05,0.0087,0.888,0.0277,-4.25,125,False,-23.85,-0.0127,0.8542,-0.0317,-28.6,343,,,,,,,True,False,
e25f3f22,2026-06-11T22:17:37.002006+00:00,bayesian,,170.0,280.0,0.6,0.1,60.0,0.7,35,0.0,0.06999999999999999,0.13999999999999999,0.87,0.13,600.0,40.0,20000.0,5.0,10.0,8.0,5,0.85,60.0,0.17,0.84,80.0,0.2,0.35,0.8,0.0006000000000000001,0.30000000000000004,45.0,30.75,0.039,0.7801,0.0739,-8.25,141,False,-31.15,-0.0117,0.7383,-0.0195,-40.9,470,,,,,,,True,False,
03cbc022,2026-06-11T22:23:36.347269+00:00,bayesian,,190.0,260.0,0.65,0.13999999999999999,70.0,0.35,40,0.1,0.09,0.11,0.95,0.09,500.0,0.0,15000.0,5.0,10.0,8.0,5,0.9,80.0,0.03,0.7999999999999999,60.0,0.25,0.4,0.85,0.0002,0.2,40.0,-6.6,-0.0619,0.72,-0.1196,-9.6,25,True,,,,,,,,,,,,,True,False,
de3a0733,2026-06-11T22:24:55.751314+00:00,bayesian,,220.0,270.0,0.85,0.07,40.0,0.25,50,0.15000000000000002,0.04,0.13,0.89,0.11,200.0,20.0,26000.0,5.0,10.0,8.0,5,0.9,100.0,0.12,0.86,90.0,0.1,0.45,0.8,0.0005,0.25,35.0,1.8,0.1364,1.0,3.3282,0.0,3,False,-2.45,-0.0516,0.8182,-0.1159,-3.0,11,,,,,,,True,False,
95f097ea,2026-06-11T22:30:52.937951+00:00,bayesian,,200.0,280.0,0.6,0.11,20.0,0.5,30,0.15000000000000002,0.02,0.08,0.9099999999999999,0.15,400.0,60.0,19000.0,5.0,10.0,8.0,5,0.85,80.0,0.07,0.7799999999999999,80.0,0.07,0.30000000000000004,0.75,0.0009000000000000001,0.35000000000000003,45.0,-0.6,-0.0234,0.8333,-0.051,-1.3,6,True,,,,,,,,,,,,,True,False,
e41a0788,2026-06-11T22:32:13.561498+00:00,bayesian,,170.0,270.0,0.75,0.09000000000000001,80.0,0.7,15,0.05,0.04,0.13999999999999999,0.87,0.13,300.0,100.0,23000.0,5.0,10.0,8.0,5,0.95,60.0,0.16,0.9199999999999999,20.0,0.13,0.25,0.9,0.00030000000000000003,0.30000000000000004,50.0,30.35,0.0533,0.8365,0.114,-0.75,104,False,-23.6,-0.0145,0.7745,-0.0261,-30.15,306,,,,,,,True,False,
6672f432,2026-06-11T22:38:12.577614+00:00,bayesian,,180.0,250.0,0.65,0.12000000000000001,60.0,0.9500000000000001,25,0.25,0.05,0.11,0.89,0.05,500.0,80.0,12000.0,5.0,10.0,8.0,5,0.8,120.0,0.05,0.8999999999999999,60.0,0.16999999999999998,0.4,0.8,0.0006000000000000001,0.25,25.0,-5.45,-0.2665,0.6,-0.4031,-5.45,5,True,,,,,,,,,,,,,True,False,
bd462690,2026-06-11T22:39:32.960695+00:00,bayesian,,190.0,260.0,0.8,0.04,50.0,0.6000000000000001,20,0.1,0.060000000000000005,0.13,0.99,0.15,400.0,40.0,3000.0,5.0,10.0,8.0,5,0.9,100.0,0.1,0.82,40.0,0.2,0.35,0.85,0.0013000000000000002,0.45,40.0,12.85,0.0297,0.9143,0.1045,-3.8,70,False,-22.4,-0.0201,0.8531,-0.0467,-24.75,177,,,,,,,True,False,
9e0a074d,2026-06-11T22:45:31.383893+00:00,bayesian,,190.0,270.0,0.7,0.12000000000000001,50.0,0.55,50,0.0,0.02,0.11,0.97,0.13,400.0,20.0,20000.0,5.0,10.0,8.0,5,0.9,80.0,0.04,0.86,90.0,0.22000000000000003,0.4,0.8,0.0007000000000000001,0.30000000000000004,45.0,18.05,0.0378,0.7812,0.067,-2.0,96,False,8.0,0.005,0.7337,0.008,-13.75,323,-14.3,-0.0178,0.716,-0.0266,-32.55,169,False,False,
c0b2e3a6,2026-06-11T22:53:13.536301+00:00,bayesian,,200.0,270.0,0.7,0.13,60.0,0.45,45,0.0,0.02,0.12000000000000001,0.97,0.13,300.0,20.0,9000.0,5.0,10.0,8.0,5,0.9,80.0,0.03,0.8799999999999999,90.0,0.22999999999999998,0.4,0.8,0.0008,0.35000000000000003,45.0,9.1,0.0313,0.7619,0.0538,-5.0,63,False,-7.1,-0.0066,0.7252,-0.0101,-26.3,222,,,,,,,True,False,
ac972a7d,2026-06-11T22:59:09.802367+00:00,bayesian,,180.0,270.0,0.85,0.11,40.0,0.75,50,0.05,0.01,0.13,0.95,0.11,300.0,0.0,17000.0,5.0,10.0,8.0,5,0.85,60.0,0.05,0.86,80.0,0.22000000000000003,0.45,0.95,0.0005,0.30000000000000004,50.0,20.75,0.0507,0.8415,0.106,-1.85,82,False,3.3,0.0029,0.7735,0.0051,-1.75,234,-36.15,-0.0512,0.7466,-0.0894,-42.35,146,False,False,
7f1d6571,2026-06-11T23:06:50.911546+00:00,bayesian,,190.0,260.0,0.65,0.13999999999999999,70.0,0.6000000000000001,40,0.0,0.03,0.11,0.97,0.13,600.0,160.0,20000.0,5.0,10.0,8.0,5,0.95,100.0,0.08,0.84,70.0,0.19,0.45,0.8,0.0006000000000000001,0.4,40.0,3.15,0.008,0.75,0.0131,-4.6,80,False,-6.75,-0.0047,0.727,-0.0072,-42.55,293,,,,,,,True,False,
9b8e8a0f,2026-06-11T23:12:48.594343+00:00,bayesian,,210.0,280.0,0.6,0.12000000000000001,10.0,0.8500000000000001,50,0.05,0.02,0.09000000000000001,0.9299999999999999,0.15,400.0,20.0,27000.0,5.0,10.0,8.0,5,0.85,80.0,0.04,0.7999999999999999,80.0,0.24,0.4,0.75,0.00030000000000000003,0.30000000000000004,35.0,8.2,0.0378,0.8367,0.0703,-0.8,49,False,26.8,0.043,0.7872,0.071,-1.8,141,-2.2,-0.0048,0.7982,-0.0078,-11.4,109,False,False,
00358566,2026-06-11T23:20:24.356788+00:00,bayesian,,230.0,290.0,0.6,0.1,10.0,0.9,45,0.1,0.09,0.08,0.9099999999999999,0.15,500.0,20.0,28000.0,5.0,10.0,8.0,5,0.85,60.0,0.06,0.7999999999999999,70.0,0.24,0.4,0.75,0.0002,0.35000000000000003,30.0,-7.2,-0.1706,0.7,-0.2938,-12.75,10,True,,,,,,,,,,,,,True,False,
68f04c64,2026-06-11T23:21:43.472771+00:00,bayesian,,210.0,280.0,0.6,0.13999999999999999,10.0,1.05,15,0.45,0.09999999999999999,0.05,0.9299999999999999,0.15,400.0,40.0,26000.0,5.0,10.0,8.0,5,0.85,80.0,0.04,0.76,80.0,0.24,0.35,0.75,0.00030000000000000003,0.2,30.0,0,0.0,0.0,0.0,0.0,0,True,,,,,,,,,,,,,True,False,
e218065f,2026-06-11T23:23:04.070382+00:00,bayesian,,210.0,280.0,0.6,0.06,20.0,1.2000000000000002,50,0.4,0.08,0.06,0.9299999999999999,0.01,200.0,60.0,29000.0,5.0,10.0,8.0,5,0.8,180.0,0.03,0.7799999999999999,80.0,0.22000000000000003,0.45,0.75,0.0002,0.30000000000000004,20.0,0.35,0.0753,1.0,0.0,0.0,1,False,0.35,0.0753,1.0,0.0,0.0,1,-3.5,-0.2593,0.6667,-0.3813,-3.5,3,False,False,
e9a6a04a,2026-06-11T23:32:04.479229+00:00,bayesian,,200.0,290.0,0.65,0.09000000000000001,90.0,1.05,45,0.15000000000000002,0.03,0.09000000000000001,0.89,0.13,800.0,180.0,24000.0,5.0,10.0,8.0,5,0.85,80.0,0.04,0.82,90.0,0.25,0.4,0.75,0.0004,0.4,35.0,2.05,0.0477,0.8889,0.1303,0.0,9,False,-13.5,-0.0801,0.7297,-0.1633,-16.3,37,,,,,,,True,False,
65bba96b,2026-06-11T23:39:03.658792+00:00,bayesian,,230.0,280.0,0.6,0.03,30.0,0.8500000000000001,35,0.05,0.06999999999999999,0.08,0.9099999999999999,0.13,300.0,0.0,22000.0,5.0,10.0,8.0,5,0.75,140.0,0.08,0.7999999999999999,70.0,0.09,0.35,0.8,0.0001,0.25,40.0,-3.2,-0.0088,0.88,-0.0259,-13.2,75,True,,,,,,,,,,,,,True,False,
```


## strategies/base.py

```py
"""
strategies/base.py
──────────────────
Shared data types and the BaseStrategy interface.

All strategies must implement BaseStrategy.  SmartPaperTrader knows nothing
about individual strategies beyond this interface.

Data types (FVState, PMState, Position) are defined here so strategies can
import them without creating a circular dependency on smart_paper_trader.py.
smart_paper_trader.py re-exports them for backward compatibility.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ── Market data types ──────────────────────────────────────────────────────────

@dataclass
class FVState:
    """
    Latest snapshot from the FV engine (fv_engine.py → Channel.FV_STREAM).

    Populated by SmartPaperTrader._on_fv() and passed read-only to strategies.
    """
    ts_ms:          int   = 0
    market_id:      str   = ""
    boundary_ts:    int   = 0
    prob_up:        float = 0.5
    prob_down:      float = 0.5
    sigma:          float = 0.0
    btc_price:      float = 0.0
    intra_vol:      float = 0.0    # raw EWMA vol before floor; 0.0 during warmup
    is_sigma_real:  bool  = False  # True when sigma from intra-window EWMA
    z_score:        float = 0.0    # standardised distance from strike
    strike:         float = 0.0    # BTC/USD strike price for this window


@dataclass
class PMState:
    """
    Latest snapshot from the prediction-market order book (pm_daemon → Channel.PM_BOOK).

    Populated by SmartPaperTrader._on_pm() and passed read-only to strategies.
    """
    ts_ms:      int             = 0
    market_id:  str             = ""
    bid_up:     Optional[float] = None
    ask_up:     Optional[float] = None
    bid_down:   Optional[float] = None
    ask_down:   Optional[float] = None
    market_ts:  int             = 0   # unix epoch of window open
    end_ts:     int             = 0   # unix epoch of window close
    liq_up:     float           = 0.0
    liq_down:   float           = 0.0
    # Pre-computed by pm_daemon (Task 3.3); None when from old-format message.
    _combined_ask: Optional[float] = None

    def combined_ask(self) -> Optional[float]:
        """
        Sum of both-side asks; None if either leg is absent.
        Uses the pre-computed value from pm_daemon when available, otherwise
        falls back to on-demand computation for backward compatibility.
        """
        if self._combined_ask is not None:
            return self._combined_ask
        if self.ask_up is None or self.ask_down is None:
            return None
        return self.ask_up + self.ask_down


@dataclass
class Position:
    """An open paper-trading position held by SmartPaperTrader."""
    market_id: str
    side:      str
    shares:    float = 0.0
    cost:      float = 0.0

    @property
    def avg_entry(self) -> float:
        return self.cost / self.shares if self.shares > 0 else 0.0

    def unrealised_pct(self, current_bid: float) -> float:
        """Unrealised P&L as a fraction of cost. Negative = loss."""
        if self.cost <= 0:
            return 0.0
        return (current_bid * self.shares - self.cost) / self.cost


# ── Strategy result type ───────────────────────────────────────────────────────

@dataclass
class EntrySignal:
    """
    Returned by BaseStrategy.evaluate_entry() when the strategy wants an entry.

    SmartPaperTrader checks position caps and cooldowns before executing.
    The strategy is responsible only for the DECISION; execution is the
    orchestrator's job.
    """
    side:      str
    ask:       float
    fv:        float
    edge:      float
    elapsed_s: float
    z_score:   float


# ── Strategy interface ─────────────────────────────────────────────────────────

class BaseStrategy(ABC):
    """
    Interface every trading strategy must implement.

    SmartPaperTrader calls these methods and knows nothing else about the
    strategy internals.  Each concrete strategy owns its own config, stats,
    and signal logic.

    Lifecycle per market window
    ───────────────────────────
    1. on_fv_update(fv)       — called on every FV message
    2. evaluate_entry(fv, pm) — called on every PM tick to decide entries
    3. evaluate_exit(...)     — called per open position on every PM tick
    4. reset_for_market()     — called when the market window transitions

    Diagnostics
    ───────────
    get_diagnostics() must answer:
      • Why was a trade accepted?
      • Why was a trade rejected?
      • Which filter blocked it?
      • Which signal voted for or against it?
    without requiring the caller to inspect strategy internals.
    """

    # ── Required: entry / exit decisions ──────────────────────────────────────

    @abstractmethod
    def evaluate_entry(self, fv: FVState, pm: PMState) -> List[EntrySignal]:
        """
        Evaluate entry criteria for the current market tick.

        Returns a (possibly empty) list of EntrySignal objects.
        SmartPaperTrader applies position-cap and fill-cooldown checks before
        executing any signal — the strategy does not need to check these.

        Returns [] when no entry is warranted.
        """

    @abstractmethod
    def evaluate_exit(
        self,
        pos:   Position,
        fv:    FVState,
        pm:    PMState,
        ts_ms: int,
    ) -> Optional[str]:
        """
        Evaluate whether an open position should be exited mid-window.

        Returns the exit reason string (e.g. "TAKE_PROFIT", "STOP_LOSS",
        "EMERGENCY_CUT") or None to hold.

        Settlement exits are handled by SmartPaperTrader, not here.
        """

    # ── Optional: hooks ───────────────────────────────────────────────────────

    def on_fv_update(self, fv: FVState) -> None:
        """
        Called on every FV tick.

        Override to maintain strategy-internal state that depends on FV
        history (e.g. the 30-second BTC price lookback for momentum signals).
        Default: no-op.
        """

    def reset_for_market(self) -> None:
        """
        Called when the market window transitions (new market_id on PM book).

        Override to reset per-window counters and state.
        Default: no-op.
        """

    # ── Required: diagnostics ─────────────────────────────────────────────────

    @abstractmethod
    def get_diagnostics(self) -> Dict[str, Any]:
        """
        Return structured diagnostics answering:
          • Why was the last entry accepted or rejected?
          • Which filter blocked it?
          • Which signal voted for or against it?

        The returned dict is flat or nested — SmartPaperTrader uses it for
        status display without needing to know strategy internals.
        """

    # ── Optional: metadata ────────────────────────────────────────────────────

    @property
    def name(self) -> str:
        """Human-readable strategy name used in log output."""
        return type(self).__name__
```


## strategies/tos_signal/__init__.py

```py
from strategies.tos_signal.strategy import TOSSignalStrategy
from strategies.tos_signal.signal_stack import SignalStack

__all__ = ["TOSSignalStrategy", "SignalStack"]
```

## strategies/tos_signal/config.py

```py
"""
strategies/tos_signal/config.py
────────────────────────────────
Configuration for TOS_SIGNAL.

TOS_SIGNAL uses the same entry thresholds as TOS (timing, z-score, prob, edge,
liquidity) and adds the signal-stack gate on top.  All values are imported
from strategies.tos.config — there is intentionally no duplication.

Signal-stack thresholds (0.0004 momentum gate, 0.40 imbalance gate, 20-share
minimum, 2-second BTC history tolerance) are hardcoded in SignalStack because
they are part of the signal logic, not operational configuration.  They live
alongside the logic in signal_stack.py.
"""

from strategies.tos.config import (   # noqa: F401  (re-export for callers)
    TOS_ENTRY_START_S,
    TOS_ENTRY_END_S,
    TOS_MIN_PROB,
    TOS_MIN_EDGE,
    TOS_MIN_LIQUIDITY,
    TOS_Z_THRESHOLD,
    EXIT_POLICY,
)
import os

TOS_MAX_STRIKE_CROSSES: int = int(os.getenv("TOS_MAX_STRIKE_CROSSES", "999"))
TOS_MIN_EFFICIENCY_RATIO: float = float(os.getenv("TOS_MIN_EFFICIENCY_RATIO", "0.0"))

__all__ = [
    "TOS_ENTRY_START_S",
    "TOS_ENTRY_END_S",
    "TOS_MIN_PROB",
    "TOS_MIN_EDGE",
    "TOS_MIN_LIQUIDITY",
    "TOS_Z_THRESHOLD",
    "EXIT_POLICY",
    "TOS_MAX_STRIKE_CROSSES",
    "TOS_MIN_EFFICIENCY_RATIO",
]
```

## strategies/tos_signal/signal_stack.py

```py
"""
strategies/tos_signal/signal_stack.py
──────────────────────────────────────
Architecture C signal stack for TOS_SIGNAL.

Provides two independent signals that vote on directional conviction:
  1. btc_momentum_signal  — BTC displacement from K with 30-second persistence gate
  2. orderbook_imbalance_signal — liquidity skew on the PM book

Consensus rule: plurality voting.
  up > dn AND up >= 1  →  "UP"
  dn > up AND dn >= 1  →  "DOWN"
  up == dn             →  None  (tie: conflict, both-None, or no view)

Concretely:
  momentum=UP,   imbalance=None  →  "UP"   (single clear signal)
  momentum=None, imbalance=DOWN  →  "DOWN" (single clear signal)
  momentum=UP,   imbalance=DOWN  →  None   (active conflict — blocked)
  momentum=None, imbalance=None  →  None   (no view — blocked)
  momentum=UP,   imbalance=UP    →  "UP"   (both agree)

DO NOT change signal thresholds or gate logic here without updating
tests/test_smart_paper_trader.py and progress.md.

Threshold values (BTC_MOMENTUM_GATE, ORDERBOOK_IMBALANCE_GATE,
SIGNAL_MIN_LIQUIDITY) are read from environment variables so the
optimizer can override them per-run.  Defaults preserve original behaviour.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from strategies.base import FVState, PMState

# ── Tunable signal-stack thresholds ───────────────────────────────────────────
# These were previously hardcoded literals.  They are now read from environment
# variables so the optimizer can override them per-run without code changes.
# Defaults match the original hardcoded values exactly.

# BTC displacement from strike (as a fraction of K) required for the momentum
# signal to fire.  0.0004 = 0.04% of K.
BTC_MOMENTUM_GATE: float = float(os.getenv("BTC_MOMENTUM_GATE", "0.0004"))

# Minimum directional liquidity skew of combined PM depth before the imbalance
# signal fires.  0.40 = 40% skew toward one side.
ORDERBOOK_IMBALANCE_GATE: float = float(os.getenv("ORDERBOOK_IMBALANCE_GATE", "0.40"))

# Minimum combined PM depth (liq_up + liq_down) required before the imbalance
# signal is evaluated.  Below this the book is too thin to trust.
SIGNAL_MIN_LIQUIDITY: float = float(os.getenv("SIGNAL_MIN_LIQUIDITY", "20.0"))


class SignalStack:
    """Architecture C signal stack — momentum + orderbook imbalance."""

    def btc_momentum_signal(
        self,
        btc_now: float,
        btc_30s_ago: float,
        pm_K: float,
    ) -> Optional[str]:
        """
        BTC displacement from strike with 30-second persistence gate.

        Gate 1 — magnitude: |delta_now| >= BTC_MOMENTUM_GATE (default 0.04% of K).
        Gate 2 — persistence: BTC must have been on the same side of K 30s ago.

        Returns "UP", "DOWN", or None.
        """
        if btc_30s_ago <= 0 or pm_K <= 0:
            return None

        delta_now = (btc_now - pm_K) / pm_K        # displacement from K right now
        delta_30s = (btc_30s_ago - pm_K) / pm_K    # displacement from K 30s ago

        # Gate 1 — magnitude
        if abs(delta_now) < BTC_MOMENTUM_GATE:
            return None

        # Gate 2 — persistence: same side of K 30s ago
        if _sign(delta_now) != _sign(delta_30s):
            return None

        return "UP" if delta_now > 0 else "DOWN"

    def orderbook_imbalance_signal(self, pm: "PMState") -> Optional[str]:
        """
        Orderbook liquidity imbalance gate.

        Requires >= SIGNAL_MIN_LIQUIDITY shares combined (default 20) and
        >= ORDERBOOK_IMBALANCE_GATE directional skew (default 40%) of combined
        depth before a signal fires.

        Returns "UP", "DOWN", or None.
        """
        total_liq = pm.liq_up + pm.liq_down
        if total_liq < SIGNAL_MIN_LIQUIDITY:
            return None

        imbalance = (pm.liq_up - pm.liq_down) / total_liq
        if imbalance > ORDERBOOK_IMBALANCE_GATE:
            return "UP"
        if imbalance < -ORDERBOOK_IMBALANCE_GATE:
            return "DOWN"
        return None

    def evaluate(
        self,
        fv: "FVState",
        pm: "PMState",
        btc_30s_ago: float,
    ) -> Optional[str]:
        """
        Evaluate the full signal stack and return the consensus direction, or None.

        Plurality voting (any single unambiguous signal is sufficient):
          up > dn AND up >= 1  →  "UP"
          dn > up AND dn >= 1  →  "DOWN"
          up == dn             →  None  (tie, conflict, or both-None)
        """
        mom_sig = self.btc_momentum_signal(fv.btc_price, btc_30s_ago, fv.strike)
        imb_sig = self.orderbook_imbalance_signal(pm)

        up = int(mom_sig == "UP") + int(imb_sig == "UP")
        dn = int(mom_sig == "DOWN") + int(imb_sig == "DOWN")

        if up > dn and up >= 1:
            return "UP"
        if dn > up and dn >= 1:
            return "DOWN"
        return None


def _sign(x: float) -> int:
    """Return +1 for positive, -1 for negative, 0 for zero."""
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0
```

## strategies/tos_signal/stats.py

```py
"""
strategies/tos_signal/stats.py
────────────────────────────────
Per-window diagnostics for the TOS_SIGNAL strategy.

These counters were previously embedded in the monolithic Stats dataclass in
smart_paper_trader.py.  They belong here because they answer questions specific
to the TOS_SIGNAL signal gate, not to the overall trading session.

Usage
─────
After a live run, get_diagnostics() answers:
  • How many TOS candidates reached the signal gate?
  • Which signal was the most common blocker?
  • Was there a BTC history warmup issue?
  • How many candidates were accepted end-to-end?
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class TOSSignalStats:
    """
    Per-window rejection counters for the TOS_SIGNAL signal gate.

    All counters reset on window transition via reset_for_market().

    Counter semantics
    ─────────────────
    tos_candidates       — TOS entry decisions that reached the signal gate.
                           (TOS rejected before the signal gate do NOT count here.)
    rejected_btc_history — no BTC history point within 2s of the 30s-ago target.
    rejected_momentum    — btc_momentum_signal() returned None (incremented even
                           when consensus still passes via imbalance alone).
    rejected_imbalance   — orderbook_imbalance_signal() returned None (same rule).
    rejected_consensus   — both signals returned None → no view at all.
    rejected_disagreement— signals returned conflicting directions, OR a single
                           signal fired but pointed opposite to the TOS decision.
    accepted_signal      — candidate passed all signal gates → entry attempted.

    Note: rejected_momentum and rejected_imbalance are per-signal None counters.
    They are incremented independently of the final consensus outcome.
    A candidate can increment rejected_momentum AND accepted_signal in the same
    tick (if momentum=None but imbalance agrees with TOS → plurality passes).
    """

    tos_candidates:        int = 0
    rejected_btc_history:  int = 0
    rejected_chaos:        int = 0   # rejected by chaos metrics (crosses or eff ratio)
    rejected_momentum:     int = 0   # per-signal None counter (not exclusive)
    rejected_imbalance:    int = 0   # per-signal None counter (not exclusive)
    rejected_consensus:    int = 0   # both signals None → no view
    rejected_disagreement: int = 0   # conflict or direction mismatch with TOS
    accepted_signal:       int = 0   # all gates cleared

    def reset_for_market(self) -> None:
        """Reset all counters on window transition."""
        self.tos_candidates        = 0
        self.rejected_btc_history  = 0
        self.rejected_chaos        = 0
        self.rejected_momentum     = 0
        self.rejected_imbalance    = 0
        self.rejected_consensus    = 0
        self.rejected_disagreement = 0
        self.accepted_signal       = 0

    def as_dict(self) -> Dict[str, Any]:
        """Flat representation for get_diagnostics()."""
        return {
            "tos_candidates":        self.tos_candidates,
            "rejected_btc_history":  self.rejected_btc_history,
            "rejected_chaos":        self.rejected_chaos,
            "rejected_momentum":     self.rejected_momentum,
            "rejected_imbalance":    self.rejected_imbalance,
            "rejected_consensus":    self.rejected_consensus,
            "rejected_disagreement": self.rejected_disagreement,
            "accepted_signal":       self.accepted_signal,
        }
```

## strategies/tos_signal/strategy.py

```py
"""
strategies/tos_signal/strategy.py
──────────────────────────────────
TOS + Signal Stack strategy (Architecture C).

TOS_SIGNAL wraps a TOSStrategy and adds a two-signal confirmation gate:
  1. TOS base evaluation (timing, z-score, prob, edge, liquidity)
  2. BTC history availability check (30s lookback within 2s tolerance)
  3. Signal stack consensus:
       momentum  = btc_momentum_signal(btc_now, btc_30s_ago, K)
       imbalance = orderbook_imbalance_signal(pm)
       consensus = plurality vote
  4. Consensus must match the TOS direction (UP/DOWN)

If all four stages pass, the TOS EntrySignal is forwarded unchanged.
If any stage fails, [] is returned and the relevant counter is incremented.

BTC history is maintained internally via on_fv_update(), which is called
by SmartPaperTrader on every FV tick.  This makes the strategy self-contained
and independently testable without access to SmartPaperTrader state.

Diagnostics are split into two namespaces:
  "tos"          → TOSStrategy.get_diagnostics()  (entry gate counters)
  "signal_gate"  → TOSSignalStats.as_dict()        (signal gate counters)
"""

from __future__ import annotations

import logging
from collections import deque
from typing import Any, Dict, List, Optional

from strategies.base import BaseStrategy, EntrySignal, FVState, PMState, Position
from strategies.tos.strategy import TOSStrategy
from strategies.tos_signal.signal_stack import SignalStack
from strategies.tos_signal.stats import TOSSignalStats
from strategies.tos_signal.config import TOS_MAX_STRIKE_CROSSES, TOS_MIN_EFFICIENCY_RATIO

log = logging.getLogger(__name__)

# Maximum seconds between a history point and the 30s-ago target timestamp.
# Points older than this are considered too imprecise for momentum evaluation.
_BTC_HISTORY_TOLERANCE_S: float = 2.0

# BTC history downsampling rate: keep at most one point per second.
# maxlen=600 then covers 600s = 10min (2 full windows).
_BTC_HISTORY_SAMPLE_S: float = 1.0
_BTC_HISTORY_MAXLEN:   int   = 600


class TOSSignalStrategy(BaseStrategy):
    """
    TOS entry gate + Architecture C signal confirmation.

    Call on_fv_update(fv) on every FV tick to maintain BTC price history.
    Call evaluate_entry(fv, pm) on every PM tick to get entry decisions.
    """

    def __init__(self) -> None:
        # Inner TOS strategy for base evaluation
        self._tos = TOSStrategy()

        # Signal infrastructure
        self._signal_stack = SignalStack()
        self._btc_history: deque[tuple[float, float]] = deque(maxlen=_BTC_HISTORY_MAXLEN)

        # Per-window signal gate stats
        self._stats = TOSSignalStats()

        # Chaos tracking variables
        self._strike_crosses: int = 0
        self._path_length: float = 0.0
        self._first_price: Optional[float] = None
        self._prev_side: Optional[str] = None
        self._prev_tick_price: Optional[float] = None

    @property
    def name(self) -> str:
        return "TOS_SIGNAL"

    # ── BaseStrategy interface ─────────────────────────────────────────────────

    def on_fv_update(self, fv: FVState) -> None:
        """
        Maintain a downsampled BTC price history for the momentum signal.

        Downsampled to 1 Hz so maxlen=600 covers 10 minutes regardless of
        the underlying FV tick rate (~110 Hz from Binance BBO).  Without
        downsampling, the deque would only cover ~5.5s of lookback — far
        too short for the 30s momentum window.
        """
        ts_s = fv.ts_ms / 1000.0
        btc_price = float(fv.btc_price)
        
        # ── Chaos Metrics Update ──
        if self._first_price is None:
            self._first_price = btc_price
            
        if self._prev_tick_price is not None:
            self._path_length += abs(btc_price - self._prev_tick_price)
            
        self._prev_tick_price = btc_price
        
        if btc_price > fv.strike:
            side = "UP"
        elif btc_price < fv.strike:
            side = "DOWN"
        else:
            side = self._prev_side
            
        if self._prev_side is not None and side != self._prev_side:
            self._strike_crosses += 1
            
        self._prev_side = side

        # ── Downsampled BTC History Update ──
        if not self._btc_history or (ts_s - self._btc_history[-1][0]) >= _BTC_HISTORY_SAMPLE_S:
            self._btc_history.append((ts_s, btc_price))

    def evaluate_entry(self, fv: FVState, pm: PMState) -> List[EntrySignal]:
        """
        Evaluate TOS_SIGNAL entry criteria.

        Stage 1: TOS base gate.
        Stage 2: BTC history availability.
        Stage 3: Signal stack consensus.
        Stage 4: Consensus direction matches TOS direction.

        Returns [EntrySignal] on full pass, [] on any failure.
        """
        # ── Stage 1: TOS base evaluation ─────────────────────────────────────
        tos_signals = self._tos.evaluate_entry(fv, pm)
        if not tos_signals:
            return []  # TOS rejected — don't count as a signal candidate

        tos_signal = tos_signals[0]  # TOS returns at most one signal per tick
        self._stats.tos_candidates += 1

        # ── Stage 1.5: Chaos Checks ──────────────────────────────────────────
        net_movement = abs(fv.btc_price - self._first_price) if self._first_price is not None else 0.0
        efficiency_ratio = (net_movement / self._path_length) if self._path_length > 0 else 1.0

        if self._strike_crosses > TOS_MAX_STRIKE_CROSSES:
            self._stats.rejected_chaos += 1
            log.debug(
                "TOS_SIGNAL REJECT %s — chaotic market (%d crosses > %d limit)",
                tos_signal.side, self._strike_crosses, TOS_MAX_STRIKE_CROSSES
            )
            return []

        if efficiency_ratio < TOS_MIN_EFFICIENCY_RATIO:
            self._stats.rejected_chaos += 1
            log.debug(
                "TOS_SIGNAL REJECT %s — chaotic market (eff_ratio %.3f < %.3f limit)",
                tos_signal.side, efficiency_ratio, TOS_MIN_EFFICIENCY_RATIO
            )
            return []

        # ── Stage 2: BTC history availability ────────────────────────────────
        now_s      = pm.ts_ms / 1000.0
        target_ts  = now_s - 30.0
        btc_30s_ago    = 0.0
        closest_diff   = 99_999.0

        for hist_ts, hist_btc in self._btc_history:
            diff = abs(hist_ts - target_ts)
            if diff < closest_diff:
                closest_diff = diff
                btc_30s_ago  = hist_btc

        if closest_diff > _BTC_HISTORY_TOLERANCE_S:
            self._stats.rejected_btc_history += 1
            log.debug(
                "TOS_SIGNAL REJECT %s — btc_history_not_ready "
                "(closest=%.1fs diff, need <=%.1fs)",
                tos_signal.side, closest_diff, _BTC_HISTORY_TOLERANCE_S,
            )
            return []

        # ── Stage 3: individual signal evaluation ────────────────────────────
        mom_sig   = self._signal_stack.btc_momentum_signal(fv.btc_price, btc_30s_ago, fv.strike)
        imb_sig   = self._signal_stack.orderbook_imbalance_signal(pm)
        consensus = self._signal_stack.evaluate(fv, pm, btc_30s_ago)

        # Per-signal None counters (independent of consensus outcome)
        if mom_sig is None:
            self._stats.rejected_momentum += 1
        if imb_sig is None:
            self._stats.rejected_imbalance += 1

        log.debug(
            "TOS_SIGNAL EVAL %s — momentum=%s  imbalance=%s  consensus=%s  "
            "BTC=%.2f  btc_30s=%.2f  K=%.2f  delta=%.4f%%  liq_up=%.1f  liq_dn=%.1f",
            tos_signal.side,
            mom_sig, imb_sig, consensus,
            fv.btc_price, btc_30s_ago, fv.strike,
            abs(fv.btc_price - fv.strike) / fv.strike * 100 if fv.strike > 0 else 0.0,
            pm.liq_up, pm.liq_down,
        )

        # ── Stage 4: consensus present and aligned with TOS ──────────────────
        if consensus is None:
            if mom_sig is not None or imb_sig is not None:
                # At least one signal fired but they actively conflict
                self._stats.rejected_disagreement += 1
                log.debug(
                    "TOS_SIGNAL REJECT %s — signal_stack_disagreement "
                    "(momentum=%s, imbalance=%s)",
                    tos_signal.side, mom_sig, imb_sig,
                )
            else:
                # Neither signal had a view
                self._stats.rejected_consensus += 1
                log.debug(
                    "TOS_SIGNAL REJECT %s — signal_stack_no_consensus (both None)",
                    tos_signal.side,
                )
            return []

        if consensus != tos_signal.side:
            self._stats.rejected_disagreement += 1
            log.debug(
                "TOS_SIGNAL REJECT %s — signal_direction_mismatch  "
                "consensus=%s but TOS wants %s",
                tos_signal.side, consensus, tos_signal.side,
            )
            return []

        # All stages cleared
        self._stats.accepted_signal += 1
        log.debug(
            "TOS_SIGNAL ACCEPT %s — momentum=%s  imbalance=%s  "
            "consensus=%s  edge=%.4f  z=%.3f",
            tos_signal.side, mom_sig, imb_sig,
            consensus, tos_signal.edge, tos_signal.z_score,
        )

        return [tos_signal]

    def evaluate_exit(
        self,
        pos:   Position,
        fv:    FVState,
        pm:    PMState,
        ts_ms: int,
    ) -> Optional[str]:
        """TOS_SIGNAL holds every position to settlement — no mid-window exits."""
        return None

    def reset_for_market(self) -> None:
        """Reset all per-window counters on window transition."""
        self._tos.reset_for_market()
        self._stats.reset_for_market()
        self._strike_crosses = 0
        self._path_length = 0.0
        self._first_price = None
        self._prev_side = None
        self._prev_tick_price = None

    def get_diagnostics(self) -> Dict[str, Any]:
        """
        Return diagnostics for both the TOS gate and the signal gate.

        Structure:
          {
            "strategy":     "TOS_SIGNAL",
            "tos":          { ... TOSStrategy diagnostics ... },
            "signal_gate":  { ... TOSSignalStats ... }
          }

        The "signal_gate" namespace answers:
          • How many candidates reached the signal gate this window?
          • Which signal/gate caused the most rejections?
          • Were accepted_signal > 0? (non-zero = at least one entry fired)
        """
        return {
            "strategy":    "TOS_SIGNAL",
            "tos":         self._tos.get_diagnostics(),
            "signal_gate": self._stats.as_dict(),
        }
```

## tools/analyze_chaos.py

```py
import sys
import json
import base64
import msgpack
import argparse
from collections import defaultdict
from datetime import datetime

def analyze_chaos(filepaths):
    # market_id -> list of fv_stream ticks
    markets = defaultdict(list)
    
    print(f"Reading files: {filepaths}")
    for filepath in filepaths:
        with open(filepath, "r") as f:
            for line in f:
                obj = json.loads(line)
                if "fv_stream.ipc" not in obj.get("channel", ""):
                    continue
                
                raw = base64.b64decode(obj["data"])
                parsed = msgpack.unpackb(raw, raw=False)
                
                # Format: [ts_ms, boundary_ts, prob_up, prob_down, sigma, btc_price, intra_vol, is_sigma_real, strike]
                if len(parsed) < 9:
                    continue
                
                ts_ms = parsed[0]
                boundary_ts = parsed[1]
                sigma = parsed[4]
                btc_price = parsed[5]
                intra_vol = parsed[6]
                strike = parsed[8]
                
                markets[boundary_ts].append({
                    "ts_ms": ts_ms,
                    "btc_price": btc_price,
                    "strike": strike,
                    "intra_vol": intra_vol,
                    "sigma": sigma
                })
    
    print(f"Finished reading. Found {len(markets)} distinct markets.")
    
    # Calculate metrics per market
    results = []
    for boundary_ts, ticks in markets.items():
        if not ticks:
            continue
            
        ticks.sort(key=lambda x: x["ts_ms"])
        
        crosses = 0
        path_length = 0.0
        sum_intra_vol = 0.0
        
        first_price = ticks[0]["btc_price"]
        last_price = ticks[-1]["btc_price"]
        net_movement = abs(last_price - first_price)
        
        prev_side = None
        for i, tick in enumerate(ticks):
            sum_intra_vol += tick["intra_vol"]
            
            # Strike cross logic
            if tick["btc_price"] > tick["strike"]:
                side = "UP"
            elif tick["btc_price"] < tick["strike"]:
                side = "DOWN"
            else:
                side = prev_side
                
            if prev_side is not None and side != prev_side:
                crosses += 1
            prev_side = side
            
            # Path length logic
            if i > 0:
                path_length += abs(tick["btc_price"] - ticks[i-1]["btc_price"])
                
        avg_intra_vol = sum_intra_vol / len(ticks)
        efficiency_ratio = (net_movement / path_length) if path_length > 0 else 1.0
        
        results.append({
            "boundary_ts": boundary_ts,
            "window": datetime.fromtimestamp(boundary_ts).strftime('%Y-%m-%d %H:%M:%S'),
            "ticks": len(ticks),
            "crosses": crosses,
            "efficiency_ratio": efficiency_ratio,
            "avg_intra_vol": avg_intra_vol,
            "path_length": path_length,
            "net_movement": net_movement
        })
        
    results.sort(key=lambda x: x["boundary_ts"])
    
    # Group into hourly buckets to see trends over the days
    hourly = defaultdict(lambda: {"markets": 0, "crosses": 0, "efficiency_ratio": 0.0, "avg_intra_vol": 0.0})
    for r in results:
        hour = datetime.fromtimestamp(r["boundary_ts"]).strftime('%Y-%m-%d %H:00')
        hourly[hour]["markets"] += 1
        hourly[hour]["crosses"] += r["crosses"]
        hourly[hour]["efficiency_ratio"] += r["efficiency_ratio"]
        hourly[hour]["avg_intra_vol"] += r["avg_intra_vol"]
        
    print("\n=== Hourly Aggregates ===")
    print(f"{'Hour':<16} | {'Mkts':>4} | {'Avg Crosses':>11} | {'Avg Eff Ratio':>13} | {'Avg IntraVol':>12}")
    print("-" * 66)
    
    for hour in sorted(hourly.keys()):
        h = hourly[hour]
        m = h["markets"]
        if m == 0: continue
        avg_crosses = h["crosses"] / m
        avg_er = h["efficiency_ratio"] / m
        avg_vol = h["avg_intra_vol"] / m
        print(f"{hour:<16} | {m:>4} | {avg_crosses:>11.1f} | {avg_er:>13.3f} | {avg_vol:>12.4f}")
        
    print("\n=== Top 20 Most Chaotic Markets (by Strike Crosses) ===")
    results.sort(key=lambda x: x["crosses"], reverse=True)
    print(f"{'Market Window':<20} | {'Crosses':>7} | {'Eff Ratio':>9} | {'Avg Vol':>7} | {'Path Len':>8} | {'Net Move':>8}")
    print("-" * 71)
    for r in results[:20]:
        print(f"{r['window']:<20} | {r['crosses']:>7} | {r['efficiency_ratio']:>9.3f} | {r['avg_intra_vol']:>7.3f} | {r['path_length']:>8.1f} | {r['net_movement']:>8.1f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze market chaos from captures.")
    parser.add_argument("files", nargs="+", help="JSONL capture files")
    args = parser.parse_args()
    
    analyze_chaos(args.files)
```

## tools/compare_runs.py

```py
#!/usr/bin/env python3
"""
tools/compare_runs.py
──────────────────────
Compares the output of a live paper-trading run against a replay run.
Calculates PnL, Win Rates, and matches trades by market_id and side 
to report any divergence in execution timing, price, or strategy output.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

def load_jsonl(path: str):
    p = Path(path)
    if not p.exists():
        return []
    with open(p, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]

def ts_span(rows):
    ts_values = [r.get("ts_ms") for r in rows if r.get("ts_ms")]
    if not ts_values:
        return None
    return min(ts_values), max(ts_values)

def fmt_span(span):
    if span is None:
        return "n/a"
    start, end = span
    start_s = datetime.fromtimestamp(start / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    end_s = datetime.fromtimestamp(end / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return f"{start_s} UTC -> {end_s} UTC"

def calc_pnl(fills, exits):
    total_cost = sum(f.get("cost", 0) for f in fills)
    total_proceeds = sum(e.get("proceeds", 0) for e in exits)
    net_pnl = total_proceeds - total_cost

    settlement_exits = [e for e in exits if e.get("exit_reason") == "SETTLEMENT"]
    early_exits = [e for e in exits if e.get("exit_reason") != "SETTLEMENT"]
    
    wins = sum(1 for e in exits if e.get("pnl", 0) > 0)
    losses = sum(1 for e in exits if e.get("pnl", 0) < 0)
    draws = sum(1 for e in exits if e.get("pnl", 0) == 0)
    
    return {
        "cost": total_cost,
        "proceeds": total_proceeds,
        "net": net_pnl,
        "wins": wins,
        "losses": losses,
        "draws": draws,
        "settlement_exits": len(settlement_exits),
        "early_exits": len(early_exits),
        "total_exits": len(exits)
    }

def main():
    live_fills_path = "fills_tos.jsonl"
    live_exits_path = "exits_tos.jsonl"
    replay_fills_path = "replay_fills.jsonl"
    replay_exits_path = "replay_exits.jsonl"

    live_fills = load_jsonl(live_fills_path)
    live_exits = load_jsonl(live_exits_path)
    replay_fills = load_jsonl(replay_fills_path)
    replay_exits = load_jsonl(replay_exits_path)

    live_stats = calc_pnl(live_fills, live_exits)
    replay_stats = calc_pnl(replay_fills, replay_exits)

    print("══════════════════════════════════════════════════════")
    print("  Run Comparison: Live (TOS) vs Replay")
    print("══════════════════════════════════════════════════════\n")

    live_span = ts_span(live_fills + live_exits)
    replay_span = ts_span(replay_fills + replay_exits)
    print(f"Live span:   {fmt_span(live_span)}")
    print(f"Replay span: {fmt_span(replay_span)}")
    if live_span and replay_span and (live_span[1] < replay_span[0] or replay_span[1] < live_span[0]):
        print("WARNING: live and replay output time ranges do not overlap.\n")
    else:
        print("")

    print(f"{'Metric':<25} | {'Live (TOS)':<15} | {'Replay':<15} | {'Delta':<15}")
    print("-" * 78)
    print(f"{'Fills':<25} | {len(live_fills):<15} | {len(replay_fills):<15} | {len(live_fills) - len(replay_fills):<+15}")
    print(f"{'Terminal Records':<25} | {len(live_exits):<15} | {len(replay_exits):<15} | {len(live_exits) - len(replay_exits):<+15}")
    print(f"{'Settlement Records':<25} | {live_stats['settlement_exits']:<15} | {replay_stats['settlement_exits']:<15} | {live_stats['settlement_exits'] - replay_stats['settlement_exits']:<+15}")
    print(f"{'Early Sell Records':<25} | {live_stats['early_exits']:<15} | {replay_stats['early_exits']:<15} | {live_stats['early_exits'] - replay_stats['early_exits']:<+15}")
    print("-" * 78)
    print(f"{'Total Cost':<25} | ${live_stats['cost']:<14.2f} | ${replay_stats['cost']:<14.2f} | ${live_stats['cost'] - replay_stats['cost']:<+14.2f}")
    print(f"{'Total Proceeds':<25} | ${live_stats['proceeds']:<14.2f} | ${replay_stats['proceeds']:<14.2f} | ${live_stats['proceeds'] - replay_stats['proceeds']:<+14.2f}")
    print(f"{'Net PnL':<25} | ${live_stats['net']:<+14.2f} | ${replay_stats['net']:<+14.2f} | ${live_stats['net'] - replay_stats['net']:<+14.2f}")
    print("-" * 78)
    print(f"{'Winning Exits':<25} | {live_stats['wins']:<15} | {replay_stats['wins']:<15} | {live_stats['wins'] - replay_stats['wins']:<+15}")
    print(f"{'Losing Exits':<25} | {live_stats['losses']:<15} | {replay_stats['losses']:<15} | {live_stats['losses'] - replay_stats['losses']:<+15}")
    
    live_wr = (live_stats['wins'] / live_stats['total_exits'] * 100) if live_stats['total_exits'] > 0 else 0.0
    replay_wr = (replay_stats['wins'] / replay_stats['total_exits'] * 100) if replay_stats['total_exits'] > 0 else 0.0
        
    print(f"{'Win Rate (Exits)':<25} | {live_wr:<14.1f}% | {replay_wr:<14.1f}% | {live_wr - replay_wr:<+14.1f}%")
    if live_stats["early_exits"] == 0 and live_stats["settlement_exits"] > 0:
        print("Note: Live TOS terminal records are all SETTLEMENT records; no early sells were recorded.")
    print("══════════════════════════════════════════════════════\n")

    # Index by (market_id, side) -> list of fills
    live_index = defaultdict(list)
    for f in live_fills:
        live_index[(f.get("market_id"), f.get("side"))].append(f)
        
    replay_index = defaultdict(list)
    for f in replay_fills:
        replay_index[(f.get("market_id"), f.get("side"))].append(f)

    common_keys = set(live_index.keys()).intersection(set(replay_index.keys()))
    live_only_keys = set(live_index.keys()) - set(replay_index.keys())
    replay_only_keys = set(replay_index.keys()) - set(live_index.keys())

    print(f"Matching unique positions (market + side):")
    print(f"  Common:      {len(common_keys)}")
    print(f"  Live Only:   {len(live_only_keys)}")
    print(f"  Replay Only: {len(replay_only_keys)}")

    if len(common_keys) > 0:
        print("\nExecution Divergence Analysis (for common positions):")
        ts_diffs = []
        price_diffs = []
        fv_diffs = []
        
        for key in common_keys:
            lf = live_index[key][0]
            rf = replay_index[key][0]
            
            ts_diffs.append(abs(lf.get("ts_ms", 0) - rf.get("ts_ms", 0)))
            price_diffs.append(abs(lf.get("ask", 0) - rf.get("ask", 0)))
            fv_diffs.append(abs(lf.get("fv", 0) - rf.get("fv", 0)))
            
        print(f"  Avg Entry Time Divergence:  {sum(ts_diffs)/len(ts_diffs):.1f} ms")
        print(f"  Max Entry Time Divergence:  {max(ts_diffs):.1f} ms")
        print(f"  Avg Entry Price Divergence: {sum(price_diffs)/len(price_diffs):.5f}")
        print(f"  Max Entry Price Divergence: {max(price_diffs):.5f}")
        print(f"  Avg FV Divergence:          {sum(fv_diffs)/len(fv_diffs):.5f}")
        print(f"  Max FV Divergence:          {max(fv_diffs):.5f}")

        if max(ts_diffs) == 0 and max(price_diffs) == 0:
            print("\n✅ PERFECT MATCH: Replay execution exactly matches live execution for common positions!")
        else:
            print("\n⚠ DIVERGENCE DETECTED: Replay execution differs from live execution.")
            print("This can happen if:  ")
            print("1. Strategy config was changed since the live run.")
            print("2. Live data captures lost some ticks or had different WS latency.")
            print("3. Time-dependent logic (e.g. wall-clock checks instead of data timestamps).")

    if len(live_only_keys) > 0 and len(replay_fills) > 0:
        print("\nNote: There are live entries that didn't happen in replay.")
        print("This is normal if your capture file covers a smaller time window than the live run.")

if __name__ == "__main__":
    main()
```

## tools/optimizer.py

```py
#!/usr/bin/env python3
import argparse
import csv
import datetime
import json
import logging
import os
import random
import sys
import uuid
from pathlib import Path

# Ensure we can import from the project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.parameter_registry import PARAMETERS, get_fixed_env, sample_random, grid_points, suggest_optuna
from tools.replay_engine import run_backtest

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
except ImportError:
    optuna = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("optimizer")

def _merge_captures(files: list[Path], output_path: Path):
    events = []
    for f in files:
        with open(f, "r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh):
                line = line.strip()
                if not line: continue
                try:
                    ev = json.loads(line)
                    ev["_line_no"] = line_no
                    events.append(ev)
                except Exception:
                    pass
    
    events.sort(key=lambda x: (x["ts_ms"], x["_line_no"]))
    
    with open(output_path, "w", encoding="utf-8") as out:
        for ev in events:
            ev.pop("_line_no", None)
            out.write(json.dumps(ev) + "\n")

def _count_capture_markets(capture_path: Path) -> int:
    """
    Scan a merged capture JSONL and return the number of distinct market_ids.
    This is the full universe the strategy was exposed to — the correct
    denominator for participation rate (trades / total_markets).
    """
    market_ids = set()
    with open(capture_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
                mid = ev.get("market_id")
                if mid:
                    market_ids.add(mid)
            except Exception:
                pass
    return len(market_ids)


def _write_best_results(input_csv: Path, output_csv: Path, top_n: int = 20):
    if not input_csv.exists():
        return
    with open(input_csv, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    valid_rows = [r for r in rows if r.get("passed_validation") == "True"]
    
    def sort_key(r):
        return (
            float(r.get("val_sharpe", 0) or 0),
            float(r.get("val_net_pnl", 0) or 0),
            -float(r.get("val_drawdown", 0) or 0)
        )
        
    valid_rows.sort(key=sort_key, reverse=True)
    top_rows = valid_rows[:top_n]
    
    if not top_rows:
        return
        
    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(top_rows)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["random", "grid", "bayesian"], required=True)
    parser.add_argument("--n-iter", type=int, default=50)
    parser.add_argument("--filter-start", type=str, default="")
    parser.add_argument("--filter-end", type=str, default="")
    parser.add_argument("--train-start", type=str, required=True)
    parser.add_argument("--train-end", type=str, required=True)
    parser.add_argument("--val-start", type=str, required=True)
    parser.add_argument("--val-end", type=str, required=True)
    parser.add_argument("--captures-dir", type=str, required=True)
    parser.add_argument("--output-dir", type=str, required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--min-train-sharpe", type=float, default=0.0)
    parser.add_argument("--stage", choices=["all", "filter", "train_val"], default="all")
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--min-filter-trades", type=int, default=30,
                        help="Minimum number of filter-stage trades required.")
    parser.add_argument("--min-filter-participation", type=float, default=0.1,
                        help="Minimum fraction of available capture markets that must have been "
                             "traded for a config to pass the filter gate. E.g. 0.05 means the "
                             "strategy must have entered at least 5%% of all markets seen in the "
                             "filter capture. Prevents inflated Sharpe from 2-trade flukes in a "
                             "288-market universe from passing.")
    parser.add_argument("--min-train-trades", type=int, default=0,
                        help="Minimum number of train-stage trades required to proceed to "
                             "validation. When > 0, the train Sharpe is also confidence-penalised "
                             "by min(1, trades / (2 * min_train_trades)) before being compared to "
                             "--min-train-sharpe, so a 2-trade fluke over 864 markets cannot "
                             "produce an inflated Sharpe that passes the gate. "
                             "Defaults to 0 (disabled); recommended: ~3x --min-filter-trades "
                             "since the train window is typically 3x longer than the filter.")

    args = parser.parse_args()

    if args.mode == "bayesian" and optuna is None:
        log.error("Optuna is required for bayesian mode. Please 'pip install optuna'.")
        return

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    cap_dir = Path(args.captures_dir)
    
    def date_from_filename(p: Path):
        return p.stem
    
    all_files = sorted([f for f in cap_dir.glob("*.jsonl")])
    train_files = [f for f in all_files if args.train_start <= date_from_filename(f) <= args.train_end]
    val_files = [f for f in all_files if args.val_start <= date_from_filename(f) <= args.val_end]
    filter_files = []
    if args.filter_start and args.filter_end:
        filter_files = [f for f in all_files if args.filter_start <= date_from_filename(f) <= args.filter_end]
    
    if not train_files:
        log.error("No train files found.")
        return
    if not val_files:
        log.error("No val files found.")
        return
        
    log.info(f"Train files: {[f.name for f in train_files]}")
    log.info(f"Val files: {[f.name for f in val_files]}")
    if filter_files:
        log.info(f"Filter files: {[f.name for f in filter_files]}")
    
    train_merged = out_dir / "train_merged.jsonl"
    val_merged = out_dir / "val_merged.jsonl"
    filter_merged = out_dir / "filter_merged.jsonl"
    
    if args.stage != "train_val":
        log.info("Merging train files...")
        _merge_captures(train_files, train_merged)
        log.info("Merging val files...")
        _merge_captures(val_files, val_merged)
        if filter_files:
            log.info("Merging filter files...")
            _merge_captures(filter_files, filter_merged)
    
    # Count total distinct markets in each capture once, so the participation-rate
    # gate inside evaluate_params doesn't have to re-scan the file on every trial.
    total_filter_markets: int = 0
    if filter_files and filter_merged.exists():
        total_filter_markets = _count_capture_markets(filter_merged)
        log.info(f"Total filter markets available: {total_filter_markets}")

    total_train_markets: int = 0
    if train_merged.exists():
        total_train_markets = _count_capture_markets(train_merged)
        log.info(f"Total train markets available: {total_train_markets}")

    rng = random.Random(args.seed)
    
    csv_path = out_dir / "optimization_results.csv"
    param_keys = list(PARAMETERS.keys())
    
    fieldnames = [
        "run_id", "timestamp", "mode", "seed"
    ] + param_keys + [
        "filter_net_pnl", "filter_roi", "filter_win_rate", "filter_sharpe", "filter_drawdown", "filter_trades", "train_skipped",
        "train_net_pnl", "train_roi", "train_win_rate", "train_sharpe", "train_drawdown", "train_trades",
        "val_net_pnl", "val_roi", "val_win_rate", "val_sharpe", "val_drawdown", "val_trades",
        "val_skipped", "passed_validation", "error"
    ]
    
    if not csv_path.exists():
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

    def evaluate_params(params, run_id, timestamp, mode, seed_val):
        row = {
            "run_id": run_id,
            "timestamp": timestamp,
            "mode": mode,
            "seed": seed_val,
            "train_skipped": False,
            "val_skipped": False,
            "passed_validation": False,
            "error": "",
            **params
        }

        # Stage 1: Filter
        if filter_files:
            log.info(f"[{run_id}] Stage 1: Running filter backtest...")
            f_fills = out_dir / f"filter_fills_{run_id}.jsonl"
            f_exits = out_dir / f"filter_exits_{run_id}.jsonl"
            
            f_metrics = run_backtest(
                capture_path=filter_merged,
                env_overrides={**get_fixed_env(), **params},
                fills_path=f_fills,
                exits_path=f_exits,
                speed=0.0
            )
            
            if f_fills.exists(): f_fills.unlink()
            if f_exits.exists(): f_exits.unlink()
            
            row.update({
                "filter_net_pnl": f_metrics["net_pnl"],
                "filter_roi": f_metrics["roi"],
                "filter_win_rate": f_metrics["win_rate"],
                "filter_sharpe": f_metrics["sharpe"],
                "filter_drawdown": f_metrics["max_drawdown"],
                "filter_trades": f_metrics["total_trades"]
            })
            
            if f_metrics["error"] is not None:
                log.warning(f"[{run_id}] Filter run failed: {f_metrics['error']}")
                row["error"] = f_metrics["error"]
                row["train_skipped"] = True
                row["val_skipped"] = True
                return row
                
            # If the strategy bleeds money on the worst day (net_pnl <= 0), skip.
            # Using net_pnl perfectly covers 0 trades (pnl=0), 1 trade that loses, and multiple trades.
            if f_metrics["net_pnl"] <= 0.0:
                log.info(f"[{run_id}] Failed filter stage (pnl=${f_metrics['net_pnl']}, trades={f_metrics['total_trades']}, sharpe={f_metrics['sharpe']}). Skipping train/val.")
                row["train_skipped"] = True
                row["val_skipped"] = True
                return row

            # Enforce participation rate: reject configs that only took 1-2 lucky
            # trades out of ~288 available markets — their Sharpe is meaningless.
            if args.min_filter_participation > 0 and total_filter_markets > 0:
                participation = f_metrics["total_trades"] / total_filter_markets
                if participation < args.min_filter_participation:
                    log.info(
                        f"[{run_id}] Failed filter participation: "
                        f"{f_metrics['total_trades']} trades / {total_filter_markets} markets "
                        f"= {participation:.3f} < {args.min_filter_participation}. Skipping."
                    )
                    row["train_skipped"] = True
                    row["val_skipped"] = True
                    return row

            log.info(f"[{run_id}] PASSED filter stage! (pnl=${f_metrics['net_pnl']}, sharpe={f_metrics['sharpe']}, trades={f_metrics['total_trades']})")

        if args.stage == "filter":
            row["train_skipped"] = True
            row["val_skipped"] = True
            return row

        # Stage 2: Train
        log.info(f"[{run_id}] Stage 2: Running train backtest...")
        train_fills = out_dir / f"train_fills_{run_id}.jsonl"
        train_exits = out_dir / f"train_exits_{run_id}.jsonl"
        
        train_metrics = run_backtest(
            capture_path=train_merged,
            env_overrides={**get_fixed_env(), **params},
            fills_path=train_fills,
            exits_path=train_exits,
            speed=0.0
        )
        
        if train_fills.exists(): train_fills.unlink()
        if train_exits.exists(): train_exits.unlink()
        
        row.update({
            "train_net_pnl": train_metrics["net_pnl"],
            "train_roi": train_metrics["roi"],
            "train_win_rate": train_metrics["win_rate"],
            "train_sharpe": train_metrics["sharpe"],
            "train_drawdown": train_metrics["max_drawdown"],
            "train_trades": train_metrics["total_trades"]
        })
        
        if train_metrics["error"] is not None:
            log.warning(f"[{run_id}] Train run failed: {train_metrics['error']}")
            row["error"] = train_metrics["error"]
            row["val_skipped"] = True
            return row

        # Confidence-penalised train Sharpe.
        # run_backtest computes Sharpe only over trades that happened — so 2
        # winning trades out of 864 markets produce an artificially huge Sharpe
        # because the ~862 zero-return slots are excluded from the calculation.
        # We scale the raw Sharpe down by a confidence factor that ramps from 0.5
        # (at the minimum trade floor) to 1.0 (at 2× the floor), forcing the gate
        # to reject low-participation configs regardless of their raw Sharpe.
        _min_train_threshold = args.min_train_trades if args.min_train_trades > 0 else max(1, args.min_filter_trades * 2)
        train_confidence = min(1.0, train_metrics["total_trades"] / max(1, _min_train_threshold * 2))
        penalized_train_sharpe = train_metrics["sharpe"] * train_confidence

        # Optional hard minimum-trades gate for the train stage.
        if args.min_train_trades > 0 and train_metrics["total_trades"] < args.min_train_trades:
            log.info(
                f"[{run_id}] Skipping validation: train trades {train_metrics['total_trades']} "
                f"< --min-train-trades {args.min_train_trades}"
            )
            row["val_skipped"] = True
            return row

        if penalized_train_sharpe < args.min_train_sharpe:
            log.info(
                f"[{run_id}] Skipping validation: raw train_sharpe={train_metrics['sharpe']:.3f}, "
                f"confidence={train_confidence:.3f}, penalized={penalized_train_sharpe:.3f} "
                f"< --min-train-sharpe {args.min_train_sharpe}"
            )
            row["val_skipped"] = True
            return row

        # Stage 3: Val
        log.info(f"[{run_id}] Stage 3: Running val backtest...")
        val_fills = out_dir / f"val_fills_{run_id}.jsonl"
        val_exits = out_dir / f"val_exits_{run_id}.jsonl"
        
        val_metrics = run_backtest(
            capture_path=val_merged,
            env_overrides={**get_fixed_env(), **params},
            fills_path=val_fills,
            exits_path=val_exits,
            speed=0.0
        )
        
        if val_fills.exists(): val_fills.unlink()
        if val_exits.exists(): val_exits.unlink()
        
        row.update({
            "val_net_pnl": val_metrics["net_pnl"],
            "val_roi": val_metrics["roi"],
            "val_win_rate": val_metrics["win_rate"],
            "val_sharpe": val_metrics["sharpe"],
            "val_drawdown": val_metrics["max_drawdown"],
            "val_trades": val_metrics["total_trades"]
        })
        
        if val_metrics["error"] is not None:
            log.warning(f"[{run_id}] Val run failed: {val_metrics['error']}")
            row["error"] = val_metrics["error"]
            return row

        # Final Evaluation
        passed = True
        if val_metrics["sharpe"] <= 0.3: passed = False
        if val_metrics["win_rate"] <= 0.50: passed = False
        if val_metrics["net_pnl"] <= 0: passed = False
        
        if train_metrics["sharpe"] > 0:
            decay = (train_metrics["sharpe"] - val_metrics["sharpe"]) / train_metrics["sharpe"]
            if decay >= 0.50:
                passed = False
        else:
            passed = False
            
        row["passed_validation"] = passed
        log.info(f"[{run_id}] Validation passed: {passed}")
        
        return row

    def save_row(row):
        with open(csv_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow({k: row.get(k, "") for k in fieldnames})

    # Execution paths
    if args.stage == "train_val":
        log.info(f"Running train_val stage for top {args.top_n} configs...")
        if not csv_path.exists():
            log.error("No optimization_results.csv found. Run filter stage first.")
            return
            
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
        # Filter rows that passed the filter stage.
        # Hard gate: pnl > 0 AND trade count meets minimum threshold so that
        # configs with 1-2 lucky trades can't win via inflated Sharpe.
        valid_rows = []
        for r in rows:
            try:
                pnl = float(r.get("filter_net_pnl", 0) or 0)
                trades = float(r.get("filter_trades", 0) or 0)
                if pnl > 0 and trades >= args.min_filter_trades:
                    valid_rows.append(r)
            except ValueError:
                pass

        log.info(
            f"{len(valid_rows)} rows passed filter gate "
            f"(pnl>0, trades>={args.min_filter_trades}) out of {len(rows)} total."
        )

        # Rank by a confidence-penalised Sharpe so that raw PnL volume is
        # rewarded alongside risk-adjusted quality.
        #
        # confidence  = min(1, trades / min_filter_trades*2)
        #   → ramps from 0.5 at the minimum trade floor up to 1.0 once the
        #     config has twice the minimum number of trades.
        #   → configs with exactly min_filter_trades trades get 0.5× Sharpe;
        #     those with 2× min_filter_trades (or more) get the full Sharpe.
        # Primary sort:  penalised_sharpe  (risk-quality × volume confidence)
        # Tiebreaker:    filter_net_pnl    (absolute dollar return)
        def _rank_key(r):
            pnl    = float(r.get("filter_net_pnl", 0) or 0)
            sharpe = float(r.get("filter_sharpe", 0) or 0)
            trades = float(r.get("filter_trades", 0) or 0)
            confidence = min(1.0, trades / (args.min_filter_trades * 2))
            penalised_sharpe = sharpe * confidence
            return (penalised_sharpe, pnl)

        valid_rows.sort(key=_rank_key, reverse=True)
        top_rows = valid_rows[:args.top_n]

        log.info("Top-N filter ranking:")
        for rank, r in enumerate(top_rows, 1):
            trades     = float(r.get("filter_trades", 0) or 0)
            sharpe     = float(r.get("filter_sharpe", 0) or 0)
            pnl        = float(r.get("filter_net_pnl", 0) or 0)
            confidence = min(1.0, trades / (args.min_filter_trades * 2))
            log.info(
                f"  #{rank:>2}  run_id={r['run_id']}  "
                f"pnl=${pnl:.2f}  sharpe={sharpe:.2f}  trades={int(trades)}  "
                f"confidence={confidence:.2f}  "
                f"score={sharpe * confidence:.2f}"
            )
        
        stage2_csv = out_dir / "optimization_results_stage2.csv"
        if not stage2_csv.exists():
            with open(stage2_csv, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
        log.info("Merging train and val files for train_val stage...")
        _merge_captures(train_files, train_merged)
        _merge_captures(val_files, val_merged)
                
        for i, row in enumerate(top_rows):
            log.info(f"--- Top {i+1}/{len(top_rows)} (run_id: {row['run_id']}) ---")
            params = {k: float(row[k]) if PARAMETERS[k]["type"] == "float" else int(row[k]) for k in param_keys if k in row and row[k]}
            
            run_id = row["run_id"]
            timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
            
            new_row = evaluate_params(params, run_id, timestamp, "train_val", row.get("seed", ""))
            
            with open(stage2_csv, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writerow({k: new_row.get(k, "") for k in fieldnames})

        best_csv_path = out_dir / "best_results_stage2.csv"
        _write_best_results(stage2_csv, best_csv_path)

    elif args.mode in ["random", "grid"]:
        grid_iterator = grid_points() if args.mode == "grid" else None
        
        for i in range(args.n_iter):
            log.info(f"--- Iteration {i+1}/{args.n_iter} ---")
            if args.mode == "random":
                params = sample_random(rng)
            else:
                try:
                    params = next(grid_iterator)
                except StopIteration:
                    log.info("Grid search exhausted.")
                    break
                    
            run_id = str(uuid.uuid4())[:8]
            timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
            
            row = evaluate_params(params, run_id, timestamp, args.mode, args.seed if args.mode == "random" else "")
            save_row(row)

    elif args.mode == "bayesian":
        def objective(trial):
            run_id = str(uuid.uuid4())[:8]
            timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
            log.info(f"--- Optuna Trial {trial.number} ({run_id}) ---")
            
            params = suggest_optuna(trial)
            row = evaluate_params(params, run_id, timestamp, "bayesian", "")
            save_row(row)
            
            # What does Optuna optimize?
            # We want it to maximize Train Sharpe. If Train Sharpe wasn't reached,
            # we give it a confidence-penalised Filter Sharpe so it still learns
            # from the failure — but can't be gamed by 2-trade flukes.
            # If both failed/errored, return a very bad score.
            if row.get("error"):
                return -999.0

            if row.get("train_skipped"):
                filter_pnl    = float(row.get("filter_net_pnl", 0) or 0)
                filter_sharpe = float(row.get("filter_sharpe", 0) or 0)
                filter_trades = float(row.get("filter_trades", 0) or 0)
                if filter_pnl <= 0:
                    return -99.0
                # Same confidence penalty as the top-N selector
                confidence = min(1.0, filter_trades / (args.min_filter_trades * 2))
                return filter_sharpe * confidence

            train_sharpe = row.get("train_sharpe")
            if train_sharpe is None:
                return -99.0
            # Apply the same confidence penalty used in the gate check so Optuna
            # can't be gamed by configs that post a huge Sharpe on 2 trades.
            train_trades = float(row.get("train_trades", 0) or 0)
            _min_train_threshold = args.min_train_trades if args.min_train_trades > 0 else max(1, args.min_filter_trades * 2)
            train_confidence = min(1.0, train_trades / max(1, _min_train_threshold * 2))
            return float(train_sharpe) * train_confidence
            
            

        db_path = out_dir / "optuna_study.db"
        study = optuna.create_study(
            storage=f"sqlite:///{db_path}",
            study_name="btc_bot_optimization",
            load_if_exists=True,
            direction="maximize"
        )
        log.info(f"Starting Bayesian Optimization for {args.n_iter} trials...")
        study.optimize(objective, n_trials=args.n_iter)
        
        log.info("Optuna Optimization Complete.")
        log.info(f"Best trial: {study.best_trial.number} with train_sharpe: {study.best_trial.value}")

    # Finalize
    if args.stage != "train_val":
        best_csv_path = out_dir / "best_results.csv"
        _write_best_results(csv_path, best_csv_path)
    
    if train_merged.exists(): train_merged.unlink()
    if val_merged.exists(): val_merged.unlink()
    if filter_files and filter_merged.exists(): filter_merged.unlink()

if __name__ == "__main__":
    main()
```

## tools/parameter_registry.py

```py
"""
tools/parameter_registry.py
────────────────────────────
Defines the full TOS_SIGNAL parameter search space for the optimizer.

Each entry in PARAMETERS has:
    type     : "float" | "int"
    default  : the current production default (matches env var defaults)
    min      : lower bound of the search range
    max      : upper bound of the search range
    step     : grid search step size
    tune     : True  → optimizer varies this parameter
               False → always run at `default` (registered for logging only)
    group    : logical grouping (entry_gate | pre_strategy | position_sizing |
                                  exit | signal_stack)
    note     : one-line description of what this parameter controls

Exposed helpers:
    get_tunable()      → {name: spec} for all params where tune=True
    get_fixed_env()    → {name: str_value} for always-fixed operational env vars
    sample_random(rng) → {name: value} — one random draw from the tunable space
    grid_points()      → iterator of {name: value} dicts — cartesian product
                         of all tunable params (use with care: may be huge)
"""

from __future__ import annotations

import itertools
import random
from typing import Any, Dict, Iterator

# ── Parameter definitions ──────────────────────────────────────────────────────

PARAMETERS: Dict[str, Dict[str, Any]] = {

    # ── Entry gate — TOS base layer ───────────────────────────────────────────
    # (strategies/tos/config.py, re-exported via strategies/tos_signal/config.py)

    "TOS_ENTRY_START_S": {
        "type": "float",
        "default": 210.0,
        "min": 150.0,
        "max": 240.0,
        "step": 10.0,
        "tune": True,
        "group": "entry_gate",
        "note": "Opening second of the late-window entry band (within the 300s window).",
    },
    "TOS_ENTRY_END_S": {
        "type": "float",
        "default": 270.0,
        "min": 240.0,
        "max": 290.0,
        "step": 10.0,
        "tune": True,
        "group": "entry_gate",
        "note": "Closing second of the late-window entry band.",
    },
    "TOS_MIN_PROB": {
        "type": "float",
        "default": 0.70,
        "min": 0.60,
        "max": 0.85,
        "step": 0.05,
        "tune": True,
        "group": "entry_gate",
        "note": "Minimum FV winning-side probability required for entry. Higher = more selective.",
    },
    "TOS_MIN_EDGE": {
        "type": "float",
        "default": 0.05,
        "min": 0.02,
        "max": 0.15,
        "step": 0.01,
        "tune": True,
        "group": "entry_gate",
        "note": "Minimum edge (winning_prob − ask) required. Primary guard against thin markets.",
    },
    "TOS_MIN_LIQUIDITY": {
        "type": "float",
        "default": 20.0,
        "min": 10.0,
        "max": 100.0,
        "step": 10.0,
        "tune": True,
        "group": "entry_gate",
        "note": "Minimum PM depth on the winning side before entry is allowed.",
    },
    "TOS_Z_THRESHOLD": {
        "type": "float",
        "default": 0.524,   # NormalDist().inv_cdf(0.70)
        "min": 0.25,
        "max": 1.25,
        "step": 0.05,
        "tune": True,
        "group": "entry_gate",
        "note": "Model z-score conviction gate. Can be tuned independently of TOS_MIN_PROB.",
    },
    "TOS_MAX_STRIKE_CROSSES": {
        "type": "int",
        "default": 999,
        "min": 5,
        "max": 50,
        "step": 5,
        "tune": True,
        "group": "entry_gate",
        "note": "Maximum allowed strike crosses during the window before aborting trading.",
    },
    "TOS_MIN_EFFICIENCY_RATIO": {
        "type": "float",
        "default": 0.0,
        "min": 0.0,
        "max": 0.5,
        "step": 0.05,
        "tune": True,
        "group": "entry_gate",
        "note": "Minimum efficiency ratio (net movement / path length) required.",
    },

    # ── Entry gate — pre-strategy layer ──────────────────────────────────────
    # (cmd/smart_paper_trader.py module-level env vars)

    "MIN_EDGE_THRESHOLD": {
        "type": "float",
        "default": 0.03,
        "min": 0.01,
        "max": 0.10,
        "step": 0.01,
        "tune": True,
        "group": "pre_strategy",
        "note": "Hard edge floor applied before strategy evaluation (FV − ask ≥ this).",
    },
    "MIN_ENTRY_ASK": {
        "type": "float",
        "default": 0.05,
        "min": 0.02,
        "max": 0.15,
        "step": 0.01,
        "tune": True,
        "group": "pre_strategy",
        "note": "Minimum ask price required. Blocks near-settled markets with fake FV edge.",
    },
    "FV_ENTRY_MAX": {
        "type": "float",
        "default": 0.97,
        "min": 0.85,
        "max": 0.99,
        "step": 0.02,
        "tune": True,
        "group": "pre_strategy",
        "note": "Upper FV gate — rejects entries when FV is near 1.0 (market already decided).",
    },
    "FV_ENTRY_MIN": {
        "type": "float",
        "default": 0.03,
        "min": 0.01,
        "max": 0.15,
        "step": 0.02,
        "tune": True,
        "group": "pre_strategy",
        "note": "Lower FV gate (symmetric to FV_ENTRY_MAX).",
    },
    "FV_STALE_MS": {
        "type": "float",
        "default": 500.0,
        "min": 200.0,
        "max": 1000.0,
        "step": 100.0,
        "tune": True,
        "group": "pre_strategy",
        "note": "FV data staleness cutoff (ms). Tighter = fresher data only, fewer fills.",
    },
    "MIN_WINDOW_AGE_S": {
        "type": "float",
        "default": 100.0,
        "min": 0.0,
        "max": 180.0,
        "step": 20.0,
        "tune": True,
        "group": "pre_strategy",
        "note": "Minimum seconds into the window before entries open. Blocks K-snap noise.",
    },
    "FILL_COOLDOWN_MS": {
        "type": "float",
        "default": 5000.0,
        "min": 1000.0,
        "max": 30000.0,
        "step": 1000.0,
        "tune": True,
        "group": "pre_strategy",
        "note": "Per-side re-entry cooldown (ms). Prevents rapid re-entry after a fill.",
    },

    # ── Position sizing & risk ─────────────────────────────────────────────────
    # Registered for logging; tune=False — these are market minimums / capital
    # constraints that should not be varied during strategy optimization.

    "PAPER_TRADE_SHARES": {
        "type": "float",
        "default": 5.0,
        "min": 5.0,
        "max": 20.0,
        "step": 5.0,
        "tune": False,
        "group": "position_sizing",
        "note": "Shares per simulated fill. Scales all PnL linearly. Fixed at exchange minimum.",
    },
    "MAX_SHARES_PER_SIDE": {
        "type": "float",
        "default": 10.0,
        "min": 5.0,
        "max": 30.0,
        "step": 5.0,
        "tune": False,
        "group": "position_sizing",
        "note": "Max accumulated exposure per side per market. Capital constraint.",
    },
    "MAX_SPEND_PER_MARKET": {
        "type": "float",
        "default": 8.0,
        "min": 5.0,
        "max": 50.0,
        "step": 5.0,
        "tune": False,
        "group": "position_sizing",
        "note": "USDC budget cap per market window. Capital constraint.",
    },
    "MAX_ENTRIES_PER_WINDOW": {
        "type": "int",
        "default": 5,
        "min": 1,
        "max": 10,
        "step": 1,
        "tune": False,
        "group": "position_sizing",
        "note": "Hard entry count ceiling per window. Capital constraint.",
    },

    # ── Exit thresholds — time-aware exit system ──────────────────────────────
    # (USE_TIME_AWARE_EXITS=1 is always fixed; TOS_SIGNAL uses time-aware exits)

    "EARLY_HIGH_CONFIDENCE_BID": {
        "type": "float",
        "default": 0.88,
        "min": 0.75,
        "max": 0.95,
        "step": 0.05,
        "tune": True,
        "group": "exit",
        "note": "Early-zone TP: exit if bid exceeds this (most value already captured).",
    },
    "LATE_WINDOW_SECONDS": {
        "type": "float",
        "default": 120.0,
        "min": 60.0,
        "max": 180.0,
        "step": 20.0,
        "tune": True,
        "group": "exit",
        "note": "Seconds before settlement when the 'late zone' light-exit logic activates.",
    },
    "LATE_SL_FLOOR": {
        "type": "float",
        "default": 0.08,
        "min": 0.03,
        "max": 0.20,
        "step": 0.01,
        "tune": True,
        "group": "exit",
        "note": "Late-zone SL: cut if bid falls below this (market implies ≤N% win chance).",
    },
    "LATE_TP_BID": {
        "type": "float",
        "default": 0.82,
        "min": 0.70,
        "max": 0.92,
        "step": 0.02,
        "tune": True,
        "group": "exit",
        "note": "Late-zone TP: lock in gains when bid exceeds this threshold.",
    },
    "EMERGENCY_SECONDS": {
        "type": "float",
        "default": 60.0,
        "min": 20.0,
        "max": 90.0,
        "step": 10.0,
        "tune": True,
        "group": "exit",
        "note": "Duration of the emergency zone (seconds before settlement).",
    },
    "EMERGENCY_CUT_PRICE": {
        "type": "float",
        "default": 0.12,
        "min": 0.05,
        "max": 0.25,
        "step": 0.01,
        "tune": True,
        "group": "exit",
        "note": "Emergency SL bid floor. Must pair with EMERGENCY_FV_CONFIRM (double-confirmation).",
    },
    "EMERGENCY_FV_CONFIRM": {
        "type": "float",
        "default": 0.30,
        "min": 0.15,
        "max": 0.45,
        "step": 0.05,
        "tune": True,
        "group": "exit",
        "note": "FV must be below this for emergency cut to fire (prevents premature SL).",
    },
    "EMERGENCY_TP_BID": {
        "type": "float",
        "default": 0.88,
        "min": 0.75,
        "max": 0.95,
        "step": 0.05,
        "tune": True,
        "group": "exit",
        "note": "Emergency TP: lock in near-certain win when bid exceeds this near settlement.",
    },

    # ── Signal stack — TOS_SIGNAL specific ────────────────────────────────────
    # (strategies/tos_signal/signal_stack.py — promoted to env vars in Step 1)

    "BTC_MOMENTUM_GATE": {
        "type": "float",
        "default": 0.0004,
        "min": 0.0001,
        "max": 0.002,
        "step": 0.0001,
        "tune": True,
        "group": "signal_stack",
        "note": "BTC displacement from K (as fraction) required for momentum signal to fire.",
    },
    "ORDERBOOK_IMBALANCE_GATE": {
        "type": "float",
        "default": 0.40,
        "min": 0.20,
        "max": 0.60,
        "step": 0.05,
        "tune": True,
        "group": "signal_stack",
        "note": "Minimum directional liquidity skew (0–1) before imbalance signal fires.",
    },
    "SIGNAL_MIN_LIQUIDITY": {
        "type": "float",
        "default": 20.0,
        "min": 5.0,
        "max": 50.0,
        "step": 5.0,
        "tune": True,
        "group": "signal_stack",
        "note": "Minimum combined PM depth before the imbalance signal is evaluated.",
    },
}

# ── Always-fixed operational env vars ─────────────────────────────────────────
# These are never varied by the optimizer. They are injected into every
# backtest subprocess environment alongside the parameter values.

_FIXED_ENV: Dict[str, str] = {
    "ENTRY_POLICY":           "TOS_SIGNAL",
    "EXIT_POLICY":            "TOS",
    "REPLAY_MODE":            "1",
    "LIVE_TRADING":           "0",
    "PYTHONHASHSEED":         "0",
    "MARKET_WINDOW_SECONDS":  "300",
    "MIN_SIGMA_FLOOR":        "0.50",
    # Disable the hourly loss circuit breaker during backtests so it never
    # stops a run mid-way through a bad parameter set.
    "MAX_LOSS_PER_HOUR_USDC": "999999",
    "USE_TIME_AWARE_EXITS":   "1",
    # Suppress status ticker noise in backtest logs
    "STATUS_INTERVAL_S":      "9999",
}


# ── Public API ─────────────────────────────────────────────────────────────────

def get_tunable() -> Dict[str, Dict[str, Any]]:
    """Return only the parameters where tune=True."""
    return {k: v for k, v in PARAMETERS.items() if v["tune"]}


def get_fixed_env() -> Dict[str, str]:
    """
    Return the always-fixed operational env vars as a string dict ready to
    be merged into os.environ for a subprocess.
    """
    return dict(_FIXED_ENV)


def defaults() -> Dict[str, Any]:
    """Return {name: default} for all parameters (tunable and fixed)."""
    return {k: v["default"] for k, v in PARAMETERS.items()}


def sample_random(rng: random.Random) -> Dict[str, Any]:
    """
    Draw one random parameter set from the tunable search space.

    Each tunable parameter is sampled uniformly from the grid points implied
    by [min, max, step] — i.e. random.choice(grid_values_for_param).  This
    gives uniform coverage across the discrete search space rather than a
    continuous uniform draw (which could produce values never seen in grid search).

    Non-tunable parameters are included at their defaults.
    """
    result = defaults()
    for name, spec in PARAMETERS.items():
        if not spec["tune"]:
            continue
        values = _grid_values(spec)
        result[name] = rng.choice(values)
    return result


def grid_points() -> Iterator[Dict[str, Any]]:
    """
    Yield every combination of tunable parameter values (cartesian product).

    WARNING: the full grid can be enormous. Use this only for small search
    spaces or with explicit iteration limits.
    """
    tunable = get_tunable()
    names = list(tunable.keys())
    value_lists = [_grid_values(tunable[n]) for n in names]

    base = defaults()
    for combo in itertools.product(*value_lists):
        point = dict(base)
        for name, val in zip(names, combo):
            point[name] = val
        yield point


def suggest_optuna(trial: 'optuna.Trial') -> Dict[str, Any]:
    """
    Draw one parameter set using Optuna's suggestion engine.

    Non-tunable parameters are included at their defaults.
    """
    result = defaults()
    for name, spec in PARAMETERS.items():
        if not spec["tune"]:
            continue
            
        lo = spec["min"]
        hi = spec["max"]
        step = spec["step"]
        typ = spec["type"]
        
        if typ == "int":
            result[name] = trial.suggest_int(name, int(lo), int(hi), step=int(step))
        else:
            result[name] = trial.suggest_float(name, lo, hi, step=step)
            
    return result


def _grid_values(spec: Dict[str, Any]) -> list:
    """
    Build the discrete list of values for a parameter from [min, max, step].

    Uses integer arithmetic to avoid floating-point accumulation errors.
    """
    lo   = spec["min"]
    hi   = spec["max"]
    step = spec["step"]
    typ  = spec["type"]

    values = []
    v = lo
    while v <= hi + step * 1e-9:   # small epsilon avoids float cutoff at hi
        if typ == "int":
            values.append(int(round(v)))
        else:
            # Round to the same number of decimal places as `step` to keep
            # values clean (e.g. 0.0004 not 0.00040000000000000002).
            decimals = len(str(step).rstrip("0").split(".")[-1]) if "." in str(step) else 0
            values.append(round(v, decimals))
        v += step

    # Deduplicate while preserving order (relevant for int params near boundaries)
    seen = set()
    result = []
    for val in values:
        if val not in seen:
            seen.add(val)
            result.append(val)
    return result
```
