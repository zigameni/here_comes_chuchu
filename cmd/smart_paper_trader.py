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
