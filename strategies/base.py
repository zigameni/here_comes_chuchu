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
