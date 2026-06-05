"""
strategies/tos/strategy.py
──────────────────────────
Terminal Oracle Sniper (TOS) strategy.

Entry criteria (all must pass):
  1. is_sigma_real=True (intra-window EWMA, not warmup floor)
  2. elapsed_s in [TOS_ENTRY_START_S, TOS_ENTRY_END_S]
  3. |z_score| >= TOS_Z_THRESHOLD
  4. winning_prob >= TOS_MIN_PROB  AND  ask is not None
  5. edge (prob − ask) >= TOS_MIN_EDGE
  6. liquidity >= TOS_MIN_LIQUIDITY

Exit:
  TOS holds every position to settlement.  evaluate_exit() always returns None.
  Set EXIT_POLICY=TOS in SmartPaperTrader to suppress time-aware mid-window
  TP/SL events so win-rate and expectancy numbers are not contaminated.

Diagnostics:
  get_diagnostics() answers "why was the last tick accepted or rejected?"
  by exposing per-filter rejection counts and a last-tick snapshot.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from strategies.base import BaseStrategy, EntrySignal, FVState, PMState, Position
from strategies.tos.config import (
    TOS_ENTRY_START_S,
    TOS_ENTRY_END_S,
    TOS_MIN_PROB,
    TOS_MIN_EDGE,
    TOS_MIN_LIQUIDITY,
    TOS_Z_THRESHOLD,
)
from strategies.tos.stats import TOSStats

log = logging.getLogger(__name__)


class TOSStrategy(BaseStrategy):
    """
    Terminal Oracle Sniper: late-window, high-conviction entry, hold to settlement.

    This class is the single source of truth for TOS entry logic.
    SmartPaperTrader applies position-cap and fill-cooldown checks separately
    before executing any EntrySignal returned here.
    """

    def __init__(
        self,
        entry_start_s: float = TOS_ENTRY_START_S,
        entry_end_s: float   = TOS_ENTRY_END_S,
        min_prob: float      = TOS_MIN_PROB,
        min_edge: float      = TOS_MIN_EDGE,
        min_liquidity: float = TOS_MIN_LIQUIDITY,
        z_threshold: float   = TOS_Z_THRESHOLD,
    ) -> None:
        self.entry_start_s = entry_start_s
        self.entry_end_s   = entry_end_s
        self.min_prob      = min_prob
        self.min_edge      = min_edge
        self.min_liquidity = min_liquidity
        self.z_threshold   = z_threshold
        self._stats        = TOSStats()

    @property
    def name(self) -> str:
        return "TOS"

    # ── BaseStrategy interface ─────────────────────────────────────────────────

    def evaluate_entry(self, fv: FVState, pm: PMState) -> List[EntrySignal]:
        """
        Evaluate TOS entry criteria for a single PM tick.

        Returns [EntrySignal] when all gates pass (always length 0 or 1).
        Returns [] with a rejection counter incremented when any gate fails.
        """
        self._stats.total_evaluations += 1

        # Gate 1: sigma must come from real intra-window EWMA
        if not fv.is_sigma_real:
            self._stats.rejected_sigma_not_real += 1
            self._stats.last_rejection_filter = "sigma_not_real"
            log.debug("TOS REJECT — sigma_not_real (intra_vol=%.4f)", fv.intra_vol)
            return []

        # Gate 2: must be in the late entry window
        elapsed_s = (pm.ts_ms / 1000.0) - pm.market_ts
        self._stats.last_elapsed_s = elapsed_s

        if elapsed_s < self.entry_start_s or elapsed_s > self.entry_end_s:
            self._stats.rejected_timing += 1
            self._stats.last_rejection_filter = "timing"
            log.debug(
                "TOS REJECT — timing  elapsed=%.1fs  window=[%.0f, %.0f]",
                elapsed_s, self.entry_start_s, self.entry_end_s,
            )
            return []

        # Gate 3: z-score magnitude
        self._stats.last_z_score = fv.z_score
        if abs(fv.z_score) < self.z_threshold:
            self._stats.rejected_z_score += 1
            self._stats.last_rejection_filter = "z_score"
            log.debug(
                "TOS REJECT — z_score  |z|=%.4f < threshold=%.4f",
                abs(fv.z_score), self.z_threshold,
            )
            return []

        # Determine winning side
        if fv.prob_up >= fv.prob_down:
            side         = "UP"
            winning_prob = fv.prob_up
            ask          = pm.ask_up
            liquidity    = pm.liq_up
        else:
            side         = "DOWN"
            winning_prob = fv.prob_down
            ask          = pm.ask_down
            liquidity    = pm.liq_down

        self._stats.last_side = side
        self._stats.last_fv   = winning_prob

        # Gate 4: probability and ask presence
        if winning_prob < self.min_prob or ask is None:
            self._stats.rejected_prob += 1
            self._stats.last_rejection_filter = "prob"
            log.debug(
                "TOS REJECT — prob  %s  winning_prob=%.4f  ask=%s",
                side, winning_prob, ask,
            )
            return []

        # Gate 5: edge
        self._stats.last_ask = ask
        edge = winning_prob - ask
        self._stats.last_edge = edge

        if edge < self.min_edge:
            self._stats.rejected_edge += 1
            self._stats.last_rejection_filter = "edge"
            log.debug(
                "TOS REJECT — edge  %s  edge=%.4f < min=%.4f",
                side, edge, self.min_edge,
            )
            return []

        # Gate 6: liquidity
        if liquidity < self.min_liquidity:
            self._stats.rejected_liquidity += 1
            self._stats.last_rejection_filter = "liquidity"
            log.debug(
                "TOS REJECT — liquidity  %s  liq=%.1f < min=%.1f",
                side, liquidity, self.min_liquidity,
            )
            return []

        # All gates passed
        self._stats.total_accepted      += 1
        self._stats.last_rejection_filter = None  # None = accepted

        log.debug(
            "TOS ACCEPT  %s  ask=%.4f  fv=%.4f  edge=%.4f  z=%.3f  elapsed=%.1fs",
            side, ask, winning_prob, edge, fv.z_score, elapsed_s,
        )

        return [EntrySignal(
            side      = side,
            ask       = ask,
            fv        = winning_prob,
            edge      = edge,
            elapsed_s = elapsed_s,
            z_score   = fv.z_score,
        )]

    def evaluate_exit(
        self,
        pos:   Position,
        fv:    FVState,
        pm:    PMState,
        ts_ms: int,
    ) -> Optional[str]:
        """TOS holds every position to settlement — no mid-window exits."""
        return None

    def reset_for_market(self) -> None:
        """Reset per-window counters on window transition."""
        self._stats.reset_for_market()

    def get_diagnostics(self) -> Dict[str, Any]:
        """
        Return structured diagnostics answering:
          • Which filter blocked the last evaluation?
          • How many times has each filter fired this window?
          • What were the last-tick values?
        """
        return {"strategy": "TOS", **self._stats.as_dict()}
