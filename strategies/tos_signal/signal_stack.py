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

# Merton Distance (z-score) required for the momentum signal to fire.
MERTON_DISTANCE_GATE: float = float(os.getenv("MERTON_DISTANCE_GATE", "1.5"))

# Minimum OFI skew to trigger imbalance signal.
OFI_IMBALANCE_GATE: float = float(os.getenv("OFI_IMBALANCE_GATE", "50.0"))

# Minimum combined PM depth (liq_up + liq_down) required before the imbalance
# signal is evaluated.  Below this the book is too thin to trust.
SIGNAL_MIN_LIQUIDITY: float = float(os.getenv("SIGNAL_MIN_LIQUIDITY", "20.0"))



from collections import deque

class SignalStack:
    """Architecture C signal stack — merton distance + orderbook OFI."""

    def __init__(self) -> None:
        self._prev_pm: Optional["PMState"] = None
        self._ofi_up_window: deque[float] = deque(maxlen=10)
        self._ofi_dn_window: deque[float] = deque(maxlen=10)

    def reset_for_market(self) -> None:
        self._prev_pm = None
        self._ofi_up_window.clear()
        self._ofi_dn_window.clear()

    def merton_distance_signal(
        self,
        fv: "FVState",
    ) -> Optional[str]:
        """
        Normalized displacement from strike (Z-score).

        Returns "UP", "DOWN", or None.
        """
        if abs(fv.z_score) < MERTON_DISTANCE_GATE:
            return None

        return "UP" if fv.z_score > 0 else "DOWN"

    def orderbook_imbalance_signal(self, pm: "PMState") -> Optional[str]:
        """
        Order Flow Imbalance (OFI) signal.

        Approximates OFI using liq_up and liq_down changes tick-to-tick.
        Returns "UP", "DOWN", or None.
        """
        total_liq = pm.liq_up + pm.liq_down
        if total_liq < SIGNAL_MIN_LIQUIDITY:
            self._prev_pm = pm
            return None

        if self._prev_pm is None:
            self._prev_pm = pm
            return None

        # Compute OFI for UP
        mid_up = (pm.bid_up + pm.ask_up) / 2 if pm.bid_up and pm.ask_up else 0.5
        prev_mid_up = (self._prev_pm.bid_up + self._prev_pm.ask_up) / 2 if self._prev_pm.bid_up and self._prev_pm.ask_up else 0.5
        delta_p_up = mid_up - prev_mid_up
        delta_v_up = pm.liq_up - self._prev_pm.liq_up
        e_up = delta_v_up if delta_p_up >= 0 else -delta_v_up
        self._ofi_up_window.append(e_up)

        # Compute OFI for DOWN
        mid_dn = (pm.bid_down + pm.ask_down) / 2 if pm.bid_down and pm.ask_down else 0.5
        prev_mid_dn = (self._prev_pm.bid_down + self._prev_pm.ask_down) / 2 if self._prev_pm.bid_down and self._prev_pm.ask_down else 0.5
        delta_p_dn = mid_dn - prev_mid_dn
        delta_v_dn = pm.liq_down - self._prev_pm.liq_down
        e_dn = delta_v_dn if delta_p_dn >= 0 else -delta_v_dn
        self._ofi_dn_window.append(e_dn)

        self._prev_pm = pm

        sum_ofi_up = sum(self._ofi_up_window)
        sum_ofi_dn = sum(self._ofi_dn_window)

        if sum_ofi_up > OFI_IMBALANCE_GATE and sum_ofi_dn < -OFI_IMBALANCE_GATE:
            return "UP"
        if sum_ofi_dn > OFI_IMBALANCE_GATE and sum_ofi_up < -OFI_IMBALANCE_GATE:
            return "DOWN"
        
        # Single-sided OFI dominance
        if sum_ofi_up - sum_ofi_dn > OFI_IMBALANCE_GATE:
            return "UP"
        if sum_ofi_dn - sum_ofi_up > OFI_IMBALANCE_GATE:
            return "DOWN"

        return None

    def evaluate(
        self,
        fv: "FVState",
        pm: "PMState",
    ) -> Optional[str]:
        """
        Evaluate the full signal stack and return the consensus direction, or None.

        Plurality voting (any single unambiguous signal is sufficient):
          up > dn AND up >= 1  →  "UP"
          dn > up AND dn >= 1  →  "DOWN"
          up == dn             →  None  (tie, conflict, or both-None)
        """
        mom_sig = self.merton_distance_signal(fv)
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
