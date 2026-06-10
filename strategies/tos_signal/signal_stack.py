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
