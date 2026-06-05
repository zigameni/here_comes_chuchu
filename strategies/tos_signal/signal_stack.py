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
tests/strategies/test_tos_signal_strategy.py and progress.md.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from strategies.base import FVState, PMState


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

        Gate 1 — magnitude: |delta_now| >= 0.0004 (0.04% of K).
        Gate 2 — persistence: BTC must have been on the same side of K 30s ago.

        Returns "UP", "DOWN", or None.
        """
        if btc_30s_ago <= 0 or pm_K <= 0:
            return None

        delta_now = (btc_now - pm_K) / pm_K        # displacement from K right now
        delta_30s = (btc_30s_ago - pm_K) / pm_K    # displacement from K 30s ago

        # Gate 1 — magnitude
        if abs(delta_now) < 0.0004:
            return None

        # Gate 2 — persistence: same side of K 30s ago
        if _sign(delta_now) != _sign(delta_30s):
            return None

        return "UP" if delta_now > 0 else "DOWN"

    def orderbook_imbalance_signal(self, pm: "PMState") -> Optional[str]:
        """
        Orderbook liquidity imbalance gate.

        Requires >= 20 shares combined (not 500 per side) and >=40% directional
        skew of combined depth before a signal fires.

        Returns "UP", "DOWN", or None.
        """
        total_liq = pm.liq_up + pm.liq_down
        if total_liq < 20.0:
            return None

        imbalance = (pm.liq_up - pm.liq_down) / total_liq
        if imbalance > 0.40:
            return "UP"
        if imbalance < -0.40:
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
