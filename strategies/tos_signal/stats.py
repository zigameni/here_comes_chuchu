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
