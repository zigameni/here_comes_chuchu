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
    rejected_warmup      — Not enough BTC history for Variance Ratio calculation.
    rejected_var_ratio   — rejected by Variance Ratio check (mean-reverting).
    rejected_merton      — merton_distance_signal() returned None.
    rejected_ofi         — orderbook_imbalance_signal() returned None.
    rejected_consensus   — both signals returned None → no view at all.
    rejected_disagreement— signals returned conflicting directions, OR a single
                           signal fired but pointed opposite to the TOS decision.
    accepted_signal      — candidate passed all signal gates → entry attempted.
    """

    tos_candidates:        int = 0
    rejected_warmup:       int = 0
    rejected_var_ratio:    int = 0
    rejected_merton:       int = 0
    rejected_ofi:          int = 0
    rejected_consensus:    int = 0
    rejected_disagreement: int = 0
    accepted_signal:       int = 0

    def reset_for_market(self) -> None:
        """Reset all counters on window transition."""
        self.tos_candidates        = 0
        self.rejected_warmup       = 0
        self.rejected_var_ratio    = 0
        self.rejected_merton       = 0
        self.rejected_ofi          = 0
        self.rejected_consensus    = 0
        self.rejected_disagreement = 0
        self.accepted_signal       = 0

    def as_dict(self) -> Dict[str, Any]:
        """Flat representation for get_diagnostics()."""
        return {
            "tos_candidates":        self.tos_candidates,
            "rejected_warmup":       self.rejected_warmup,
            "rejected_var_ratio":    self.rejected_var_ratio,
            "rejected_merton":       self.rejected_merton,
            "rejected_ofi":          self.rejected_ofi,
            "rejected_consensus":    self.rejected_consensus,
            "rejected_disagreement": self.rejected_disagreement,
            "accepted_signal":       self.accepted_signal,
        }
