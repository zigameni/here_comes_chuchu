"""
strategies/tos/stats.py
───────────────────────
Per-market-window statistics and diagnostics for the TOS strategy.

These counters are maintained by TOSStrategy and exposed via get_diagnostics().
They answer WHY a specific TOS evaluation passed or was blocked — independently
of anything inside SmartPaperTrader.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class TOSStats:
    """
    Per-window diagnostics for TOS entry evaluation.

    Counters reset on every call to reset_for_market() (window transition).
    Preserved across windows: none — all fields are per-window.

    Usage
    ─────
    After a live run, get_diagnostics() answers:
      • How many ticks were evaluated?
      • Which filter fired most often?
      • What was the last accepted / rejected tick?
    """

    # ── Evaluation volume ──────────────────────────────────────────────────────
    total_evaluations: int = 0      # every evaluate() call, including rejections

    # ── Rejection counters (by filter) ────────────────────────────────────────
    rejected_sigma_not_real: int = 0   # is_sigma_real=False
    rejected_timing:         int = 0   # elapsed_s outside [start, end]
    rejected_z_score:        int = 0   # |z_score| < z_threshold
    rejected_prob:           int = 0   # winning_prob < min_prob or ask is None
    rejected_edge:           int = 0   # edge < min_edge
    rejected_liquidity:      int = 0   # liquidity < min_liquidity

    # ── Acceptance counter ────────────────────────────────────────────────────
    total_accepted: int = 0

    # ── Last-tick diagnostic snapshot ─────────────────────────────────────────
    # Overwritten on every evaluate() call.  Lets operators see the last state
    # without scraping logs — particularly useful when entries are rare.
    last_side:             Optional[str]   = None
    last_elapsed_s:        float           = 0.0
    last_z_score:          float           = 0.0
    last_fv:               float           = 0.0
    last_ask:              float           = 0.0
    last_edge:             float           = 0.0
    last_rejection_filter: Optional[str]   = None   # None when accepted

    def reset_for_market(self) -> None:
        """Reset all per-window counters on window transition."""
        self.total_evaluations       = 0
        self.rejected_sigma_not_real = 0
        self.rejected_timing         = 0
        self.rejected_z_score        = 0
        self.rejected_prob           = 0
        self.rejected_edge           = 0
        self.rejected_liquidity      = 0
        self.total_accepted          = 0
        self.last_side               = None
        self.last_elapsed_s          = 0.0
        self.last_z_score            = 0.0
        self.last_fv                 = 0.0
        self.last_ask                = 0.0
        self.last_edge               = 0.0
        self.last_rejection_filter   = None

    def as_dict(self) -> Dict[str, Any]:
        """Flat representation for get_diagnostics()."""
        return {
            "total_evaluations":       self.total_evaluations,
            "rejected_sigma_not_real": self.rejected_sigma_not_real,
            "rejected_timing":         self.rejected_timing,
            "rejected_z_score":        self.rejected_z_score,
            "rejected_prob":           self.rejected_prob,
            "rejected_edge":           self.rejected_edge,
            "rejected_liquidity":      self.rejected_liquidity,
            "total_accepted":          self.total_accepted,
            "last_side":               self.last_side,
            "last_elapsed_s":          round(self.last_elapsed_s, 3),
            "last_z_score":            round(self.last_z_score, 4),
            "last_fv":                 round(self.last_fv, 4),
            "last_ask":                round(self.last_ask, 4),
            "last_edge":               round(self.last_edge, 4),
            "last_rejection_filter":   self.last_rejection_filter,
        }
