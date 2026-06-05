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

__all__ = [
    "TOS_ENTRY_START_S",
    "TOS_ENTRY_END_S",
    "TOS_MIN_PROB",
    "TOS_MIN_EDGE",
    "TOS_MIN_LIQUIDITY",
    "TOS_Z_THRESHOLD",
    "EXIT_POLICY",
]
