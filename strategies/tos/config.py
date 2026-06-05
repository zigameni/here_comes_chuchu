"""
strategies/tos/config.py
────────────────────────
Environment-based configuration for the Terminal Oracle Sniper (TOS) strategy.

All values are read from environment variables with the same defaults that were
previously defined at the top of smart_paper_trader.py.  The values here are
the single source of truth; smart_paper_trader.py no longer defines them.
"""

from __future__ import annotations

import os
from statistics import NormalDist

# ── Entry timing ───────────────────────────────────────────────────────────────
# TOS only enters in the late window: [TOS_ENTRY_START_S, TOS_ENTRY_END_S] seconds
# since the window opened.  Default: 210–270 s (the 3.5–4.5 min band of a 5-min window).
TOS_ENTRY_START_S: float = float(os.getenv("TOS_ENTRY_START_S", "210"))
TOS_ENTRY_END_S:   float = float(os.getenv("TOS_ENTRY_END_S",   "270"))

# ── Probability / edge thresholds ─────────────────────────────────────────────
# Minimum winning-side probability required for a TOS entry.
TOS_MIN_PROB:      float = float(os.getenv("TOS_MIN_PROB",      "0.70"))
# Minimum edge (prob − ask) required for a TOS entry.
TOS_MIN_EDGE:      float = float(os.getenv("TOS_MIN_EDGE",      "0.05"))
# Minimum liquidity (shares) on the winning side required for a TOS entry.
TOS_MIN_LIQUIDITY: float = float(os.getenv("TOS_MIN_LIQUIDITY", "20.0"))

# ── Z-score threshold ─────────────────────────────────────────────────────────
# The model-implied z-score (|FVState.z_score|) must exceed this value for a
# TOS entry.  Default is derived from TOS_MIN_PROB so the probability gate and
# z-gate are equivalent at defaults — changing either one without the other is
# intentional and supported.
_TOS_DEFAULT_Z_THRESHOLD: float = NormalDist().inv_cdf(
    min(max(TOS_MIN_PROB, 1e-9), 1.0 - 1e-9)
)
TOS_Z_THRESHOLD: float = float(
    os.getenv("TOS_Z_THRESHOLD", str(_TOS_DEFAULT_Z_THRESHOLD))
)

# ── Exit policy ───────────────────────────────────────────────────────────────
# TOS holds every position to settlement.  EXIT_POLICY=TOS suppresses all
# mid-window TP/SL events so win-rate and expectancy numbers are not corrupted
# by premature exits.
EXIT_POLICY: str = os.getenv("EXIT_POLICY", "legacy").strip().upper()
