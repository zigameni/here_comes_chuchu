"""
tools/parameter_registry.py
────────────────────────────
Defines the full TOS_SIGNAL parameter search space for the optimizer.

Each entry in PARAMETERS has:
    type     : "float" | "int"
    default  : the current production default (matches env var defaults)
    min      : lower bound of the search range
    max      : upper bound of the search range
    step     : grid search step size
    tune     : True  → optimizer varies this parameter
               False → always run at `default` (registered for logging only)
    group    : logical grouping (entry_gate | pre_strategy | position_sizing |
                                  exit | signal_stack)
    note     : one-line description of what this parameter controls

Exposed helpers:
    get_tunable()      → {name: spec} for all params where tune=True
    get_fixed_env()    → {name: str_value} for always-fixed operational env vars
    sample_random(rng) → {name: value} — one random draw from the tunable space
    grid_points()      → iterator of {name: value} dicts — cartesian product
                         of all tunable params (use with care: may be huge)
"""

from __future__ import annotations

import itertools
import random
from typing import Any, Dict, Iterator

# ── Parameter definitions ──────────────────────────────────────────────────────

PARAMETERS: Dict[str, Dict[str, Any]] = {

    # ── Entry gate — TOS base layer ───────────────────────────────────────────
    # (strategies/tos/config.py, re-exported via strategies/tos_signal/config.py)

    "TOS_ENTRY_START_S": {
        "type": "float",
        "default": 210.0,
        "min": 150.0,
        "max": 240.0,
        "step": 10.0,
        "tune": True,
        "group": "entry_gate",
        "note": "Opening second of the late-window entry band (within the 300s window).",
    },
    "TOS_ENTRY_END_S": {
        "type": "float",
        "default": 270.0,
        "min": 240.0,
        "max": 290.0,
        "step": 10.0,
        "tune": True,
        "group": "entry_gate",
        "note": "Closing second of the late-window entry band.",
    },
    "TOS_MIN_PROB": {
        "type": "float",
        "default": 0.70,
        "min": 0.60,
        "max": 0.85,
        "step": 0.05,
        "tune": True,
        "group": "entry_gate",
        "note": "Minimum FV winning-side probability required for entry. Higher = more selective.",
    },
    "TOS_MIN_EDGE": {
        "type": "float",
        "default": 0.05,
        "min": 0.02,
        "max": 0.15,
        "step": 0.01,
        "tune": True,
        "group": "entry_gate",
        "note": "Minimum edge (winning_prob − ask) required. Primary guard against thin markets.",
    },
    "TOS_MIN_LIQUIDITY": {
        "type": "float",
        "default": 20.0,
        "min": 10.0,
        "max": 100.0,
        "step": 10.0,
        "tune": True,
        "group": "entry_gate",
        "note": "Minimum PM depth on the winning side before entry is allowed.",
    },
    "TOS_Z_THRESHOLD": {
        "type": "float",
        "default": 0.524,   # NormalDist().inv_cdf(0.70)
        "min": 0.25,
        "max": 1.25,
        "step": 0.05,
        "tune": True,
        "group": "entry_gate",
        "note": "Model z-score conviction gate. Can be tuned independently of TOS_MIN_PROB.",
    },
    "TOS_MAX_STRIKE_CROSSES": {
        "type": "int",
        "default": 999,
        "min": 5,
        "max": 50,
        "step": 5,
        "tune": True,
        "group": "entry_gate",
        "note": "Maximum allowed strike crosses during the window before aborting trading.",
    },
    "TOS_MIN_EFFICIENCY_RATIO": {
        "type": "float",
        "default": 0.0,
        "min": 0.0,
        "max": 0.5,
        "step": 0.05,
        "tune": True,
        "group": "entry_gate",
        "note": "Minimum efficiency ratio (net movement / path length) required.",
    },

    # ── Entry gate — pre-strategy layer ──────────────────────────────────────
    # (cmd/smart_paper_trader.py module-level env vars)

    "MIN_EDGE_THRESHOLD": {
        "type": "float",
        "default": 0.03,
        "min": 0.01,
        "max": 0.10,
        "step": 0.01,
        "tune": True,
        "group": "pre_strategy",
        "note": "Hard edge floor applied before strategy evaluation (FV − ask ≥ this).",
    },
    "MIN_ENTRY_ASK": {
        "type": "float",
        "default": 0.05,
        "min": 0.02,
        "max": 0.15,
        "step": 0.01,
        "tune": True,
        "group": "pre_strategy",
        "note": "Minimum ask price required. Blocks near-settled markets with fake FV edge.",
    },
    "FV_ENTRY_MAX": {
        "type": "float",
        "default": 0.97,
        "min": 0.85,
        "max": 0.99,
        "step": 0.02,
        "tune": True,
        "group": "pre_strategy",
        "note": "Upper FV gate — rejects entries when FV is near 1.0 (market already decided).",
    },
    "FV_ENTRY_MIN": {
        "type": "float",
        "default": 0.03,
        "min": 0.01,
        "max": 0.15,
        "step": 0.02,
        "tune": True,
        "group": "pre_strategy",
        "note": "Lower FV gate (symmetric to FV_ENTRY_MAX).",
    },
    "FV_STALE_MS": {
        "type": "float",
        "default": 500.0,
        "min": 200.0,
        "max": 1000.0,
        "step": 100.0,
        "tune": True,
        "group": "pre_strategy",
        "note": "FV data staleness cutoff (ms). Tighter = fresher data only, fewer fills.",
    },
    "MIN_WINDOW_AGE_S": {
        "type": "float",
        "default": 100.0,
        "min": 0.0,
        "max": 180.0,
        "step": 20.0,
        "tune": True,
        "group": "pre_strategy",
        "note": "Minimum seconds into the window before entries open. Blocks K-snap noise.",
    },
    "FILL_COOLDOWN_MS": {
        "type": "float",
        "default": 5000.0,
        "min": 1000.0,
        "max": 30000.0,
        "step": 1000.0,
        "tune": True,
        "group": "pre_strategy",
        "note": "Per-side re-entry cooldown (ms). Prevents rapid re-entry after a fill.",
    },

    # ── Position sizing & risk ─────────────────────────────────────────────────
    # Registered for logging; tune=False — these are market minimums / capital
    # constraints that should not be varied during strategy optimization.

    "PAPER_TRADE_SHARES": {
        "type": "float",
        "default": 5.0,
        "min": 5.0,
        "max": 20.0,
        "step": 5.0,
        "tune": False,
        "group": "position_sizing",
        "note": "Shares per simulated fill. Scales all PnL linearly. Fixed at exchange minimum.",
    },
    "MAX_SHARES_PER_SIDE": {
        "type": "float",
        "default": 10.0,
        "min": 5.0,
        "max": 30.0,
        "step": 5.0,
        "tune": False,
        "group": "position_sizing",
        "note": "Max accumulated exposure per side per market. Capital constraint.",
    },
    "MAX_SPEND_PER_MARKET": {
        "type": "float",
        "default": 8.0,
        "min": 5.0,
        "max": 50.0,
        "step": 5.0,
        "tune": False,
        "group": "position_sizing",
        "note": "USDC budget cap per market window. Capital constraint.",
    },
    "MAX_ENTRIES_PER_WINDOW": {
        "type": "int",
        "default": 5,
        "min": 1,
        "max": 10,
        "step": 1,
        "tune": False,
        "group": "position_sizing",
        "note": "Hard entry count ceiling per window. Capital constraint.",
    },

    # ── Exit thresholds — time-aware exit system ──────────────────────────────
    # (USE_TIME_AWARE_EXITS=1 is always fixed; TOS_SIGNAL uses time-aware exits)

    "EARLY_HIGH_CONFIDENCE_BID": {
        "type": "float",
        "default": 0.88,
        "min": 0.75,
        "max": 0.95,
        "step": 0.05,
        "tune": True,
        "group": "exit",
        "note": "Early-zone TP: exit if bid exceeds this (most value already captured).",
    },
    "LATE_WINDOW_SECONDS": {
        "type": "float",
        "default": 120.0,
        "min": 60.0,
        "max": 180.0,
        "step": 20.0,
        "tune": True,
        "group": "exit",
        "note": "Seconds before settlement when the 'late zone' light-exit logic activates.",
    },
    "LATE_SL_FLOOR": {
        "type": "float",
        "default": 0.08,
        "min": 0.03,
        "max": 0.20,
        "step": 0.01,
        "tune": True,
        "group": "exit",
        "note": "Late-zone SL: cut if bid falls below this (market implies ≤N% win chance).",
    },
    "LATE_TP_BID": {
        "type": "float",
        "default": 0.82,
        "min": 0.70,
        "max": 0.92,
        "step": 0.02,
        "tune": True,
        "group": "exit",
        "note": "Late-zone TP: lock in gains when bid exceeds this threshold.",
    },
    "EMERGENCY_SECONDS": {
        "type": "float",
        "default": 60.0,
        "min": 20.0,
        "max": 90.0,
        "step": 10.0,
        "tune": True,
        "group": "exit",
        "note": "Duration of the emergency zone (seconds before settlement).",
    },
    "EMERGENCY_CUT_PRICE": {
        "type": "float",
        "default": 0.12,
        "min": 0.05,
        "max": 0.25,
        "step": 0.01,
        "tune": True,
        "group": "exit",
        "note": "Emergency SL bid floor. Must pair with EMERGENCY_FV_CONFIRM (double-confirmation).",
    },
    "EMERGENCY_FV_CONFIRM": {
        "type": "float",
        "default": 0.30,
        "min": 0.15,
        "max": 0.45,
        "step": 0.05,
        "tune": True,
        "group": "exit",
        "note": "FV must be below this for emergency cut to fire (prevents premature SL).",
    },
    "EMERGENCY_TP_BID": {
        "type": "float",
        "default": 0.88,
        "min": 0.75,
        "max": 0.95,
        "step": 0.05,
        "tune": True,
        "group": "exit",
        "note": "Emergency TP: lock in near-certain win when bid exceeds this near settlement.",
    },

    # ── Signal stack — TOS_SIGNAL specific ────────────────────────────────────
    # (strategies/tos_signal/signal_stack.py — promoted to env vars in Step 1)

    "BTC_MOMENTUM_GATE": {
        "type": "float",
        "default": 0.0004,
        "min": 0.0001,
        "max": 0.002,
        "step": 0.0001,
        "tune": True,
        "group": "signal_stack",
        "note": "BTC displacement from K (as fraction) required for momentum signal to fire.",
    },
    "ORDERBOOK_IMBALANCE_GATE": {
        "type": "float",
        "default": 0.40,
        "min": 0.20,
        "max": 0.60,
        "step": 0.05,
        "tune": True,
        "group": "signal_stack",
        "note": "Minimum directional liquidity skew (0–1) before imbalance signal fires.",
    },
    "SIGNAL_MIN_LIQUIDITY": {
        "type": "float",
        "default": 20.0,
        "min": 5.0,
        "max": 50.0,
        "step": 5.0,
        "tune": True,
        "group": "signal_stack",
        "note": "Minimum combined PM depth before the imbalance signal is evaluated.",
    },
}

# ── Always-fixed operational env vars ─────────────────────────────────────────
# These are never varied by the optimizer. They are injected into every
# backtest subprocess environment alongside the parameter values.

_FIXED_ENV: Dict[str, str] = {
    "ENTRY_POLICY":           "TOS_SIGNAL",
    "EXIT_POLICY":            "TOS",
    "REPLAY_MODE":            "1",
    "LIVE_TRADING":           "0",
    "PYTHONHASHSEED":         "0",
    "MARKET_WINDOW_SECONDS":  "300",
    "MIN_SIGMA_FLOOR":        "0.50",
    # Disable the hourly loss circuit breaker during backtests so it never
    # stops a run mid-way through a bad parameter set.
    "MAX_LOSS_PER_HOUR_USDC": "999999",
    "USE_TIME_AWARE_EXITS":   "1",
    # Suppress status ticker noise in backtest logs
    "STATUS_INTERVAL_S":      "9999",
}


# ── Public API ─────────────────────────────────────────────────────────────────

def get_tunable() -> Dict[str, Dict[str, Any]]:
    """Return only the parameters where tune=True."""
    return {k: v for k, v in PARAMETERS.items() if v["tune"]}


def get_fixed_env() -> Dict[str, str]:
    """
    Return the always-fixed operational env vars as a string dict ready to
    be merged into os.environ for a subprocess.
    """
    return dict(_FIXED_ENV)


def defaults() -> Dict[str, Any]:
    """Return {name: default} for all parameters (tunable and fixed)."""
    return {k: v["default"] for k, v in PARAMETERS.items()}


def sample_random(rng: random.Random) -> Dict[str, Any]:
    """
    Draw one random parameter set from the tunable search space.

    Each tunable parameter is sampled uniformly from the grid points implied
    by [min, max, step] — i.e. random.choice(grid_values_for_param).  This
    gives uniform coverage across the discrete search space rather than a
    continuous uniform draw (which could produce values never seen in grid search).

    Non-tunable parameters are included at their defaults.
    """
    result = defaults()
    for name, spec in PARAMETERS.items():
        if not spec["tune"]:
            continue
        values = _grid_values(spec)
        result[name] = rng.choice(values)
    return result


def grid_points() -> Iterator[Dict[str, Any]]:
    """
    Yield every combination of tunable parameter values (cartesian product).

    WARNING: the full grid can be enormous. Use this only for small search
    spaces or with explicit iteration limits.
    """
    tunable = get_tunable()
    names = list(tunable.keys())
    value_lists = [_grid_values(tunable[n]) for n in names]

    base = defaults()
    for combo in itertools.product(*value_lists):
        point = dict(base)
        for name, val in zip(names, combo):
            point[name] = val
        yield point


def suggest_optuna(trial: 'optuna.Trial') -> Dict[str, Any]:
    """
    Draw one parameter set using Optuna's suggestion engine.

    Non-tunable parameters are included at their defaults.
    """
    result = defaults()
    for name, spec in PARAMETERS.items():
        if not spec["tune"]:
            continue
            
        lo = spec["min"]
        hi = spec["max"]
        step = spec["step"]
        typ = spec["type"]
        
        if typ == "int":
            result[name] = trial.suggest_int(name, int(lo), int(hi), step=int(step))
        else:
            result[name] = trial.suggest_float(name, lo, hi, step=step)
            
    return result


def _grid_values(spec: Dict[str, Any]) -> list:
    """
    Build the discrete list of values for a parameter from [min, max, step].

    Uses integer arithmetic to avoid floating-point accumulation errors.
    """
    lo   = spec["min"]
    hi   = spec["max"]
    step = spec["step"]
    typ  = spec["type"]

    values = []
    v = lo
    while v <= hi + step * 1e-9:   # small epsilon avoids float cutoff at hi
        if typ == "int":
            values.append(int(round(v)))
        else:
            # Round to the same number of decimal places as `step` to keep
            # values clean (e.g. 0.0004 not 0.00040000000000000002).
            decimals = len(str(step).rstrip("0").split(".")[-1]) if "." in str(step) else 0
            values.append(round(v, decimals))
        v += step

    # Deduplicate while preserving order (relevant for int params near boundaries)
    seen = set()
    result = []
    for val in values:
        if val not in seen:
            seen.add(val)
            result.append(val)
    return result
