"""
shared/metrics.py
─────────────────
Phase 1c / Task O2 — in-process metrics emitter.

Every process in the pipeline calls emit() to append a structured JSON line
to METRICS_PATH (default: metrics.jsonl).  tools/dashboard.py tails that file
and renders a live terminal display.  Phase 1b Task R4 will add alerting on
top of the same stream.

Design decisions
────────────────
- Module-level singleton: import emit() anywhere with no object to thread
  through the call stack.
- Thread-safe: a threading.Lock guards all file writes.
- Non-blocking: the lock is held only for one write + flush.  At the expected
  ~2 emits/s this is negligible (<1 µs).
- Zero-overhead when disabled: METRICS_ENABLED=0 turns every emit() into a
  no-op with a single bool check.
- Process-safe: each process opens its own file handle in append mode.
  Multiple processes can write to the same file without corruption because
  POSIX guarantees that O_APPEND writes under 4096 bytes are atomic.

Line format
───────────
    {"event":"entry","ts_ms":1719000000123,...field-specific fields...}

Callers
───────
    from shared.metrics import emit

    # At every entry (smart_paper_trader._simulate_entry):
    emit("entry", ts_ms=ts, market_id=mid[:8], side=side, ask=ask,
         fv=fv, edge=edge, cost=cost, z_score=z, sigma=sigma,
         is_sigma_real=real, elapsed_s=elapsed)

    # At every exit (smart_paper_trader._exit_position):
    emit("exit", ts_ms=ts, market_id=mid[:8], side=side, reason=reason,
         pnl=pnl, cost=cost, proceeds=proceeds, exit_price=ex_price)

    # FV heartbeat (core/fv_engine, every STATUS_INTERVAL_S seconds):
    emit("fv_status", ts_ms=ts, sigma=sigma, intra_vol=intra_vol,
         is_sigma_real=is_real, prob_up=prob_up,
         btc_price=btc, strike=K, z_score=z, t_remaining_s=T)

    # System heartbeat (smart_paper_trader._print_status, every STATUS_INTERVAL_S):
    emit("heartbeat", ts_ms=ts, entries=n, exits_tp=tp, exits_sl=sl,
         settlement_wins=w, settlement_losses=l, net_pnl=pnl,
         total_cost=cost, sigma_not_real_pct=pct,
         fv_age_ms=fv_age, pm_age_ms=pm_age,
         stale_skips=stale, window_mismatches=mm,
         open_pos=n_open, exit_policy=policy)
"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

# ── Config ─────────────────────────────────────────────────────────────────────

METRICS_ENABLED: bool = os.getenv("METRICS_ENABLED", "1") != "0"
METRICS_PATH:    Path = Path(os.getenv("METRICS_PATH", "metrics.jsonl"))

# ── Singleton state ────────────────────────────────────────────────────────────

_lock = threading.Lock()
_file = None   # opened on first emit() call; avoids creating the file at import


def _ensure_open() -> None:
    """Open METRICS_PATH in append mode.  Called under _lock."""
    global _file
    if _file is None:
        # line-buffered (buffering=1) so each line is flushed immediately
        # without an explicit flush() call on every write.
        _file = open(METRICS_PATH, "a", encoding="utf-8", buffering=1)


def emit(event: str, **fields) -> None:
    """
    Append one JSON metrics line to METRICS_PATH.

    Parameters
    ----------
    event   : event type, e.g. "entry", "exit", "fv_status", "heartbeat"
    **fields: arbitrary key-value data attached to the line.
              ts_ms is auto-populated from wall-clock time if not provided.
    """
    if not METRICS_ENABLED:
        return

    if "ts_ms" not in fields:
        fields["ts_ms"] = int(time.time() * 1000)

    line = json.dumps({"event": event, **fields}, default=str)

    with _lock:
        try:
            _ensure_open()
            _file.write(line + "\n")
        except OSError:
            # Never crash the trading process over a metrics write failure.
            # Silently skip — the dashboard will simply show stale values.
            pass


def close() -> None:
    """
    Flush and close the metrics file handle.

    Call once on process shutdown (e.g. in a finally block after the event
    loop exits) to ensure the last few lines are flushed to disk.
    """
    global _file
    with _lock:
        if _file is not None:
            try:
                _file.flush()
                _file.close()
            except OSError:
                pass
            finally:
                _file = None
