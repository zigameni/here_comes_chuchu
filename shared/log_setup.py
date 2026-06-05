"""
shared/log_setup.py
───────────────────
Phase 1c / Task O1 — unified structured (JSON) logging for every process in
the BTC bot pipeline.

Replaces the mixed loguru + logging.basicConfig pattern.  Every module calls:

    from shared.log_setup import setup_logging
    log = setup_logging("my_module")

All processes write the same compact JSON line format to stderr (visible in the
terminal) and optionally to LOG_FILE (default: btcbot.log) for post-hoc grep.

Line format
───────────
    {"ts":1719000000.123,"level":"INFO","logger":"risk","msg":"..."}

Extra fields are attached via the standard logging.extra= mechanism:
    log.info("Entry fired", extra={"market_id": "abc123", "edge": 0.07})
→   {"ts":...,"level":"INFO","logger":"risk","msg":"Entry fired",
     "market_id":"abc123","edge":0.07}

Why this exists
───────────────
1.  loguru and stdlib logging produce different timestamp formats, making grep
    and log aggregation unreliable across processes.
2.  logging.basicConfig() called in multiple modules creates independent root
    configs that silently shadow each other depending on import order.
3.  One JSON formatter makes every log line machine-parseable without regex,
    ready for the metrics alerting added in Task R4.
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Optional

# ── Config (env-driven so each process can override without code changes) ───────

LOG_FILE:  str = os.getenv("LOG_FILE",  "btcbot.log")   # "" to disable file sink
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

# ── Internal state ──────────────────────────────────────────────────────────────

_CONFIGURED = False   # root logger is configured at most once per process


# Fields that exist on every LogRecord and are either already captured in the
# fixed top-level keys or are Python-internal bookkeeping we don't export.
_SKIP = frozenset({
    "name", "msg", "args", "levelname", "levelno",
    "pathname", "filename", "module",
    "exc_info", "exc_text", "stack_info",
    "lineno", "funcName",
    "created", "msecs", "relativeCreated",
    "thread", "threadName", "processName", "process",
    "taskName", "message",
})


class _JsonFormatter(logging.Formatter):
    """Emit every log record as a single compact JSON line."""

    def format(self, record: logging.LogRecord) -> str:
        obj: dict = {
            "ts":     round(record.created, 3),
            "level":  record.levelname,
            "logger": record.name,
            "msg":    record.getMessage(),
        }
        # Attach any extra= fields the caller provided.
        for key, val in record.__dict__.items():
            if key not in _SKIP and not key.startswith("_"):
                obj[key] = val
        # Append formatted exception if present.
        if record.exc_info:
            obj["exc"] = self.formatException(record.exc_info)
        return json.dumps(obj, default=str)


def setup_logging(
    name:  str,
    level: Optional[str] = None,
) -> logging.Logger:
    """
    Configure the root logger (once per process) and return a named child.

    Safe to call from every module — subsequent calls after the first are
    no-ops for the root configuration but always return a correctly-named
    logger.

    Parameters
    ----------
    name  : dotted module name, e.g. "risk", "fv_engine", "pm_daemon"
    level : override LOG_LEVEL for this specific logger only (optional)

    Returns
    -------
    logging.Logger with the given name, writing JSON to stderr (+ file).
    """
    global _CONFIGURED

    if not _CONFIGURED:
        root = logging.getLogger()
        root.setLevel(logging.DEBUG)   # root accepts all; handlers apply own filter

        fmt = _JsonFormatter()

        # stderr sink — always active, respects LOG_LEVEL
        sh = logging.StreamHandler(sys.stderr)
        sh.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
        sh.setFormatter(fmt)
        root.addHandler(sh)

        # file sink — optional; disabled when LOG_FILE=""
        if LOG_FILE:
            try:
                from logging.handlers import RotatingFileHandler
                
                class SafeRotatingFileHandler(RotatingFileHandler):
                    def doRollover(self):
                        try:
                            super().doRollover()
                        except (PermissionError, OSError) as e:
                            # On Windows/WSL, renaming files held open by other processes
                            # can throw PermissionError. We ignore it and keep writing.
                            # The stream will be reopened by FileHandler.emit automatically.
                            pass

                # Split extension to inject the module name, e.g. btcbot.log -> btcbot_binance_daemon.log
                base, ext = os.path.splitext(LOG_FILE)
                process_log_file = f"{base}_{name}{ext}"
                
                fh = SafeRotatingFileHandler(process_log_file, maxBytes=50*1024*1024, backupCount=3, encoding="utf-8")
                fh.setLevel(logging.DEBUG)   # capture everything to disk
                fh.setFormatter(fmt)
                root.addHandler(fh)
            except OSError as exc:
                # Don't crash the process if we can't open the log file.
                root.warning(
                    "Could not open log file %r: %s — file logging disabled",
                    LOG_FILE, exc,
                )

        _CONFIGURED = True

    logger = logging.getLogger(name)
    if level:
        logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    return logger
