#!/usr/bin/env python3
"""
tools/replay_engine.py
──────────────────────
Reads a capture JSONL file and replays events through ZMQ sockets
at the correct relative timing (or at maximum speed for offline backtesting).

Usage:
    python -m tools.replay_engine captures/2026-06-01.jsonl --speed 1.0
"""

import argparse
import asyncio
import base64
import json
try:
    import orjson
    JSON_LOADS = orjson.loads
except ImportError:
    JSON_LOADS = json.loads
import logging
import os
import sys
import time
from pathlib import Path

# Fix Windows terminal encoding
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
if sys.stderr.encoding.lower() != 'utf-8':
    sys.stderr.reconfigure(encoding='utf-8')

# Ensure we can import from the project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Force REPLAY_MODE to 1 so that our publishers bind to the isolated replay sockets
os.environ["REPLAY_MODE"] = "1"

import zmq
import zmq.asyncio as azmq

from shared.ipc import Channel, get_publisher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("replay")


class ReplayEngine:
    def __init__(self, capture_path: Path, speed_multiplier: float = 0.0):
        self.capture_path = capture_path
        self.speed = float(speed_multiplier)
        
        self.ctx = zmq.Context.instance()
        
        # In Phase 5 deterministic replay, we broadcast all events over a single stream
        from shared.ipc import _resolve_addr
        # Use PUSH instead of PUB for backpressure so max speed doesn't OOM
        self.replay_pub = self.ctx.socket(zmq.PUSH)
        self.replay_pub.set_hwm(10000)
        self.replay_pub.bind(_resolve_addr(Channel.REPLAY_STREAM))
        
        # Readiness PULL socket for handshake
        self.ready_sock = self.ctx.socket(zmq.PULL)
        self.ready_sock.bind(_resolve_addr(Channel.REPLAY_READY))

    def _count_lines(self) -> int:
        with open(self.capture_path, "rb") as f:
            return sum(1 for _ in f)

    def run(self):
        total_events = self._count_lines()
        if total_events == 0:
            log.warning("No events found in capture file.")
            return

        log.info(f"Starting replay of {total_events} events. Speed: {'MAX' if self.speed <= 0 else f'{self.speed}x'}")
        
        # Wait for trader readiness handshake
        log.info("Replay waiting for trader readiness...")
        self.ready_sock.recv()
        log.info("Trader ready, beginning replay.")
        
        sent_fv = 0
        sent_pm = 0
        messages_sent = 0
        last_log_time = time.time()
        
        base_ts_ms = None
        replay_start = time.time()
        
        with open(self.capture_path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                    
                event = JSON_LOADS(line)
                channel = event["channel"]
                ts_ms = event["ts_ms"]
                data = base64.b64decode(event["data"])
                
                if base_ts_ms is None:
                    base_ts_ms = ts_ms
                
                if self.speed > 0:
                    event_offset_ms = ts_ms - base_ts_ms
                    target_elapsed_s = (event_offset_ms / 1000.0) / self.speed
                    actual_elapsed_s = time.time() - replay_start
                    sleep_s = target_elapsed_s - actual_elapsed_s
                    
                    if sleep_s > 0:
                        time.sleep(sleep_s)
                
                self.replay_pub.send_multipart([channel.encode("utf-8"), data])
                messages_sent += 1
                
                if channel == Channel.FV_STREAM: sent_fv += 1
                elif channel == Channel.PM_BOOK: sent_pm += 1
                
                now = time.time()
                if now - last_log_time > 5.0:
                    percent = (messages_sent / total_events) * 100
                    log.info(f"Replay progress: {messages_sent}/{total_events} ({percent:.1f}%)")
                    last_log_time = now
                
        log.info(f"Replay complete! Sent FV: {sent_fv}, Sent PM: {sent_pm}")

        # Send an in-band EOF after all market-data messages.  Because it uses
        # the same PUB socket, a trader that receives this marker has already
        # processed every preceding replay message.
        self.replay_pub.send_multipart([b"__REPLAY_EOF__", b""])

        log.info("Waiting for trader EOF acknowledgement...")
        poller = zmq.Poller()
        poller.register(self.ready_sock, zmq.POLLIN)
        if poller.poll(30000):  # 30s timeout
            ack = self.ready_sock.recv()
            if ack == b"EOF_ACK":
                log.info("Trader acknowledged replay drain.")
            else:
                log.warning("Unexpected trader acknowledgement: %r", ack)
        else:
            log.warning("Timed out waiting for trader EOF acknowledgement.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BTC Bot Replay Engine")
    parser.add_argument("capture", type=str, help="Path to the JSONL capture file")
    parser.add_argument("-s", "--speed", type=float, default=0.0, 
                        help="Playback speed multiplier (0 = max speed, 1 = real-time, 10 = 10x)")
    
    args = parser.parse_args()
    
    capture_path = Path(args.capture)
    if not capture_path.exists():
        log.error(f"Capture file not found: {capture_path}")
        sys.exit(1)
        
    engine = ReplayEngine(capture_path, args.speed)
    
    try:
        engine.run()
    except KeyboardInterrupt:
        log.info("Replay stopped by user.")
    finally:
        # Clean up ZMQ context (linger=0 prevents hanging on infinite HWM queue)
        engine.replay_pub.close(linger=0)
        engine.ready_sock.close(linger=0)
        engine.ctx.term()


# ── Programmatic API (used by tools/optimizer.py) ─────────────────────────────

def run_backtest(
    capture_path: "str | Path",
    env_overrides: dict,
    fills_path: "str | Path",
    exits_path: "str | Path",
    speed: float = 0.0,
    trader_timeout_s: float = 60.0,
) -> dict:
    """
    Run one complete backtest and return performance metrics.

    Orchestration
    ─────────────
    1. Build the full subprocess environment: os.environ base + env_overrides
       (which contains both the always-fixed operational vars from
       get_fixed_env() and the per-run parameter values).
    2. Set FILLS_PATH / EXITS_PATH so the trader writes output to isolated
       temp files supplied by the caller.
    3. Launch smart_paper_trader as a subprocess.
    4. Run the ReplayEngine coroutine in THIS process (it only binds ZMQ
       sockets; no separate process needed).
    5. Wait for the trader subprocess to exit cleanly.
    6. Parse the fills/exits JSONL files and compute metrics.

    Returns
    ───────
    {
        "net_pnl":      float,
        "roi":          float,   # net_pnl / total_cost, or 0.0 if no cost
        "win_rate":     float,   # fraction 0–1, or 0.0 if no trades
        "total_trades": int,
        "max_drawdown": float,   # most negative cumulative PnL seen (≤ 0)
        "sharpe":       float,   # mean per-trade PnL / std (0.0 if < 2 trades)
        "error":        str | None,  # non-None if the run crashed
    }
    """
    import subprocess
    import statistics

    capture_path = Path(capture_path)
    fills_path   = Path(fills_path)
    exits_path   = Path(exits_path)

    # ── Build subprocess environment ──────────────────────────────────────────
    env = dict(os.environ)
    env.update({k: str(v) for k, v in env_overrides.items()})
    env["FILLS_PATH"] = str(fills_path)
    env["EXITS_PATH"] = str(exits_path)
    # Ensure REPLAY_MODE is set (may already be in env_overrides, but be explicit)
    env["REPLAY_MODE"] = "1"

    _bt_log = log.getChild("backtest")

    # ── Launch trader subprocess ───────────────────────────────────────────────
    trader_proc = subprocess.Popen(
        [sys.executable, "-u", "-m", "cmd.smart_paper_trader"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=str(Path(__file__).resolve().parents[1]),
    )

    # ── Run replay engine in-process ──────────────────────────────────────────
    engine = ReplayEngine(capture_path, speed_multiplier=speed)
    try:
        engine.run()
    except Exception as exc:
        _bt_log.error("Replay engine error: %s", exc)
        trader_proc.kill()
        trader_proc.wait(timeout=10)
        return _error_metrics(str(exc))
    finally:
        engine.replay_pub.close(linger=0)
        engine.ready_sock.close(linger=0)
        engine.ctx.term()

    # ── Replay finished: Force trader shutdown and read files ─────────────────
    # The trader has already sent EOF_ACK, meaning it has processed all events
    # and flushed its output files. We don't need to wait for a clean exit.
    trader_proc.terminate()
    try:
        trader_proc.wait(timeout=2.0)
    except subprocess.TimeoutExpired:
        trader_proc.kill()
        trader_proc.wait(timeout=2.0)

    # ── Compute and return metrics ────────────────────────────────────────────
    return _compute_metrics(fills_path, exits_path)


def _compute_metrics(fills_path: "str | Path", exits_path: "str | Path") -> dict:
    """
    Parse fills and exits JSONL files and compute all performance metrics.

    This function is pure I/O + arithmetic — no ZMQ, no subprocesses — so it
    can be unit-tested independently and called after any replay run.

    Sharpe ratio is computed per-trade (mean PnL / std PnL), which is the
    most meaningful metric when trade count varies across parameter sets.
    Returns 0.0 when fewer than 2 trades have settled.
    """
    import statistics

    def _load(p: Path) -> list:
        p = Path(p)
        if not p.exists():
            return []
        lines = p.read_text(encoding="utf-8").splitlines()
        records = []
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except Exception:
                pass
        return records

    fills = _load(fills_path)
    exits = _load(exits_path)

    total_cost = sum(f.get("cost", 0.0) for f in fills)
    pnls       = [e.get("pnl", 0.0) for e in exits]
    proceeds   = sum(e.get("proceeds", 0.0) for e in exits)

    net_pnl      = proceeds - total_cost
    total_trades = len(exits)
    wins         = sum(1 for p in pnls if p > 0)
    losses       = sum(1 for p in pnls if p < 0)

    roi      = (net_pnl / total_cost) if total_cost > 0 else 0.0
    win_rate = (wins / (wins + losses)) if (wins + losses) > 0 else 0.0

    # Max drawdown: most negative point in the running cumulative PnL series
    cum = 0.0
    max_drawdown = 0.0
    for p in pnls:
        cum += p
        if cum < max_drawdown:
            max_drawdown = cum

    # Per-trade Sharpe: mean / std of individual trade PnLs
    if len(pnls) >= 2:
        mean_pnl = statistics.mean(pnls)
        std_pnl  = statistics.stdev(pnls)
        sharpe   = (mean_pnl / std_pnl) if std_pnl > 0 else 0.0
    else:
        sharpe = 0.0

    return {
        "net_pnl":      round(net_pnl,  4),
        "roi":          round(roi,       4),
        "win_rate":     round(win_rate,  4),
        "total_trades": total_trades,
        "max_drawdown": round(max_drawdown, 4),
        "sharpe":       round(sharpe,    4),
        "error":        None,
    }


def _error_metrics(reason: str) -> dict:
    """Return a zeroed metrics dict with an error message."""
    return {
        "net_pnl":      0.0,
        "roi":          0.0,
        "win_rate":     0.0,
        "total_trades": 0,
        "max_drawdown": 0.0,
        "sharpe":       0.0,
        "error":        reason,
    }

