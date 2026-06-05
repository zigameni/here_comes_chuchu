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
        
        self.ctx = azmq.Context.instance()
        
        # In Phase 5 deterministic replay, we broadcast all events over a single stream
        # CRITICAL: hwm=0 disables High Water Mark drops for MAX-speed bursts.
        from shared.ipc import _resolve_addr
        self.replay_pub = get_publisher(Channel.REPLAY_STREAM, hwm=0)
        
        # Readiness PULL socket for handshake
        self.ready_sock = self.ctx.socket(zmq.PULL)
        self.ready_sock.bind(_resolve_addr(Channel.REPLAY_READY))

    def _load_events(self):
        log.info(f"Loading events from {self.capture_path}...")
        events = []
        capture_counts = {"fv": 0, "pm": 0}
        with open(self.capture_path, "r", encoding="utf-8") as f:
            for line_no, line in enumerate(f):
                if not line.strip():
                    continue
                try:
                    event = json.loads(line)
                    event["_line_no"] = line_no
                    events.append(event)
                    ch = event.get("channel", "")
                    if ch == Channel.FV_STREAM: capture_counts["fv"] += 1
                    elif ch == Channel.PM_BOOK: capture_counts["pm"] += 1
                except Exception as e:
                    log.error(f"Failed to parse line: {e}")
        
        log.info(f"Capture counts: FV={capture_counts['fv']} PM={capture_counts['pm']}")
        log.info(f"Loaded {len(events)} events. Sorting by timestamp/file order...")
        # seq is recorder-local and may reset when a daily file is appended after
        # a restart.  Timestamp order is the replay clock; file order breaks ties.
        events.sort(key=lambda x: (x["ts_ms"], x["_line_no"]))
        return events

    async def run(self):
        events = self._load_events()
        if not events:
            log.warning("No events found in capture file.")
            return

        base_ts_ms = events[0]["ts_ms"]
        replay_start = time.time()
        
        log.info(f"Starting replay. Speed: {'MAX' if self.speed <= 0 else f'{self.speed}x'}")
        
        # Wait for trader readiness handshake
        log.info("Replay waiting for trader readiness...")
        await self.ready_sock.recv()
        log.info("Trader ready, beginning replay.")
        
        sent_fv = 0
        sent_pm = 0
        messages_sent = 0
        last_log_time = time.time()
        BATCH_SIZE = 100
        
        for i, event in enumerate(events):
            channel = event["channel"]
            ts_ms = event["ts_ms"]
            data = base64.b64decode(event["data"])
            
            if self.speed > 0:
                event_offset_ms = ts_ms - base_ts_ms
                target_elapsed_s = (event_offset_ms / 1000.0) / self.speed
                actual_elapsed_s = time.time() - replay_start
                sleep_s = target_elapsed_s - actual_elapsed_s
                
                if sleep_s > 0:
                    await asyncio.sleep(sleep_s)
            
            self.replay_pub.send_multipart([channel.encode("utf-8"), data])
            messages_sent += 1
            
            if channel == Channel.FV_STREAM: sent_fv += 1
            elif channel == Channel.PM_BOOK: sent_pm += 1
            
            # Yield control to allow consumer ZMQ sockets to drain
            if i % BATCH_SIZE == 0:
                await asyncio.sleep(0)
            
            now = time.time()
            if now - last_log_time > 5.0:
                percent = (messages_sent / len(events)) * 100
                log.info(f"Replay progress: {messages_sent}/{len(events)} ({percent:.1f}%)")
                last_log_time = now
                
        log.info(f"Replay complete! Sent FV: {sent_fv}, Sent PM: {sent_pm}")

        # Send an in-band EOF after all market-data messages.  Because it uses
        # the same PUB socket, a trader that receives this marker has already
        # processed every preceding replay message.
        self.replay_pub.send_multipart([b"__REPLAY_EOF__", b""])

        log.info("Waiting for trader EOF acknowledgement...")
        try:
            ack = await asyncio.wait_for(self.ready_sock.recv(), timeout=30.0)
            if ack == b"EOF_ACK":
                log.info("Trader acknowledged replay drain.")
            else:
                log.warning("Unexpected trader acknowledgement: %r", ack)
        except asyncio.TimeoutError:
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
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        log.info("Replay stopped by user.")
    finally:
        # Clean up ZMQ context (linger=0 prevents hanging on infinite HWM queue)
        engine.replay_pub.close(linger=0)
        engine.ready_sock.close(linger=0)
        engine.ctx.term()
