#!/usr/bin/env python3
"""
tools/data_recorder.py
──────────────────────
Captures live events from all IPC streams (BINANCE_BBO, FV_STREAM, PM_BOOK)
and records them to daily JSONL files for offline replay and backtesting.

Format:
    {"channel": "ipc:///tmp/fv_stream.ipc", "ts_ms": 1704067200000, "data": "<b64>"}
"""

import asyncio
import base64
import json
import logging
import os
import sys
import time
import itertools
from datetime import datetime, timezone
from pathlib import Path

# Fix Windows terminal encoding
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
if sys.stderr.encoding.lower() != 'utf-8':
    sys.stderr.reconfigure(encoding='utf-8')

# Ensure we can import from the project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import zmq
import zmq.asyncio as azmq

from shared.ipc import Channel, unpack, _resolve_addr

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("recorder")


class DataRecorder:
    def __init__(self, capture_dir: Path):
        self.capture_dir = capture_dir
        self.capture_dir.mkdir(parents=True, exist_ok=True)
        self.seq_counter = itertools.count(1)  # Thread/Async-safe monotonic counter

        self.ctx = azmq.Context.instance()
        self.subs = {}
        self.current_date = None
        self.file = None

        channels = [
            Channel.BINANCE_BBO,
            Channel.FV_STREAM,
            Channel.PM_BOOK,
        ]

        for channel_addr in channels:
            sock = self.ctx.socket(zmq.SUB)
            resolved = _resolve_addr(channel_addr)
            try:
                sock.connect(resolved)
                sock.setsockopt(zmq.SUBSCRIBE, b"")
                self.subs[channel_addr] = sock
                log.info(f"Subscribed to {channel_addr} (resolved to {resolved})")
            except Exception as e:
                log.error(f"Failed to connect to {channel_addr}: {e}")

    def _get_file(self):
        """Rotate files at midnight UTC based on the local system clock."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self.current_date != today or self.file is None:
            if self.file:
                try:
                    self.file.close()
                except Exception:
                    pass
            self.capture_dir.mkdir(parents=True, exist_ok=True)
            path = self.capture_dir / f"{today}.jsonl"
            try:
                self.file = path.open("a", encoding="utf-8")
                self.current_date = today
                log.info(f"Writing captures to {path}")
            except Exception as e:
                # Handle WSL/Windows 'delete pending' zombie locks (Notepad++)
                if isinstance(e, (FileNotFoundError, PermissionError)):
                    fallback_path = self.capture_dir / f"{today}_{int(time.time())}.jsonl"
                    try:
                        self.file = fallback_path.open("a", encoding="utf-8")
                        self.current_date = today
                        log.info(f"Writing captures to fallback {fallback_path}")
                        return self.file
                    except Exception:
                        pass
                self.file = None
                raise e
        return self.file

    async def _drain(self, channel_addr: str, sock: zmq.Socket):
        last_error_time = 0
        while True:
            try:
                payload = await sock.recv()
                
                # Unpack the message to find the true payload timestamp
                # All schemas (BINANCE_BBO, FV_STREAM, PM_BOOK) start with ts_ms
                parsed = unpack(payload)
                if parsed and isinstance(parsed, (list, tuple)) and len(parsed) > 0:
                    ts_ms = int(parsed[0])
                else:
                    ts_ms = int(time.time() * 1000)
                
                line = {
                    "seq": next(self.seq_counter),
                    "channel": channel_addr,
                    "ts_ms": ts_ms,
                    "data": base64.b64encode(payload).decode("ascii")
                }
                
                f = self._get_file()
                f.write(json.dumps(line) + "\n")
                f.flush()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                now = time.time()
                if now - last_error_time > 5:
                    log.error(f"Error recording {channel_addr}: {e}")
                    last_error_time = now
                await asyncio.sleep(0.01) # Small backoff to prevent maxing CPU

    async def run(self):
        tasks = []
        for channel_addr, sock in self.subs.items():
            tasks.append(asyncio.create_task(self._drain(channel_addr, sock)))
        
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for t in tasks:
                t.cancel()
        finally:
            if self.file:
                self.file.close()

if __name__ == "__main__":
    # Prevent multiple concurrent recorders from corrupting the JSONL file
    lock_file = Path(__file__).resolve().parents[1] / "captures" / "recorder.lock"
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # On Windows and Unix, exclusive creation prevents multiple writers
        fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
    except FileExistsError:
        # Check if the process is actually still running
        try:
            with open(lock_file, "r") as f:
                old_pid = int(f.read().strip())
            # Simple check if process exists (cross-platform enough for this)
            os.kill(old_pid, 0)
            print(f"DataRecorder already running (PID {old_pid}). Exiting.")
            sys.exit(0)
        except (ValueError, OSError, ProcessLookupError):
            # Process is dead, take over the lock
            with open(lock_file, "w") as f:
                f.write(str(os.getpid()))

    recorder = DataRecorder(Path(__file__).resolve().parents[1] / "captures")
    log.info("Starting DataRecorder...")
    try:
        asyncio.run(recorder.run())
    except KeyboardInterrupt:
        log.info("Recorder stopped by user.")
    finally:
        try:
            lock_file.unlink(missing_ok=True)
        except Exception:
            pass
