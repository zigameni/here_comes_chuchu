"""
cmd/binance_daemon.py
─────────────────────
Phase 1 — Binance Spot BBO daemon.

Connects to the Binance SPOT WebSocket stream `btcusdt@bookTicker`,
extracts the best bid/ask on every tick, packs the data with msgpack, and
publishes it over a ZMQ PUB socket.

⚠  SPOT not FUTURES — important for Polymarket alignment
─────────────────────────────────────────────────────────
Polymarket's BTC UP/DOWN 5-min markets resolve via the Chainlink BTC/USD
data stream, which aggregates SPOT prices from Coinbase, Bitstamp, Kraken
etc.  Binance Futures (fstream.binance.com) trades at a basis premium over
spot — typically $50–$200 on a $100k BTC — causing the FV engine to
compute probabilities against a price ~$100 higher than Polymarket's oracle.

Fix: use wss://stream.binance.com:9443 (spot) not fstream.binance.com (futures).
Binance spot closely tracks the Chainlink aggregate; any residual USDT/USD
peg difference is < $5 and irrelevant for a 0.02 edge threshold.

Optional Chainlink cross-check
───────────────────────────────
Set CHAINLINK_CHECK=1 in your .env to periodically fetch the live Chainlink
BTC/USD price from the Polygon mainnet contract and log the delta between
the Binance spot feed and the oracle.  This lets you see exactly how much
basis error remains without eyeballing charts.

Consumers subscribe to `Channel.BINANCE_BBO` and receive:
    [timestamp_ms: int, best_bid: float, best_ask: float]

Run
---
    python -m cmd.binance_daemon          # from repo root
    python cmd/binance_daemon.py          # direct
"""

from __future__ import annotations

import asyncio
import json
import logging
from shared.log_setup import setup_logging
import os
import signal
import sys
import time
from pathlib import Path

import websockets
import websockets.exceptions

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from shared.ipc import Channel, get_publisher, pack

log = setup_logging("binance_daemon")

# ── URLs ───────────────────────────────────────────────────────────────────────
# SPOT — matches Chainlink oracle source (aggregates Coinbase, Kraken etc.)
BINANCE_WS_URL  = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"

# Reference: what we used before (wrong — futures premium ≈ $100)
# BINANCE_WS_URL = "wss://fstream.binance.com/ws/btcusdt@bookTicker"

# Chainlink BTC/USD on Polygon mainnet — this is what Polymarket resolves against
CHAINLINK_POLYGON_ADDR = "0xc907E116054Ad103354f2D350FD2514433D57F6F"
CHAINLINK_ABI = [{"inputs":[],"name":"latestRoundData","outputs":[
    {"name":"roundId","type":"uint80"},{"name":"answer","type":"int256"},
    {"name":"startedAt","type":"uint256"},{"name":"updatedAt","type":"uint256"},
    {"name":"answeredInRound","type":"uint80"}],"stateMutability":"view","type":"function"}]

# ── Config ─────────────────────────────────────────────────────────────────────
BACKOFF_INIT_S   = 0.5
BACKOFF_MAX_S    = 64.0
STALE_TIMEOUT_S  = 5.0

# Enable Chainlink cross-check: logs delta between Binance spot and oracle
CHAINLINK_CHECK         = os.getenv("CHAINLINK_CHECK", "0") == "1"
CHAINLINK_CHECK_INTERVAL = 30.0   # seconds between oracle polls

# ── Publisher socket ───────────────────────────────────────────────────────────
_pub = get_publisher(Channel.BINANCE_BBO, hwm=200)
log.info("ZMQ PUB bound → %s", Channel.BINANCE_BBO)
log.info("Price source:  Binance SPOT (stream.binance.com)  ← matches Chainlink oracle")

# ── Chainlink cross-check (optional) ──────────────────────────────────────────

async def _chainlink_monitor(latest_mid_ref: list[float], stop_event: asyncio.Event) -> None:
    """
    Periodically fetch the live Chainlink BTC/USD price from Polygon and log
    the delta vs the Binance spot feed.  Runs as a background task.

    Requires web3 and a working Polygon RPC (reuses exchange.py's RPC endpoint).
    Disabled unless CHAINLINK_CHECK=1.
    """
    try:
        from web3 import Web3
    except ImportError:
        log.warning("web3 not installed — Chainlink cross-check disabled")
        return

    polygon_rpc = os.getenv("POLYGON_RPC", "https://polygon-bor-rpc.publicnode.com")
    w3 = Web3(Web3.HTTPProvider(polygon_rpc))
    contract = w3.eth.contract(
        address=w3.to_checksum_address(CHAINLINK_POLYGON_ADDR),
        abi=CHAINLINK_ABI,
    )
    log.info("Chainlink cross-check enabled — polling every %gs", CHAINLINK_CHECK_INTERVAL)

    while not stop_event.is_set():
        await asyncio.sleep(CHAINLINK_CHECK_INTERVAL)
        try:
            def _fetch():
                data = contract.functions.latestRoundData().call()
                # answer has 8 decimal places
                price = data[1] / 1e8
                updated_at = data[3]
                age_s = time.time() - updated_at
                return price, age_s

            cl_price, age_s = await asyncio.get_event_loop().run_in_executor(None, _fetch)
            binance_mid = latest_mid_ref[0]

            if binance_mid > 0:
                delta = binance_mid - cl_price
                pct   = delta / cl_price * 100
                level = logging.WARNING if abs(delta) > 50 else logging.INFO
                log.log(
                    level,
                    "Chainlink BTC/USD = $%.2f  (age=%ds)  "
                    "Binance spot = $%.2f  delta = %+.2f (%+.3f%%)",
                    cl_price, int(age_s), binance_mid, delta, pct,
                )
                if abs(delta) > 50:
                    log.warning(
                        "⚠  Delta > $50 — check if Binance spot feed is healthy"
                    )
            else:
                log.info("Chainlink BTC/USD = $%.2f  (Binance not yet connected)", cl_price)

        except Exception as exc:
            log.debug("Chainlink poll error: %s", exc)


# ── Core WebSocket loop ────────────────────────────────────────────────────────

async def _stream(stop_event: asyncio.Event, latest_mid: list[float]) -> None:
    async with websockets.connect(
        BINANCE_WS_URL,
        ping_interval=20,
        ping_timeout=10,
        close_timeout=5,
    ) as ws:
        log.info("Connected → %s", BINANCE_WS_URL)
        last_msg_time = time.monotonic()
        
        last_bid = 0.0
        last_ask = 0.0

        while not stop_event.is_set():
            now = time.monotonic()
            if now - last_msg_time > STALE_TIMEOUT_S:
                log.warning("No tick for %.1fs — forcing reconnect.", STALE_TIMEOUT_S)
                break

            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=STALE_TIMEOUT_S)
            except asyncio.TimeoutError:
                continue

            last_msg_time = time.monotonic()

            # Binance bookTicker: {"u":..,"s":"BTCUSDT","b":"104892.10","a":"104892.20",...}
            try:
                msg = json.loads(raw)
                bid = float(msg["b"])
                ask = float(msg["a"])
            except (KeyError, ValueError, json.JSONDecodeError) as exc:
                log.debug("Malformed frame: %s — %s", exc, raw[:80])
                continue
                
            # Filter out quantity-only updates to prevent duplicate downstream ticks
            if bid == last_bid and ask == last_ask:
                continue
                
            last_bid = bid
            last_ask = ask

            timestamp_ms = int(time.time() * 1000)
            latest_mid[0] = (bid + ask) / 2.0

            _pub.send(pack([timestamp_ms, bid, ask]))


async def run(stop_event: asyncio.Event) -> None:
    backoff    = BACKOFF_INIT_S
    latest_mid = [0.0]   # shared state for Chainlink cross-check

    # Start optional Chainlink monitor
    cl_task = None
    if CHAINLINK_CHECK:
        cl_task = asyncio.create_task(
            _chainlink_monitor(latest_mid, stop_event)
        )

    while not stop_event.is_set():
        try:
            await _stream(stop_event, latest_mid)
        except (
            websockets.exceptions.ConnectionClosed,
            websockets.exceptions.WebSocketException,
            OSError,
        ) as exc:
            log.warning("WebSocket error: %s — reconnecting in %.1fs", exc, backoff)
        except Exception as exc:
            log.error("Unexpected error: %s — reconnecting in %.1fs", exc, backoff)

        if stop_event.is_set():
            break

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, BACKOFF_MAX_S)
        log.info("Reconnecting… (next backoff: %.1fs)", backoff)

    if cl_task:
        cl_task.cancel()
    log.info("Daemon stopped cleanly.")


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    try:
        import uvloop
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        log.info("Event loop: uvloop")
    except ImportError:
        log.info("Event loop: asyncio (uvloop not available)")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    stop_event = asyncio.Event()

    def _handle_signal(sig_num, _frame):
        log.info("Signal %s — shutting down.", sig_num)
        loop.call_soon_threadsafe(stop_event.set)

    signal.signal(signal.SIGINT, _handle_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _handle_signal)

    try:
        loop.run_until_complete(run(stop_event))
    finally:
        loop.close()
        _pub.close()
        log.info("ZMQ socket closed.")


if __name__ == "__main__":
    main()
