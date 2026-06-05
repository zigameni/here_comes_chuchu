"""
cmd/pm_daemon.py
────────────────
Phase 3 — Polymarket Book Daemon.

Discovers the active BTC 5-minute market using gamma.py, subscribes to its
order book via feed.py (WebSocket + REST fallback), and publishes the best
bid and ask prices for UP and DOWN tokens to Channel.PM_BOOK on every update.

Published schema (msgpack)
--------------------------
    [timestamp_ms, market_id, bid_up, ask_up, bid_dn, ask_dn,
     market_ts, end_ts, liq_up, liq_dn, combined_ask]

    market_id    — Polymarket condition_id (str)
    bid_up       — None if no bid present on the UP side
    ask_up       — None if no ask present on the UP side
    bid_dn       — None if no bid present on the DOWN side
    ask_dn       — None if no ask present on the DOWN side
    market_ts    — Unix timestamp (s) when the market window opened
    end_ts       — Unix timestamp (s) when the market window closes
    liq_up       — shares available at the best UP ask
    liq_dn       — shares available at the best DOWN ask
    combined_ask — ask_up + ask_dn (None if either leg is absent)

Design
------
* One MarketFeed per active 5-min window.  When the window closes, the daemon
  discovers the next market and spins up a new feed seamlessly.
* The ZMQ publisher is created once and reused across market transitions.
* gamma.py's get_next_tradable_market() drives market selection; the daemon
  re-queries Gamma every REFRESH_INTERVAL_S seconds so it never misses a
  new window.
* Stale-detection: if no PM book tick arrives for PM_STALE_TIMEOUT_S seconds,
  a warning is logged but the daemon keeps running (REST fallback in feed.py
  is already handling it).

Run
---
    python -m cmd.pm_daemon          # from repo root
    python cmd/pm_daemon.py          # direct

Requires
--------
    Channel.BINANCE_BBO publisher (binance_daemon.py) is NOT required for this
    daemon — it is independent.  paper_trader.py joins the two streams.
"""

from __future__ import annotations

import asyncio
import logging
from shared.log_setup import setup_logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from shared.ipc import Channel, get_publisher, pack
from core.gamma import MarketDiscovery
from core.feed import MarketFeed
from core.exchange import Exchange

log = setup_logging("pm_daemon")

# ── Config ─────────────────────────────────────────────────────────────────────

# How often to re-query Gamma for the current tradable market.
REFRESH_INTERVAL_S: float = float(os.getenv("PM_REFRESH_INTERVAL_S", "15"))

# How often to push a PM_BOOK message even if the book hasn't changed.
HEARTBEAT_INTERVAL_S: float = float(os.getenv("PM_HEARTBEAT_INTERVAL_S", "1.0"))

# Warn if no book update has arrived from feed.py for this many seconds.
PM_STALE_TIMEOUT_S: float = float(os.getenv("PM_STALE_TIMEOUT_S", "10.0"))


class PMDaemon:
    """
    Manages market discovery and book publishing lifecycle.

    State machine:
        IDLE  → discovering next tradable market
        LIVE  → feeding book updates from an active MarketFeed
        (loop back to IDLE when the window closes)
    """

    def __init__(self) -> None:
        self._pub = get_publisher(Channel.PM_BOOK)
        self._exchange = Exchange()
        self._discovery = MarketDiscovery()

        self._feed: Optional[MarketFeed] = None
        self._current_market: Optional[dict] = None
        self._last_publish_t: float = 0.0
        self._last_book_update_t: float = 0.0

        # Track last published values for change detection / terminal dedup
        self._last_ask_up: Optional[float] = None
        self._last_ask_dn: Optional[float] = None
        self._last_bid_up: Optional[float] = None
        self._last_bid_dn: Optional[float] = None

        log.info(
            "PMDaemon ready — refresh=%.0fs  heartbeat=%.1fs",
            REFRESH_INTERVAL_S, HEARTBEAT_INTERVAL_S,
        )

    # ── Main loop ──────────────────────────────────────────────────────────────

    async def run(self, stop_event: asyncio.Event) -> None:
        async with self._discovery:
            market_check_t = 0.0

            while not stop_event.is_set():
                now = time.time()

                _BOUNDARY_S    = 45.0
                _MID_REFRESH_S = 60.0

                if self._current_market is not None:
                    seconds_left = self._current_market["end_ts"] - now
                    effective_interval = (
                        REFRESH_INTERVAL_S if seconds_left <= _BOUNDARY_S
                        else _MID_REFRESH_S
                    )
                else:
                    effective_interval = REFRESH_INTERVAL_S

                if now - market_check_t >= effective_interval:
                    await self._refresh_market()
                    market_check_t = now

                if self._feed is not None and self._current_market is not None:
                    await self._publish_book()

                    if (
                        self._last_book_update_t > 0
                        and now - self._last_book_update_t > PM_STALE_TIMEOUT_S
                    ):
                        log.warning(
                            "PM book stale for %.0fs — REST fallback should be active",
                            now - self._last_book_update_t,
                        )

                await asyncio.sleep(HEARTBEAT_INTERVAL_S)

        await self._teardown()
        log.info("PMDaemon stopped.")

    # ── Market lifecycle ───────────────────────────────────────────────────────

    async def _refresh_market(self) -> None:
        """Discover the current tradable market; start/stop feeds as needed."""
        markets = await self._discovery.load_btc_markets()
        candidate = self._discovery.get_next_tradable_market()

        if candidate is None:
            if self._current_market is not None:
                log.info("No tradable market found — tearing down current feed.")
                await self._teardown()
            else:
                log.debug("No tradable market available yet — waiting.")
            return

        if (
            self._current_market is not None
            and candidate["condition_id"] == self._current_market["condition_id"]
        ):
            return

        log.info(
            "Market transition: %s → %s",
            self._current_market["slug"] if self._current_market else "none",
            candidate["slug"],
        )
        await self._teardown()
        await self._start_feed(candidate)

    async def _start_feed(self, market: dict) -> None:
        """Spin up a MarketFeed for the given market."""
        if not await self._discovery.safe_enter_market(market):
            log.warning("safe_enter_market rejected %s — skipping.", market["slug"])
            return

        self._current_market = market
        self._feed = MarketFeed(
            token_id_up=market["token_up"],
            token_id_down=market["token_down"],
            exchange=self._exchange,
        )
        await self._feed.start()

        log.info(
            "Feed started — slug=%s  token_up=%.8s…  token_dn=%.8s…",
            market["slug"],
            market["token_up"],
            market["token_down"],
        )

    async def _teardown(self) -> None:
        """Stop the current feed and clear market state."""
        if self._feed is not None:
            await self._feed.stop()
            self._feed = None
        self._current_market = None
        self._last_ask_up = None
        self._last_ask_dn = None
        self._last_bid_up = None
        self._last_bid_dn = None
        self._last_book_update_t = 0.0

    # ── Publishing ─────────────────────────────────────────────────────────────

    async def _publish_book(self) -> None:
        """
        Read the current best bids and asks and publish to Channel.PM_BOOK.

        Schema: [ts_ms, market_id, bid_up, ask_up, bid_dn, ask_dn, market_ts,
                 end_ts, liq_up, liq_dn, combined_ask]

        market_ts    — Unix timestamp (s) when this 5-min window opened (= the
                       BTC strike K anchor).  Consumers use it to match this PM
                       market to the FV engine's boundary_ts.
        end_ts       — Unix timestamp (s) when this window expires.
        combined_ask — ask_up + ask_dn, pre-computed for arb scanner (Task 3.3).
                       None if either leg is absent.

        All four prices are read unconditionally before any comparison so
        there is no risk of referencing an unbound local variable.
        """
        book = self._feed.book  # type: ignore[union-attr]

        ask_up = book.best_ask_up
        ask_dn = book.best_ask_down
        bid_up = book.best_bid_up
        bid_dn = book.best_bid_down
        liq_up = book.liq_up
        liq_dn = book.liq_down

        # Task 3.3: pre-compute combined ask for arb scanner consumers.
        combined_ask = (ask_up + ask_dn) if (ask_up is not None and ask_dn is not None) else None

        self._last_book_update_t = time.time()

        if (ask_up != self._last_ask_up or ask_dn != self._last_ask_dn
                or bid_up != self._last_bid_up or bid_dn != self._last_bid_dn):
            self._last_ask_up = ask_up
            self._last_ask_dn = ask_dn
            self._last_bid_up = bid_up
            self._last_bid_dn = bid_dn

        market_id = self._current_market["condition_id"]  # type: ignore[index]
        market_ts = int(self._current_market.get("market_ts", 0))  # window open timestamp
        end_ts    = int(self._current_market.get("end_ts",    0))  # window close timestamp
        ts_ms     = int(time.time() * 1000)

        msg = pack([
            ts_ms,
            market_id,
            bid_up,
            ask_up,
            bid_dn,
            ask_dn,
            market_ts,
            end_ts,
            liq_up,
            liq_dn,
            combined_ask,
        ])
        self._pub.send(msg)

        self._maybe_print(ask_up, ask_dn, market_id)

    def _maybe_print(
        self,
        ask_up: Optional[float],
        ask_dn: Optional[float],
        market_id: str,
    ) -> None:
        """Print a status line when the book changes, suppressed when None."""
        if ask_up is None or ask_dn is None:
            return
        combined = ask_up + ask_dn
        print(
            f"  PM  UP={ask_up:.4f}  DN={ask_dn:.4f}  "
            f"Σ={combined:.4f}  mkt={market_id[:8]}…",
            flush=True,
        )


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    try:
        import uvloop, warnings
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

    daemon = PMDaemon()
    log.info("Publishing PM book → %s", Channel.PM_BOOK)
    print(f"\n  {'UP ask':>8}  {'DN ask':>8}  {'Σ ask':>7}  market_id")
    print("─" * 55)

    try:
        loop.run_until_complete(daemon.run(stop_event))
    finally:
        loop.close()
        log.info("ZMQ socket closed.")


if __name__ == "__main__":
    main()
