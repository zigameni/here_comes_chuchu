"""
Real-time order book feed via Polymarket WebSocket.

Maintains best ask for UP and DOWN tokens.
Falls back to REST polling if the WebSocket drops.

Phase 3 additions (OrderBook):
  _mid_history     — deque of recent mid-price readings used to compute
                     short-term realized volatility (std dev of changes).
  _vol_baseline    — longer-horizon deque sampled every
                     VOL_BASELINE_INTERVAL_SECS; used to contextualise the
                     current vol reading as a percentile so the adaptive
                     ladder reacts to relative spikes, not absolute levels.
  realized_vol     — current short-term vol signal (float, 0.0 if warming up).
  hourly_vol_avg   — mean of the baseline; used by the vol-pause check.
  vol_percentile   — fraction 0.0–1.0 of baseline samples below realized_vol;
                     0.5 is returned during warmup so behaviour is neutral.

Phase 3.5 additions (OrderBook):
  best_bid_up      — highest resting buy price on UP side (exit price proxy).
  best_bid_down    — highest resting buy price on DOWN side.
"""
import asyncio
import json
import math
import statistics
import time
from collections import deque
from typing import Optional
from loguru import logger

import websockets

from config import WS_HOST, POLL_INTERVAL_SECONDS, VOL_BUFFER_SIZE, VOL_BASELINE_SIZE, VOL_BASELINE_INTERVAL_SECS


class OrderBook:
    def __init__(self):
        self._book_up: dict[float, float] = {}
        self._book_down: dict[float, float] = {}
        # Separate bid books (highest buy orders) — populated from WS/REST bids field
        self._bid_book_up: dict[float, float] = {}
        self._bid_book_down: dict[float, float] = {}
        self._lock = asyncio.Lock()

        # ── Phase 3: volatility signal ─────────────────────────────────────
        # Short-term ring buffer: last VOL_BUFFER_SIZE mid-price readings.
        # Each entry is a float (the mid price at that moment).
        self._mid_history: deque[float] = deque(maxlen=VOL_BUFFER_SIZE)

        # Longer-horizon baseline: sampled every VOL_BASELINE_INTERVAL_SECS.
        # Stores realized_vol snapshots so we can express current vol as a
        # percentile of recent history rather than a raw absolute number.
        self._vol_baseline: deque[float] = deque(maxlen=VOL_BASELINE_SIZE)
        self._last_baseline_push: float  = 0.0   # epoch seconds

    @property
    def best_ask_up(self) -> Optional[float]:
        valid = [p for p, s in self._book_up.items() if s > 0]
        return min(valid) if valid else None

    @property
    def best_ask_down(self) -> Optional[float]:
        valid = [p for p, s in self._book_down.items() if s > 0]
        return min(valid) if valid else None

    @property
    def best_bid_up(self) -> Optional[float]:
        """Highest resting buy price on the UP side — realistic exit price."""
        valid = [p for p, s in self._bid_book_up.items() if s > 0]
        return max(valid) if valid else None

    @property
    def best_bid_down(self) -> Optional[float]:
        """Highest resting buy price on the DOWN side."""
        valid = [p for p, s in self._bid_book_down.items() if s > 0]
        return max(valid) if valid else None

    @property
    def liq_up(self) -> float:
        valid = [p for p, s in self._book_up.items() if s > 0]
        return self._book_up[min(valid)] if valid else 0.0

    @property
    def liq_down(self) -> float:
        valid = [p for p, s in self._book_down.items() if s > 0]
        return self._book_down[min(valid)] if valid else 0.0

    @property
    def combined_ask(self) -> Optional[float]:
        best_up = self.best_ask_up
        best_down = self.best_ask_down
        if best_up is None or best_down is None:
            return None
        return best_up + best_down

    # ── Phase 3: volatility properties ────────────────────────────────────────

    @property
    def realized_vol(self) -> float:
        """
        Std dev of first-differences across _mid_history.

        Returns 0.0 when fewer than 3 samples are available so callers always
        get a safe float — no None checks needed.
        """
        buf = list(self._mid_history)
        if len(buf) < 3:
            return 0.0
        changes = [buf[i] - buf[i - 1] for i in range(1, len(buf))]
        try:
            return statistics.stdev(changes)
        except statistics.StatisticsError:
            return 0.0

    @property
    def hourly_vol_avg(self) -> float:
        """
        Mean of the vol baseline deque.

        Returns 0.0 during warmup (fewer than 3 baseline readings).
        MarketRunner uses this as the denominator for the spike multiplier,
        so 0.0 disables the pause check cleanly during warmup.
        """
        base = list(self._vol_baseline)
        if len(base) < 3:
            return 0.0
        return statistics.mean(base)

    @property
    def vol_percentile(self) -> float:
        """
        Current realized_vol expressed as a percentile (0.0–1.0) of the
        vol baseline.  Higher = more volatile than usual.

        Returns 0.5 (neutral) during warmup so the adaptive ladder posts
        at the default width rather than defaulting to tight or wide.
        """
        base = list(self._vol_baseline)
        if len(base) < 3:
            return 0.5   # neutral during warmup
        vol = self.realized_vol
        below = sum(1 for v in base if v < vol)
        return below / len(base)

    # ── Phase 3: internal mid-price push ──────────────────────────────────────

    def _push_mid(self) -> None:
        """
        Push current mid price into _mid_history and conditionally sample
        into the longer-term _vol_baseline.
        """
        best_up = self.best_ask_up
        best_down = self.best_ask_down
        if best_up is None or best_down is None:
            return

        mid = (best_up + best_down) / 2.0
        self._mid_history.append(mid)

        # Snapshot current realized_vol into the baseline at a controlled rate
        now = time.monotonic()
        if now - self._last_baseline_push >= VOL_BASELINE_INTERVAL_SECS:
            vol = self.realized_vol
            if vol > 0.0:   # don't pollute the baseline during cold-start
                self._vol_baseline.append(vol)
            self._last_baseline_push = now

    async def update_from_ws_message(self, msg: dict):
        async with self._lock:
            side = msg.get("_side")   # injected by MarketFeed below

            ask_book = self._book_up      if side == "UP" else self._book_down
            bid_book = self._bid_book_up  if side == "UP" else self._bid_book_down

            for item in msg.get("asks", []):
                price = float(item["price"])
                size  = float(item.get("size", 0))
                if size <= 0.0:
                    ask_book.pop(price, None)
                else:
                    ask_book[price] = size

            for item in msg.get("bids", []):
                price = float(item["price"])
                size  = float(item.get("size", 0))
                if size <= 0.0:
                    bid_book.pop(price, None)
                else:
                    bid_book[price] = size

            self._push_mid()   # Phase 3: update vol signal after every WS tick

    async def update_from_rest(self, token_id: str, book_data: dict, side: str):
        async with self._lock:
            ask_book = self._book_up      if side == "UP" else self._book_down
            bid_book = self._bid_book_up  if side == "UP" else self._bid_book_down

            # REST is a full snapshot — clear both sides first
            ask_book.clear()
            bid_book.clear()

            for item in book_data.get("asks", []):
                price = float(item["price"])
                size  = float(item.get("size", 0))
                if size > 0.0:
                    ask_book[price] = size

            for item in book_data.get("bids", []):
                price = float(item["price"])
                size  = float(item.get("size", 0))
                if size > 0.0:
                    bid_book[price] = size

            self._push_mid()   # Phase 3: update vol signal after every REST poll


class MarketFeed:
    """Manages the WS connection and REST fallback for one market."""

    def __init__(self, token_id_up: str, token_id_down: str, exchange):
        self.token_id_up   = token_id_up
        self.token_id_down = token_id_down
        self.exchange      = exchange
        self.book          = OrderBook()
        self._running      = False
        self._ws_task:     Optional[asyncio.Task] = None
        self._poll_task:   Optional[asyncio.Task] = None

    async def start(self):
        self._running  = True
        self._ws_task  = asyncio.create_task(self._ws_loop())
        self._poll_task = asyncio.create_task(self._poll_loop())

    async def stop(self):
        self._running = False
        for t in (self._ws_task, self._poll_task):
            if t and not t.done():
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass

    async def _ws_loop(self):
        uri = WS_HOST
        while self._running:
            try:
                async with websockets.connect(uri, ping_interval=20) as ws:
                    subscribe = {
                        "auth":    {},
                        "type":    "Market",
                        "markets": [],
                        "assets":  [self.token_id_up, self.token_id_down],
                    }
                    await ws.send(json.dumps(subscribe))
                    logger.info("📡 WebSocket connected")

                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            events = json.loads(raw)
                            if not isinstance(events, list):
                                events = [events]
                            for evt in events:
                                if evt.get("type") == "PING":
                                    await ws.send(json.dumps({"type": "PONG"}))
                                    logger.debug("WS PING → PONG")
                                    continue
                                asset_id = evt.get("asset_id", "")
                                if asset_id == self.token_id_up:
                                    evt["_side"] = "UP"
                                elif asset_id == self.token_id_down:
                                    evt["_side"] = "DOWN"
                                else:
                                    continue
                                await self.book.update_from_ws_message(evt)
                        except Exception as e:
                            logger.debug(f"WS parse error: {e}")

            except Exception as e:
                if self._running:
                    logger.warning(f"WS disconnected ({e}), reconnecting in 3s…")
                    await asyncio.sleep(3)

    async def _poll_loop(self):
        """REST fallback — runs every POLL_INTERVAL_SECONDS regardless."""
        await asyncio.sleep(2)
        while self._running:
            try:
                up_book   = await self.exchange.get_order_book(self.token_id_up)
                down_book = await self.exchange.get_order_book(self.token_id_down)
                await self.book.update_from_rest(self.token_id_up,   up_book,   "UP")
                await self.book.update_from_rest(self.token_id_down, down_book, "DOWN")
            except Exception as e:
                logger.debug(f"Poll error: {e}")
            await asyncio.sleep(POLL_INTERVAL_SECONDS)