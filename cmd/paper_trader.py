"""
cmd/paper_trader.py
───────────────────
Phase 3 — Paper Taker (Sandbox Validator).

Subscribes to:
    Channel.FV_STREAM  — [ts_ms, market_id, prob_up, prob_down, sigma, btc_price]
    Channel.PM_BOOK    — [ts_ms, market_id, best_ask_up, best_ask_down]

On every PM_BOOK tick it checks whether an edge exists between the FV engine's
fair-value probability and the Polymarket best ask.  When the edge exceeds
MIN_EDGE_THRESHOLD it logs a hypothetical fill and tracks cumulative P&L.

Edge definition
---------------
    edge_up   = fv_up   − pm_ask_up    (positive → PM is cheap vs FV)
    edge_down = fv_down − pm_ask_down  (positive → PM is cheap vs FV)

A fill is simulated when:
    edge_up   > MIN_EDGE_THRESHOLD   →  buy UP   @ pm_ask_up
    edge_down > MIN_EDGE_THRESHOLD   →  buy DOWN  @ pm_ask_down
    (checked independently; both sides can trigger in the same tick)

P&L accounting
--------------
Each simulated fill costs `pm_ask × PAPER_TRADE_SHARES` USDC.
At market resolution (detected when the market_id changes or the window ends)
the paper trader settles open positions at 1.0 (win) or 0.0 (loss) by
checking the current FV at expiry:
    FV_UP  > 0.5 at window end → UP wins → UP positions settle at $1
    FV_UP  < 0.5 at window end → DOWN wins → DOWN positions settle at $1

All fills are appended to fills.jsonl for later analysis.

Stale-data guard
----------------
FV and PM book messages carry a timestamp_ms.  If the most recent FV tick is
more than FV_STALE_MS old when a PM tick arrives, the tick is skipped and
logged as STALE — no fill can be triggered on stale data.

Run
---
    python -m cmd.paper_trader           # from repo root
    python cmd/paper_trader.py           # direct

Requires
--------
    binance_daemon.py  → fv_engine.py   publishing on Channel.FV_STREAM
    pm_daemon.py                        publishing on Channel.PM_BOOK

Validation gate (Phase 3 → 4)
------------------------------
Run for ≥ 24 hours.  Proceed to Phase 4 only if:
    * Hypothetical fills are consistently EV-positive (paper P&L > 0)
    * FV leads PM price moves (check by reviewing fills.jsonl timestamps)
    * Edge > MIN_EDGE_THRESHOLD fires on a reasonable subset of windows
      (too-frequent triggers → lower threshold; too-rare → raise it)
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
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

import zmq
import zmq.asyncio as azmq

from shared.ipc import Channel, unpack

log = setup_logging("paper_trader")

# ── Config ─────────────────────────────────────────────────────────────────────

# Minimum edge (FV − PM ask) required to simulate a fill.
# 3% = 0.03.  Do not lower below 2% without extended paper validation.
# TEMPORARILY LOWERED FOR TESTING - restore to 0.03 after validation
MIN_EDGE_THRESHOLD: float = float(os.getenv("MIN_EDGE_THRESHOLD", "0.01"))

# Simulated shares per hypothetical fill.
# Matches the planned live order size from Phase 4.
PAPER_TRADE_SHARES: float = float(os.getenv("PAPER_TRADE_SHARES", "5.0"))

# How stale FV data is allowed to be (ms) before a PM tick is ignored.
FV_STALE_MS: float = float(os.getenv("FV_STALE_MS", "10000"))  # 10s — FV is BTC-derived, updates every ~100ms

# How long to wait (ms) after a fill before allowing another fill on the same
# side of the same market.  Prevents hammering on a persistent edge.
FILL_COOLDOWN_MS: float = float(os.getenv("FILL_COOLDOWN_MS", "5000"))

# Where to write fill records.
FILLS_PATH: Path = Path(os.getenv("FILLS_PATH", "fills.jsonl"))

# Status print interval (seconds)
STATUS_INTERVAL_S: float = float(os.getenv("STATUS_INTERVAL_S", "10.0"))


# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class FVState:
    """Most-recent snapshot from fv_engine."""
    ts_ms:      int     = 0
    market_id:  str     = ""
    prob_up:    float   = 0.5
    prob_down:  float   = 0.5
    sigma:      float   = 0.0
    btc_price:  float   = 0.0

@dataclass
class PMState:
    """Most-recent snapshot from pm_daemon."""
    ts_ms:      int             = 0
    market_id:  str             = ""
    ask_up:     Optional[float] = None
    ask_down:   Optional[float] = None

@dataclass
class Fill:
    """A single simulated fill record."""
    ts_ms:      int
    market_id:  str
    side:       str    # "UP" or "DOWN"
    ask:        float  # price paid
    shares:     float
    cost:       float  # ask × shares
    fv:         float  # fair value at trigger
    edge:       float  # fv − ask
    btc_price:  float
    sigma:      float

@dataclass
class Position:
    """Open paper position for one market + side."""
    market_id:  str
    side:       str
    shares:     float = 0.0
    cost:       float = 0.0

    @property
    def avg_price(self) -> float:
        return self.cost / self.shares if self.shares > 0 else 0.0

@dataclass
class PaperPnL:
    """Running P&L totals."""
    fills:          int   = 0
    cost_usdc:      float = 0.0
    realized_usdc:  float = 0.0
    settled_markets: int  = 0

    @property
    def net_usdc(self) -> float:
        return self.realized_usdc - self.cost_usdc

    @property
    def roi_pct(self) -> float:
        return (self.net_usdc / self.cost_usdc * 100) if self.cost_usdc > 0 else 0.0


# ── Paper Trader ───────────────────────────────────────────────────────────────

class PaperTrader:
    """
    Stateful paper-trading engine.

    Receives FV and PM book updates and simulates fills when edge is present.
    Tracks open positions per market and settles them at window close.
    """

    def __init__(self) -> None:
        # Use zmq.asyncio so sockets are polled natively in the event loop —
        # no executor threads needed.  ZMQ sockets are not thread-safe so the
        # old run_in_executor approach caused the FV socket to silently stop
        # delivering after the first message.
        _ctx = azmq.Context.instance()

        from shared.ipc import _resolve_addr, _WINDOWS_IPC_MAP
        fv_addr  = _resolve_addr(Channel.FV_STREAM)
        pm_addr  = _resolve_addr(Channel.PM_BOOK)

        self._fv_sub = _ctx.socket(zmq.SUB)
        self._fv_sub.set_hwm(1000)
        self._fv_sub.connect(fv_addr)
        self._fv_sub.setsockopt(zmq.SUBSCRIBE, b"")

        self._pm_sub = _ctx.socket(zmq.SUB)
        self._pm_sub.set_hwm(1000)
        self._pm_sub.connect(pm_addr)
        self._pm_sub.setsockopt(zmq.SUBSCRIBE, b"")

        self._fv  = FVState()
        self._pm  = PMState()
        self._pnl = PaperPnL()

        # market_id → {side → Position}
        self._positions: dict[str, dict[str, Position]] = defaultdict(dict)

        # market_id → {side → last fill ts_ms}  (cooldown tracking)
        self._last_fill_ts: dict[str, dict[str, float]] = defaultdict(dict)

        self._fills_file = FILLS_PATH.open("a")
        self._last_pm_market: str  = ""

        log.info(
            "PaperTrader ready — edge=%.2f%%  shares=%.0f  stale=%.0fms  cooldown=%.0fms",
            MIN_EDGE_THRESHOLD * 100, PAPER_TRADE_SHARES, FV_STALE_MS, FILL_COOLDOWN_MS,
        )

    # ── Main loop ──────────────────────────────────────────────────────────────

    async def run(self, stop_event: asyncio.Event) -> None:

        async def drain_fv():
            while not stop_event.is_set():
                try:
                    raw = await self._fv_sub.recv()
                    self._on_fv(raw)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    log.debug("FV recv error: %s", e)

        async def drain_pm():
            while not stop_event.is_set():
                try:
                    raw = await self._pm_sub.recv()
                    self._on_pm(raw)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    log.debug("PM recv error: %s", e)

        async def status_ticker():
            while not stop_event.is_set():
                await asyncio.sleep(STATUS_INTERVAL_S)
                if not stop_event.is_set():
                    self._print_status()

        tasks = [
            asyncio.create_task(drain_fv()),
            asyncio.create_task(drain_pm()),
            asyncio.create_task(status_ticker()),
        ]

        # Wait until stop is requested, then cancel all socket tasks
        await stop_event.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        self._fills_file.close()
        log.info("PaperTrader stopped. Final P&L: net=%.4f USDC  fills=%d",
                 self._pnl.net_usdc, self._pnl.fills)
        self._print_status(final=True)

    # ── Message handlers ───────────────────────────────────────────────────────

    def _on_fv(self, raw: bytes) -> None:
        """Update FV state from fv_engine message."""
        try:
            ts_ms, market_id, prob_up, prob_down, sigma, btc_price = unpack(raw)
            self._fv = FVState(
                ts_ms=ts_ms,
                market_id=market_id,
                prob_up=prob_up,
                prob_down=prob_down,
                sigma=sigma,
                btc_price=btc_price,
            )
        except Exception as e:
            log.debug("FV parse error: %s", e)

    def _on_pm(self, raw: bytes) -> None:
        """Update PM book state and check for fills."""
        try:
            ts_ms, market_id, ask_up, ask_down = unpack(raw)
        except Exception as e:
            log.debug("PM parse error: %s", e)
            return

        # Detect market transition → settle old positions
        if market_id != self._last_pm_market and self._last_pm_market:
            self._settle_market(self._last_pm_market)
        self._last_pm_market = market_id

        self._pm = PMState(
            ts_ms=ts_ms,
            market_id=market_id,
            ask_up=ask_up,
            ask_down=ask_down,
        )

        # Stale FV guard
        fv_age_ms = ts_ms - self._fv.ts_ms
        if fv_age_ms > FV_STALE_MS:
            log.debug("Skipping PM tick — FV stale by %.0fms (FV age: %.1fs)", fv_age_ms, fv_age_ms/1000)
            return

        # NOTE: fv_engine publishes a static market_id ("phase2-dynamic-K") because
        # FV is a pure BTC Black-Scholes signal — it has no knowledge of which
        # Polymarket condition_id is currently live.  The PM book IS market-specific.
        # We use the PM market_id as the source of truth; no equality check needed.

        self._check_edge(market_id, ask_up, ask_down, ts_ms)

    # ── Edge detection ─────────────────────────────────────────────────────────

    def _check_edge(
        self,
        market_id: str,
        ask_up:    Optional[float],
        ask_down:  Optional[float],
        ts_ms:     int,
    ) -> None:
        """Check both sides for edge and simulate fills if threshold is met."""
        sides = [
            ("UP",   ask_up,   self._fv.prob_up),
            ("DOWN", ask_down, self._fv.prob_down),
        ]
        for side, ask, fv in sides:
            if ask is None:
                continue
            edge = fv - ask
            log.debug("Edge check: %s side=%s fv=%.4f ask=%.4f edge=%.4f (threshold=%.4f)",
                     market_id[:8], side, fv, ask, edge, MIN_EDGE_THRESHOLD)
            if edge <= MIN_EDGE_THRESHOLD:
                continue

            # Cooldown check
            last_fill = self._last_fill_ts.get(market_id, {}).get(side, 0.0)
            if ts_ms - last_fill < FILL_COOLDOWN_MS:
                log.debug(
                    "Cooldown active for %s %s (%.0fms remaining)",
                    market_id[:8], side, FILL_COOLDOWN_MS - (ts_ms - last_fill),
                )
                continue

            self._simulate_fill(market_id, side, ask, fv, edge, ts_ms)

    def _simulate_fill(
        self,
        market_id: str,
        side:      str,
        ask:       float,
        fv:        float,
        edge:      float,
        ts_ms:     int,
    ) -> None:
        """Record a hypothetical fill and update positions."""
        cost = ask * PAPER_TRADE_SHARES

        fill = Fill(
            ts_ms=ts_ms,
            market_id=market_id,
            side=side,
            ask=ask,
            shares=PAPER_TRADE_SHARES,
            cost=cost,
            fv=fv,
            edge=edge,
            btc_price=self._fv.btc_price,
            sigma=self._fv.sigma,
        )

        # Update position
        pos = self._positions[market_id].get(side)
        if pos is None:
            pos = Position(market_id=market_id, side=side)
            self._positions[market_id][side] = pos
        pos.shares += PAPER_TRADE_SHARES
        pos.cost   += cost

        # Update P&L and cooldown
        self._pnl.fills      += 1
        self._pnl.cost_usdc  += cost
        self._last_fill_ts[market_id][side] = float(ts_ms)

        # Persist fill
        self._fills_file.write(json.dumps(asdict(fill)) + "\n")
        self._fills_file.flush()

        # Terminal output
        _c_edge  = "\033[32m" if edge > 0.06 else "\033[33m"
        _c_reset = "\033[0m"
        print(
            f"  FILL  {side:<4}  ask={ask:.4f}  fv={fv:.4f}  "
            f"edge={_c_edge}{edge:+.4f}{_c_reset}  "
            f"BTC={self._fv.btc_price:.2f}  σ={self._fv.sigma:.3f}  "
            f"cost={cost:.2f} USDC  mkt={market_id[:8]}…",
            flush=True,
        )

    # ── Settlement ─────────────────────────────────────────────────────────────

    def _settle_market(self, market_id: str) -> None:
        """
        Settle all open positions for a closed market.

        Settlement price:
            FV at the last tick when market_id was current.
            prob_up > 0.5 → UP wins (UP pays $1, DOWN pays $0)
            prob_up < 0.5 → DOWN wins
            prob_up = 0.5 → call it a draw (settle at $0.50 for both sides)
        """
        positions = self._positions.get(market_id, {})
        if not positions:
            return

        prob_up = self._fv.prob_up
        if prob_up > 0.5:
            win_side, lose_side = "UP", "DOWN"
        elif prob_up < 0.5:
            win_side, lose_side = "DOWN", "UP"
        else:
            win_side = lose_side = "DRAW"

        settled_pnl = 0.0

        for side, pos in positions.items():
            if pos.shares <= 0:
                continue
            if win_side == "DRAW":
                proceeds = pos.shares * 0.5
            elif side == win_side:
                proceeds = pos.shares * 1.0
            else:
                proceeds = 0.0

            pnl = proceeds - pos.cost
            settled_pnl += pnl
            self._pnl.realized_usdc += proceeds

            result_str = "\033[32mWIN \033[0m" if pnl > 0 else "\033[31mLOSS\033[0m"
            log.info(
                "SETTLE  %s  %s  shares=%.0f  avg_cost=%.4f  "
                "proceeds=%.4f  pnl=%+.4f USDC",
                result_str, side, pos.shares, pos.avg_price, proceeds, pnl,
            )

        self._pnl.settled_markets += 1
        del self._positions[market_id]

        log.info(
            "Market %s…  settled  winner=%s  window_pnl=%+.4f USDC  "
            "cumulative net=%+.4f USDC",
            market_id[:8], win_side, settled_pnl, self._pnl.net_usdc,
        )

    # ── Status output ──────────────────────────────────────────────────────────

    def _print_status(self, final: bool = False) -> None:
        label = "FINAL" if final else "STATUS"

        fv_age_ms = int(time.time() * 1000) - self._fv.ts_ms if self._fv.ts_ms else -1
        pm_age_ms = int(time.time() * 1000) - self._pm.ts_ms if self._pm.ts_ms else -1

        # Count open positions
        open_positions = sum(
            len(sides) for sides in self._positions.values()
        )

        print(
            f"\n  [{label}]  "
            f"fills={self._pnl.fills}  "
            f"cost={self._pnl.cost_usdc:.2f}  "
            f"realized={self._pnl.realized_usdc:.2f}  "
            f"net={self._pnl.net_usdc:+.4f} USDC  "
            f"ROI={self._pnl.roi_pct:+.1f}%  "
            f"open_pos={open_positions}  "
            f"settled_mkts={self._pnl.settled_markets}",
            flush=True,
        )
        print(
            f"         FV: mkt={self._fv.market_id[:8]}…  "
            f"p_up={self._fv.prob_up:.4f}  "
            f"BTC={self._fv.btc_price:.2f}  "
            f"age={fv_age_ms}ms",
            flush=True,
        )
        print(
            f"         PM: mkt={self._pm.market_id[:8]}…  "
            f"ask_up={self._pm.ask_up}  "
            f"ask_dn={self._pm.ask_down}  "
            f"age={pm_age_ms}ms\n",
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

    trader = PaperTrader()
    log.info(
        "Subscribing to %s and %s",
        Channel.FV_STREAM, Channel.PM_BOOK,
    )

    print(f"\n  Min edge: {MIN_EDGE_THRESHOLD:.1%}   "
          f"Shares/fill: {PAPER_TRADE_SHARES:.0f}   "
          f"FV stale timeout: {FV_STALE_MS:.0f}ms")
    print(f"  Fills logged to: {FILLS_PATH.resolve()}")
    print("─" * 75)

    try:
        loop.run_until_complete(trader.run(stop_event))
    finally:
        loop.close()
        log.info("ZMQ sockets closed.")


if __name__ == "__main__":
    main()