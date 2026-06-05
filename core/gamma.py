"""
Market discovery and resolution polling.

STRATEGY: Instead of paginating all 10,000+ Gamma markets, we compute the
expected btc-updown-{type}-{timestamp} slugs for the current window and the next
~12 windows, then fetch only those slugs directly from the Gamma API.
This is fast, targeted, and never hits pagination limits.

Key fixes vs original code:
  - Slug-targeted fetch (no more 10k market scan → 422 error at offset 10100)
  - clobTokenIds is a JSON-encoded string in Gamma responses — parsed with json.loads()
  - Uses GAMMA_HOST/markets (not CLOB_HOST) for all metadata
  - Added get_next_tradable_market() which main.py requires

Environment variables:
  MARKET_TYPE          5m | 15m  (default: 5m)
                       Controls slug pattern: btc-updown-5m-* or btc-updown-15m-*
  MARKET_INTERVAL_SECONDS  300 | 900  (default: 300 — set in config.py)
                       Must match MARKET_TYPE: 5m→300, 15m→900.
"""
import asyncio
import json
import math
import os
import aiohttp
from datetime import datetime, timezone
from typing import Optional
import logging
log = logging.getLogger("gamma")

from config import CLOB_HOST, GAMMA_HOST, ORACLE_WAIT_SECONDS, MARKET_WINDOW_SECONDS

# Task 2.7: market type selects slug pattern and display label.
# MARKET_INTERVAL_SECONDS (→ MARKET_WINDOW_SECONDS) must be set consistently:
#   MARKET_TYPE=5m  → MARKET_INTERVAL_SECONDS=300  (default)
#   MARKET_TYPE=15m → MARKET_INTERVAL_SECONDS=900
MARKET_TYPE = os.getenv("MARKET_TYPE", "5m").lower()

_SLUG_PREFIX: dict[str, str] = {
    "5m":  "btc-updown-5m",
    "15m": "btc-updown-15m",
}
_MARKET_LABEL: dict[str, str] = {
    "5m":  "5-MIN",
    "15m": "15-MIN",
}

# Validate at import time so misconfiguration is caught immediately.
if MARKET_TYPE not in _SLUG_PREFIX:
    raise ValueError(
        f"Unsupported MARKET_TYPE={MARKET_TYPE!r}. Supported values: {list(_SLUG_PREFIX)}"
    )


class MarketDiscovery:
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self.all_markets: list[dict] = []

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, *_):
        if self._session:
            await self._session.close()

    # ── Targeted slug-based fetch ──────────────────────────────────────────────

    def _candidate_slugs(self, lookahead_windows: int = 14) -> list[str]:
        """
        Build the list of btc-updown-{type}-{ts} slugs we should look for.
        Covers: 1 window before now through lookahead_windows ahead.
        Uses MARKET_TYPE to select the slug prefix (5m or 15m).
        """
        prefix = _SLUG_PREFIX[MARKET_TYPE]
        now_ts = int(datetime.now(timezone.utc).timestamp())
        base   = math.floor(now_ts / MARKET_WINDOW_SECONDS) * MARKET_WINDOW_SECONDS
        slugs  = []
        for i in range(-1, lookahead_windows + 1):
            ts = base + i * MARKET_WINDOW_SECONDS
            slugs.append(f"{prefix}-{ts}")
        return slugs

    async def _fetch_gamma_markets_by_slugs(self, slugs: list[str]) -> list[dict]:
        """
        Fetch specific markets from the Gamma API using the slug= array filter.
        Sends one request with all slugs as repeated query params:
            GET /markets?slug=btc-updown-5m-123&slug=btc-updown-5m-456&...
        Returns the raw list of market dicts.
        """
        url    = f"{GAMMA_HOST}/markets"
        # aiohttp accepts list of tuples for repeated params
        params = [("slug", s) for s in slugs] + [("closed", "false")]

        try:
            async with self._session.get(url, params=params) as resp:
                if resp.status == 422:
                    log.debug("Gamma API 422 on slug fetch — no matching markets")
                    return []
                if resp.status == 500:
                    log.warning(f"Gamma API 500 — service may be down or slug format changed")
                    return []
                resp.raise_for_status()
                data = await resp.json()
        except aiohttp.ClientResponseError as e:
            log.debug(f"Gamma HTTP error: {e.status} {e.message}")
            return []
        except Exception as e:
            log.debug(f"Gamma slug fetch failed: {e}")
            return []

        if isinstance(data, list):
            return data
        return data.get("data", data.get("markets", []))

    # ── Token ID parsing ───────────────────────────────────────────────────────

    @staticmethod
    def _parse_token_ids(market: dict) -> list[str]:
        """
        clobTokenIds in Gamma API responses is a JSON-encoded string:
            '["0xtoken1", "0xtoken2"]'
        We must parse it with json.loads(), NOT iterate it as a list.
        """
        raw = market.get("clobTokenIds")
        if isinstance(raw, list):
            return [str(t) for t in raw if t]
        if isinstance(raw, str) and raw.strip():
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    return [str(t) for t in parsed if t]
            except json.JSONDecodeError:
                log.debug(f"Could not parse clobTokenIds: {raw!r}")
        return []

    # ── Resolution polling ─────────────────────────────────────────────────────

    async def _gamma_get(self, path: str, params: dict = None) -> dict:
        url = GAMMA_HOST + path
        async with self._session.get(url, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def wait_for_resolution(
        self, market_id: str, timeout: int = ORACLE_WAIT_SECONDS
    ) -> Optional[dict]:
        log.info(f"⏳ Polling resolution for {market_id} (timeout={timeout}s)…")
        deadline = asyncio.get_event_loop().time() + timeout
        while asyncio.get_event_loop().time() < deadline:
            try:
                m = await self._gamma_get(f"/markets/{market_id}")
                if m.get("resolved") or m.get("winner") or m.get("closed"):
                    log.info(f"✅ Resolved: {m.get('outcome') or m.get('winner')}")
                    return m
            except Exception as e:
                log.debug(f"Resolution poll error: {e}")
            await asyncio.sleep(10)
        log.warning("Resolution poll timed out — attempting redeem anyway")
        return None

    # ── Core market loading ────────────────────────────────────────────────────

    async def load_btc_markets(self) -> list[dict]:
        """
        Fetch btc-updown-{type}-* markets by computing expected slugs and querying
        the Gamma API directly for just those slugs.  No full-table scan needed.
        MARKET_TYPE controls whether 5m or 15m slugs are requested.
        """
        now_ts = int(datetime.now(timezone.utc).timestamp())
        slugs  = self._candidate_slugs(lookahead_windows=14)
        label  = _MARKET_LABEL.get(MARKET_TYPE, MARKET_TYPE.upper())

        log.info(f"Fetching {len(slugs)} candidate BTC {label} slugs from Gamma…")
        raw = await self._fetch_gamma_markets_by_slugs(slugs)
        log.info(f"Gamma returned {len(raw)} markets for our slug list")

        # Expected slug prefix for this market type — used to filter out
        # any cross-contamination from the Gamma API response.
        expected_prefix = _SLUG_PREFIX[MARKET_TYPE]

        parsed: list[dict] = []
        for m in raw:
            slug = (m.get("slug") or "").lower().strip()

            # Sanity-check: slug must match our configured market type prefix.
            # This replaces the hardcoded "5m" check so 15m slugs aren't dropped.
            if not slug.startswith(expected_prefix):
                continue

            parts = slug.split("-")
            try:
                market_ts = int(parts[-1])
            except (ValueError, IndexError):
                continue

            end_ts    = market_ts + MARKET_WINDOW_SECONDS
            time_diff = market_ts - now_ts

            if end_ts <= now_ts:          # already ended
                continue

            token_ids = self._parse_token_ids(m)
            if len(token_ids) < 2:
                log.warning(
                    f"Skipping {slug}: only {len(token_ids)} token IDs parsed "
                    f"(raw clobTokenIds={m.get('clobTokenIds')!r})"
                )
                continue

            condition_id = (
                m.get("conditionId") or m.get("condition_id") or ""
            )

            parsed.append({
                "id":            m.get("id") or condition_id or slug,
                "condition_id":  condition_id,
                "slug":          slug,
                "question":      m.get("question", slug),
                "token_up":      token_ids[0],
                "token_down":    token_ids[1],
                "start_time":    datetime.fromtimestamp(market_ts, tz=timezone.utc),
                "end_time":      datetime.fromtimestamp(end_ts,    tz=timezone.utc),
                "market_ts":     market_ts,
                "end_ts":        end_ts,
                "time_diff_min": time_diff / 60,
                "is_active":     time_diff <= 0 and end_ts > now_ts,
                "is_future":     time_diff > 0,
            })

        parsed.sort(key=lambda x: x["market_ts"])

        # Print summary table
        now_dt = datetime.fromtimestamp(now_ts, tz=timezone.utc)
        print(f"\n{'='*70}", flush=True)
        print(
            f"  LOADED {len(parsed)} BTC {label} MARKETS  "
            f"(now={now_dt.strftime('%H:%M:%S')} UTC)",
            flush=True,
        )
        print(f"  {'#':<4} {'SLUG':<35} {'STATUS':<8} {'START':<10} {'END'}", flush=True)
        print(f"  {'-'*65}", flush=True)
        for i, mkt in enumerate(parsed):
            status = "◀ NOW " if mkt["is_active"] else "FUTURE"
            print(
                f"  [{i:<2}] {mkt['slug']:<35} {status:<8} "
                f"{mkt['start_time'].strftime('%H:%M:%S')}   "
                f"{mkt['end_time'].strftime('%H:%M:%S')}",
                flush=True,
            )
        print(f"{'='*70}\n", flush=True)

        if not parsed:
            log.debug(
                f"⚠️  NO BTC {label} MARKETS FOUND this cycle — will retry on next refresh"
            )

        self.all_markets = parsed
        return parsed

    # ── Market selection helpers ───────────────────────────────────────────────

    def get_next_tradable_market(self, lookahead_minutes: float = 2.0) -> Optional[dict]:
        """
        Return the best market to trade right now from self.all_markets.

        Priority:
          1. Currently active (window open and not yet ended)
          2. A future market starting within lookahead_minutes (pre-warm)

        IMPORTANT: we never trust the stale is_active / is_future flags that
        were set at load time. Everything is recalculated live from market_ts
        and end_ts vs the current clock so a market from hours ago cannot
        accidentally appear as the top candidate.

        NOTE: WINDOW_ENTRY_TIMEOUT is deliberately NOT applied here.
        That config value is a MarketRunner gate (how late _phase_init will
        accept a market after open) — not a discovery-layer gate.  Applying
        it here would cause pm_daemon to tear down a live feed only 10 s
        after the window opens, every single refresh cycle.
        """
        now_ts    = int(datetime.now(timezone.utc).timestamp())
        lookahead = int(lookahead_minutes * 60)

        for m in self.all_markets:
            market_ts = m["market_ts"]
            end_ts    = m["end_ts"]

            # Skip anything that has already ended
            if end_ts <= now_ts:
                continue

            if market_ts <= now_ts:
                # Window is open — return it as long as it hasn't ended.
                # Do NOT gate on WINDOW_ENTRY_TIMEOUT here (see docstring).
                return m
            else:
                # Window is in the future — return it if it starts soon enough
                starts_in = market_ts - now_ts
                if starts_in <= lookahead:
                    return m
                # Nothing imminent; stop scanning (list is sorted by start time)
                break

        return None

    def get_market_after(self, current_market_ts: int) -> Optional[dict]:
        """Return the next market that starts after current_market_ts."""
        now_ts = int(datetime.now(timezone.utc).timestamp())
        for m in self.all_markets:
            if m["market_ts"] > current_market_ts and m["end_ts"] > now_ts:
                return m
        return None

    # ── Clock helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def seconds_to_next_boundary() -> float:
        now_ts = datetime.now(timezone.utc).timestamp()
        next_b = (
            math.floor(now_ts / MARKET_WINDOW_SECONDS) + 1
        ) * MARKET_WINDOW_SECONDS
        return next_b - now_ts

    @staticmethod
    def current_boundary() -> int:
        now_ts = int(datetime.now(timezone.utc).timestamp())
        return now_ts - (now_ts % MARKET_WINDOW_SECONDS)

    async def safe_enter_market(self, market: dict) -> bool:
        """
        Validate that a market is safe and ready to enter before launching a runner.

        Checks:
          - Both token IDs are present and non-empty
          - condition_id is present
          - The market window hasn't already ended
          - At least MIN_REMAINING_SECONDS are left in the window

        NOTE: We deliberately do NOT check WINDOW_ENTRY_TIMEOUT here.
        That config value controls how late MarketRunner._phase_init() will
        accept a market after it opens — it is not a discovery-layer gate.
        Applying it here would reject every market more than 10 s old.
        """
        import time
        MIN_REMAINING_SECONDS = 30   # must have at least 30s left to be worth launching

        slug         = market.get("slug", "?")
        token_up     = market.get("token_up", "")
        token_down   = market.get("token_down", "")
        condition_id = market.get("condition_id", "")
        end_ts       = market.get("end_ts", 0)
        now          = time.time()

        if not token_up or not token_down:
            log.warning(f"safe_enter_market: {slug} — missing token IDs, skipping")
            return False

        if not condition_id:
            log.warning(f"safe_enter_market: {slug} — missing condition_id, skipping")
            return False

        if end_ts and now >= end_ts:
            log.warning(f"safe_enter_market: {slug} — market already ended, skipping")
            return False

        remaining = end_ts - now if end_ts else float("inf")
        if remaining < MIN_REMAINING_SECONDS:
            log.warning(
                f"safe_enter_market: {slug} — only {remaining:.0f}s remaining "
                f"(min={MIN_REMAINING_SECONDS}s), skipping"
            )
            return False

        log.debug(f"safe_enter_market: {slug} — passed all checks ✓ ({remaining:.0f}s remaining)")
        return True