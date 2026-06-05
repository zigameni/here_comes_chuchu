"""
polymarket_bot/main.py

Entry point. Uses pattern-based market discovery with strict validation
to ensure we only trade the correct BTC 5-min markets.

No Nautilus cache required.
"""
import asyncio
import sys
import os
from datetime import datetime, timezone

# Fix Windows terminal encoding for emojis
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
if sys.stderr.encoding.lower() != 'utf-8':
    sys.stderr.reconfigure(encoding='utf-8')

from loguru import logger

from config import MARKET_WINDOW_SECONDS, MAX_MARKETS_TO_TRADE
from core.exchange import Exchange
from core.gamma import MarketDiscovery
from market_runner import MarketRunner


def setup_logging():
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
        level="INFO",
        colorize=True,
    )
    logger.add(
        "logs/bot_{time:YYYY-MM-DD}.log",
        rotation="50 MB",
        retention=3,
        level="DEBUG",
    )


class Bot:
    """
    Outer loop using pattern-based market discovery:
      1. Construct btc-updown-5m-* markets from known slug pattern
      2. Validate strictly to prevent wrong-market trades
      3. Fetch token IDs from CLOB only when needed for trading
      4. Switch markets every 5 minutes based on UTC boundaries
    """

    def __init__(self):
        self.exchange = Exchange()
        self.discovery = MarketDiscovery()  # No cache needed!
        self.markets: list[dict] = []
        self.current_idx: int = -1
        self.current_runner: MarketRunner | None = None
        self.runner_task: asyncio.Task | None = None
        self.background_tasks: set[asyncio.Task] = set()
        self.bot_start_time = datetime.now(timezone.utc)
        self.restart_after_minutes = 60
        self.markets_traded = 0

    async def run(self):
        async with self.discovery:
            await self._load_markets()
            await self._start_current_market()
            await self._timer_loop()

    # ── Market loading ─────────────────────────────────────────────────────────

    async def _load_markets(self):
        """Load and validate markets from pattern (no API call)."""
        self.markets = await self.discovery.load_btc_markets()
        self.current_idx = -1
        logger.info(f"✓ Loaded {len(self.markets)} validated BTC 5-min markets")

    # ── Find and start the active market ──────────────────────────────────────

    async def _start_current_market(self):
        """Find next valid market using the discovery helper."""
        market = self.discovery.get_next_tradable_market(lookahead_minutes=2.0)
        
        if not market:
            wait_time = self.discovery.seconds_to_next_boundary()
            logger.info(f"⏰ No valid market ready. Waiting {wait_time:.0f}s for next boundary…")
            await asyncio.sleep(wait_time + 2)
            await self._load_markets()
            market = self.discovery.get_next_tradable_market()
            
        if not market:
            logger.error("❌ Still no valid market after reload — will retry in 60s")
            await asyncio.sleep(60)
            await self._start_current_market()
            return

        try:
            self.current_idx = self.markets.index(market)
        except ValueError:
            logger.warning("⚠️ Market not in self.markets list — tracking by object only")
            self.current_idx = -1

        # 🔐 Final safety check before launching runner
        if not await self.discovery.safe_enter_market(market):
            logger.error("🚨 Safety check failed — aborting market launch")
            await asyncio.sleep(10)
            await self._start_current_market()
            return

        await self._launch_runner(market)

    # ── Launch a MarketRunner for a given market ───────────────────────────────

    async def _launch_runner(self, market: dict):
        """Launch runner with final logging."""
        if self.runner_task and not self.runner_task.done():
            # We don't cancel it! It needs to finish resolving and redeeming.
            # We just move it to background_tasks to prevent it from being garbage collected.
            self.background_tasks.add(self.runner_task)
            self.runner_task.add_done_callback(self.background_tasks.discard)
            self.runner_task = None

        logger.info("=" * 70)
        logger.info(f"▶ STARTING MARKET: {market['slug']}")
        logger.info(f"  Window : {market['start_time'].strftime('%H:%M:%S')} → {market['end_time'].strftime('%H:%M:%S')} UTC")
        logger.info(f"  UP     : {market.get('token_up', 'unknown')[:20]}…")
        logger.info(f"  DOWN   : {market.get('token_down', 'unknown')[:20]}…")
        logger.info(f"  Validated: ✅ Timestamp aligned, tokens verified")
        logger.info("=" * 70)

        self.current_runner = MarketRunner(market, self.exchange, self.discovery)
        self.runner_task = asyncio.create_task(self.current_runner.run())

    # ── Timer loop ─────────────────────────────────────────────────────────────

    async def _timer_loop(self):
        """Checks every 10 seconds for market switches."""
        while True:
            await asyncio.sleep(10)
            now = datetime.now(timezone.utc)

            # Auto-restart to refresh market list
            uptime = (now - self.bot_start_time).total_seconds() / 60
            if uptime >= self.restart_after_minutes:
                logger.info("♻️  Auto-reloading market list…")
                await self._load_markets()
                self.bot_start_time = now

            if self.current_idx < 0 or self.current_idx >= len(self.markets):
                continue

            current = self.markets[self.current_idx]
            if now >= current["end_time"]:
                logger.info(f"✅ Market {current['slug']} completed — switching to next…")
                
                # Check if we lost money in the market that just finished
                if self.current_runner and self.current_runner.pnl.net_pnl < 0:
                    logger.error(f"🛑 Market {current['slug']} resulted in a loss (PnL: ${self.current_runner.pnl.net_pnl:.2f}). Shutting down bot to protect capital.")
                    sys.exit(0)

                self.markets_traded += 1
                if MAX_MARKETS_TO_TRADE > 0 and self.markets_traded >= MAX_MARKETS_TO_TRADE:
                    logger.info(f"🛑 Reached MAX_MARKETS_TO_TRADE ({MAX_MARKETS_TO_TRADE}). Shutting down bot.")
                    if self.runner_task and not self.runner_task.done():
                        self.runner_task.cancel()
                    sys.exit(0)
                    
                switched = await self._switch_to_next(skip_one=True)
                if not switched:
                    logger.warning("⚠️ No next valid market found — reloading and waiting")
                    await self._load_markets()
                    wait_time = self.discovery.seconds_to_next_boundary()
                    await asyncio.sleep(wait_time + 2)
                    await self._start_current_market()

    async def _switch_to_next(self, skip_one: bool = False) -> bool:
        """Switch to the next validated market."""
        next_market = self.discovery.get_next_tradable_market(lookahead_minutes=2.0)
        
        if next_market and skip_one:
            skipped_slug = next_market['slug']
            next_market = self.discovery.get_market_after(next_market["market_ts"])
            if next_market:
                logger.info(f"⏭️ Skipping market {skipped_slug} to allow redemptions to process. Next target: {next_market['slug']}")

        if not next_market:
            return False

        if not await self.discovery.safe_enter_market(next_market):
            logger.error("🚨 Safety check failed for next market — aborting switch")
            return False

        try:
            self.current_idx = self.markets.index(next_market)
        except ValueError:
            self.current_idx = -1

        logger.info(
            f"\n{'='*70}\n"
            f"  ▶ MARKET SWITCH [{datetime.now(timezone.utc).strftime('%H:%M:%S')} UTC]\n"
            f"  Slug   : {next_market['slug']}\n"
            f"  Window : {next_market['start_time'].strftime('%H:%M:%S')} → {next_market['end_time'].strftime('%H:%M:%S')} UTC\n"
            f"  Status : ✅ Validated & Safe\n"
            f"{'='*70}"
        )

        await self._launch_runner(next_market)
        return True


async def main():
    setup_logging()
    logger.info("🤖 Polymarket MM Bot starting…")
    logger.info(f"   Market interval: {MARKET_WINDOW_SECONDS}s (5-min)")
    logger.info("🛡️  Safety: Strict market validation ENABLED (pattern-based)")

    bot = Bot()
    try:
        await bot.run()
    except KeyboardInterrupt:
        logger.info("👋 Shutting down gracefully…")
        if bot.runner_task and not bot.runner_task.done():
            bot.runner_task.cancel()
            try:
                await bot.runner_task
            except asyncio.CancelledError:
                pass
    except Exception as e:
        logger.exception(f"💥 Unhandled error: {e}")
        raise


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    asyncio.run(main())