import os
from dotenv import load_dotenv

load_dotenv()

# ── Credentials ────────────────────────────────────────────────────────────────
PRIVATE_KEY       = os.environ["PRIVATE_KEY"]
CLOB_API_KEY      = os.environ["CLOB_API_KEY"]
CLOB_SECRET       = os.environ["CLOB_SECRET"]
CLOB_PASSPHRASE   = os.environ["CLOB_PASSPHRASE"]
WALLET_ADDRESS    = os.environ["WALLET_ADDRESS"]

# ── Trading mode ──────────────────────────────────────────────────────────────
# TRADING_MODE is the explicit operational mode. LIVE_TRADING is kept for
# backward compatibility with older scripts, but live startup must require both.
TRADING_MODE: str = os.getenv("TRADING_MODE", "paper").strip().lower()
if TRADING_MODE not in ("paper", "live"):
    raise ValueError("TRADING_MODE must be 'paper' or 'live'")

# Set LIVE_TRADING=1 to enable real order placement and startup reconciliation.
# Default is False (paper trading). Required before live deployment.
LIVE_TRADING: bool = os.getenv("LIVE_TRADING", "0").lower() in ("1", "true", "yes")
if TRADING_MODE == "live" and not LIVE_TRADING:
    raise ValueError("TRADING_MODE=live requires LIVE_TRADING=1")
if TRADING_MODE == "paper" and LIVE_TRADING:
    raise ValueError("LIVE_TRADING=1 requires TRADING_MODE=live")

# ── Polymarket endpoints ───────────────────────────────────────────────────────
CLOB_HOST         = "https://clob.polymarket.com"
GAMMA_HOST        = "https://gamma-api.polymarket.com"
WS_HOST           = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# ── Strategy parameters ────────────────────────────────────────────────────────

# Limit how many consecutive markets to trade before stopping (0 = run forever)
MAX_MARKETS_TO_TRADE = int(os.getenv("MAX_MARKETS_TO_TRADE", "0"))


# Full level set for reference — uncomment and widen once wallet > $100.
# At that point the Phase 3 vol filter will clip this list dynamically so
# you never post the full width during a spike.
#
# PRICE_LEVELS = [0.40, 0.45, 0.48, 0.52, 0.55, 0.60]   # $100+ wallet
# PRICE_LEVELS = [0.45, 0.48, 0.52, 0.55]                # $50+ wallet

# $10 wallet: single tight band around fair value.
# Both levels are exactly 0.50, so one-sided exposure is minimal.
# UP orders: 0.50  |  DOWN orders: 0.50
# Max one-sided exposure: 0.50 × 5 shares = $2.50
PRICE_LEVELS = [0.50]

# Shares per ladder level.
# At 5 (exchange minimum), one full ladder = 4 orders × ~$2.50 = $10 locked.
# Leaves $10 free for FOK arb and unmatched-fill buffer.
LADDER_SIZE_PER_LEVEL   = float(os.getenv("LADDER_SIZE_PER_LEVEL",   5))

# Hard cap on total USDC spent per market window.
# Set just below wallet size so one bad window can't drain everything.
MAX_SPEND_PER_MARKET    = float(os.getenv("MAX_SPEND_PER_MARKET",   8))

# Hard cap on the maximum number of shares held on either side.
MAX_POSITION_SHARES     = float(os.getenv("MAX_POSITION_SHARES",    10))

# Limit max entries per window to avoid runaway entries on a single market
MAX_ENTRIES_PER_WINDOW  = int(os.getenv("MAX_ENTRIES_PER_WINDOW",   5))

# Minimum arb edge required before firing a FOK trade (2% = 0.02).
# Do not lower this — at $20 you cannot absorb a marginal arb that goes wrong.
TARGET_EDGE             = float(os.getenv("TARGET_EDGE",           0.02))

# Maximum USDC committed to a single FOK arb attempt.
# $4 at combined ≈ 0.95 → ~8 shares per leg, above MIN_ARB_SHARES.
MAX_TAKER_FILL_USDC     = float(os.getenv("MAX_TAKER_FILL_USDC",    4))

# risk.py last-resort halt: imbalance > 2× this value triggers a circuit break.
# Kept proportional to wallet size (was 10 at $20 scale).
MAX_INVENTORY_IMBALANCE = float(os.getenv("MAX_INVENTORY_IMBALANCE", 5))

# Merge matched UP+DOWN pairs once we have this many USDC worth.
# MUST be ≤ LADDER_SIZE_PER_LEVEL (5) so pairs are recycled within the window
# 2.5 USDC corresponds to 2.5 shares matched.
MERGE_THRESHOLD_USDC    = 2.5

# Out-of-band kill switch. If this file exists, trading halts.
KILL_SWITCH_FILE        = os.getenv("KILL_SWITCH_FILE", "/tmp/btcbot_halt")

MARKET_INTERVAL_SECONDS  = int(os.getenv("MARKET_INTERVAL_SECONDS", "300"))
MARKET_WINDOW_SECONDS    = MARKET_INTERVAL_SECONDS
STOP_BUYING_BEFORE_CLOSE = 15      # Stop new buys N seconds before close
ORACLE_WAIT_SECONDS      = 320     # Wait after close before attempting redeem
COMBINED_ASK_STOP        = 1.02    # Circuit breaker on mispriced book
MAX_PRICE_GAP            = float(os.getenv("MAX_PRICE_GAP", 0.30))  # Max difference between UP and DOWN prices

# Below this ask price, a side is considered a strong loser by the market.
# The bot will stop posting NEW limit buys on that side to avoid accumulating
# worthless shares filled by informed sellers.
# e.g. if DOWN ask = 0.20, market thinks DOWN has only 20% chance — skip DOWN posts.
# Set higher (e.g. 0.40) to be more conservative; 0.0 disables this guard.
LOSING_SIDE_THRESHOLD    = float(os.getenv("LOSING_SIDE_THRESHOLD", 0.35))

# Kill switch: halt trading after losing this much in any 60-minute period.
# $2.5 = 25% of a $10 wallet — aggressive but appropriate; a single bad hour
# should not wipe more than a quarter of capital before the bot stops itself.
MAX_LOSS_PER_HOUR_USDC  = float(os.getenv("MAX_LOSS_PER_HOUR_USDC",  2.5))

POLL_INTERVAL_SECONDS   = 1.5     # Fallback book polling interval
WINDOW_ENTRY_BUFFER     = 1       # Seconds to wait after window open
WINDOW_ENTRY_TIMEOUT    = 10      # Stop entering if more than N seconds late

# Lowered from 10 to 5 (exchange minimum) so FOK arb can fire at small sizes.
# At $6 max taker fill and ask ≈ 0.50, max shares ≈ 12 — well above this floor.
# Without this change the arb check almost never triggers at $20 scale.
MIN_ARB_SHARES          = 5

# ── Phase 1: inventory control & ladder refresh ────────────────────────────────
# Thresholds scaled to $10 wallet / 5-share orders.
# Soft → begin skewing (Phase 2).  Hard → cancel heavy-side orders immediately.
# Stack must satisfy: SOFT < HARD < MAX_INVENTORY_IMBALANCE < IMBALANCE × 2
#   2 < 4 < 5 < 10  ✓
MAX_INVENTORY_SOFT      = float(os.getenv("MAX_INVENTORY_SOFT",   2))
MAX_INVENTORY_HARD      = float(os.getenv("MAX_INVENTORY_HARD",   LADDER_SIZE_PER_LEVEL))
LADDER_REFRESH_SECS     = int(os.getenv("LADDER_REFRESH_SECS",   75))

# ── Phase 2: inventory skewing ─────────────────────────────────────────────────
# SKEW_FACTOR unchanged: 0.001 × 3-share imbalance = 0.003 price shift.
# MAX_SKEW_OFFSET tightened to 0.02: with PRICE_LEVELS = [0.48, 0.52] a
# 0.05 offset would push bids outside the tight band — counterproductive.
SKEW_FACTOR         = float(os.getenv("SKEW_FACTOR",      0.001))
MAX_SKEW_OFFSET     = float(os.getenv("MAX_SKEW_OFFSET",  0.02))

# ── Phase 3: volatility detection ──────────────────────────────────────────────
# VOL_BUFFER_SIZE            — short-term ring buffer depth (~15s at 1.5s/tick).
# VOL_BASELINE_SIZE          — longer-horizon baseline (240 × 15s ≈ 1 hour).
# VOL_BASELINE_INTERVAL_SECS — how often a snapshot is pushed to the baseline.
# VOL_PAUSE_MULTIPLIER       — spike threshold vs hourly average.
#                              Lowered to 2.5 (vs default 3.0): at $20 the cost
#                              of adverse selection during a spike is proportionally
#                              much larger, so we exit the book earlier.
# VOL_PAUSE_SECONDS          — how long to stay out after a spike (unchanged).
# VOL_SPREAD_TIGHT / WIDE    — adaptive ladder band endpoints.
#                              With PRICE_LEVELS = [0.48, 0.52] the tight band
#                              (0.48–0.52) matches exactly, so at low vol the
#                              filter passes both levels cleanly.  At high vol
#                              the wide band (0.44–0.56) would accept wider
#                              levels if PRICE_LEVELS is expanded later.
VOL_BUFFER_SIZE            = int(os.getenv("VOL_BUFFER_SIZE",             10))
VOL_BASELINE_SIZE          = int(os.getenv("VOL_BASELINE_SIZE",          240))
VOL_BASELINE_INTERVAL_SECS = int(os.getenv("VOL_BASELINE_INTERVAL_SECS",  15))
VOL_PAUSE_MULTIPLIER       = float(os.getenv("VOL_PAUSE_MULTIPLIER",     2.5))
VOL_PAUSE_SECONDS          = int(os.getenv("VOL_PAUSE_SECONDS",           30))
VOL_SPREAD_TIGHT: tuple[float, float] = (0.48, 0.52)
VOL_SPREAD_WIDE:  tuple[float, float] = (0.44, 0.56)
