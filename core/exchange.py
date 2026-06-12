"""
Thin async wrapper around the Polymarket py-clob-client-v2.

All order placement, cancellation, merging, and redemption goes through here.
The underlying py-clob-client-v2 is synchronous; we run it in an executor so the
rest of the bot stays fully async.

Migration notes (v1 → v2):
  - Package: py_clob_client_v2 (pip install py-clob-client-v2)
  - Side is now an enum: Side.BUY / Side.SELL (not strings)
  - create_and_post_order() now takes named params: order_args=, options=, order_type=
  - PartialCreateOrderOptions now requires tick_size (string: "0.01", "0.001", etc.)
  - ClobClient: signature_type=3 (POLY_1271), funder= is required
  - cancel_market_orders: takes market= keyword instead of market_id=
  - FOK limit orders: use create_and_post_order with OrderType.FOK
"""
import asyncio
from types import SimpleNamespace
from typing import Optional
from loguru import logger
import math
from web3 import Web3

CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097CAeceaf8b46B2"
USDC_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

CTF_ABI = [
    {
        "constant": False,
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "partition", "type": "uint256[]"},
            {"name": "amount", "type": "uint256"}
        ],
        "name": "mergePositions",
        "outputs": [],
        "payable": False,
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": False,
        "inputs": [
            {"name": "collateralToken", "type": "address"},
            {"name": "parentCollectionId", "type": "bytes32"},
            {"name": "conditionId", "type": "bytes32"},
            {"name": "indexSets", "type": "uint256[]"}
        ],
        "name": "redeemPositions",
        "outputs": [],
        "payable": False,
        "stateMutability": "nonpayable",
        "type": "function"
    }
]

ERC20_ABI = [
    {
        "constant": True,
        "inputs": [{"name": "account", "type": "address"}],
        "name": "balanceOf",
        "outputs": [{"name": "", "type": "uint256"}],
        "payable": False,
        "stateMutability": "view",
        "type": "function"
    }
]

from py_clob_client_v2 import (
    ApiCreds,
    ClobClient,
    OrderArgs,
    OrderMarketCancelParams,
    OrderType,
    PartialCreateOrderOptions,
    Side,
    TradeParams,
)

import config

# Polygon mainnet chain ID
POLYGON = 137

# Default tick size for BTC binary markets (most use 0.01)
DEFAULT_TICK_SIZE = "0.01"

# CLOB minimum order requirements
MIN_ORDER_NOTIONAL = 1.0   # price × size must be >= $1 USDC
MIN_ORDER_SHARES   = 5.0   # size must be >= 5 shares

# Max concurrent order placements (avoids WinError 10035 on Windows)
_ORDER_CONCURRENCY = 8


class Exchange:
    # Keys that indicate .env hasn't been filled in yet
    _PLACEHOLDER_KEYS = {
        "0xyour_private_key_here",
        "your_private_key_here",
        "",
        None,
    }

    def __init__(self):
        self._loop = asyncio.get_event_loop()
        self._order_sem = asyncio.Semaphore(_ORDER_CONCURRENCY)
        self._client_lock = asyncio.Lock()

        raw_key = config.PRIVATE_KEY
        self._read_only = raw_key in Exchange._PLACEHOLDER_KEYS

        if self._read_only:
            # Paper-trading / pm_daemon path: no signing needed.
            # ClobClient is initialised without a key so get_order_book()
            # (a public endpoint) still works.
            logger.warning(
                "Exchange: PRIVATE_KEY not set — running in read-only mode. "
                "Order placement will raise if attempted."
            )
            self._client = ClobClient(
                host           = config.CLOB_HOST,
                chain_id       = POLYGON,
                signature_type = 0,
            )
            self.account = None
            self.w3 = None
            self.ctf_contract = None
            self.usdc_contract = None
        else:
            creds = ApiCreds(
                api_key        = config.CLOB_API_KEY,
                api_secret     = config.CLOB_SECRET,
                api_passphrase = config.CLOB_PASSPHRASE,
            )
            self._client = ClobClient(
                host           = config.CLOB_HOST,
                chain_id       = POLYGON,
                key            = raw_key,
                creds          = creds,
                signature_type = 3,                    # POLY_1271 — required for v2
                funder         = config.WALLET_ADDRESS,
            )
            self.w3 = Web3(Web3.HTTPProvider("https://polygon-bor-rpc.publicnode.com"))
            self.account = self.w3.eth.account.from_key(raw_key)
            self.ctf_contract = self.w3.eth.contract(
                address=self.w3.to_checksum_address(CTF_ADDRESS),
                abi=CTF_ABI
            )
            self.usdc_contract = self.w3.eth.contract(
                address=self.w3.to_checksum_address(USDC_ADDRESS),
                abi=ERC20_ABI
            )

    # ── Helpers ────────────────────────────────────────────────────────────────

    async def _run(self, fn, *args, **kwargs):
        """Run a blocking call in the default executor."""
        return await self._loop.run_in_executor(None, lambda: fn(*args, **kwargs))

    async def _run_client(self, fn, *args, **kwargs):
        """Run a ClobClient blocking call sequentially to avoid requests.Session thread-safety issues."""
        async with self._client_lock:
            return await self._loop.run_in_executor(None, lambda: fn(*args, **kwargs))

    # ── Order book ─────────────────────────────────────────────────────────────

    async def get_order_book(self, token_id: str) -> dict:
        """Return raw order book dict for a single token."""
        return await self._run_client(self._client.get_order_book, token_id)

    # ── Order placement ────────────────────────────────────────────────────────

    async def place_limit_buy(
        self,
        token_id:  str,
        price:     float,
        size:      float,
        tick_size: str = DEFAULT_TICK_SIZE,
    ) -> Optional[str]:
        """
        Place a GTC limit buy. Returns order_id or None on failure.

        Pre-validates against CLOB minimums before sending to avoid 400 errors:
          - price * size >= MIN_ORDER_NOTIONAL ($1 USDC)
          - size >= MIN_ORDER_SHARES (5 shares)
        """
        notional = round(price * size, 4)
        if notional < MIN_ORDER_NOTIONAL:
            logger.debug(
                f"  skip limit buy: notional ${notional:.4f} < ${MIN_ORDER_NOTIONAL} "
                f"(price={price} size={size})"
            )
            return None
        if size < MIN_ORDER_SHARES:
            logger.debug(
                f"  skip limit buy: size {size} < {MIN_ORDER_SHARES} shares"
            )
            return None

        async with self._order_sem:
            try:
                args = OrderArgs(
                    token_id = token_id,
                    price    = round(price, 4),
                    size     = float(math.floor(size)), # Force to whole number
                    side     = Side.BUY,
                )
                resp = await self._run_client(
                    self._client.create_and_post_order,
                    order_args  = args,
                    options     = PartialCreateOrderOptions(tick_size=tick_size),
                    order_type  = OrderType.GTC,
                )
                order_id = resp.get("orderID") or resp.get("order_id")
                logger.debug(
                    f"  limit buy placed: token={token_id[:8]}… "
                    f"price={price} size={size} notional=${notional:.2f} id={order_id}"
                )
                return order_id
            except Exception as e:
                logger.error(f"place_limit_buy failed: {e}")
                return None

    async def place_fok_buy(
        self,
        token_id:  str,
        price:     float,
        size:      float,
        tick_size: str = DEFAULT_TICK_SIZE,
    ) -> bool:
        """
        Place a FOK (Fill-Or-Kill) limit buy at a specific price.
        Returns True if filled.
        """
        notional = round(price * size, 4)
        if notional < MIN_ORDER_NOTIONAL or size < MIN_ORDER_SHARES:
            logger.debug(f"  skip FOK buy: notional=${notional:.4f} size={size}")
            return False

        async with self._order_sem:
            try:
                args = OrderArgs(
                    token_id = token_id,
                    price    = round(price, 4),
                    size     = round(size,  2),
                    side     = Side.BUY,
                )
                resp = await self._run_client(
                    self._client.create_and_post_order,
                    order_args  = args,
                    options     = PartialCreateOrderOptions(tick_size=tick_size),
                    order_type  = OrderType.FOK,
                )
                status = resp.get("status", "")
                filled = status in ("matched", "filled", "MATCHED", "FILLED")
                if not filled:
                    logger.warning(f"FOK not filled: status={status} resp={resp}")
                return filled
            except Exception as e:
                logger.error(f"place_fok_buy failed: {e}")
                return False

    async def place_limit_sell(
        self,
        token_id:  str,
        price:     float,
        size:      float,
        tick_size: str = DEFAULT_TICK_SIZE,
    ) -> Optional[str]:
        """
        Place a GTC limit sell. Returns order_id or None on failure.

        Pre-validates against CLOB minimums before sending to avoid 400 errors:
          - price * size >= MIN_ORDER_NOTIONAL ($1 USDC)
          - size >= MIN_ORDER_SHARES (5 shares)
        """
        notional = round(price * size, 4)
        if notional < MIN_ORDER_NOTIONAL:
            logger.debug(
                f"  skip limit sell: notional ${notional:.4f} < ${MIN_ORDER_NOTIONAL} "
                f"(price={price} size={size})"
            )
            return None
        if size < MIN_ORDER_SHARES:
            logger.debug(
                f"  skip limit sell: size {size} < {MIN_ORDER_SHARES} shares"
            )
            return None

        async with self._order_sem:
            try:
                args = OrderArgs(
                    token_id = token_id,
                    price    = round(price, 4),
                    size     = float(math.floor(size)), # Force to whole number
                    side     = Side.SELL,
                )
                resp = await self._run_client(
                    self._client.create_and_post_order,
                    order_args  = args,
                    options     = PartialCreateOrderOptions(tick_size=tick_size),
                    order_type  = OrderType.GTC,
                )
                order_id = resp.get("orderID") or resp.get("order_id")
                logger.debug(
                    f"  limit sell placed: token={token_id[:8]}… "
                    f"price={price} size={size} notional=${notional:.2f} id={order_id}"
                )
                return order_id
            except Exception as e:
                logger.error(f"place_limit_sell failed: {e}")
                return None

    async def place_fok_sell(
        self,
        token_id:  str,
        price:     float,
        size:      float,
        tick_size: str = DEFAULT_TICK_SIZE,
    ) -> bool:
        """
        Place a FOK (Fill-Or-Kill) limit sell at a specific price.
        Returns True if filled.
        """
        notional = round(price * size, 4)
        if notional < MIN_ORDER_NOTIONAL or size < MIN_ORDER_SHARES:
            logger.debug(f"  skip FOK sell: notional=${notional:.4f} size={size}")
            return False

        async with self._order_sem:
            try:
                args = OrderArgs(
                    token_id = token_id,
                    price    = round(price, 4),
                    size     = round(size,  2),
                    side     = Side.SELL,
                )
                resp = await self._run_client(
                    self._client.create_and_post_order,
                    order_args  = args,
                    options     = PartialCreateOrderOptions(tick_size=tick_size),
                    order_type  = OrderType.FOK,
                )
                status = resp.get("status", "")
                filled = status in ("matched", "filled", "MATCHED", "FILLED")
                if not filled:
                    logger.warning(f"FOK sell not filled: status={status} resp={resp}")
                return filled
            except Exception as e:
                logger.error(f"place_fok_sell failed: {e}")
                return False

    async def place_bulk_limit_buys(
        self,
        orders: list[dict],
    ) -> list[Optional[str]]:
        """
        Place multiple limit buys with controlled concurrency.
        Each dict: {token_id, price, size} — optionally {tick_size}.
        Returns list of order_ids (None for skipped/failed orders).

        Concurrency is capped by _order_sem (_ORDER_CONCURRENCY slots) to avoid
        WinError 10035 (Windows non-blocking socket exhaustion) when the
        underlying sync client fires many requests simultaneously.
        """
        tasks = [
            self.place_limit_buy(
                o["token_id"],
                o["price"],
                o["size"],
                o.get("tick_size", DEFAULT_TICK_SIZE),
            )
            for o in orders
        ]
        return list(await asyncio.gather(*tasks, return_exceptions=False))

    # ── Cancellation ───────────────────────────────────────────────────────────

    async def cancel_order(self, order_id: str) -> bool:
        try:
            # v2 uses .cancel_orders([order_id])
            await self._run_client(self._client.cancel_orders, [order_id])
            return True
        except Exception as e:
            logger.warning(f"Failed to cancel order {order_id}: {e}")
            return False

    async def cancel_all_orders(self, market_id: str) -> bool:
        """
        Cancel all open orders for a market.

        v2 note: cancel_market_orders() takes market= (condition ID) or
        asset_id= (token ID).  We pass market= which matches the condition ID.
        Falls back to cancel_all() if market-level cancel fails.
        """
        try:
            await self._run_client(
                self._client.cancel_market_orders,
                OrderMarketCancelParams(market=market_id),
            )
            logger.info(f"Cancelled all orders for market {market_id}")
            return True
        except Exception as e:
            logger.warning(f"cancel_market_orders failed ({e}), trying cancel_all()")
            try:
                await self._run_client(self._client.cancel_all)
                logger.info("Cancelled ALL open orders (fallback)")
                return True
            except Exception as e2:
                logger.error(f"cancel_all fallback also failed: {e2}")
                return False

    # ── On-chain: merge & redeem ───────────────────────────────────────────────

    async def merge_positions(self, condition_id: str, amount: float) -> bool:
        """
        Merge matched UP+DOWN pairs into USDC.
        Calls the ConditionalTokens contract directly via Web3.
        """
        try:
            amount_base = int(amount * 1e6)
            cond_bytes = bytes.fromhex(condition_id.replace("0x", ""))
            
            def _do_merge():
                nonce = self.w3.eth.get_transaction_count(self.account.address)
                tx = self.ctf_contract.functions.mergePositions(
                    self.w3.to_checksum_address(USDC_ADDRESS),
                    b'\x00' * 32,
                    cond_bytes,
                    [1, 2],
                    amount_base
                ).build_transaction({
                    'from': self.account.address,
                    'nonce': nonce,
                    'gasPrice': self.w3.eth.gas_price,
                })
                signed_tx = self.account.sign_transaction(tx)
                raw_tx = getattr(signed_tx, 'raw_transaction', getattr(signed_tx, 'rawTransaction', None))
                tx_hash = self.w3.eth.send_raw_transaction(raw_tx)
                self.w3.eth.wait_for_transaction_receipt(tx_hash)
                return tx_hash.hex()
                
            tx_hash = await self._run(_do_merge)
            logger.info(f"merge_positions tx: {tx_hash}")
            return True
        except Exception as e:
            logger.error(f"merge_positions failed: {e}")
            return False

    async def redeem_positions(self, condition_id: str) -> float:
        """
        Redeem all remaining positions after market resolution.
        Calls the ConditionalTokens contract directly via Web3.
        """
        try:
            cond_bytes = bytes.fromhex(condition_id.replace("0x", ""))
            
            def _do_redeem():
                balance_before = self.usdc_contract.functions.balanceOf(self.account.address).call()
                
                nonce = self.w3.eth.get_transaction_count(self.account.address)
                tx = self.ctf_contract.functions.redeemPositions(
                    self.w3.to_checksum_address(USDC_ADDRESS),
                    b'\x00' * 32,
                    cond_bytes,
                    [1, 2]
                ).build_transaction({
                    'from': self.account.address,
                    'nonce': nonce,
                    'gasPrice': self.w3.eth.gas_price,
                })
                signed_tx = self.account.sign_transaction(tx)
                raw_tx = getattr(signed_tx, 'raw_transaction', getattr(signed_tx, 'rawTransaction', None))
                tx_hash = self.w3.eth.send_raw_transaction(raw_tx)
                self.w3.eth.wait_for_transaction_receipt(tx_hash)
                
                balance_after = self.usdc_contract.functions.balanceOf(self.account.address).call()
                return tx_hash.hex(), float(balance_after - balance_before) / 1e6
                
            tx_hash, amount = await self._run(_do_redeem)
            logger.info(f"redeem_positions tx: {tx_hash} (amount: ${amount:.2f})")
            return amount
        except Exception as e:
            logger.error(f"redeem_positions failed: {e}")
            return 0.0
            
    async def get_order_fills(self, order_id: str) -> float:
        """Fetch the order status and return the size that has been matched/filled."""
        try:
            resp = await self._run_client(self._client.get_order, order_id)
            # Polymarket API returns 'size_matched' for partial or full fills
            return float(resp.get("size_matched", 0.0))
        except Exception as e:
            logger.debug(f"Failed to get order {order_id}: {e}")
            return 0.0

    # ── Dual-leg concurrent FOK (Architecture B) ────────────────────────────────

    async def _place_fok_impl(
        self,
        token_id:  str,
        price:     float,
        size:      float,
        side:      Side = Side.BUY,
        tick_size: str  = DEFAULT_TICK_SIZE,
    ) -> dict:
        """
        Internal FOK helper that returns the raw CLOB response dict.

        Deliberately bypasses _client_lock so two calls can be submitted
        concurrently via asyncio.gather (used by execute_concurrent_fok).
        Thread safety: create_and_post_order builds and signs a fresh HTTP
        request on each call; there is no shared mutable session auth state
        between concurrent executor invocations.  The _order_sem still caps
        total in-flight order placements at _ORDER_CONCURRENCY (8).

        Returns {} on validation failure or exception (caller treats non-"matched"
        status as a non-fill).
        """
        notional = round(price * size, 4)
        if notional < MIN_ORDER_NOTIONAL:
            logger.debug("_place_fok_impl skip: notional=%.4f < %.2f", notional, MIN_ORDER_NOTIONAL)
            return {"status": "skipped", "reason": "notional_too_low"}
        if size < MIN_ORDER_SHARES:
            logger.debug("_place_fok_impl skip: size=%.2f < %.0f shares", size, MIN_ORDER_SHARES)
            return {"status": "skipped", "reason": "size_too_low"}

        async with self._order_sem:
            try:
                order_args = OrderArgs(
                    token_id = token_id,
                    price    = round(price, 4),
                    size     = round(size,  2),
                    side     = side,
                )
                # _run (no lock) — intentional; see docstring above.
                resp = await self._run(
                    self._client.create_and_post_order,
                    order_args  = order_args,
                    options     = PartialCreateOrderOptions(tick_size=tick_size),
                    order_type  = OrderType.FOK,
                )
                return resp if isinstance(resp, dict) else {}
            except Exception as exc:
                logger.error("_place_fok_impl failed token=%s side=%s: %s", token_id[:8], side, exc)
                return {"status": "error", "error": str(exc)}

    async def _emergency_market_sell(
        self,
        token_id:  str,
        size:      float,
        tick_size: str = DEFAULT_TICK_SIZE,
    ) -> bool:
        """
        Compensating FOK sell at a distressed price (0.01) to exit a naked leg.

        Called when one leg of a dual-leg arb filled and the other failed.
        Price 0.01 crosses any live PM bid, guaranteeing a fill at market.
        Logs at WARNING level always; logs at ERROR if the sell itself fails.
        Returns True if the compensating sell was confirmed filled.
        """
        logger.warning(
            "_emergency_market_sell: token=%s size=%.2f — naked leg, exiting at market",
            token_id[:8], size,
        )
        try:
            order_args = OrderArgs(
                token_id = token_id,
                price    = 0.01,
                size     = round(size, 2),
                side     = Side.SELL,
            )
            async with self._order_sem:
                # Use _run_client here (lock is fine — this is a single sequential call).
                resp = await self._run_client(
                    self._client.create_and_post_order,
                    order_args  = order_args,
                    options     = PartialCreateOrderOptions(tick_size=tick_size),
                    order_type  = OrderType.FOK,
                )
            resp = resp if isinstance(resp, dict) else {}
            status = resp.get("status", "")
            filled = status in ("matched", "filled", "MATCHED", "FILLED")
            if not filled:
                logger.error(
                    "_emergency_market_sell NOT filled: token=%s status=%s resp=%s",
                    token_id[:8], status, resp,
                )
            return filled
        except Exception as exc:
            logger.error("_emergency_market_sell exception: token=%s %s", token_id[:8], exc)
            return False

    async def execute_concurrent_fok(
        self,
        token_up:   str,
        token_down: str,
        ask_up:     float,
        ask_down:   float,
        shares:     float,
        compensate: bool = True,
    ) -> dict:
        """
        Fire both FOK legs simultaneously for a dual-leg structural arb.

        Submits the UP and DOWN FOK buys concurrently via asyncio.gather.
        If one leg fills and the other does not, fires an emergency sell on
        the filled leg (when compensate=True) to eliminate naked directional
        exposure.

        Args:
            token_up:   Polymarket token ID for the UP outcome.
            token_down: Polymarket token ID for the DOWN outcome.
            ask_up:     Current PM ask price for the UP leg.
            ask_down:   Current PM ask price for the DOWN leg.
            shares:     Shares to buy on each leg (same for both).
            compensate: If True (default), fire an emergency sell on a
                        filled leg when the other leg did not fill.

        Returns:
            {
                "up_filled":   bool   — True if the UP FOK filled,
                "down_filled": bool   — True if the DOWN FOK filled,
                "compensated": bool   — True if an emergency sell was fired,
                "net_cost":    float  — USDC committed (fills minus any
                                        compensation proceeds are NOT deducted
                                        here; caller accounts for those),
            }
        """
        if self._read_only:
            logger.warning("execute_concurrent_fok called in read-only mode — no-op")
            return {"up_filled": False, "down_filled": False, "compensated": False, "net_cost": 0.0}

        # Fire both legs simultaneously.  return_exceptions=True so a failure
        # in one leg never cancels the other.
        results = await asyncio.gather(
            self._place_fok_impl(token_up,   ask_up,   shares, Side.BUY),
            self._place_fok_impl(token_down, ask_down, shares, Side.BUY),
            return_exceptions=True,
        )

        def _is_filled(r) -> bool:
            if isinstance(r, BaseException):
                return False
            status = r.get("status", "") if isinstance(r, dict) else ""
            return status in ("matched", "filled", "MATCHED", "FILLED")

        up_ok = _is_filled(results[0])
        dn_ok = _is_filled(results[1])

        compensated = False
        net_cost = 0.0

        if up_ok and dn_ok:
            net_cost = round((ask_up + ask_down) * shares, 4)
            logger.info(
                "execute_concurrent_fok: BOTH legs filled — net_cost=%.4f shares=%.2f",
                net_cost, shares,
            )
            return {"up_filled": True, "down_filled": True, "compensated": False, "net_cost": net_cost}

        if up_ok and not dn_ok:
            net_cost = round(ask_up * shares, 4)
            logger.warning(
                "execute_concurrent_fok: UP filled, DOWN missed — net_cost=%.4f; compensating",
                net_cost,
            )
            if compensate:
                compensated = await self._emergency_market_sell(token_up, shares)
                if not compensated:
                    logger.error(
                        "execute_concurrent_fok: compensation FAILED — naked UP position of %.2f shares",
                        shares,
                    )
            return {
                "up_filled":   True,
                "down_filled": False,
                "compensated": compensated,
                "net_cost":    net_cost,
            }

        if dn_ok and not up_ok:
            net_cost = round(ask_down * shares, 4)
            logger.warning(
                "execute_concurrent_fok: DOWN filled, UP missed — net_cost=%.4f; compensating",
                net_cost,
            )
            if compensate:
                compensated = await self._emergency_market_sell(token_down, shares)
                if not compensated:
                    logger.error(
                        "execute_concurrent_fok: compensation FAILED — naked DOWN position of %.2f shares",
                        shares,
                    )
            return {
                "up_filled":   False,
                "down_filled": True,
                "compensated": compensated,
                "net_cost":    net_cost,
            }

        # Neither leg filled.
        logger.info("execute_concurrent_fok: neither leg filled — no position opened")
        return {"up_filled": False, "down_filled": False, "compensated": False, "net_cost": 0.0}

    # ── Position history ────────────────────────────────────────────────────────

    async def get_open_positions(self) -> list:
        """
        Reconstruct net held positions from CLOB trade history for startup
        reconciliation.  Returns a list of SimpleNamespace objects with fields:
            market_id  — Polymarket condition_id (str)
            side       — "UP" or "DOWN" (mapped from CLOB outcome)
            shares     — net shares held (BUY fills minus SELL fills)
            avg_entry  — average entry price of BUY fills (cost basis proxy)

        In read-only / paper-trading mode this always returns [] immediately.
        On any CLOB error the exception is caught, logged, and [] is returned so
        the bot can still start (the halted-entry state from a prior kill-switch
        is more dangerous than a missed reconciliation).
        """
        if self._read_only:
            return []
        try:
            trades = await self._run_client(
                self._client.get_trades,
                TradeParams(maker_address=str(self.account.address)),
            )
            # Aggregate fills by (condition_id, outcome).
            # Key: (market: str, outcome_lower: str)
            # Value: {market_id, side, buy_shares, buy_cost, sell_shares}
            agg: dict = {}
            for t in (trades or []):
                market   = t.get("market", "") or ""
                outcome  = t.get("outcome", "") or ""
                side_raw = (t.get("side", "") or "").upper()
                size     = float(t.get("size",  0.0))
                price    = float(t.get("price", 0.0))
                if not market or not outcome or size <= 0:
                    continue
                key = (market, outcome.lower())
                if key not in agg:
                    # Map Polymarket outcome string → internal side notation.
                    # BTC 5-min markets use "Up"/"Down"; binary fallback uses "Yes"/"No".
                    internal_side = (
                        "UP" if outcome.lower() in ("up", "yes") else "DOWN"
                    )
                    agg[key] = {
                        "market_id":   market,
                        "side":        internal_side,
                        "buy_shares":  0.0,
                        "buy_cost":    0.0,
                        "sell_shares": 0.0,
                    }
                if side_raw == "BUY":
                    agg[key]["buy_shares"] += size
                    agg[key]["buy_cost"]   += size * price
                elif side_raw == "SELL":
                    agg[key]["sell_shares"] += size

            result = []
            for data in agg.values():
                net = data["buy_shares"] - data["sell_shares"]
                if net < 0.01:   # position fully closed or negligible
                    continue
                avg_entry = (
                    data["buy_cost"] / data["buy_shares"]
                    if data["buy_shares"] > 0 else 0.0
                )
                result.append(SimpleNamespace(
                    market_id = data["market_id"],
                    side      = data["side"],
                    shares    = net,
                    avg_entry = avg_entry,
                ))

            logger.info(
                "get_open_positions: %d net position(s) reconstructed from trade history",
                len(result),
            )
            return result
        except Exception as e:
            logger.error(
                "get_open_positions failed — startup reconciliation skipped: %s", e
            )
            return []