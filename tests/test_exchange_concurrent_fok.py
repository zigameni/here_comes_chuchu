"""
tests/test_exchange_concurrent_fok.py

Unit tests for Phase 3 Task 3.1:
  Exchange._place_fok_impl()
  Exchange._emergency_market_sell()
  Exchange.execute_concurrent_fok()

Run:
    python tests/test_exchange_concurrent_fok.py
    python -m pytest tests/test_exchange_concurrent_fok.py -v
"""

from __future__ import annotations

import asyncio
import sys
import types
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))


# ── Minimal stubs so exchange.py can be imported without real deps ────────────

def _install_stubs():
    os.environ.setdefault("PRIVATE_KEY",     "0xdummy_key_for_tests_not_real")
    os.environ.setdefault("CLOB_API_KEY",    "dummy")
    os.environ.setdefault("CLOB_SECRET",     "dummy")
    os.environ.setdefault("CLOB_PASSPHRASE", "dummy")
    os.environ.setdefault("WALLET_ADDRESS",  "0x0000000000000000000000000000000000000001")

    dotenv_mod = types.ModuleType("dotenv")
    dotenv_mod.load_dotenv = lambda *a, **kw: None
    sys.modules.setdefault("dotenv", dotenv_mod)

    loguru_mod = types.ModuleType("loguru")
    loguru_mod.logger = MagicMock()
    sys.modules.setdefault("loguru", loguru_mod)

    web3_mod = types.ModuleType("web3")
    web3_mod.Web3 = MagicMock()
    sys.modules.setdefault("web3", web3_mod)

    # py_clob_client_v2: provide just the names exchange.py imports
    clob_mod = types.ModuleType("py_clob_client_v2")
    clob_mod.ApiCreds          = MagicMock()
    clob_mod.ClobClient        = MagicMock()
    clob_mod.OrderArgs         = MagicMock(side_effect=lambda **kw: types.SimpleNamespace(**kw))
    clob_mod.OrderMarketCancelParams = MagicMock()
    clob_mod.OrderType         = types.SimpleNamespace(GTC="GTC", FOK="FOK")
    clob_mod.PartialCreateOrderOptions = MagicMock(side_effect=lambda **kw: types.SimpleNamespace(**kw))
    clob_mod.Side              = types.SimpleNamespace(BUY="BUY", SELL="SELL")
    clob_mod.TradeParams       = MagicMock()
    sys.modules.setdefault("py_clob_client_v2", clob_mod)

    import importlib
    import config  # noqa: F401 — force load with stubs in place
    return importlib.import_module("core.exchange")


_install_stubs()
import core.exchange as _ex_module
from core.exchange import Exchange


# ── Helper: build a read-only Exchange instance without touching real deps ────

def _ensure_event_loop():
    """Ensure there is a current event loop in the main thread (Python 3.12 compat)."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError("loop closed")
        return loop
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def _make_exchange() -> Exchange:
    """Return an Exchange in read-only mode (no private key)."""
    _ensure_event_loop()
    import config as _cfg
    original = _cfg.PRIVATE_KEY
    _cfg.PRIVATE_KEY = ""     # "" is in Exchange._PLACEHOLDER_KEYS → read-only
    try:
        with patch("exchange.ClobClient") as MockClient:
            MockClient.return_value = MagicMock()
            exc = Exchange()
    finally:
        _cfg.PRIVATE_KEY = original
    assert exc._read_only, "Expected read-only exchange for tests"
    return exc


def _make_writable_exchange() -> Exchange:
    """Return a writable Exchange backed entirely by mocked ClobClient."""
    _ensure_event_loop()
    with patch("exchange.ClobClient") as MockClient, \
         patch("exchange.Web3") as MockWeb3:
        client_inst = MagicMock()
        MockClient.return_value = client_inst
        web3_inst = MagicMock()
        MockWeb3.return_value = web3_inst
        web3_inst.eth.account.from_key.return_value = MagicMock()
        web3_inst.eth.contract.return_value = MagicMock()

        exc = Exchange()
        exc._client = client_inst
        exc._read_only = False
    return exc


# ── _place_fok_impl tests ────────────────────────────────────────────────────

def test_place_fok_impl_returns_skipped_on_low_notional():
    """_place_fok_impl returns a 'skipped' dict when notional < MIN_ORDER_NOTIONAL."""
    exc = _make_writable_exchange()

    async def run():
        # price=0.01, size=5 → notional=0.05 < $1
        result = await exc._place_fok_impl("token_abc", price=0.01, size=5.0)
        assert result.get("status") == "skipped"
        assert result.get("reason") == "notional_too_low"

    asyncio.run(run())


def test_place_fok_impl_returns_skipped_on_low_size():
    """_place_fok_impl returns 'skipped' when size < MIN_ORDER_SHARES (5)."""
    exc = _make_writable_exchange()

    async def run():
        # size=4 < 5 (min), notional fine
        result = await exc._place_fok_impl("token_abc", price=0.50, size=4.0)
        assert result.get("status") == "skipped"
        assert result.get("reason") == "size_too_low"

    asyncio.run(run())


def test_place_fok_impl_returns_raw_response_on_success():
    """_place_fok_impl passes through the CLOB response dict unchanged."""
    exc = _make_writable_exchange()
    fake_resp = {"status": "matched", "orderID": "ord-123"}

    async def fake_create(*args, **kwargs):
        return fake_resp

    exc._run = AsyncMock(return_value=fake_resp)

    async def run():
        result = await exc._place_fok_impl("token_abc", price=0.50, size=10.0)
        assert result == fake_resp

    asyncio.run(run())


def test_place_fok_impl_returns_empty_on_exception():
    """_place_fok_impl returns {} (not raises) when the CLOB call throws."""
    exc = _make_writable_exchange()
    exc._run = AsyncMock(side_effect=RuntimeError("network timeout"))

    async def run():
        result = await exc._place_fok_impl("token_abc", price=0.50, size=10.0)
        assert isinstance(result, dict)
        assert result.get("status") == "error"

    asyncio.run(run())


# ── _emergency_market_sell tests ─────────────────────────────────────────────

def test_emergency_market_sell_returns_true_on_matched():
    """_emergency_market_sell returns True when CLOB responds 'matched'."""
    exc = _make_writable_exchange()
    exc._run_client = AsyncMock(return_value={"status": "matched"})

    async def run():
        ok = await exc._emergency_market_sell("token_up", 10.0)
        assert ok is True

    asyncio.run(run())


def test_emergency_market_sell_returns_false_on_unmatched():
    """_emergency_market_sell returns False when CLOB returns a non-fill status."""
    exc = _make_writable_exchange()
    exc._run_client = AsyncMock(return_value={"status": "cancelled"})

    async def run():
        ok = await exc._emergency_market_sell("token_up", 10.0)
        assert ok is False

    asyncio.run(run())


def test_emergency_market_sell_returns_false_on_exception():
    """_emergency_market_sell never raises — always returns False on error."""
    exc = _make_writable_exchange()
    exc._run_client = AsyncMock(side_effect=Exception("CLOB down"))

    async def run():
        ok = await exc._emergency_market_sell("token_up", 10.0)
        assert ok is False

    asyncio.run(run())


# ── execute_concurrent_fok tests ─────────────────────────────────────────────

def test_concurrent_fok_read_only_is_noop():
    """execute_concurrent_fok is a no-op when Exchange is in read-only mode."""
    exc = _make_exchange()   # read-only

    async def run():
        result = await exc.execute_concurrent_fok(
            "token_up", "token_down", 0.48, 0.48, 10.0
        )
        assert result["up_filled"]   is False
        assert result["down_filled"] is False
        assert result["compensated"] is False
        assert result["net_cost"]    == 0.0

    asyncio.run(run())


def test_concurrent_fok_both_legs_fill():
    """Both legs fill → up_filled=True, down_filled=True, compensated=False."""
    exc = _make_writable_exchange()

    matched = {"status": "matched"}
    exc._place_fok_impl = AsyncMock(return_value=matched)

    async def run():
        result = await exc.execute_concurrent_fok(
            "token_up", "token_down", 0.48, 0.49, 10.0
        )
        assert result["up_filled"]   is True
        assert result["down_filled"] is True
        assert result["compensated"] is False
        # net_cost = (0.48 + 0.49) * 10 = 9.7
        assert abs(result["net_cost"] - 9.7) < 1e-6

    asyncio.run(run())


def test_concurrent_fok_up_fills_down_misses_triggers_compensation():
    """UP fills, DOWN misses → compensating sell fires on UP token."""
    exc = _make_writable_exchange()

    call_count = [0]

    async def fake_place_fok_impl(token_id, price, size, side="BUY", tick_size="0.01"):
        call_count[0] += 1
        if token_id == "token_up":
            return {"status": "matched"}
        return {"status": "cancelled"}

    exc._place_fok_impl = fake_place_fok_impl
    exc._emergency_market_sell = AsyncMock(return_value=True)

    async def run():
        result = await exc.execute_concurrent_fok(
            "token_up", "token_down", 0.48, 0.49, 10.0
        )
        assert result["up_filled"]   is True
        assert result["down_filled"] is False
        assert result["compensated"] is True
        # Emergency sell was called on the UP token
        exc._emergency_market_sell.assert_awaited_once_with("token_up", 10.0)
        # net_cost reflects only the filled UP leg
        assert abs(result["net_cost"] - 4.8) < 1e-6   # 0.48 * 10

    asyncio.run(run())


def test_concurrent_fok_down_fills_up_misses_triggers_compensation():
    """DOWN fills, UP misses → compensating sell fires on DOWN token."""
    exc = _make_writable_exchange()

    async def fake_place_fok_impl(token_id, price, size, side="BUY", tick_size="0.01"):
        if token_id == "token_down":
            return {"status": "matched"}
        return {"status": "cancelled"}

    exc._place_fok_impl = fake_place_fok_impl
    exc._emergency_market_sell = AsyncMock(return_value=True)

    async def run():
        result = await exc.execute_concurrent_fok(
            "token_up", "token_down", 0.48, 0.49, 10.0
        )
        assert result["up_filled"]   is False
        assert result["down_filled"] is True
        assert result["compensated"] is True
        exc._emergency_market_sell.assert_awaited_once_with("token_down", 10.0)
        assert abs(result["net_cost"] - 4.9) < 1e-6   # 0.49 * 10

    asyncio.run(run())


def test_concurrent_fok_neither_fills():
    """Neither leg fills → all False, net_cost=0."""
    exc = _make_writable_exchange()
    exc._place_fok_impl = AsyncMock(return_value={"status": "cancelled"})
    exc._emergency_market_sell = AsyncMock(return_value=False)

    async def run():
        result = await exc.execute_concurrent_fok(
            "token_up", "token_down", 0.48, 0.49, 10.0
        )
        assert result["up_filled"]   is False
        assert result["down_filled"] is False
        assert result["compensated"] is False
        assert result["net_cost"]    == 0.0
        # No compensation attempt when neither filled
        exc._emergency_market_sell.assert_not_awaited()

    asyncio.run(run())


def test_concurrent_fok_exception_in_one_leg_treated_as_nonfill():
    """If one leg raises (returned as exception by gather), it counts as non-fill."""
    exc = _make_writable_exchange()

    call_count = [0]

    async def fake_place_fok_impl(token_id, price, size, side="BUY", tick_size="0.01"):
        call_count[0] += 1
        if token_id == "token_up":
            # Return error dict (as _place_fok_impl itself catches exceptions)
            return {"status": "error", "error": "timeout"}
        return {"status": "matched"}

    exc._place_fok_impl = fake_place_fok_impl
    exc._emergency_market_sell = AsyncMock(return_value=True)

    async def run():
        result = await exc.execute_concurrent_fok(
            "token_up", "token_down", 0.48, 0.49, 10.0
        )
        # UP errored → non-fill; DOWN matched
        assert result["up_filled"]   is False
        assert result["down_filled"] is True
        assert result["compensated"] is True
        exc._emergency_market_sell.assert_awaited_once_with("token_down", 10.0)

    asyncio.run(run())


def test_concurrent_fok_no_compensation_when_disabled():
    """compensate=False skips the emergency sell even on partial fill."""
    exc = _make_writable_exchange()

    async def fake_place_fok_impl(token_id, price, size, side="BUY", tick_size="0.01"):
        if token_id == "token_up":
            return {"status": "matched"}
        return {"status": "cancelled"}

    exc._place_fok_impl = fake_place_fok_impl
    exc._emergency_market_sell = AsyncMock(return_value=False)

    async def run():
        result = await exc.execute_concurrent_fok(
            "token_up", "token_down", 0.48, 0.49, 10.0,
            compensate=False,
        )
        assert result["up_filled"]   is True
        assert result["down_filled"] is False
        assert result["compensated"] is False   # skipped
        exc._emergency_market_sell.assert_not_awaited()

    asyncio.run(run())


def test_concurrent_fok_compensation_failure_is_reported():
    """compensated=False is returned even when compensation was attempted but failed."""
    exc = _make_writable_exchange()

    async def fake_place_fok_impl(token_id, price, size, side="BUY", tick_size="0.01"):
        if token_id == "token_up":
            return {"status": "matched"}
        return {"status": "cancelled"}

    exc._place_fok_impl = fake_place_fok_impl
    exc._emergency_market_sell = AsyncMock(return_value=False)  # sell failed

    async def run():
        result = await exc.execute_concurrent_fok(
            "token_up", "token_down", 0.48, 0.49, 10.0
        )
        assert result["up_filled"]   is True
        assert result["down_filled"] is False
        # compensated=False because the sell did NOT succeed
        assert result["compensated"] is False

    asyncio.run(run())


# ── Runner ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_place_fok_impl_returns_skipped_on_low_notional,
        test_place_fok_impl_returns_skipped_on_low_size,
        test_place_fok_impl_returns_raw_response_on_success,
        test_place_fok_impl_returns_empty_on_exception,
        test_emergency_market_sell_returns_true_on_matched,
        test_emergency_market_sell_returns_false_on_unmatched,
        test_emergency_market_sell_returns_false_on_exception,
        test_concurrent_fok_read_only_is_noop,
        test_concurrent_fok_both_legs_fill,
        test_concurrent_fok_up_fills_down_misses_triggers_compensation,
        test_concurrent_fok_down_fills_up_misses_triggers_compensation,
        test_concurrent_fok_neither_fills,
        test_concurrent_fok_exception_in_one_leg_treated_as_nonfill,
        test_concurrent_fok_no_compensation_when_disabled,
        test_concurrent_fok_compensation_failure_is_reported,
    ]

    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except Exception as exc:
            import traceback
            print(f"  FAIL  {t.__name__}  ->  {exc}")
            traceback.print_exc()
            failed += 1

    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)
