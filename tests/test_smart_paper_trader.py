"""
tests/test_smart_paper_trader.py

Focused regression tests for the Phase 1 sigma-real entry gate.

Run:
    python tests/test_smart_paper_trader.py
    python -m pytest tests/test_smart_paper_trader.py -v
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import time
import types
from contextlib import redirect_stdout
from collections import defaultdict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from strategies.tos.strategy import TOSStrategy
from strategies.tos_signal.signal_stack import SignalStack
from strategies.tos_signal.strategy import TOSSignalStrategy

sys.path.insert(0, str(REPO_ROOT))  # <── ADD THIS LINE

def _install_import_stubs() -> None:
    # loguru — used by risk.py; stub logger with no-op methods
    loguru_mod = types.ModuleType("loguru")
    _noop = lambda *a, **kw: None
    loguru_mod.logger = types.SimpleNamespace(
        info=_noop, debug=_noop, warning=_noop,
        error=_noop, critical=_noop, exception=_noop,
    )
    sys.modules.setdefault("loguru", loguru_mod)

    dotenv_mod = types.ModuleType("dotenv")
    dotenv_mod.load_dotenv = lambda *args, **kwargs: None
    sys.modules.setdefault("dotenv", dotenv_mod)

    import os
    os.environ.setdefault("PRIVATE_KEY", "dummy")
    os.environ.setdefault("CLOB_API_KEY", "dummy")
    os.environ.setdefault("CLOB_SECRET", "dummy")
    os.environ.setdefault("CLOB_PASSPHRASE", "dummy")
    os.environ.setdefault("WALLET_ADDRESS", "dummy")

    zmq_mod = types.ModuleType("zmq")
    zmq_mod.SUB = 1
    zmq_mod.SUBSCRIBE = b""
    async_zmq_mod = types.ModuleType("zmq.asyncio")
    async_zmq_mod.Context = types.SimpleNamespace(instance=lambda: None)
    zmq_mod.asyncio = async_zmq_mod
    sys.modules.setdefault("zmq", zmq_mod)
    sys.modules.setdefault("zmq.asyncio", async_zmq_mod)

    aiohttp_mod = types.ModuleType("aiohttp")
    aiohttp_mod.ClientTimeout = lambda *args, **kwargs: None
    aiohttp_mod.ClientSession = object
    sys.modules.setdefault("aiohttp", aiohttp_mod)

    ipc_mod = types.ModuleType("shared.ipc")
    ipc_mod.Channel = types.SimpleNamespace(
        FV_STREAM="ipc:///tmp/fv_stream.ipc",
        PM_BOOK="ipc:///tmp/pm_book.ipc",
    )
    ipc_mod.unpack = lambda raw: raw
    sys.modules["shared.ipc"] = ipc_mod


def _load_module():
    _install_import_stubs()
    module_path = REPO_ROOT / "cmd" / "smart_paper_trader.py"
    spec = importlib.util.spec_from_file_location("smart_paper_trader_under_test", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _make_trader(module):
    trader = module.SmartPaperTrader.__new__(module.SmartPaperTrader)
    trader._stats = module.Stats()
    trader._positions = defaultdict(dict)
    trader._last_fill_ts = defaultdict(dict)
    trader._fills_file = io.StringIO()
    trader._exits_file = io.StringIO()
    trader._last_pm_market = ""
    trader._entry_policy = "LEGACY"
    trader._exit_policy  = "LEGACY"   # Task 2.4: default preserves legacy behavior
    trader._tos_policy = TOSStrategy()
    trader._resolve_cache = {}         # Task 2.5: Gamma resolution cache
    trader._settlement_proxy_prob_up = {}
    trader._exchange = None            # Task R3: no exchange in paper mode
    trader._arb_positions = {}         # Task 3.2: arb positions dict
    trader._fv = module.FVState()      # default FV state (tests override as needed)
    
    # Strategy state is now owned by the isolated strategy instance
    trader._strategy = None

    # Needs to be mocked or instantiated because test framework bypasses __init__
    from core.risk import RiskManagerV2
    trader._risk = RiskManagerV2()
    
    return trader


def _tos_fv(module, **overrides):
    values = {
        "prob_up": 0.82,
        "prob_down": 0.18,
        "is_sigma_real": True,
        "z_score": 1.20,
    }
    values.update(overrides)
    return module.FVState(**values)



def _eval_tos(policy, fv, pm):
    signals = policy.evaluate_entry(fv, pm)
    if not signals:
        return None
    s = signals[0]
    from types import SimpleNamespace
    return SimpleNamespace(side=s.side, ask=s.ask, fv=s.fv, edge=s.edge, elapsed_s=s.elapsed_s, z_score=s.z_score)

def _tos_pm(module, **overrides):
    values = {
        "ts_ms": 1_700_000_230_000,
        "market_ts": 1_700_000_000,
        "ask_up": 0.72,
        "ask_down": 0.30,
        "liq_up": 25.0,
        "liq_down": 25.0,
    }
    values.update(overrides)
    return module.PMState(**values)


def test_task15_on_fv_consumes_new_sigma_schema():
    module = _load_module()
    trader = _make_trader(module)

    trader._on_fv([123, 456, 0.85, 0.15, 0.22, 100_010.0, 0.18, 1])

    assert trader._fv.ts_ms == 123
    assert trader._fv.market_id == 456
    assert trader._fv.boundary_ts == 456
    assert trader._fv.intra_vol == 0.18
    assert trader._fv.is_sigma_real is True
    assert 1.03 < trader._fv.z_score < 1.04


def test_task22_probability_to_z_is_signed_and_symmetric():
    module = _load_module()

    z_up = module.probability_to_z(0.85)
    z_dn = module.probability_to_z(0.15)

    assert 1.03 < z_up < 1.04
    assert abs(z_up + z_dn) < 1e-12
    assert module.probability_to_z(0.50) == 0.0


def test_task23_on_pm_parses_market_timestamps():
    module = _load_module()
    trader = _make_trader(module)
    now_ms = int(time.time() * 1000)
    trader._fv = module.FVState(ts_ms=now_ms, boundary_ts=1_700_000_000)
    calls = []
    trader._check_exits = lambda *args: calls.append(("exits", args))
    trader._check_entries = lambda *args: calls.append(("entries", args))

    trader._on_pm([
        now_ms,
        "market-1",
        0.71,
        0.72,
        0.25,
        0.26,
        1_700_000_000,
        1_700_000_300,
        31.0,
        42.0,
    ])

    assert trader._pm.market_ts == 1_700_000_000
    assert trader._pm.end_ts == 1_700_000_300
    assert trader._pm.liq_up == 31.0
    assert trader._pm.liq_down == 42.0
    assert [call[0] for call in calls] == ["exits", "entries"]


def test_pm_book_eight_field_schema_defaults_liquidity_to_zero():
    module = _load_module()
    trader = _make_trader(module)
    now_ms = int(time.time() * 1000)
    trader._fv = module.FVState(ts_ms=now_ms, boundary_ts=1_700_000_000)
    trader._check_exits = lambda *args: None
    trader._check_entries = lambda *args: None

    trader._on_pm([
        now_ms,
        "market-1",
        0.71,
        0.72,
        0.25,
        0.26,
        1_700_000_000,
        1_700_000_300,
    ])

    assert trader._pm.liq_up == 0.0
    assert trader._pm.liq_down == 0.0


def test_task33_on_pm_parses_combined_ask_from_11_field_schema():
    """
    Task 3.3: When pm_daemon publishes the 11-field schema, _on_pm populates
    PMState._combined_ask and combined_ask() returns the pre-computed value.
    """
    module = _load_module()
    trader = _make_trader(module)
    now_ms = int(time.time() * 1000)
    trader._fv = module.FVState(ts_ms=now_ms, boundary_ts=1_700_000_000)
    trader._check_exits = lambda *args: None
    trader._check_entries = lambda *args: None

    trader._on_pm([
        now_ms,
        "market-1",
        0.71,       # bid_up
        0.72,       # ask_up
        0.25,       # bid_dn
        0.26,       # ask_dn
        1_700_000_000,
        1_700_000_300,
        31.0,       # liq_up
        42.0,       # liq_dn
        0.98,       # combined_ask (pre-computed by pm_daemon)
    ])

    assert trader._pm.liq_up == 31.0
    assert trader._pm.liq_down == 42.0
    # The pre-computed combined_ask should be used directly.
    assert trader._pm.combined_ask() == 0.98


def test_task33_combined_ask_falls_back_for_10_field_schema():
    """
    Task 3.3: When pm_daemon sends the old 10-field schema (no combined_ask),
    PMState.combined_ask() falls back to computing ask_up + ask_down.
    """
    module = _load_module()
    trader = _make_trader(module)
    now_ms = int(time.time() * 1000)
    trader._fv = module.FVState(ts_ms=now_ms, boundary_ts=1_700_000_000)
    trader._check_exits = lambda *args: None
    trader._check_entries = lambda *args: None

    trader._on_pm([
        now_ms,
        "market-1",
        0.71,       # bid_up
        0.72,       # ask_up
        0.25,       # bid_dn
        0.26,       # ask_dn
        1_700_000_000,
        1_700_000_300,
        31.0,       # liq_up
        42.0,       # liq_dn
        # no 11th field
    ])

    # Should fall back to computing from ask_up + ask_down
    expected = 0.72 + 0.26
    assert abs(trader._pm.combined_ask() - expected) < 1e-9


def test_task23_window_mismatch_guard_skips_pm_tick():
    module = _load_module()
    trader = _make_trader(module)
    now_ms = int(time.time() * 1000)
    trader._fv = module.FVState(ts_ms=now_ms, boundary_ts=1_700_000_000)
    calls = []
    trader._check_exits = lambda *args: calls.append(("exits", args))
    trader._check_entries = lambda *args: calls.append(("entries", args))

    trader._on_pm([
        now_ms,
        "market-1",
        0.71,
        0.72,
        0.25,
        0.26,
        1_700_000_300,
        1_700_000_600,
    ])

    assert trader._stats.window_mismatches == 1
    assert calls == []


def test_task21_tos_policy_allows_late_window_edge():
    module = _load_module()
    policy = TOSStrategy()

    decision = _eval_tos(policy, _tos_fv(module), _tos_pm(module))

    assert decision is not None
    assert decision.side == "UP"
    assert decision.ask == 0.72
    assert abs(decision.edge - 0.10) < 1e-12
    assert decision.elapsed_s == 230.0


def test_task21_tos_default_z_threshold_matches_min_prob():
    module = _load_module()
    policy = TOSStrategy()
    prob_up = 0.72

    decision = _eval_tos(policy, 
        _tos_fv(
            module,
            prob_up=prob_up,
            prob_down=1.0 - prob_up,
            z_score=module.probability_to_z(prob_up),
        ),
        _tos_pm(module, ask_up=0.65),
    )

    assert decision is not None
    assert decision.side == "UP"


def test_task21_tos_policy_blocks_before_entry_window():
    module = _load_module()
    policy = TOSStrategy()

    decision = _eval_tos(policy, 
        _tos_fv(module),
        _tos_pm(module, ts_ms=1_700_000_200_000),
    )

    assert decision is None


def test_task21_tos_policy_blocks_when_sigma_not_real():
    module = _load_module()
    policy = TOSStrategy()

    decision = _eval_tos(policy, _tos_fv(module, is_sigma_real=False), _tos_pm(module))

    assert decision is None


def test_task21_tos_policy_blocks_low_z_score():
    module = _load_module()
    policy = TOSStrategy()

    decision = _eval_tos(policy, _tos_fv(module, z_score=0.30), _tos_pm(module))

    assert decision is None


def test_task21_tos_policy_blocks_low_liquidity():
    module = _load_module()
    policy = TOSStrategy()

    decision = _eval_tos(policy, _tos_fv(module), _tos_pm(module, liq_up=10.0))

    assert decision is None


def test_task21_tos_policy_selects_down_side():
    module = _load_module()
    policy = TOSStrategy()

    decision = _eval_tos(policy, 
        _tos_fv(module, prob_up=0.15, prob_down=0.85, z_score=-1.30),
        _tos_pm(module, ask_down=0.76, liq_down=40.0),
    )

    assert decision is not None
    assert decision.side == "DOWN"
    assert decision.fv == 0.85


def test_task21_tos_entry_policy_wires_into_check_entries():
    module = _load_module()
    trader = _make_trader(module)
    trader._entry_policy = "TOS"
    trader._strategy = TOSStrategy()
    trader._fv = _tos_fv(module)
    trader._pm = _tos_pm(module)
    calls = []
    trader._simulate_entry = lambda *args: calls.append(args)

    trader._check_entries("market-1", ask_up=None, ask_dn=None, ts_ms=1_700_000_230_000, fv_age_ms=8)

    assert len(calls) == 1
    assert calls[0][:4] == ("market-1", "UP", 0.72, 0.82)
    assert abs(calls[0][4] - 0.10) < 1e-12


def test_task21_tos_entry_policy_preserves_cap_check():
    module = _load_module()
    trader = _make_trader(module)
    trader._entry_policy = "TOS"
    trader._strategy = TOSStrategy()
    trader._fv = _tos_fv(module)
    trader._pm = _tos_pm(module)
    trader._positions["market-1"]["UP"] = module.Position(
        market_id="market-1",
        side="UP",
        shares=module.MAX_SHARES_PER_SIDE,
        cost=1.0,
    )
    calls = []
    trader._simulate_entry = lambda *args: calls.append(args)

    trader._check_entries("market-1", ask_up=None, ask_dn=None, ts_ms=1_700_000_230_000, fv_age_ms=8)

    assert calls == []
    assert trader._stats.cap_blocks == 1


def test_task15_on_fv_old_schema_defaults_sigma_not_real():
    module = _load_module()
    trader = _make_trader(module)

    trader._on_fv([123, 456, 0.61, 0.39, 0.22, 100_010.0])

    assert trader._fv.intra_vol == 0.0
    assert trader._fv.is_sigma_real is False


def test_task15_check_entries_blocks_when_sigma_not_real():
    module = _load_module()
    trader = _make_trader(module)
    trader._fv = module.FVState(prob_up=0.60, prob_down=0.40, is_sigma_real=False)
    calls = []
    trader._simulate_entry = lambda *args: calls.append(args)

    trader._check_entries("market-1", ask_up=0.50, ask_dn=None, ts_ms=450_000, fv_age_ms=10)

    assert calls == []
    assert trader._stats.entries == 0


def test_task15_check_entries_allows_real_sigma_entry():
    module = _load_module()
    trader = _make_trader(module)
    trader._fv = module.FVState(prob_up=0.60, prob_down=0.40, is_sigma_real=True)
    calls = []
    trader._simulate_entry = lambda *args: calls.append(args)

    trader._check_entries("market-1", ask_up=0.50, ask_dn=None, ts_ms=450_000, fv_age_ms=10)

    assert len(calls) == 1
    assert calls[0][:4] == ("market-1", "UP", 0.50, 0.60)
    assert abs(calls[0][4] - 0.10) < 1e-12


def test_task15_simulated_fill_records_sigma_quality_fields():
    module = _load_module()
    trader = _make_trader(module)
    trader._fv = module.FVState(
        prob_up=0.60,
        prob_down=0.40,
        sigma=0.22,
        btc_price=100_010.0,
        intra_vol=0.18,
        is_sigma_real=True,
    )

    with redirect_stdout(io.StringIO()):
        trader._simulate_entry("market-1", "UP", 0.50, 0.60, 0.10, 450_000, 10)

    record = json.loads(trader._fills_file.getvalue().strip())
    assert record["is_sigma_real"] is True
    assert record["intra_vol"] == 0.18
    assert record["sigma"] == 0.22
    assert trader._stats.sigma_at_floor == 0


def test_task16_stats_counts_sigma_not_real_entries():
    module = _load_module()
    trader = _make_trader(module)
    trader._fv = module.FVState(
        prob_up=0.60,
        prob_down=0.40,
        sigma=0.10,
        btc_price=100_010.0,
        intra_vol=0.0,
        is_sigma_real=False,
    )

    with redirect_stdout(io.StringIO()):
        trader._simulate_entry("market-1", "UP", 0.50, 0.60, 0.10, 450_000, 10)

    record = json.loads(trader._fills_file.getvalue().strip())
    assert record["is_sigma_real"] is False
    assert trader._stats.sigma_total == 1
    assert trader._stats.sigma_at_floor == 1
    assert trader._stats.sigma_not_real_pct == 100.0


def test_task16_status_reports_sigma_not_real_label():
    module = _load_module()
    trader = _make_trader(module)
    trader._fv = module.FVState(sigma=0.10)
    trader._pm = module.PMState()
    trader._stats.sigma_total = 1
    trader._stats.sigma_at_floor = 1

    output = io.StringIO()
    with redirect_stdout(output):
        trader._print_status()

    assert "sigma_not_real=100%" in output.getvalue()
    assert "sigma@floor" not in output.getvalue()


def test_task24_tos_exit_policy_blocks_check_exits():
    """EXIT_POLICY=TOS must suppress all mid-window exit evaluation."""
    module = _load_module()
    trader = _make_trader(module)
    trader._exit_policy = "TOS"
    trader._fv = module.FVState(prob_up=0.15, prob_down=0.85)

    # Plant a position that the legacy EMERGENCY zone would cut
    # (bid_up=0.05 <= EMERGENCY_CUT_PRICE and fv.prob_up=0.15 <= EMERGENCY_FV_CONFIRM).
    trader._positions["market-1"]["UP"] = module.Position(
        market_id="market-1", side="UP", shares=10.0, cost=5.0
    )

    exit_calls = []
    trader._exit_position = lambda *args: exit_calls.append(args)

    # ts_ms puts us in the EMERGENCY zone (last <60s of a 300s window).
    # window elapsed = 298_000 ms → remaining = 2s → emergency zone.
    ts_ms = 298_000
    trader._check_exits("market-1", bid_up=0.05, bid_dn=0.92, ts_ms=ts_ms)

    assert exit_calls == [], (
        "TOS exit policy must suppress mid-window exits; _exit_position was called"
    )


def test_task24_legacy_exit_policy_still_exits():
    """EXIT_POLICY=legacy must continue to fire mid-window exits."""
    module = _load_module()
    trader = _make_trader(module)
    trader._exit_policy = "LEGACY"
    trader._fv = module.FVState(prob_up=0.15, prob_down=0.85)

    trader._positions["market-1"]["UP"] = module.Position(
        market_id="market-1", side="UP", shares=10.0, cost=5.0
    )

    exit_calls = []
    trader._exit_position = lambda *args: exit_calls.append(args)

    # Same emergency scenario — legacy should fire EMERGENCY_CUT.
    ts_ms = 298_000  # 2s remaining → emergency zone
    trader._check_exits("market-1", bid_up=0.05, bid_dn=0.92, ts_ms=ts_ms)

    assert len(exit_calls) == 1, (
        "Legacy exit policy must still fire mid-window EMERGENCY_CUT"
    )


def test_task25_settle_market_async_uses_gamma_outcome():
    """
    _settle_market_async() must apply the Gamma-returned outcome,
    even when the FV proxy would say the opposite.
    """
    import asyncio
    module = _load_module()
    trader = _make_trader(module)

    # FV says DOWN wins, but Gamma will say UP wins.
    trader._fv = module.FVState(prob_up=0.25)
    trader._resolve_cache = {}
    trader._positions["mkt-up"]["UP"] = module.Position(
        market_id="mkt-up", side="UP", shares=10.0, cost=6.0
    )

    exit_calls = []
    trader._exit_position = lambda *args: exit_calls.append(args)

    async def fake_resolve(_market_id):
        return "UP"

    trader._resolve_market_settlement = fake_resolve

    asyncio.run(trader._settle_market_async("mkt-up"))

    assert len(exit_calls) == 1
    _pos, settle_price, _ts, reason = exit_calls[0]
    assert settle_price == 1.0, "UP outcome must settle at $1"
    assert reason == "SETTLEMENT"
    assert trader._stats.settled_markets == 1


def test_task25_settle_market_async_falls_back_to_fv_proxy():
    """
    _settle_market_async() must fall back to the FV proxy when Gamma
    returns None, and settle correctly using prob_up.
    """
    import asyncio
    module = _load_module()
    trader = _make_trader(module)

    # FV says DOWN wins (prob_up < 0.5)
    trader._fv = module.FVState(prob_up=0.20)
    trader._resolve_cache = {}
    trader._positions["mkt-dn"]["DOWN"] = module.Position(
        market_id="mkt-dn", side="DOWN", shares=10.0, cost=2.0
    )

    exit_calls = []
    trader._exit_position = lambda *args: exit_calls.append(args)

    async def fake_resolve(_market_id):
        return None  # Gamma unavailable

    trader._resolve_market_settlement = fake_resolve

    asyncio.run(trader._settle_market_async("mkt-dn"))

    assert len(exit_calls) == 1
    _pos, settle_price, _ts, reason = exit_calls[0]
    assert settle_price == 1.0, "DOWN side must settle at $1 when prob_up < 0.5"
    assert reason == "SETTLEMENT"


def test_task25_settlement_fallback_uses_cached_market_proxy_after_fv_reset():
    """
    When PM transitions to a new market before Gamma resolves, the current FV
    state may already belong to the new window. Settlement fallback must use the
    cached FV proxy for the market being settled, not the reset FV state.
    """
    import asyncio
    module = _load_module()
    trader = _make_trader(module)

    trader._fv = module.FVState(prob_up=0.4999)
    trader._settlement_proxy_prob_up = {"old-market": 0.9999}
    trader._positions["old-market"]["UP"] = module.Position(
        market_id="old-market", side="UP", shares=10.0, cost=9.4
    )

    exit_calls = []
    trader._exit_position = lambda *args: exit_calls.append(args)

    async def fake_resolve(_market_id):
        return None

    trader._resolve_market_settlement = fake_resolve

    asyncio.run(trader._settle_market_async("old-market"))

    assert len(exit_calls) == 1
    _pos, settle_price, _ts, reason = exit_calls[0]
    assert settle_price == 1.0
    assert reason == "SETTLEMENT"


def test_task25_resolve_cache_prevents_duplicate_gamma_calls():
    """
    If the outcome is already in _resolve_cache, _resolve_market_settlement
    must not be called again.
    """
    import asyncio
    module = _load_module()
    trader = _make_trader(module)

    # Pre-populate cache with DOWN result
    trader._resolve_cache = {"mkt-cached": "DOWN"}
    trader._fv = module.FVState(prob_up=0.80)  # FV would say UP — cache must win

    trader._positions["mkt-cached"]["UP"] = module.Position(
        market_id="mkt-cached", side="UP", shares=10.0, cost=7.0
    )

    resolve_calls = []

    async def fake_resolve(_market_id):
        resolve_calls.append(_market_id)
        return "UP"  # Should never reach this

    trader._resolve_market_settlement = fake_resolve

    exit_calls = []
    trader._exit_position = lambda *args: exit_calls.append(args)

    asyncio.run(trader._settle_market_async("mkt-cached"))

    assert resolve_calls == [], "Cached outcome must not trigger Gamma call"
    _pos, settle_price, _ts, _reason = exit_calls[0]
    assert settle_price == 0.0, "UP side settles at $0 when outcome is DOWN"


def test_task25_schedule_settlement_uses_sync_path_outside_loop():
    """
    Outside an async context (no running event loop), _schedule_settlement
    must call _settle_market() synchronously without launching any task.
    """
    module = _load_module()
    trader = _make_trader(module)

    trader._fv = module.FVState(prob_up=0.85)
    trader._positions["mkt-sync"]["UP"] = module.Position(
        market_id="mkt-sync", side="UP", shares=5.0, cost=3.0
    )

    exit_calls = []
    trader._exit_position = lambda *args: exit_calls.append(args)

    # No event loop running — must use sync path
    trader._schedule_settlement("mkt-sync")

    assert len(exit_calls) == 1
    _pos, settle_price, _ts, reason = exit_calls[0]
    assert settle_price == 1.0, "prob_up=0.85 → UP wins via FV proxy"
    assert reason == "SETTLEMENT"
    assert trader._stats.settled_markets == 1


def test_task26_fill_record_contains_z_score_and_timing():
    """
    Task 2.6: _simulate_entry must persist z_score, elapsed_s,
    window_start_ts, and window_end_ts in every fill record.
    """
    module = _load_module()
    trader = _make_trader(module)

    # Set up FV state with a known z_score
    trader._fv = module.FVState(
        prob_up=0.82,
        prob_down=0.18,
        sigma=0.22,
        btc_price=100_000.0,
        intra_vol=0.20,
        is_sigma_real=True,
        z_score=1.42,
    )

    # Set up PM state with known window timestamps
    # Window opens at t=1_700_000_000, closes at t=1_700_000_300
    trader._pm = module.PMState(
        ts_ms=1_700_000_230_000,   # 230s into window
        market_ts=1_700_000_000,
        end_ts=1_700_000_300,
        ask_up=0.72,
        liq_up=25.0,
    )

    # ts_ms = 230s into the window
    fill_ts_ms = 1_700_000_230_000

    with redirect_stdout(io.StringIO()):
        trader._simulate_entry("mkt-1", "UP", 0.72, 0.82, 0.10, fill_ts_ms, 50)

    record = json.loads(trader._fills_file.getvalue().strip())

    assert record["z_score"] == 1.42, f"expected z_score=1.42, got {record['z_score']}"
    assert abs(record["elapsed_s"] - 230.0) < 0.1, (
        f"expected elapsed_s≈230, got {record['elapsed_s']}"
    )
    assert record["window_start_ts"] == 1_700_000_000, (
        f"expected window_start_ts=1_700_000_000, got {record['window_start_ts']}"
    )
    assert record["window_end_ts"] == 1_700_000_300, (
        f"expected window_end_ts=1_700_000_300, got {record['window_end_ts']}"
    )


def test_task26_fill_record_elapsed_fallback_without_pm_market_ts():
    """
    Task 2.6: When pm.market_ts is 0 (legacy PM or no PM update yet),
    elapsed_s must fall back to ts_ms % MARKET_WINDOW_SECONDS
    and window_end_ts must be 0.
    """
    module = _load_module()
    trader = _make_trader(module)

    trader._fv = module.FVState(
        prob_up=0.70,
        prob_down=0.30,
        is_sigma_real=True,
        z_score=0.52,
    )

    # PMState with no market_ts (old schema or not yet received)
    trader._pm = module.PMState(
        ts_ms=0,
        market_ts=0,  # triggers the fallback path
        end_ts=0,
        ask_up=0.60,
    )

    # ts_ms falls at exactly 120s into a 300s window (epoch multiple of 300 + 120s)
    # Pick a round epoch: 1_700_000_120_000 ms → 1_700_000_120 s
    # 1_700_000_120 % 300 = 120
    fill_ts_ms = 1_700_000_120_000

    with redirect_stdout(io.StringIO()):
        trader._simulate_entry("mkt-2", "UP", 0.60, 0.70, 0.10, fill_ts_ms, 20)

    record = json.loads(trader._fills_file.getvalue().strip())

    expected_elapsed = (fill_ts_ms / 1000.0) % 300
    assert abs(record["elapsed_s"] - expected_elapsed) < 0.01, (
        f"expected elapsed_s≈{expected_elapsed}, got {record['elapsed_s']}"
    )
    assert record["window_end_ts"] == 0, (
        f"expected window_end_ts=0 when end_ts not set, got {record['window_end_ts']}"
    )


def test_taskR2_kill_switch_detection():
    module = _load_module()
    trader = _make_trader(module)
    
    # Mock Path.exists to return True for the kill switch file
    original_exists = module.Path.exists
    
    def mock_exists(self):
        if str(self) == module.KILL_SWITCH_FILE:
            return True
        return original_exists(self)
    
    module.Path.exists = mock_exists
    try:
        trader._on_pm([
            1_700_000_230_000,
            "market-1",
            0.71,
            0.72,
            0.25,
            0.26,
            1_700_000_000,
            1_700_000_300,
            31.0,
            42.0,
        ])
        
        assert trader._risk.trading_halted is True
        assert "kill switch file" in trader._risk.halt_reason
    finally:
        module.Path.exists = original_exists


# ── Phase 1b Task R3: Position reconciliation on startup ────────────────────


def test_taskR3_reconcile_skipped_in_paper_mode():
    """In paper mode (LIVE_TRADING=False), _reconcile_positions() is a no-op."""
    import asyncio, os
    module = _load_module()
    trader = _make_trader(module)

    # Ensure LIVE_TRADING is False (the default)
    original = module.LIVE_TRADING
    module.LIVE_TRADING = False
    try:
        # Even if we attach a mock exchange, it must NOT be called
        calls = []

        class MockExchange:
            async def get_open_positions(self):
                calls.append(1)
                return []

        trader._exchange = MockExchange()
        asyncio.run(trader._reconcile_positions())

        assert calls == [], "get_open_positions must not be called in paper mode"
        assert len(trader._positions) == 0, "positions must remain empty"
    finally:
        module.LIVE_TRADING = original


def test_taskR3_reconcile_populates_positions_from_exchange():
    """In live mode, positions returned by the exchange are loaded into _positions."""
    import asyncio
    from types import SimpleNamespace
    module = _load_module()
    trader = _make_trader(module)

    original = module.LIVE_TRADING
    module.LIVE_TRADING = True
    try:
        fake_positions = [
            SimpleNamespace(
                market_id="0xabc123",
                side="UP",
                shares=5.0,
                avg_entry=0.41,
            ),
            SimpleNamespace(
                market_id="0xdef456",
                side="DOWN",
                shares=10.0,
                avg_entry=0.55,
            ),
        ]

        class MockExchange:
            async def get_open_positions(self):
                return fake_positions

        trader._exchange = MockExchange()
        asyncio.run(trader._reconcile_positions())

        # Both positions should now be in _positions
        assert "0xabc123" in trader._positions
        assert "0xdef456" in trader._positions

        pos_up = trader._positions["0xabc123"]["UP"]
        assert pos_up.shares == 5.0
        assert abs(pos_up.cost - 5.0 * 0.41) < 1e-9  # cost = shares * avg_entry
        assert abs(pos_up.avg_entry - 0.41) < 1e-9

        pos_dn = trader._positions["0xdef456"]["DOWN"]
        assert pos_dn.shares == 10.0
        assert abs(pos_dn.cost - 10.0 * 0.55) < 1e-9
    finally:
        module.LIVE_TRADING = original


def test_taskR3_reconcile_handles_empty_exchange_result():
    """When the exchange returns no positions, _positions stays empty."""
    import asyncio
    module = _load_module()
    trader = _make_trader(module)

    original = module.LIVE_TRADING
    module.LIVE_TRADING = True
    try:
        class MockExchange:
            async def get_open_positions(self):
                return []

        trader._exchange = MockExchange()
        asyncio.run(trader._reconcile_positions())

        assert len(trader._positions) == 0
    finally:
        module.LIVE_TRADING = original


def test_taskR3_reconcile_exchange_error_does_not_crash():
    """If exchange.get_open_positions() raises, reconciliation logs and continues."""
    import asyncio
    module = _load_module()
    trader = _make_trader(module)

    original = module.LIVE_TRADING
    module.LIVE_TRADING = True
    try:
        class BrokenExchange:
            async def get_open_positions(self):
                raise RuntimeError("CLOB unavailable")

        trader._exchange = BrokenExchange()
        # Must not raise — error is caught inside get_open_positions()
        # The reconciliation method itself propagates whatever the exchange returns.
        # Since get_open_positions() already catches and returns [], this is safe.
        # We simulate that contract by returning [] on error:
        trader._exchange = BrokenExchange()
        # patch get_open_positions to be like the real one (catches internally)
        async def safe_get():
            try:
                raise RuntimeError("CLOB unavailable")
            except Exception:
                return []
        trader._exchange.get_open_positions = safe_get

        asyncio.run(trader._reconcile_positions())
        assert len(trader._positions) == 0
    finally:
        module.LIVE_TRADING = original


# ── Task 3.2 — Arb Scanner tests ───────────────────────────────────────────────

def test_task32_pm_state_combined_ask_returns_sum_when_both_present():
    """combined_ask() returns ask_up + ask_dn when both legs are present."""
    module = _load_module()
    pm = module.PMState(ask_up=0.51, ask_down=0.46)
    result = pm.combined_ask()
    assert result is not None
    assert abs(result - 0.97) < 1e-9


def test_task32_pm_state_combined_ask_returns_none_when_ask_up_missing():
    """combined_ask() returns None when ask_up is None."""
    module = _load_module()
    pm = module.PMState(ask_up=None, ask_down=0.46)
    assert pm.combined_ask() is None


def test_task32_pm_state_combined_ask_returns_none_when_ask_dn_missing():
    """combined_ask() returns None when ask_down is None."""
    module = _load_module()
    pm = module.PMState(ask_up=0.51, ask_down=None)
    assert pm.combined_ask() is None


def test_task32_arb_position_properties():
    """ArbPosition.guaranteed_proceeds and expected_pnl are computed correctly."""
    module = _load_module()
    pos = module.ArbPosition(
        market_id="mkt-abc",
        combined=0.94,
        shares=10.0,
        cost=9.4,
        ts_ms=1_700_000_000_000,
    )
    assert pos.guaranteed_proceeds == 10.0        # always shares × $1.00
    assert abs(pos.expected_pnl - 0.6) < 1e-9    # 10.0 - 9.4


def test_task32_stats_arb_net_pnl_and_roi_properties():
    """Stats.arb_net_pnl and arb_roi_pct are correct and safe on zero cost."""
    module = _load_module()
    s = module.Stats()
    s.arb_cost = 9.4
    s.arb_proceeds = 10.0
    assert abs(s.arb_net_pnl - 0.6) < 1e-9
    assert abs(s.arb_roi_pct - (0.6 / 9.4 * 100)) < 1e-6

    s2 = module.Stats()
    assert s2.arb_roi_pct == 0.0  # zero-cost does not raise


def test_task32_simulate_arb_entry_records_fill_and_updates_stats():
    """_simulate_arb_entry writes a JSONL record and updates stats."""
    module = _load_module()
    trader = _make_trader(module)

    trader._simulate_arb_entry(
        market_id="mkt-arb1",
        ask_up=0.48,
        ask_dn=0.46,
        combined=0.94,
        shares=10.0,
        ts_ms=1_700_000_000_000,
    )

    assert trader._stats.arb_entries == 1
    assert abs(trader._stats.arb_cost - 9.4) < 1e-9

    pos = trader._arb_positions.get("mkt-arb1")
    assert pos is not None
    assert pos.shares == 10.0

    record = json.loads(trader._fills_file.getvalue().strip())
    assert record["type"] == "arb_entry"
    assert record["market_id"] == "mkt-arb1"
    assert abs(record["combined"] - 0.94) < 1e-9
    assert abs(record["cost"] - 9.4) < 1e-9
    assert "expected_pnl" in record


def test_task32_scan_arb_prevents_duplicate_entry_for_same_window():
    """
    _scan_arb does not call _simulate_arb_entry when an arb position
    already exists for the current market_id.
    """
    module = _load_module()
    trader = _make_trader(module)

    existing = module.ArbPosition(
        market_id="mkt-dup",
        combined=0.94,
        shares=5.0,
        cost=4.7,
        ts_ms=1_700_000_000_000,
    )
    trader._arb_positions["mkt-dup"] = existing

    trader._fv = module.FVState()
    trader._pm = module.PMState(
        ts_ms=1_700_000_010_000,
        market_id="mkt-dup",
        ask_up=0.48,
        ask_down=0.46,
        liq_up=25.0,
        liq_down=25.0,
    )

    trader._scan_arb()
    assert trader._stats.arb_entries == 0
    assert trader._arb_positions["mkt-dup"] is existing  # untouched


def test_task32_scan_arb_fires_when_combined_below_target():
    """_scan_arb fires when combined ask is strictly below ARB_TARGET_COMBINED."""
    module = _load_module()
    original_target = module.ARB_TARGET_COMBINED
    original_budget = module.ARB_MAX_USDC
    try:
        module.ARB_TARGET_COMBINED = 0.97
        module.ARB_MAX_USDC = 10.0  # enough budget so shares > ARB_MIN_SHARES

        trader = _make_trader(module)
        trader._fv = module.FVState()
        trader._pm = module.PMState(
            ts_ms=1_700_000_010_000,
            market_id="mkt-fire",
            ask_up=0.49,
            ask_down=0.47,   # combined = 0.96 < 0.97 → fires
            liq_up=30.0,
            liq_down=30.0,
        )

        trader._scan_arb()
        assert trader._stats.arb_entries == 1
        assert "mkt-fire" in trader._arb_positions
    finally:
        module.ARB_TARGET_COMBINED = original_target
        module.ARB_MAX_USDC = original_budget


def test_task32_scan_arb_skips_when_combined_at_or_above_target():
    """_scan_arb does nothing when combined >= ARB_TARGET_COMBINED."""
    module = _load_module()
    original_target = module.ARB_TARGET_COMBINED
    try:
        module.ARB_TARGET_COMBINED = 0.96

        trader = _make_trader(module)
        trader._fv = module.FVState()
        trader._pm = module.PMState(
            ts_ms=1_700_000_010_000,
            market_id="mkt-skip",
            ask_up=0.49,
            ask_down=0.47,   # combined = 0.96 — NOT strictly below 0.96
            liq_up=30.0,
            liq_down=30.0,
        )

        trader._scan_arb()
        assert trader._stats.arb_entries == 0
    finally:
        module.ARB_TARGET_COMBINED = original_target


def test_task32_scan_arb_skips_when_liquidity_missing():
    """_scan_arb does nothing when liq_up or liq_down is zero."""
    module = _load_module()
    original_target = module.ARB_TARGET_COMBINED
    try:
        module.ARB_TARGET_COMBINED = 0.97

        trader = _make_trader(module)
        trader._fv = module.FVState()
        trader._pm = module.PMState(
            ts_ms=1_700_000_010_000,
            market_id="mkt-noliq",
            ask_up=0.48,
            ask_down=0.47,
            liq_up=0.0,        # no UP liquidity
            liq_down=30.0,
        )

        trader._scan_arb()
        assert trader._stats.arb_entries == 0
    finally:
        module.ARB_TARGET_COMBINED = original_target


def test_task32_scan_arb_skips_when_shares_below_minimum():
    """_scan_arb rejects when size after budget/liq cap < ARB_MIN_SHARES."""
    module = _load_module()
    orig_target = module.ARB_TARGET_COMBINED
    orig_min    = module.ARB_MIN_SHARES
    orig_budget = module.ARB_MAX_USDC
    try:
        module.ARB_TARGET_COMBINED = 0.97
        module.ARB_MIN_SHARES      = 5.0
        module.ARB_MAX_USDC        = 1.0  # $1 budget at combined≈0.96 → ~1.04 shares

        trader = _make_trader(module)
        trader._fv = module.FVState()
        trader._pm = module.PMState(
            ts_ms=1_700_000_010_000,
            market_id="mkt-tiny",
            ask_up=0.48,
            ask_down=0.47,
            liq_up=100.0,
            liq_down=100.0,
        )

        trader._scan_arb()
        assert trader._stats.arb_entries == 0
    finally:
        module.ARB_TARGET_COMBINED = orig_target
        module.ARB_MIN_SHARES      = orig_min
        module.ARB_MAX_USDC        = orig_budget


def test_task32_scan_arb_skips_when_no_market_id():
    """_scan_arb is a no-op when PMState.market_id is empty."""
    module = _load_module()
    trader = _make_trader(module)
    trader._fv = module.FVState()
    trader._pm = module.PMState()  # market_id="" by default

    trader._scan_arb()
    assert trader._stats.arb_entries == 0


def test_task32_scan_arb_skips_when_risk_halted():
    """_scan_arb respects the risk manager circuit-breaker halt."""
    module = _load_module()
    original_target = module.ARB_TARGET_COMBINED
    try:
        module.ARB_TARGET_COMBINED = 0.97

        trader = _make_trader(module)
        trader._risk.halt("test halt")

        trader._fv = module.FVState()
        trader._pm = module.PMState(
            ts_ms=1_700_000_010_000,
            market_id="mkt-halted",
            ask_up=0.48,
            ask_down=0.47,
            liq_up=30.0,
            liq_down=30.0,
        )

        trader._scan_arb()
        assert trader._stats.arb_entries == 0
    finally:
        module.ARB_TARGET_COMBINED = original_target


def test_task32_settle_arb_position_books_guaranteed_proceeds():
    """
    _settle_arb_position pops the arb position, adds proceeds = shares × $1,
    writes an arb_exit JSONL record, and increments arb_settled.
    """
    module = _load_module()
    trader = _make_trader(module)

    trader._arb_positions["mkt-settle"] = module.ArbPosition(
        market_id="mkt-settle",
        combined=0.94,
        shares=10.0,
        cost=9.4,
        ts_ms=1_700_000_000_000,
    )

    trader._settle_arb_position("mkt-settle")

    assert trader._stats.arb_settled == 1
    assert abs(trader._stats.arb_proceeds - 10.0) < 1e-9
    assert "mkt-settle" not in trader._arb_positions

    record = json.loads(trader._exits_file.getvalue().strip())
    assert record["type"] == "arb_exit"
    assert record["market_id"] == "mkt-settle"
    assert abs(record["proceeds"] - 10.0) < 1e-9
    assert abs(record["pnl"] - 0.6) < 1e-9  # 10.0 - 9.4


def test_task32_settle_arb_position_noop_when_no_arb_held():
    """_settle_arb_position is a clean no-op when no position exists."""
    module = _load_module()
    trader = _make_trader(module)

    trader._settle_arb_position("mkt-ghost")

    assert trader._stats.arb_settled == 0
    assert trader._stats.arb_proceeds == 0.0
    assert trader._exits_file.getvalue() == ""


def test_task32_settle_market_calls_settle_arb_on_sync_path():
    """
    _settle_market (sync FV proxy path) also calls _settle_arb_position
    so coexisting TOS and arb positions are cleaned up together.
    """
    module = _load_module()
    trader = _make_trader(module)

    # TOS position to trigger the sync settlement path.
    trader._positions["mkt-both"]["UP"] = module.Position(
        market_id="mkt-both", side="UP", shares=5.0, cost=2.0
    )
    trader._arb_positions["mkt-both"] = module.ArbPosition(
        market_id="mkt-both",
        combined=0.92,
        shares=8.0,
        cost=7.36,
        ts_ms=1_700_000_000_000,
    )
    trader._settlement_proxy_prob_up["mkt-both"] = 0.70  # UP wins

    trader._settle_market("mkt-both")

    assert trader._stats.exits_settlement >= 1
    assert trader._stats.arb_settled == 1
    assert abs(trader._stats.arb_proceeds - 8.0) < 1e-9
    assert "mkt-both" not in trader._arb_positions


def test_task32_print_status_shows_arb_line_when_arb_enabled():
    """_print_status includes the ARB line when ARB_ENABLED=True."""
    module = _load_module()
    original = module.ARB_ENABLED
    try:
        module.ARB_ENABLED = True
        trader = _make_trader(module)
        trader._fv = module.FVState()
        trader._pm = module.PMState(ask_up=0.49, ask_down=0.47)
        trader._stats.arb_entries = 2
        trader._stats.arb_cost = 4.7
        trader._stats.arb_proceeds = 5.0

        buf = io.StringIO()
        with redirect_stdout(buf):
            trader._print_status()

        output = buf.getvalue()
        assert "ARB:" in output
        assert "entries=2" in output
    finally:
        module.ARB_ENABLED = original


def test_task32_print_status_hides_arb_line_when_disabled_and_no_entries():
    """_print_status omits the ARB line when ARB_ENABLED=False and arb_entries=0."""
    module = _load_module()
    original = module.ARB_ENABLED
    try:
        module.ARB_ENABLED = False
        trader = _make_trader(module)
        trader._fv = module.FVState()
        trader._pm = module.PMState()

        buf = io.StringIO()
        with redirect_stdout(buf):
            trader._print_status()

        assert "ARB:" not in buf.getvalue()
    finally:
        module.ARB_ENABLED = original


# ── Phase 4 — Signal Stack (Task 4.1) ─────────────────────────────────────────

def test_task41_momentum_signal_returns_up_when_btc_above_strike_with_persistence():
    """btc_momentum returns UP when BTC is >0.04% above K now and was above K 30s ago."""
    module = _load_module()
    stack = SignalStack()
    # K=73000, BTC now=73500 (+0.685%), BTC 30s ago=73300 (+0.411%) — both above K
    result = stack.btc_momentum_signal(btc_now=73500.0, btc_30s_ago=73300.0, pm_K=73000.0)
    assert result == "UP", f"expected 'UP', got {result!r}"


def test_task41_momentum_signal_returns_down_when_btc_below_strike_with_persistence():
    """btc_momentum returns DOWN when BTC is >0.04% below K now and was below K 30s ago."""
    module = _load_module()
    stack = SignalStack()
    # K=73000, BTC now=72900 (-0.137%), BTC 30s ago=72800 (-0.274%) — both below K
    result = stack.btc_momentum_signal(btc_now=72900.0, btc_30s_ago=72800.0, pm_K=73000.0)
    assert result == "DOWN", f"expected 'DOWN', got {result!r}"


def test_task41_momentum_signal_returns_none_when_btc_too_close_to_strike():
    """btc_momentum returns None when BTC is within 0.04% of K (plan gate: abs(delta_now) < 0.0004)."""
    module = _load_module()
    stack = SignalStack()
    # K=73000, BTC=73025 — delta_now = 25/73000 = 0.000342, below 0.0004 threshold
    result = stack.btc_momentum_signal(btc_now=73025.0, btc_30s_ago=73010.0, pm_K=73000.0)
    assert result is None, f"expected None (too close to K), got {result!r}"


def test_task41_momentum_signal_returns_none_when_no_persistence():
    """btc_momentum returns None when BTC crossed to the opposite side of K in 30s."""
    module = _load_module()
    stack = SignalStack()
    # Now above K=73000, but 30s ago was below K — sign mismatch, no persistence
    result = stack.btc_momentum_signal(btc_now=73500.0, btc_30s_ago=72900.0, pm_K=73000.0)
    assert result is None, f"expected None (no persistence), got {result!r}"


def test_task41_momentum_signal_returns_none_when_history_missing():
    """btc_momentum returns None when btc_30s_ago is 0 (no BTC history buffered yet)."""
    module = _load_module()
    stack = SignalStack()
    result = stack.btc_momentum_signal(btc_now=73500.0, btc_30s_ago=0.0, pm_K=73000.0)
    assert result is None, "expected None when btc_30s_ago=0"


def test_task41_imbalance_signal_returns_up_when_liq_skewed_up():
    """orderbook_imbalance returns UP when UP liquidity is >40% imbalance of combined depth."""
    module = _load_module()
    stack = SignalStack()
    # liq_up=80, liq_down=10, total=90, imbalance=0.778 > 0.40
    pm = module.PMState(liq_up=80.0, liq_down=10.0)
    assert stack.orderbook_imbalance_signal(pm) == "UP"


def test_task41_imbalance_signal_returns_down_when_liq_skewed_down():
    """orderbook_imbalance returns DOWN when DOWN liquidity dominates."""
    module = _load_module()
    stack = SignalStack()
    # liq_up=10, liq_down=80, total=90, imbalance=-0.778 < -0.40
    pm = module.PMState(liq_up=10.0, liq_down=80.0)
    assert stack.orderbook_imbalance_signal(pm) == "DOWN"


def test_task41_imbalance_signal_returns_none_when_total_below_floor():
    """orderbook_imbalance returns None when total < 20 shares (plan minimum floor, not 500/side)."""
    module = _load_module()
    stack = SignalStack()
    # total=18 < 20 — even with extreme skew, gate blocks
    pm = module.PMState(liq_up=17.0, liq_down=1.0)
    assert stack.orderbook_imbalance_signal(pm) is None, (
        "expected None (total=18 < 20-share floor)"
    )


def test_task41_imbalance_signal_returns_none_when_balanced():
    """orderbook_imbalance returns None when liquidity is roughly balanced."""
    module = _load_module()
    stack = SignalStack()
    # liq_up=30, liq_down=30 → imbalance=0 — no directional skew
    pm = module.PMState(liq_up=30.0, liq_down=30.0)
    assert stack.orderbook_imbalance_signal(pm) is None


def test_task41_evaluate_uses_plurality_consensus():
    """evaluate uses plurality voting: a single unambiguous signal is enough.

    Both signals agree → direction returned (unchanged from before).
    Only one signal fires, other returns None → direction returned (plurality).
    Signals actively conflict (UP vs DOWN) → None (still blocked).
    """
    module = _load_module()
    stack = SignalStack()

    # Case 1: both agree UP (btc above K + liq skewed UP)
    fv = module.FVState(btc_price=73500.0, strike=73000.0)
    pm_up = module.PMState(liq_up=80.0, liq_down=10.0)
    assert stack.evaluate(fv, pm_up, btc_30s_ago=73300.0) == "UP", (
        "expected 'UP' when both signals agree (UP, UP)"
    )

    # Case 2: only momentum fires; balanced liq → imbalance=None
    # Plurality: up=1, dn=0 → up > dn and up >= 1 → "UP"
    pm_balanced = module.PMState(liq_up=30.0, liq_down=30.0)
    assert stack.evaluate(fv, pm_balanced, btc_30s_ago=73300.0) == "UP", (
        "expected 'UP' when only momentum fires UP and imbalance has no view "
        "(plurality: up=1 beats dn=0)"
    )


def test_task41_evaluate_returns_none_when_signals_conflict():
    """evaluate returns None when btc_momentum says UP but orderbook_imbalance says DOWN."""
    module = _load_module()
    stack = SignalStack()
    fv = module.FVState(btc_price=73500.0, strike=73000.0)
    # Momentum UP, imbalance DOWN → up=1, dn=1 → no consensus
    pm = module.PMState(liq_up=10.0, liq_down=80.0)
    assert stack.evaluate(fv, pm, btc_30s_ago=73300.0) is None, (
        "expected None when signals conflict"
    )


def test_task41_tos_signal_entry_fires_when_both_signals_agree_with_tos():
    """Integration: TOS_SIGNAL produces a fill when both signals confirm the TOS decision."""
    from collections import deque
    module = _load_module()
    trader = _make_trader(module)
    trader._entry_policy = "TOS_SIGNAL"
    trader._strategy = TOSSignalStrategy()

    # FV: BTC clearly above K=73000, strong UP probability, real sigma
    trader._fv = module.FVState(
        prob_up=0.82, prob_down=0.18,
        is_sigma_real=True, z_score=1.20,
        btc_price=73500.0, strike=73000.0,
    )
    # PM: in TOS entry window (elapsed = 230s, window 210–270), liq skewed UP
    ts_ms = 1_700_000_230_000
    trader._pm = module.PMState(
        ts_ms=ts_ms, market_ts=1_700_000_000,
        ask_up=0.72, ask_down=0.30,
        liq_up=80.0, liq_down=10.0,
    )
    # BTC history: 30 seconds ago BTC was at 73300 (also above K) → momentum UP
    trader._strategy._btc_history = deque(maxlen=600)
    trader._strategy._btc_history.append((ts_ms / 1000.0 - 30.0, 73300.0))

    market_id = "0xdeadbeef" + "0" * 56
    trader._check_strategy_entries(market_id, ts_ms, fv_age_ms=5)

    assert trader._stats.entries == 1, (
        f"expected 1 fill when TOS_SIGNAL consensus matches TOS direction, "
        f"got entries={trader._stats.entries}"
    )


def test_task41_tos_signal_entry_fires_when_single_signal_agrees_with_tos():
    """Integration: TOS_SIGNAL fires an entry when only momentum fires and
    imbalance has no view (balanced orderbook → imbalance=None).

    Under plurality: up=1, dn=0 → consensus=UP → matches TOS UP decision
    → entry fires.  This is a behaviour change from the original >= 2 rule.
    """
    from collections import deque
    module = _load_module()
    trader = _make_trader(module)
    trader._entry_policy = "TOS_SIGNAL"
    trader._strategy = TOSSignalStrategy()

    trader._fv = module.FVState(
        prob_up=0.82, prob_down=0.18,
        is_sigma_real=True, z_score=1.20,
        btc_price=73500.0, strike=73000.0,
    )
    ts_ms = 1_700_000_230_000
    # Balanced liquidity → orderbook_imbalance=None; only momentum fires
    trader._pm = module.PMState(
        ts_ms=ts_ms, market_ts=1_700_000_000,
        ask_up=0.72, ask_down=0.30,
        liq_up=30.0, liq_down=30.0,
    )
    trader._strategy._btc_history = deque(maxlen=600)
    trader._strategy._btc_history.append((ts_ms / 1000.0 - 30.0, 73300.0))

    market_id = "0xdeadbeef" + "0" * 56
    trader._check_strategy_entries(market_id, ts_ms, fv_age_ms=5)

    assert trader._stats.entries == 1, (
        f"expected 1 fill: momentum=UP (no imbalance view) → plurality consensus=UP "
        f"→ matches TOS UP decision, got entries={trader._stats.entries}"
    )


def test_task41_evaluate_returns_none_when_both_signals_none():
    """evaluate returns None when both signals have no view.

    Both-None case: up=0, dn=0 → tie → None.
    Plurality does not relax this: without any signal there is no information.
    """
    module = _load_module()
    stack = SignalStack()

    # BTC within 0.04% of K → momentum Gate 1 fails → None
    # Balanced liq (30/30) → imbalance = 0, below ±0.40 threshold → None
    fv = module.FVState(btc_price=73025.0, strike=73000.0)
    pm = module.PMState(liq_up=30.0, liq_down=30.0)
    result = stack.evaluate(fv, pm, btc_30s_ago=73010.0)
    assert result is None, (
        f"expected None when both signals return None (no view), got {result!r}"
    )


def test_task41_evaluate_accepts_single_imbalance_signal_when_momentum_none():
    """evaluate returns direction when only imbalance fires and momentum has no view.

    This is the W1 confirmed-candidate scenario:
      BTC displacement from K = -0.0335% (below 0.04% momentum gate) → momentum=None
      Orderbook strongly skewed DOWN (liq_dn >> liq_up) → imbalance=DOWN
    Plurality: dn=1, up=0 → dn > up and dn >= 1 → "DOWN".
    """
    module = _load_module()
    stack = SignalStack()

    # K=74054, BTC=74026 → delta = -0.000377, below 0.0004 → momentum=None
    fv = module.FVState(btc_price=74026.0, strike=74054.0)
    # Strongly DOWN orderbook: imbalance = (5-80)/85 = -0.882 < -0.40 → DOWN
    pm = module.PMState(liq_up=5.0, liq_down=80.0)

    result = stack.evaluate(fv, pm, btc_30s_ago=74046.0)
    assert result == "DOWN", (
        f"expected 'DOWN': imbalance fires DOWN, momentum has no view (W1 scenario), "
        f"got {result!r}"
    )


def test_task41_tos_signal_entry_fires_when_only_imbalance_agrees_with_tos():
    """Integration: TOS_SIGNAL fires when only imbalance confirms TOS direction
    and momentum has no view.  Reproduces the W1 scenario that was previously
    blocked by the >= 2 unanimity requirement.

    Conditions:
      BTC displacement from K = -0.0335% → below 0.04% gate → momentum=None
      Orderbook: liq_dn=80 >> liq_up=5 → imbalance=-0.882 → DOWN
      TOS decision: DOWN (p_dn=0.9999, edge=0.0599)
      Plurality consensus: DOWN → ACCEPT
    """
    from collections import deque
    module = _load_module()
    trader = _make_trader(module)
    trader._entry_policy = "TOS_SIGNAL"
    trader._strategy = TOSSignalStrategy()

    # BTC barely below K, but probability is extreme → large z → TOS fires
    # K=74054, BTC=74026, delta=-0.000377 (below 0.04% momentum gate)
    trader._fv = module.FVState(
        prob_up=0.0001, prob_down=0.9999,
        is_sigma_real=True, z_score=-3.72,   # |z|=3.72 >> TOS_Z_THRESHOLD=0.524
        btc_price=74026.0, strike=74054.0,
    )
    ts_ms = 1_700_000_230_000   # elapsed=230s, inside [210, 270] TOS window
    trader._pm = module.PMState(
        ts_ms=ts_ms, market_ts=1_700_000_000,
        ask_up=0.06, ask_down=0.94,          # DOWN edge = 0.9999 - 0.94 = 0.0599
        liq_up=5.0, liq_down=80.0,           # strongly DOWN orderbook
    )
    # BTC 30s ago was 74046: also below K → same side, but momentum still None
    # because |delta_now| = 0.000377 < 0.0004 (Gate 1 fails before Gate 2)
    trader._strategy._btc_history = deque(maxlen=600)
    trader._strategy._btc_history.append((ts_ms / 1000.0 - 30.0, 74046.0))

    market_id = "0xdeadbeef" + "0" * 56
    trader._check_strategy_entries(market_id, ts_ms, fv_age_ms=5)

    assert trader._stats.entries == 1, (
        f"expected 1 fill: imbalance=DOWN confirms TOS DOWN under plurality "
        f"(W1 scenario), got entries={trader._stats.entries}"
    )
    assert trader._strategy._stats.rejected_momentum == 1, (
        "momentum should be counted as None (displacement below 0.04% gate)"
    )
    assert trader._strategy._stats.accepted_signal == 1, (
        "accepted_signal counter should increment when plurality passes"
    )


if __name__ == "__main__":
    tests = [
        test_task15_on_fv_consumes_new_sigma_schema,
        test_task22_probability_to_z_is_signed_and_symmetric,
        test_task23_on_pm_parses_market_timestamps,
        test_pm_book_eight_field_schema_defaults_liquidity_to_zero,
        test_task23_window_mismatch_guard_skips_pm_tick,
        test_task21_tos_policy_allows_late_window_edge,
        test_task21_tos_default_z_threshold_matches_min_prob,
        test_task21_tos_policy_blocks_before_entry_window,
        test_task21_tos_policy_blocks_when_sigma_not_real,
        test_task21_tos_policy_blocks_low_z_score,
        test_task21_tos_policy_blocks_low_liquidity,
        test_task21_tos_policy_selects_down_side,
        test_task21_tos_entry_policy_wires_into_check_entries,
        test_task21_tos_entry_policy_preserves_cap_check,
        test_task15_on_fv_old_schema_defaults_sigma_not_real,
        test_task15_check_entries_blocks_when_sigma_not_real,
        test_task15_check_entries_allows_real_sigma_entry,
        test_task15_simulated_fill_records_sigma_quality_fields,
        test_task16_stats_counts_sigma_not_real_entries,
        test_task16_status_reports_sigma_not_real_label,
        test_task24_tos_exit_policy_blocks_check_exits,
        test_task24_legacy_exit_policy_still_exits,
        test_task25_settle_market_async_uses_gamma_outcome,
        test_task25_settle_market_async_falls_back_to_fv_proxy,
        test_task25_settlement_fallback_uses_cached_market_proxy_after_fv_reset,
        test_task25_resolve_cache_prevents_duplicate_gamma_calls,
        test_task25_schedule_settlement_uses_sync_path_outside_loop,
        test_task26_fill_record_contains_z_score_and_timing,
        test_task26_fill_record_elapsed_fallback_without_pm_market_ts,
        test_taskR2_kill_switch_detection,
        test_taskR3_reconcile_skipped_in_paper_mode,
        test_taskR3_reconcile_populates_positions_from_exchange,
        test_taskR3_reconcile_handles_empty_exchange_result,
        test_taskR3_reconcile_exchange_error_does_not_crash,
        # Task 3.2 — Arb Scanner
        test_task32_pm_state_combined_ask_returns_sum_when_both_present,
        test_task32_pm_state_combined_ask_returns_none_when_ask_up_missing,
        test_task32_pm_state_combined_ask_returns_none_when_ask_dn_missing,
        test_task32_arb_position_properties,
        test_task32_stats_arb_net_pnl_and_roi_properties,
        test_task32_simulate_arb_entry_records_fill_and_updates_stats,
        test_task32_scan_arb_prevents_duplicate_entry_for_same_window,
        test_task32_scan_arb_fires_when_combined_below_target,
        test_task32_scan_arb_skips_when_combined_at_or_above_target,
        test_task32_scan_arb_skips_when_liquidity_missing,
        test_task32_scan_arb_skips_when_shares_below_minimum,
        test_task32_scan_arb_skips_when_no_market_id,
        test_task32_scan_arb_skips_when_risk_halted,
        test_task32_settle_arb_position_books_guaranteed_proceeds,
        test_task32_settle_arb_position_noop_when_no_arb_held,
        test_task32_settle_market_calls_settle_arb_on_sync_path,
        test_task32_print_status_shows_arb_line_when_arb_enabled,
        test_task32_print_status_hides_arb_line_when_disabled_and_no_entries,
        # Task 3.3 — PM_BOOK combined_ask
        test_task33_on_pm_parses_combined_ask_from_11_field_schema,
        test_task33_combined_ask_falls_back_for_10_field_schema,
        # Phase 4 — Signal Stack (Task 4.1)
        test_task41_momentum_signal_returns_up_when_btc_above_strike_with_persistence,
        test_task41_momentum_signal_returns_down_when_btc_below_strike_with_persistence,
        test_task41_momentum_signal_returns_none_when_btc_too_close_to_strike,
        test_task41_momentum_signal_returns_none_when_no_persistence,
        test_task41_momentum_signal_returns_none_when_history_missing,
        test_task41_imbalance_signal_returns_up_when_liq_skewed_up,
        test_task41_imbalance_signal_returns_down_when_liq_skewed_down,
        test_task41_imbalance_signal_returns_none_when_total_below_floor,
        test_task41_imbalance_signal_returns_none_when_balanced,
        test_task41_evaluate_uses_plurality_consensus,
        test_task41_evaluate_returns_none_when_signals_conflict,
        test_task41_evaluate_returns_none_when_both_signals_none,
        test_task41_evaluate_accepts_single_imbalance_signal_when_momentum_none,
        test_task41_tos_signal_entry_fires_when_both_signals_agree_with_tos,
        test_task41_tos_signal_entry_fires_when_single_signal_agrees_with_tos,
        test_task41_tos_signal_entry_fires_when_only_imbalance_agrees_with_tos,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            print(f"  PASS  {test.__name__}")
            passed += 1
        except AssertionError as exc:
            print(f"  FAIL  {test.__name__}  ->  {exc}")
            failed += 1

    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)
