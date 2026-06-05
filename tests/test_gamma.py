"""
tests/test_gamma.py
───────────────────
Unit tests for Task 2.7: MARKET_TYPE=5m|15m slug parameterization in gamma.py.

Tests are import-time isolated: gamma.py imports config.py (which requires
credentials), so we stub out both config and aiohttp before loading the module.

Run:
    python tests/test_gamma.py
    python -m pytest tests/test_gamma.py -v
"""

from __future__ import annotations

import importlib.util
import math
import os
import sys
import types
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


# ── Stub installer ────────────────────────────────────────────────────────────

def _install_gamma_stubs() -> None:
    """Stub out credential-dependent and network modules for offline testing."""

    # dotenv — noop
    dotenv_mod = types.ModuleType("dotenv")
    dotenv_mod.load_dotenv = lambda *args, **kwargs: None
    sys.modules.setdefault("dotenv", dotenv_mod)

    # config — provide the constants gamma.py imports, no credentials needed
    config_mod = types.ModuleType("config")
    config_mod.CLOB_HOST            = "https://clob.polymarket.com"
    config_mod.GAMMA_HOST           = "https://gamma-api.polymarket.com"
    config_mod.ORACLE_WAIT_SECONDS  = 320
    config_mod.MARKET_WINDOW_SECONDS = 300   # default 5m; overridden per test
    sys.modules["config"] = config_mod

    # aiohttp — stub ClientSession so gamma.py can be imported without network
    aiohttp_mod = types.ModuleType("aiohttp")
    aiohttp_mod.ClientSession          = object
    aiohttp_mod.ClientResponseError    = Exception
    sys.modules.setdefault("aiohttp", aiohttp_mod)

    # loguru — suppress log noise during tests
    loguru_mod = types.ModuleType("loguru")
    logger_stub = types.SimpleNamespace(
        info=lambda *a, **k: None,
        debug=lambda *a, **k: None,
        warning=lambda *a, **k: None,
        error=lambda *a, **k: None,
    )
    loguru_mod.logger = logger_stub
    sys.modules.setdefault("loguru", loguru_mod)


def _load_gamma(market_type: str = "5m", window_seconds: int = 300):
    """
    Load gamma.py with MARKET_TYPE and MARKET_WINDOW_SECONDS set to the
    given values.  Returns the module.  Each call reloads the module fresh
    (important because MARKET_TYPE is a module-level constant).
    """
    _install_gamma_stubs()

    # Patch the values the module will read at import time
    os.environ["MARKET_TYPE"] = market_type
    sys.modules["config"].MARKET_WINDOW_SECONDS = window_seconds  # type: ignore[attr-defined]

    # Force a fresh module load (not from cache) so the new env values take effect
    module_key = f"gamma_under_test_{market_type}"
    if module_key in sys.modules:
        del sys.modules[module_key]

    module_path = REPO_ROOT / "core" / "gamma.py"
    spec = importlib.util.spec_from_file_location(module_key, module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_key] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_task27_default_market_type_is_5m():
    """
    Without MARKET_TYPE set, the default should be '5m' and slugs should
    use the btc-updown-5m-* pattern.
    """
    os.environ.pop("MARKET_TYPE", None)
    gamma = _load_gamma(market_type="5m", window_seconds=300)

    assert gamma.MARKET_TYPE == "5m"

    md = gamma.MarketDiscovery()
    slugs = md._candidate_slugs(lookahead_windows=2)

    assert len(slugs) > 0
    for slug in slugs:
        assert slug.startswith("btc-updown-5m-"), (
            f"Expected 5m slug prefix, got: {slug!r}"
        )


def test_task27_15m_type_generates_15m_slugs():
    """
    MARKET_TYPE=15m must generate btc-updown-15m-* slugs, not 5m ones.
    """
    gamma = _load_gamma(market_type="15m", window_seconds=900)

    assert gamma.MARKET_TYPE == "15m"

    md = gamma.MarketDiscovery()
    slugs = md._candidate_slugs(lookahead_windows=2)

    assert len(slugs) > 0
    for slug in slugs:
        assert slug.startswith("btc-updown-15m-"), (
            f"Expected 15m slug prefix, got: {slug!r}"
        )
    # Must NOT contain any 5m slugs
    assert not any("btc-updown-5m-" in s for s in slugs), (
        "15m mode must not generate 5m slugs"
    )


def test_task27_5m_slug_timestamps_align_to_300s_grid():
    """
    5m slug timestamps must be multiples of 300 seconds (the 5-min window grid).
    """
    gamma = _load_gamma(market_type="5m", window_seconds=300)
    md    = gamma.MarketDiscovery()
    slugs = md._candidate_slugs(lookahead_windows=4)

    for slug in slugs:
        ts_str = slug.split("-")[-1]
        ts     = int(ts_str)
        assert ts % 300 == 0, (
            f"5m slug timestamp {ts} is not a multiple of 300: {slug!r}"
        )


def test_task27_15m_slug_timestamps_align_to_900s_grid():
    """
    15m slug timestamps must be multiples of 900 seconds (the 15-min window grid).
    """
    gamma = _load_gamma(market_type="15m", window_seconds=900)
    md    = gamma.MarketDiscovery()
    slugs = md._candidate_slugs(lookahead_windows=4)

    for slug in slugs:
        ts_str = slug.split("-")[-1]
        ts     = int(ts_str)
        assert ts % 900 == 0, (
            f"15m slug timestamp {ts} is not a multiple of 900: {slug!r}"
        )


def test_task27_slug_count_covers_lookahead():
    """
    _candidate_slugs(lookahead_windows=N) must return exactly N+2 slugs
    (1 behind + current + N ahead).
    """
    gamma = _load_gamma(market_type="5m", window_seconds=300)
    md    = gamma.MarketDiscovery()

    for n in [0, 2, 5, 14]:
        slugs = md._candidate_slugs(lookahead_windows=n)
        expected = n + 2  # range(-1, n+1) = n+2 iterations
        assert len(slugs) == expected, (
            f"lookahead_windows={n}: expected {expected} slugs, got {len(slugs)}"
        )


def test_task27_invalid_market_type_raises():
    """
    An unsupported MARKET_TYPE value must raise ValueError at import time,
    not silently fall back to the wrong prefix.
    """
    import importlib.util as ilu

    _install_gamma_stubs()
    os.environ["MARKET_TYPE"] = "invalid"
    sys.modules["config"].MARKET_WINDOW_SECONDS = 300  # type: ignore[attr-defined]

    module_key = "gamma_under_test_invalid"
    if module_key in sys.modules:
        del sys.modules[module_key]

    module_path = REPO_ROOT / "core" / "gamma.py"
    spec = ilu.spec_from_file_location(module_key, module_path)
    assert spec and spec.loader
    module = ilu.module_from_spec(spec)
    sys.modules[module_key] = module

    try:
        spec.loader.exec_module(module)  # type: ignore[union-attr]
        assert False, "Expected ValueError for unsupported MARKET_TYPE"
    except ValueError as exc:
        assert "invalid" in str(exc).lower() or "unsupported" in str(exc).lower(), (
            f"Unexpected ValueError message: {exc}"
        )
    finally:
        # Clean up so other tests aren't affected
        os.environ.pop("MARKET_TYPE", None)
        sys.modules.pop(module_key, None)


def test_task27_slug_prefix_map_has_both_types():
    """
    The module-level _SLUG_PREFIX dict must contain entries for both '5m' and '15m'.
    """
    gamma = _load_gamma(market_type="5m", window_seconds=300)

    assert "5m"  in gamma._SLUG_PREFIX, "Missing '5m' key in _SLUG_PREFIX"
    assert "15m" in gamma._SLUG_PREFIX, "Missing '15m' key in _SLUG_PREFIX"
    assert gamma._SLUG_PREFIX["5m"]  == "btc-updown-5m"
    assert gamma._SLUG_PREFIX["15m"] == "btc-updown-15m"


# ── Self-running ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_task27_default_market_type_is_5m,
        test_task27_15m_type_generates_15m_slugs,
        test_task27_5m_slug_timestamps_align_to_300s_grid,
        test_task27_15m_slug_timestamps_align_to_900s_grid,
        test_task27_slug_count_covers_lookahead,
        test_task27_invalid_market_type_raises,
        test_task27_slug_prefix_map_has_both_types,
    ]

    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except AssertionError as exc:
            print(f"  FAIL  {t.__name__}  →  {exc}")
            failed += 1
        except Exception as exc:
            print(f"  ERROR {t.__name__}  →  {type(exc).__name__}: {exc}")
            failed += 1

    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)
