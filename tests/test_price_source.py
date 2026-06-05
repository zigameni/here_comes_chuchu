"""
tests/test_price_source.py
──────────────────────────
Tests that document the Futures vs Spot price source bug and verify the fix.

The Polymarket BTC UP/DOWN 5-min markets resolve via the Chainlink BTC/USD
data stream — a spot-price aggregate.  Using Binance Futures introduces a
basis premium (typically $50–200 on a $100k BTC) that shifts the FV engine's
entire probability curve.

Run:
    python tests/test_price_source.py
    python -m pytest tests/test_price_source.py -v
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_daemon_uses_spot_not_futures():
    """
    The binance_daemon must connect to Binance SPOT, not Futures.

    Futures endpoint:  wss://fstream.binance.com   ← wrong ($100 premium)
    Spot endpoint:     wss://stream.binance.com     ← correct (matches Chainlink)
    """
    daemon_src = Path(__file__).resolve().parents[1] / "cmd" / "binance_daemon.py"
    text = daemon_src.read_text(encoding="utf-8")

    # The active (uncommented) BINANCE_WS_URL must point to spot
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("BINANCE_WS_URL") and "=" in stripped and not stripped.startswith("#"):
            assert "stream.binance.com" in stripped, (
                f"Active BINANCE_WS_URL must use stream.binance.com (spot), got:\n  {stripped}"
            )
            assert "fstream.binance.com" not in stripped, (
                f"Active BINANCE_WS_URL must NOT use fstream.binance.com (futures), got:\n  {stripped}"
            )
            return  # found and checked

    raise AssertionError("Could not find an active BINANCE_WS_URL assignment in binance_daemon.py")


def test_futures_url_documented_as_comment():
    """
    The old futures URL should still exist as a commented reference
    so the difference is visible in code review.
    """
    daemon_src = Path(__file__).resolve().parents[1] / "cmd" / "binance_daemon.py"
    text = daemon_src.read_text(encoding="utf-8")
    assert "fstream.binance.com" in text, (
        "The futures URL should be documented as a comment for reference"
    )


def test_price_basis_impact_on_fv():
    """
    Quantify the FV error caused by a $100 futures premium.

    At $100k BTC, a $100 basis shifts the FV engine's K-relative signal
    by $100 in the wrong direction.  This can easily flip a near-ATM
    market from DOWN-edge to UP-edge — a complete signal reversal.
    """
    from shared.math_utils import black_scholes_prob

    K     = 100_000.0   # window open price (strike)
    T     = 3 * 60 / (365.25 * 24 * 3600)   # 3 min left
    sigma = 0.60  # realistic BTC vol

    # Actual BTC spot price: $50 below K (DOWN is correct call)
    S_spot    = 99_950.0
    # Futures mid: $100 premium above spot
    S_futures = S_spot + 100.0   # = 100_050.0

    p_spot    = black_scholes_prob(S_spot,    K, T, sigma)
    p_futures = black_scholes_prob(S_futures, K, T, sigma)

    # Spot correctly shows DOWN edge (P(UP) < 0.5)
    assert p_spot < 0.50, (
        f"Spot price below K: P(UP)={p_spot:.4f} should be < 0.50 (DOWN edge)"
    )

    # Futures incorrectly shows UP edge (P(UP) > 0.5) — signal reversal
    assert p_futures > 0.50, (
        f"Futures price above K: P(UP)={p_futures:.4f} should be > 0.50 (wrong signal)"
    )

    print(
        f"\n  Spot S={S_spot}: P(UP)={p_spot:.4f} -> DOWN edge  OK"
        f"\n  Futures S={S_futures} (+$100 basis): P(UP)={p_futures:.4f} -> UP edge  WRONG"
        f"\n  Signal reversal due to basis: {p_futures - p_spot:+.4f}"
    )


def test_chainlink_contract_address_in_daemon():
    """
    The Chainlink BTC/USD Polygon mainnet contract address must be present
    in the daemon for the optional cross-check feature.
    """
    daemon_src = Path(__file__).resolve().parents[1] / "cmd" / "binance_daemon.py"
    text = daemon_src.read_text(encoding="utf-8")
    # Polygon mainnet Chainlink BTC/USD proxy
    assert "0xc907E116054Ad103354f2D350FD2514433D57F6F" in text, (
        "Chainlink BTC/USD Polygon address must be present for the cross-check feature"
    )


# ── Self-running ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_daemon_uses_spot_not_futures,
        test_futures_url_documented_as_comment,
        test_price_basis_impact_on_fv,
        test_chainlink_contract_address_in_daemon,
    ]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS  {t.__name__}")
            passed += 1
        except AssertionError as e:
            print(f"  FAIL  {t.__name__}  →  {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(0 if failed == 0 else 1)
