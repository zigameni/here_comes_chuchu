#!/usr/bin/env python3
"""
Read-only live readiness checker.

This does not prove the strategy is profitable. It only checks whether the
runtime configuration is fail-closed enough to even consider launching live.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
PLACEHOLDERS = {
    "",
    "0xyour_private_key_here",
    "your_private_key_here",
    "your_api_key_here",
    "your_secret_here",
    "your_passphrase_here",
    "your_wallet_address_here",
}


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


class Report:
    def __init__(self) -> None:
        self.failures: list[str] = []
        self.warnings: list[str] = []
        self.passes: list[str] = []

    def ok(self, msg: str) -> None:
        self.passes.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    def fail(self, msg: str) -> None:
        self.failures.append(msg)

    def print(self) -> None:
        print("BTC bot live readiness")
        print("=" * 24)
        for msg in self.passes:
            print(f"OK    {msg}")
        for msg in self.warnings:
            print(f"WARN  {msg}")
        for msg in self.failures:
            print(f"FAIL  {msg}")
        print()
        print(f"Summary: {len(self.passes)} ok, {len(self.warnings)} warning(s), {len(self.failures)} failure(s)")


def truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def env_value(name: str) -> str:
    return os.getenv(name, "").strip()


def is_placeholder(value: str) -> bool:
    return value.strip().lower() in PLACEHOLDERS


def require_env(report: Report, names: Iterable[str]) -> None:
    for name in names:
        value = env_value(name)
        if not value:
            report.fail(f"{name} is missing")
        elif is_placeholder(value):
            report.fail(f"{name} is still a placeholder")
        else:
            report.ok(f"{name} is present")


def check_numeric_limit(report: Report, name: str, default: float, max_allowed: float, unit: str) -> None:
    raw = env_value(name)
    try:
        value = float(raw) if raw else default
    except ValueError:
        report.fail(f"{name} is not numeric: {raw!r}")
        return
    if value <= 0:
        report.fail(f"{name} must be positive, got {value:g}")
    elif value > max_allowed:
        report.warn(f"{name}={value:g}{unit} is above conservative go-live cap {max_allowed:g}{unit}")
    else:
        report.ok(f"{name}={value:g}{unit}")


def check_pidfile(report: Report, pidfile: Path) -> None:
    if not pidfile.exists():
        report.ok(f"{pidfile.name} is absent")
        return
    live = []
    for line in pidfile.read_text(encoding="utf-8", errors="replace").splitlines():
        parts = line.split()
        if not parts:
            continue
        try:
            pid = int(parts[0])
        except ValueError:
            continue
        proc_path = Path("/proc") / str(pid)
        if proc_path.exists():
            live.append(str(pid))
    if live:
        report.warn(f"{pidfile.name} has running process id(s): {', '.join(live)}")
    else:
        report.warn(f"{pidfile.name} exists but no listed process appears alive; clean stale run artifacts before live")


def main() -> int:
    parser = argparse.ArgumentParser(description="Check BTC bot live-readiness configuration.")
    parser.add_argument("--env-file", default=".env", help="env file to read before checks")
    parser.add_argument("--mode", choices=["paper", "live"], default=None, help="expected TRADING_MODE")
    args = parser.parse_args()

    load_dotenv(ROOT / args.env_file)
    report = Report()

    mode = env_value("TRADING_MODE") or "paper"
    live_trading = truthy(env_value("LIVE_TRADING"))
    expected_mode = args.mode or mode

    if mode not in {"paper", "live"}:
        report.fail(f"TRADING_MODE must be paper or live, got {mode!r}")
    elif mode != expected_mode:
        report.fail(f"TRADING_MODE={mode!r}, expected {expected_mode!r}")
    else:
        report.ok(f"TRADING_MODE={mode}")

    if mode == "live" and not live_trading:
        report.fail("TRADING_MODE=live requires LIVE_TRADING=1")
    elif mode == "paper" and live_trading:
        report.fail("LIVE_TRADING=1 requires TRADING_MODE=live")
    else:
        report.ok(f"LIVE_TRADING={int(live_trading)} is mode-consistent")

    kill_switch = Path(env_value("KILL_SWITCH_FILE") or "/tmp/btcbot_halt")
    if kill_switch.exists():
        report.fail(f"kill switch exists: {kill_switch}")
    else:
        report.ok(f"kill switch absent: {kill_switch}")

    if mode == "live":
        require_env(report, ["PRIVATE_KEY", "CLOB_API_KEY", "CLOB_SECRET", "CLOB_PASSPHRASE", "WALLET_ADDRESS"])
        if not truthy(env_value("CHAINLINK_CHECK")):
            report.warn("CHAINLINK_CHECK is not enabled; live mode should halt on Binance/oracle divergence")
    else:
        report.ok("credential checks skipped in paper mode")

    if truthy(env_value("REPLAY_MODE")):
        report.fail("REPLAY_MODE must be off for live startup")
    else:
        report.ok("REPLAY_MODE is off")

    check_numeric_limit(report, "MAX_SPEND_PER_MARKET", 8.0, 10.0, " USDC")
    check_numeric_limit(report, "MAX_TAKER_FILL_USDC", 4.0, 10.0, " USDC")
    check_numeric_limit(report, "MAX_LOSS_PER_HOUR_USDC", 2.5, 5.0, " USDC")
    check_numeric_limit(report, "MAX_POSITION_SHARES", 10.0, 20.0, " shares")

    if (ROOT / ".env.live").exists():
        report.warn(".env.live exists in the repo directory; verify it is ignored and contains no committed secrets")
    else:
        report.ok(".env.live is absent from repo directory")

    for pid_name in [".phase35.pids", ".phase35_tos.pids", ".phase35_tos.pids_recorder"]:
        check_pidfile(report, ROOT / pid_name)

    if not (ROOT / "LIVE_GO_LIVE_PLAN.md").exists():
        report.warn("LIVE_GO_LIVE_PLAN.md is missing")
    else:
        report.ok("go-live plan is present")

    report.print()
    return 1 if report.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
