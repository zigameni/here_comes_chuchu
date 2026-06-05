# BTC Bot Go-Live Implementation Plan

Status: implementation plan, not approval to trade live.

The bot should not go live just because `LIVE_TRADING=1` exists. The paper and replay path must first prove that the same strategy code, data joins, risk gates, settlement handling, and process supervision behave correctly under production-like conditions.

## Current Blockers

1. Replay/paper parity must be stable over captures that cover the same time range.
2. TOS paper validation must run for at least 72 hours with positive expectancy, more than 55% settlement win rate, fewer than 20 entries/day, and sigma floor below 20% of entries.
3. Live execution must use the same `SmartPaperTrader` strategy path as paper/replay. The older `main.py` live path remains a correctness risk and should not be used for go-live.
4. Startup must fail closed unless mode, credentials, kill switch, limits, and run artifacts are all sane.
5. Live reconciliation must be stronger than trade-history reconstruction before meaningful capital is used. CLOB trades are useful, but on-chain CTF balances are the source of truth after merge/redeem.

## Implementation Phases

### Phase A: Fail-Closed Runtime Mode

- Add explicit `TRADING_MODE=paper|live`.
- Require `TRADING_MODE=live` and `LIVE_TRADING=1` together before real order placement.
- Add a read-only live readiness checker.
- Add `.env.paper` and `.env.live.example`; never commit real live secrets.

### Phase B: Same Strategy Path for Paper and Live

- Keep `SmartPaperTrader` as the only go-live orchestration path.
- Add an execution backend interface:
  - paper backend records simulated fills.
  - live backend places FOK/marketable limit orders through `core.exchange.Exchange`.
- Replace `_simulate_entry()` only behind that backend boundary.
- Make every persisted live fill come from an exchange-confirmed fill response, not an assumption.

### Phase C: Live Safety Gates

- On startup, verify:
  - kill switch file is absent.
  - CLOB credentials and wallet address are present and non-placeholder.
  - configured max spend, max position, hourly loss, and arb caps are small enough for the live bankroll.
  - Chainlink cross-check is enabled for live mode.
  - feed freshness and FV/PM window alignment have been healthy during warmup.
- On every PM tick:
  - halt on stale FV/PM.
  - halt on FV/PM window mismatch.
  - halt on kill switch.
  - cancel open orders in live mode after a halt.

### Phase D: Reconciliation and Settlement

- Reconcile open orders and cancel unknown orders on startup.
- Reconcile held positions from on-chain CTF balances, not just CLOB trade history.
- For TOS, do not fake live exits. Hold to settlement, then redeem/merge according to actual market resolution.
- Persist live order IDs, CLOB responses, tx hashes, and resolution source in JSONL.

### Phase E: Deployment

- Add systemd or supervisor units for the four data/trader processes plus recorder and anomaly detector.
- Isolate logs and output files per run.
- Capture a redacted config snapshot at startup.
- Add a single command that runs readiness checks before launching live.

## Live Validation Gate

Only after Phases A-D are implemented:

- run 72 hours of paper plus recorder.
- replay the same captures and compare outputs.
- run the anomaly detector with kill-switch enabled.
- run 24 hours live at tiny size or at most 10% of intended capital.
- compare live fills to paper expectations and investigate every mismatch.

## First Implementation Slice

This repo now starts with Phase A: explicit runtime mode and `tools/live_readiness.py`. That checker is intentionally conservative; warnings mean "do not go live yet," and failed checks mean the live launcher must refuse to start.
