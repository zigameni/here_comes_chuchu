# BTC Polymarket Bot

Automated research & paper-trading system for Polymarket BTC 5-minute UP/DOWN markets.  
Built as a modular 4-process ZMQ pipeline with isolated strategies, deterministic replay, and real-time observability.

> ⚠️ **Paper Trading Only by Default**  
> Live execution (`LIVE_TRADING=1`) is structurally supported but not yet hardened for production capital. All runs simulate fills and track P&L in JSONL.

---

## 🏗 Architecture

```
Binance WS ──→ [binance_daemon] ──→ BINANCE_BBO.ipc ──→ [fv_engine] ──→ FV_STREAM.ipc ──┐
                                                                                         │
PM WebSocket ─→ [pm_daemon] ────────────────────────────→ PM_BOOK.ipc ──────────────────┤
                                                                                         ↓
                                                                              [smart_paper_trader]
                                                                                         │
                                                                       fills_*.jsonl / exits_*.jsonl
```

- **`binance_daemon`**: Streams BTC/USDT SPOT best bid/ask (~10 Hz)
- **`fv_engine`**: Computes Black-Scholes `P(UP)` using intra-window EWMA volatility, dynamic strike snapping, and sigma-real gating
- **`pm_daemon`**: Discovers active 5-min markets via Gamma API, streams Polymarket orderbook updates
- **`smart_paper_trader`**: Orchestrates strategy evaluation, position management, exits, settlement, and metrics emission

Strategies (`TOS`, `TOS_SIGNAL`, `legacy`) are isolated in `strategies/` and injected via environment variables.

---

## 🚀 Quick Start

### 1. Prerequisites
- Python 3.10+
- Linux or WSL2 (ZMQ IPC sockets perform best on Unix; Windows TCP fallback exists but is not recommended for live feeds)
- Git & Bash

### 2. Setup
```bash
git clone <repo-url> && cd btc-bot
python -m venv venv
source venv/bin/activate   # or `venv\Scripts\activate` on Windows
pip install -r requirements.txt
cp .env.example .env       # edit with your preferences (no real keys needed for paper mode)
```

### 3. Run the Full Stack (TOS Strategy)
```bash
./run_tos_standalone.sh
```
This launches all 4 daemons + the TOS paper trader.  
Terminal will stream logs. Use `./run_tos_standalone.sh stop` to halt cleanly.

---

## 🎮 Running the System

All launcher scripts support: `start` (default), `stop`, `logs`, and `analyze` (where applicable).

| Script | Purpose | Requires Daemons? |
|--------|---------|-------------------|
| `./run_tos_standalone.sh` | Full stack + TOS strategy (recommended) | No (starts them) |
| `./run_legacy.sh` | Full stack + legacy entry/exit logic | No (starts them) |
| `./run_tos.sh` | TOS trader only | ✅ Yes |
| `./run_tos_signals.sh` | TOS_SIGNAL trader (Phase 4 A/B test) | ✅ Yes |
| `./run_recorder.sh` | Market data capture for replay/backtesting | ✅ Yes |
| `./run_replay.sh <file>` | Deterministic backtest from capture | No (isolated) |
| `./run_anomaly_detector.sh` | Real-time safety monitoring & kill-switch | No (tails metrics) |

### 🔀 A/B Testing (TOS vs TOS_SIGNAL)
```bash
# Terminal 1: Base stack + TOS
./run_tos_standalone.sh

# Terminal 2: Signal stack variant (shares the same daemons)
./run_tos_signals.sh
```
Outputs are isolated to `fills_tos.jsonl` / `fills_signals.jsonl` for direct comparison.

---

## 📊 Monitoring & Safety

### Live Dashboard
```bash
python tools/dashboard.py
# or watch a specific metrics file:
python tools/dashboard.py -m replay_metrics.jsonl -i 1
```
Refreshes every 2s. Shows sigma quality, win rate, expectancy, latency, and halt status.

### Anomaly Detector (Phase 1b R4)
```bash
./run_anomaly_detector.sh
```
Tails `metrics.jsonl` and monitors for:
- Entry rate spikes
- Stop-loss cascades
- Hourly loss breaches
- FV/PM staleness
- Sigma floor lock

Triggers `/tmp/btcbot_halt` on critical conditions, which instantly pauses all entries.

---

## 🔄 Backtesting & Replay (Phase 5)

### 1. Record Market Data
```bash
./run_recorder.sh
```
Writes raw ZMQ payloads to `captures/YYYY-MM-DD.jsonl`. Runs alongside live daemons.

### 2. Replay & Backtest
```bash
# Max speed (offline backtest)
./run_replay.sh captures/2026-06-01.jsonl

# Real-time playback (watch dashboard live)
./run_replay.sh captures/2026-06-01.jsonl -s 1.0

# 10x speed
./run_replay.sh captures/2026-06-01.jsonl -s 10.0
```
Replay uses `REPLAY_MODE=1` to route ZMQ sockets to isolated `replay_*.ipc` channels. Strategy code is completely unaware it's in replay. Outputs go to `replay_fills.jsonl` / `replay_metrics.jsonl`.

---

## 📁 Output Files & Logs

| Path | Description |
|------|-------------|
| `fills_*.jsonl` | Entry records (price, edge, sigma, z-score, timing) |
| `exits_*.jsonl` | Exit records (TP/SL/settlement, P&L, reason) |
| `metrics.jsonl` | Structured heartbeat/signal metrics for dashboard & alerting |
| `captures/*.jsonl` | Raw ZMQ market data for deterministic replay |
| `alerts.log` | Anomaly detector alerts |
| `/tmp/btcbot_halt` | Kill-switch file (create to halt entries instantly) |
| `/tmp/btc_phase35_*/` | Per-run process logs |

Analyze any run:
```bash
./run_tos_standalone.sh analyze
```

---

## 🧪 Testing

All strategy logic, risk gates, and IPC schemas are covered by isolated unit tests.

```bash
python tests/test_smart_paper_trader.py
python tests/test_math_utils.py
python tests/test_fv_engine_bugs.py
python tests/test_price_source.py
python tests/test_gamma.py
python tests/test_exchange_concurrent_fok.py
```
✅ 144+ tests passing. Zero behavioral drift guaranteed across refactors.

---

## ⚙️ Configuration

All parameters are environment-driven. Override any value in `.env` or via shell export.

Key strategy toggles:
```bash
ENTRY_POLICY=legacy|TOS|TOS_SIGNAL   # Entry strategy
EXIT_POLICY=legacy|TOS               # Exit strategy (TOS = hold to settlement)
ARB_ENABLED=0|1                      # Dual-leg arb scanner (Phase 3)
REPLAY_MODE=0|1                      # Route to replay sockets (auto-set by run_replay.sh)
TRADING_MODE=paper|live              # Explicit runtime mode
LIVE_TRADING=0|1                     # Must be 1 only with TRADING_MODE=live
```

Run `./run_tos_standalone.sh analyze` or inspect `smart_fills.jsonl` to validate threshold behavior.

Before any live launch, run the fail-closed readiness check:
```bash
python tools/live_readiness.py --env-file .env.live --mode live
```
Any `FAIL` output blocks live startup. Treat `WARN` output as something to review before allowing capital.

---

## 📌 Important Notes

- **Paper Trading Only**: `LIVE_TRADING=1` enables CLOB execution but requires full risk hardening, secrets management, and systemd orchestration (Phase 6).
- **WSL/Linux Recommended**: ZMQ `ipc://` sockets require Unix domain sockets. Windows falls back to TCP loopback, which adds latency and can drop ticks under load.
- **Deterministic by Design**: Replay shifts historical timestamps to wall-clock time so staleness guards pass correctly. Relative timing and strategy logic remain identical to live.
- **No Threshold Tuning Yet**: All gates (`TOS_MIN_PROB`, `TOS_Z_THRESHOLD`, etc.) are at engineering-plan defaults. Use replay captures to safely optimize before live deployment.

---

## 🗺 Next Steps (Per Engineering Plan)

1. Accumulate 24–72h of `captures/` data
2. Run replay experiments to compare TOS vs TOS_SIGNAL expectancy
3. Tune thresholds safely using deterministic backtests
4. Harden live deployment (systemd, env separation, secrets, on-chain reconciliation)
5. Enable `LIVE_TRADING=1` at 10% capital once validation gates pass

---

## 📄 License & Disclaimer

Internal research tool. Not financial advice.  
Polymarket binary options carry significant risk. Paper results do not guarantee live performance.  
Always validate with replay data before committing capital.
