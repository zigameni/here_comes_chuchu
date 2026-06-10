# Strategy Optimization Framework — Step-by-Step Implementation Plan

## Scope

`TOS_SIGNAL` strategy only (`ENTRY_POLICY=TOS_SIGNAL`, `EXIT_POLICY=TOS`), as wired by `run_multiday_replay.sh`.

## Architecture note — ZMQ coupling

The replay engine is **not an in-process library**. It communicates over ZMQ sockets:

```
replay_engine.py ──(ZMQ PUB)──► smart_paper_trader.py ──(ZMQ PUSH)──► replay_engine
```

The optimizer must orchestrate subprocesses (not function calls) per run. Each step below is designed to be safe and independently verifiable before proceeding to the next.

---

## Step 1 — Promote signal stack thresholds to env vars

**Why first**: Every subsequent step depends on all tunable parameters being readable from env vars. This is a prerequisite for the parameter registry.

**Files changed:**

#### [MODIFY] [signal_stack.py](file:///home/ziga/workspace/btc-bot-status/strategies/tos_signal/signal_stack.py)

Replace the three hardcoded constants with `os.getenv()` reads:

| Constant | Env var | Default |
|---|---|---|
| `0.0004` (momentum magnitude gate) | `BTC_MOMENTUM_GATE` | `0.0004` |
| `0.40` (imbalance skew threshold) | `ORDERBOOK_IMBALANCE_GATE` | `0.40` |
| `20.0` (minimum combined liquidity) | `SIGNAL_MIN_LIQUIDITY` | `20.0` |

The existing defaults are preserved, so all existing behaviour is unchanged when env vars are absent.

#### [MODIFY] [tests/strategies/test_tos_signal_strategy.py](file:///home/ziga/workspace/btc-bot-status/tests/strategies/test_tos_signal_strategy.py)

Update any test that asserts the hardcoded numeric values to instead read from the env var defaults. No test logic changes — only replace the literal numbers with the constant names.

**Verification:**
```bash
# Before: confirm existing tests pass
python -m pytest tests/strategies/test_tos_signal_strategy.py -v

# After Step 1:
python -m pytest tests/strategies/test_tos_signal_strategy.py -v

# Confirm env var override works at runtime
BTC_MOMENTUM_GATE=0.001 python -c "
from strategies.tos_signal.signal_stack import SignalStack
ss = SignalStack()
print(ss.btc_momentum_gate)  # should print 0.001
"
```

**Risk:** Zero. Default values unchanged. All existing replay workflows unaffected.

---

## Step 2 — Merge capture files before replay

**Why second**: The current `run_multiday_replay.sh` loops over files one-by-one, which means the trader process restarts between days. Merging all capture files into one continuous JSONL (sorted by `ts_ms, _line_no` — the same order `replay_engine.py` already uses internally) produces a single uninterrupted replay session. This is also needed by the optimizer, which needs one deterministic backtest per parameter set.

**Context:** There are currently 5 capture files (2026-06-05 through 2026-06-09), totalling ~761 MB. The event sequence number (`seq`) is continuous across files.

**Files changed:**

#### [MODIFY] [run_multiday_replay.sh](file:///home/ziga/workspace/btc-bot-status/run_multiday_replay.sh)

Add a merge step **before** the per-day replay loop:

1. Collect all selected `*.jsonl` files (existing arg-parse logic unchanged)
2. Merge them into a single temp file: `$OUTDIR/merged_capture.jsonl`
3. Sort the merged file by `ts_ms` then by file-order (to preserve the `seq` continuity the replay engine already guarantees internally)
4. Run a **single** trader + replay pair against `merged_capture.jsonl` instead of the per-day loop
5. Delete the temp file on exit (`trap` cleanup)
6. The aggregate PnL summary (the inline Python at the bottom) reads the single `fills_merged.jsonl` and `exits_merged.jsonl` — same logic, no structural change

> [!NOTE]
> The merge sort is the same ordering already applied inside `ReplayEngine._load_events()` — `events.sort(key=lambda x: (x["ts_ms"], x["_line_no"]))`. The temp file just pre-applies this to the multi-day case so the trader sees one continuous session.

**Verification:**
```bash
# Run the merged replay on 2 days and check fills/exits are produced
./run_multiday_replay.sh captures/2026-06-05.jsonl captures/2026-06-06.jsonl

# Compare total fills count to the old per-day sum for the same two days
# (numbers should match exactly)
```

**Risk:** Low. The existing `run_multiday_replay.sh` is self-contained bash. The old per-day loop path can be kept behind a `--no-merge` flag as a fallback during validation.

---

## Step 3 — Parameter registry

**Why third**: The registry is pure Python with no side effects. It can be written and tested in isolation before any optimizer code exists.

**Files changed:**

#### [NEW] [tools/parameter_registry.py](file:///home/ziga/workspace/btc-bot-status/tools/parameter_registry.py)

A single `PARAMETERS` dict. Each entry has:

```python
"PARAM_NAME": {
    "type":    "float" | "int",
    "default": <current default>,
    "min":     <lower bound>,
    "max":     <upper bound>,
    "step":    <grid step>,
    "tune":    True | False,   # False = registered but held fixed during optimization
    "group":   "entry_gate" | "pre_strategy" | "position_sizing" | "exit" | "signal_stack",
    "note":    "one-line reason why this parameter matters",
}
```

**Full parameter list:**

| Group | Parameter | tune | Default | Min | Max | Step |
|---|---|---|---|---|---|---|
| entry_gate | `TOS_ENTRY_START_S` | ✅ | 210 | 150 | 240 | 10 |
| entry_gate | `TOS_ENTRY_END_S` | ✅ | 270 | 240 | 290 | 10 |
| entry_gate | `TOS_MIN_PROB` | ✅ | 0.70 | 0.60 | 0.85 | 0.05 |
| entry_gate | `TOS_MIN_EDGE` | ✅ | 0.05 | 0.02 | 0.15 | 0.01 |
| entry_gate | `TOS_MIN_LIQUIDITY` | ✅ | 20.0 | 10 | 100 | 10 |
| entry_gate | `TOS_Z_THRESHOLD` | ✅ | 0.524 | 0.25 | 1.25 | 0.05 |
| pre_strategy | `MIN_EDGE_THRESHOLD` | ✅ | 0.03 | 0.01 | 0.10 | 0.01 |
| pre_strategy | `MIN_ENTRY_ASK` | ✅ | 0.05 | 0.02 | 0.15 | 0.01 |
| pre_strategy | `FV_ENTRY_MAX` | ✅ | 0.97 | 0.85 | 0.99 | 0.02 |
| pre_strategy | `FV_ENTRY_MIN` | ✅ | 0.03 | 0.01 | 0.15 | 0.02 |
| pre_strategy | `FV_STALE_MS` | ✅ | 500 | 200 | 1000 | 100 |
| pre_strategy | `MIN_WINDOW_AGE_S` | ✅ | 100 | 0 | 180 | 20 |
| pre_strategy | `FILL_COOLDOWN_MS` | ✅ | 5000 | 1000 | 30000 | 1000 |
| position_sizing | `PAPER_TRADE_SHARES` | 🔒 | 5.0 | 5 | 20 | 5 |
| position_sizing | `MAX_SHARES_PER_SIDE` | 🔒 | 10.0 | 5 | 30 | 5 |
| position_sizing | `MAX_SPEND_PER_MARKET` | 🔒 | 8.0 | 5 | 50 | 5 |
| position_sizing | `MAX_ENTRIES_PER_WINDOW` | 🔒 | 5 | 1 | 10 | 1 |
| exit | `EARLY_HIGH_CONFIDENCE_BID` | ✅ | 0.88 | 0.75 | 0.95 | 0.05 |
| exit | `LATE_WINDOW_SECONDS` | ✅ | 120 | 60 | 180 | 20 |
| exit | `LATE_SL_FLOOR` | ✅ | 0.08 | 0.03 | 0.20 | 0.01 |
| exit | `LATE_TP_BID` | ✅ | 0.82 | 0.70 | 0.92 | 0.02 |
| exit | `EMERGENCY_SECONDS` | ✅ | 60 | 20 | 90 | 10 |
| exit | `EMERGENCY_CUT_PRICE` | ✅ | 0.12 | 0.05 | 0.25 | 0.01 |
| exit | `EMERGENCY_FV_CONFIRM` | ✅ | 0.30 | 0.15 | 0.45 | 0.05 |
| exit | `EMERGENCY_TP_BID` | ✅ | 0.88 | 0.75 | 0.95 | 0.05 |
| signal_stack | `BTC_MOMENTUM_GATE` | ✅ | 0.0004 | 0.0001 | 0.002 | 0.0001 |
| signal_stack | `ORDERBOOK_IMBALANCE_GATE` | ✅ | 0.40 | 0.20 | 0.60 | 0.05 |
| signal_stack | `SIGNAL_MIN_LIQUIDITY` | ✅ | 20.0 | 5 | 50 | 5 |

> [!NOTE]
> 🔒 = Registered in the registry so the optimizer logs them, but `tune=False` means the optimizer always uses the default. These are minimum market requirements (exchange minimum lot size, spend caps), not strategy knobs.

The module also exposes:
- `get_tunable()` → returns only params where `tune=True`
- `get_fixed_env()` → returns the always-fixed env vars (`ENTRY_POLICY`, `REPLAY_MODE`, etc.) as a ready-to-use dict
- `sample_random(rng)` → draw one random parameter set from the tunable space (used by optimizer)
- `grid_points()` → cartesian product of all tunable parameters (used for grid search)

**Verification:**
```bash
python -c "
from tools.parameter_registry import PARAMETERS, get_tunable, sample_random
import random
rng = random.Random(42)
sample = sample_random(rng)
print(f'Tunable params: {len(get_tunable())}')
print(f'Sample keys: {list(sample.keys())[:5]}')
"
```

**Risk:** Zero. No side effects, no imports from live code.

---

## Step 4 — Replay engine programmatic API

**Why fourth**: The optimizer needs to call `run_backtest(config)` as a Python function. This step adds that API without touching the existing CLI or live trading paths.

**Files changed:**

#### [MODIFY] [tools/replay_engine.py](file:///home/ziga/workspace/btc-bot-status/tools/replay_engine.py)

Add a new module-level function at the bottom of the file:

```python
def run_backtest(
    capture_path: str | Path,
    env_overrides: dict,
    fills_path: str | Path,
    exits_path: str | Path,
    speed: float = 0.0,
    timeout_s: float = 300.0,
) -> dict:
    """
    Run one complete backtest and return metrics.

    Orchestration:
      1. Build full env from os.environ + env_overrides + fixed backtest vars
      2. Launch smart_paper_trader as a subprocess with that env
      3. Run the replay engine coroutine in this process (binds ZMQ sockets)
      4. Wait for subprocess exit
      5. Parse fills_path / exits_path and compute metrics
      6. Return metrics dict

    Returns:
        {
            "net_pnl": float,
            "roi": float,
            "win_rate": float,
            "total_trades": int,
            "max_drawdown": float,
            "sharpe": float,
            "error": str | None,   # non-None if the run failed
        }
    """
```

Also add a helper `_compute_metrics(fills_path, exits_path) -> dict` that reads the JSONL output and computes all 6 metrics. This helper is pure I/O + math — no ZMQ, no subprocesses — so it can be unit-tested independently.

The existing `ReplayEngine` class and `if __name__ == "__main__"` block are **untouched**. The new function is additive only.

**Verification:**
```bash
# Run a single backtest using the new API, using default parameters
python -c "
from tools.replay_engine import run_backtest
from tools.parameter_registry import get_fixed_env
result = run_backtest(
    capture_path='captures/2026-06-09.jsonl',
    env_overrides=get_fixed_env(),
    fills_path='/tmp/test_fills.jsonl',
    exits_path='/tmp/test_exits.jsonl',
)
print(result)
"

# Verify the result matches a manual run_multiday_replay.sh run on the same file
./run_multiday_replay.sh captures/2026-06-09.jsonl
# PnL totals must be identical.
```

**Risk:** Low. The existing CLI path (`if __name__ == "__main__"`) is not changed. `run_backtest` is a new symbol only.

---

## Step 5 — Optimizer

**Why fifth**: Built on top of Steps 3 and 4. No new infrastructure needed.

**Files changed:**

#### [NEW] [tools/optimizer.py](file:///home/ziga/workspace/btc-bot-status/tools/optimizer.py)

```
python -m tools.optimizer \
  --mode random \
  --n-iter 50 \
  --train-start 2026-06-05 \
  --train-end   2026-06-07 \
  --val-start   2026-06-08 \
  --val-end     2026-06-09 \
  --captures-dir captures/ \
  --output-dir   optimization_results/
```

**Internal algorithm:**

```
1. Load PARAMETERS from parameter_registry
2. Filter captures/ by date range → train_files, val_files
3. Merge train_files into a single temp JSONL (same merge logic as Step 2)
4. Merge val_files into a single temp JSONL
5. For i in range(n_iter):
     a. Sample random parameter set (or next grid point)
     b. Run run_backtest(train_merged, params) → train_metrics
     c. Log run to optimization_results.csv (even if train failed)
     d. If train_metrics["sharpe"] < MIN_TRAIN_SHARPE: skip validation
     e. Run run_backtest(val_merged, params) → val_metrics
     f. Compute passed_validation flag (see overfitting rules below)
     g. Append full row to optimization_results.csv
     h. Print progress line to stdout
6. Re-read optimization_results.csv → write best_results.csv
```

**Overfitting protection rules** (a run `passed_validation=True` only if all hold):
- `val_sharpe > 0.3`
- `val_win_rate > 0.50`
- `val_net_pnl > 0`
- Sharpe decay: `(train_sharpe - val_sharpe) / train_sharpe < 0.50`

**Interruption safety**: results are flushed to CSV after every run, so a Ctrl-C does not lose completed iterations.

**Verification:**
```bash
# Smoke test: 3 iterations on a single day
python -m tools.optimizer \
  --mode random --n-iter 3 \
  --train-start 2026-06-09 --train-end 2026-06-09 \
  --captures-dir captures/ --output-dir /tmp/opt_test/

# Check output files exist and have correct columns
head -1 /tmp/opt_test/optimization_results.csv
```

**Risk:** Zero to existing workflows. The optimizer is a new entrypoint; it does not modify any source files.

---

## Step 6 — Result storage and ranking

**Why last**: Pure data processing, depends on Step 5 producing the CSV.

This step is partially implemented inside `optimizer.py` (Step 5). The separate concern here is making `best_results.csv` re-generatable at any time without re-running the optimizer:

#### [NEW] [tools/rank_results.py](file:///home/ziga/workspace/btc-bot-status/tools/rank_results.py)

A standalone script that reads an existing `optimization_results.csv` and re-generates `best_results.csv`:

```bash
python -m tools.rank_results \
  --input  optimization_results/optimization_results.csv \
  --output optimization_results/best_results.csv \
  --top 20
```

**`optimization_results.csv` columns:**

```
run_id, timestamp, mode, seed,
TOS_ENTRY_START_S, TOS_ENTRY_END_S, TOS_MIN_PROB, TOS_MIN_EDGE, TOS_MIN_LIQUIDITY,
TOS_Z_THRESHOLD, MIN_EDGE_THRESHOLD, MIN_ENTRY_ASK, FV_ENTRY_MAX, FV_ENTRY_MIN,
FV_STALE_MS, MIN_WINDOW_AGE_S, FILL_COOLDOWN_MS,
PAPER_TRADE_SHARES, MAX_SHARES_PER_SIDE, MAX_SPEND_PER_MARKET, MAX_ENTRIES_PER_WINDOW,
EARLY_HIGH_CONFIDENCE_BID, LATE_WINDOW_SECONDS, LATE_SL_FLOOR, LATE_TP_BID,
EMERGENCY_SECONDS, EMERGENCY_CUT_PRICE, EMERGENCY_FV_CONFIRM, EMERGENCY_TP_BID,
BTC_MOMENTUM_GATE, ORDERBOOK_IMBALANCE_GATE, SIGNAL_MIN_LIQUIDITY,
train_net_pnl, train_roi, train_win_rate, train_sharpe, train_drawdown, train_trades,
val_net_pnl, val_roi, val_win_rate, val_sharpe, val_drawdown, val_trades,
val_skipped, passed_validation, error
```

**`best_results.csv`:** subset where `passed_validation=True`, sorted by `val_sharpe DESC → val_net_pnl DESC → val_drawdown ASC`, top 20 rows.

**Verification:**
```bash
python -m tools.rank_results \
  --input /tmp/opt_test/optimization_results.csv \
  --output /tmp/opt_test/best_results.csv
cat /tmp/opt_test/best_results.csv
```

---

## Delivery order summary

| Step | What | Files | Breaks existing? |
|---|---|---|---|
| 1 | Signal stack → env vars | `signal_stack.py`, `test_tos_signal_strategy.py` | No |
| 2 | Merge captures in replay script | `run_multiday_replay.sh` | No (`--no-merge` fallback) |
| 3 | Parameter registry | `tools/parameter_registry.py` (new) | No |
| 4 | Replay engine API | `tools/replay_engine.py` (additive) | No |
| 5 | Optimizer | `tools/optimizer.py` (new) | No |
| 6 | Result ranking | `tools/rank_results.py` (new) | No |

Each step can be tested independently. Steps 3–6 are purely additive (new files or new functions). Steps 1–2 are surgical edits with exact fallback verification points.
