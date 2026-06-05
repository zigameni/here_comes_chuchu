# BTC Polymarket Bot — Engineering Implementation Plan

> **Document type:** Internal engineering design document  
> **Status:** Pre-implementation — pending experiment validation  
> **Based on:** Full codebase audit + `Report.md` strategy analysis + live trade data  
> **Author perspective:** Quant engineer + systems architect + trading infra engineer

---

## Data Snapshot (Confirmed Before Writing This Document)

The following statistics are computed from the actual `smart_fills.jsonl` and `smart_exits.jsonl` in the repository. All architectural conclusions are grounded in this data.

| Metric | Value |
|---|---|
| Total fills | 261 |
| Total exits recorded | 173 |
| Total cost deployed | $288.40 |
| Total proceeds | $268.85 |
| **Net P&L** | **–$19.55 (–6.8% ROI)** |
| Sigma stuck at floor (0.50) | **261/261 = 100%** |
| Stop-loss exits | 118 (68.2% of all exits) |
| Settlement exits | 5 (2.9% of all exits) |
| Take-profit exits | 48 (27.7% of all exits) |
| Emergency-cut exits | 2 |
| Settlement win rate | 3/5 = 60% |
| Avg SL entry price | 0.184 |
| Avg SL exit price | 0.110 |
| Avg SL PnL | –$0.60 per trade |
| FV age at fill (mean) | 106ms |
| FV age > 500ms | 0/261 |
| Entry timing (0–100s in window) | 0 (MIN_WINDOW_AGE_S=100 working) |
| Entry timing (100–200s) | 168 (64%) |
| Entry timing (200–270s) | 69 (26%) |
| Entry timing (270–300s) | 24 (9%) |

The sigma-at-floor figure (100%) is the single most important finding. Every trade in the dataset was priced by a broken model.

---

# Part 1 — Repository & Architecture Audit

## 1.1 Repository Structure

```
btc-bot/
├── cmd/
│   ├── binance_daemon.py        # Process 1: Binance WS → ZMQ PUB
│   ├── pm_daemon.py             # Process 3: Gamma + PM WS → ZMQ PUB
│   └── smart_paper_trader.py    # Process 4: Signal + position engine
├── core/
│   └── fv_engine.py             # Process 2: FV model → ZMQ PUB
├── shared/
│   ├── ipc.py                   # ZMQ PUB/SUB factory + Channel addresses
│   └── math_utils.py            # Black-Scholes, vol calc, time-to-expiry
├── config.py                    # All config, env-driven
├── exchange.py                  # Async py-clob-client-v2 wrapper
├── feed.py                      # OrderBook + MarketFeed (PM WS + REST)
├── gamma.py                     # Market discovery via Gamma API
├── main.py                      # Outer loop (live trading, unused in paper)
├── pnl.py                       # PnLTracker dataclass
├── risk.py                      # RiskManager (circuit breakers)
├── run_phase35.sh               # 4-process launcher + analysis script
├── smart_fills.jsonl            # Simulated fill log
├── smart_exits.jsonl            # Simulated exit log
├── requirements.txt
└── tests/
    ├── test_fv_engine_bugs.py
    ├── test_math_utils.py
    └── test_price_source.py
```

## 1.2 Process Architecture & Data Flow

The system is a **4-process pipeline** connected by ZMQ PUB/SUB IPC sockets using msgpack serialization.

```
Binance WS ──→ [binance_daemon] ──→ BINANCE_BBO.ipc ──→ [fv_engine] ──→ FV_STREAM.ipc ──┐
                                                                                           │
PM WebSocket ─→ [pm_daemon] ────────────────────────→ PM_BOOK.ipc ────────────────────────┤
    ↑                                                                                       ↓
Gamma API                                                                        [smart_paper_trader]
                                                                                  │
                                                                           smart_fills.jsonl
                                                                           smart_exits.jsonl
```

**Channel schemas (msgpack):**
- `BINANCE_BBO`: `[ts_ms: int, bid: float, ask: float]`
- `FV_STREAM`: `[ts_ms: int, boundary_ts: int, prob_up: float, prob_down: float, sigma: float, btc_price: float]`
- `PM_BOOK`: `[ts_ms: int, market_id: str, bid_up: float|None, ask_up: float|None, bid_dn: float|None, ask_dn: float|None, market_ts: int, end_ts: int]`
- `EXEC_REPORT`: `[ts_ms, ...]` — defined but unused in paper trading path

## 1.3 Responsibility Map

**`binance_daemon.py`** — Ingestion layer. Connects to `wss://stream.binance.com:9443/ws/btcusdt@bookTicker` (SPOT, matching Chainlink oracle source), extracts bid/ask, publishes on each tick. Includes optional Chainlink cross-check (disabled by default). Implements exponential backoff reconnection. Has no statefulness beyond the last-mid reference for Chainlink comparison. Has no realized-vol computation — it is a pure price pipe.

**`core/fv_engine.py`** — Probability model. Subscribes to `BINANCE_BBO`, maintains a rolling price buffer (default 3000 ticks), computes annualized realized vol using `annualize_vol()`, applies a scaled sigma floor (`MIN_SIGMA_FLOOR * sqrt(T_remaining / 300)`), snaps a new strike K at each 300-second boundary, and computes Black-Scholes `P(UP)` via `black_scholes_prob(mid, K, T, sigma)`. Publishes the result on every tick along with the current sigma and BTC price.

**`cmd/pm_daemon.py`** — Market ingestion. Discovers the active BTC 5-min market slug from Gamma API (targeted slug-based fetch, not full pagination). Creates a `MarketFeed` per market window, subscribes to PM WebSocket for real-time book updates, falls back to REST polling via `exchange.py`. Publishes `[ts_ms, market_id, bid_up, ask_up, bid_dn, ask_dn, market_ts, end_ts]` on every book update and every heartbeat (1s). Manages market lifecycle transitions cleanly.

**`cmd/smart_paper_trader.py`** — Signal engine and position manager. Subscribes to both `FV_STREAM` and `PM_BOOK`. On each PM tick: (1) checks FV staleness (500ms threshold), (2) evaluates exits on open positions, (3) evaluates entries. Entry logic: checks window age guard (100s), minimum ask (0.05), edge threshold (FV − ask ≥ 0.03), position cap (10 shares), cooldown (5000ms). Exit logic: time-aware zones (early/late/emergency) with bid-price-based thresholds, plus legacy flat TP/SL mode. Settles positions at FV when the market transitions. Persists fills and exits as JSONL.

**`feed.py`** — `OrderBook` maintains four separate price books (ask and bid for UP and DOWN), plus PM-side volatility tracking (`_mid_history`, `_vol_baseline`). `MarketFeed` manages the WS connection lifecycle with REST fallback.

**`exchange.py`** — Async wrapper around `py-clob-client-v2`. Handles limit orders, FOK orders, cancellations, merges, and redemptions. Runs sync CLOB client calls in an executor. Has read-only mode when private key is missing. This is the live trading execution layer — currently bypassed entirely in the paper trading path.

**`risk.py`** — `RiskManager` with four circuit breakers: inventory imbalance, combined ask > 1.02, per-market spend cap, hourly loss limit. No position-level stop logic (that lives in `smart_paper_trader.py`).

**`gamma.py`** — Market discovery using targeted slug computation (e.g. `btc-updown-5m-{ts}`). Avoids the pagination problem. Also handles `wait_for_resolution()` polling and `safe_enter_market()` validation.

**`main.py`** — Outer loop for the live trading path (not the paper trading path). Creates `MarketRunner` instances per market window. Currently superseded by `run_phase35.sh` + `smart_paper_trader.py` for research.

**`config.py`** — Single source of truth for all parameters. Every parameter is overridable via environment variable. This is architecturally correct.

**`shared/math_utils.py`** — Pure stdlib Black-Scholes implementation. No scipy dependency. `annualize_vol()` computes from log returns with measured tick interval. `black_scholes_prob()` returns `Φ(d₂)` for a binary call. `time_to_expiry_years()` converts seconds to years.

**`shared/ipc.py`** — ZMQ PUB/SUB factory with Windows TCP fallback. Single `Channel` class as address registry.

## 1.4 Architectural Weaknesses

**Critical:**

1. **Sigma is permanently at the floor (100% of fills).** The `PRICE_BUFFER=3000` accumulates BTC tick history across multiple 5-minute windows. But the sigma calculation over 3000 ticks spanning 30–60 minutes of BTC price history does not reflect the intra-window volatility relevant to the current 5-minute contract. The annualized vol from a long history tends to cluster around BTC's annual vol (~60–80%), while the floor is set at 50% annualized — producing outputs that depend entirely on the floor, not measured vol. Confirmed: 261/261 fills at sigma=0.50. The FV model has never used real vol in any trade.

2. **The sigma floor itself is too high.** Even if real vol were being measured, the 50% annualized floor over a 5-minute window creates substantial fake edge. At 5 minutes remaining, `sigma_scaled = 0.50 * sqrt(5/300) = 0.064`. Applied to the Black-Scholes formula, a BTC delta of $50 with vol=0.064 over T=5min gives `d₂ = ln(1.0005) / 0.064 - 0.032 ≈ 0.046`, `P(UP) ≈ 0.52`. The "edge" at most PM prices is real — but it's produced by an arbitrary constant, not information. All 261 entries represent bets on numerical coincidence, not genuine mispricing.

3. **Settlement proxy is broken.** `_settle_market()` uses the last known FV (based on floor sigma) to determine whether UP or DOWN pays $1. This is not how Polymarket actually settles. Chainlink oracle resolution depends on the BTC spot price vs. the window's opening K — not on the bot's internal probability. Simulation P&L is therefore corrupted in both directions.

4. **The 5 settlement exits are likely unrepresentative.** With only 5 out of 261 entries ever reaching settlement, the system's settlement proxy is statistically meaningless. The dominant exit is the stop-loss at avg –$0.60, which is triggered by PM price noise, not real information.

**Structural:**

5. **No single source of window timing truth.** `fv_engine.py` uses `int(now_ts) % 300` to detect window boundaries. `smart_paper_trader.py` uses `(ts_ms / 1000) % MARKET_WINDOW_SECONDS`. The `pm_daemon.py` uses `end_ts` from Gamma API. These three sources can diverge if the Gamma API's market timestamps don't align exactly with the wall-clock 300-second grid. In practice they should match, but there's no reconciliation logic and no alarm if they diverge.

6. **Stale FV detection is per-tick, not per-window.** `FV_STALE_MS=500` prevents acting on old FV. But there's no check that the FV is for the *same market window* as the PM book update. If `fv_engine.py` and `pm_daemon.py` are on different windows (e.g., during a window transition), the cross-stream join produces nonsense probabilities for several seconds.

7. **The `main.py` / `MarketRunner` live trading path is diverged from the paper trading path.** `main.py` doesn't subscribe to the `FV_STREAM` or use `smart_paper_trader.py`'s logic. If live trading is ever enabled, it will use different entry/exit logic than what was paper-tested. This is a correctness hazard.

8. **No realized-vol input to the FV engine from the paper trader.** The paper trader has no way to know if the sigma it's seeing is "real" or floor-clamped. It currently tracks `sigma_at_floor` as a counter, but does not gate entries on it.

9. **`OrderBook.realized_vol` tracks PM price volatility, not BTC volatility.** The vol signal in `feed.py` is computed from PM midprice changes, not from BTC price changes. These are correlated but not the same thing. PM price vol is not suitable as input to the Black-Scholes BTC option model.

**Technical debt:**

10. **Two logging systems in parallel.** `loguru` is used in `main.py`, `exchange.py`, `feed.py`, `gamma.py`, `pnl.py`. `logging` (stdlib) is used in `cmd/*` and `core/*`. This makes log aggregation inconsistent and filtering unreliable.

11. **`smart_paper_trader.py`'s settlement proxy is FV-based, not oracle-based.** Real settlement is determined by Chainlink oracle at window close. The paper trader uses `prob_up > 0.5 → UP wins`. Given the sigma floor is always 0.50, `prob_up` is barely above 0.50 when BTC is slightly above K, so settlement simulation is close to a coin flip.

12. **`exchange.py` is imported but never called in the paper trading path.** The paper trader creates no `Exchange` instance. But `pm_daemon.py` creates one for read-only REST calls. `MarketFeed` receives it as a dependency for polling. This coupling means a bogus private key warning is emitted on every `pm_daemon.py` startup.

13. **`main.py` is missing from the paper trading pipeline entirely.** The paper trader bypasses `main.py` and `MarketRunner`. If `MarketRunner` contains important invariants (order cancellation, position reconciliation), they're not exercised in paper mode.

## 1.5 Missing Abstractions

- **`VolatilityModel` abstraction:** There is no clean interface between "how we estimate volatility" and "how we compute probability." Vol estimation is embedded directly in `FVEngine`. Replacing the vol model requires rewriting the engine.
- **`SignalEvaluator` abstraction:** Entry logic is embedded in `_check_entries()` as inline conditions. Adding a signal stack (Architecture C) requires significant surgery to `smart_paper_trader.py`.
- **`ExitPolicy` abstraction:** Exit logic is a large if/elif tree in `_check_exits()`. Swapping between TOS (no exits), flat SL, and time-aware exits requires toggling env vars, not changing implementations.
- **`ReplayEngine` abstraction:** There is no backtesting infrastructure. Historical data cannot be replayed through the same pipeline that handles live data.
- **`WindowState` abstraction:** Window timing is computed inline at multiple points. No single object owns "which window are we in, what is its K, what is the time remaining."

## 1.6 Unsafe Assumptions

- **BTC Binance SPOT ≈ Chainlink oracle.** Correct in normal conditions, but the daemon tracks drift via `CHAINLINK_CHECK` only when explicitly enabled. In adversarial conditions (exchange outage, Binance flash crash) the models may diverge substantially with no automatic detection.
- **PM WebSocket delivers complete and timely book updates.** `feed.py` relies on incremental WS updates and falls back to REST polling at `POLL_INTERVAL_SECONDS=1.5s`. During high-activity periods, the WS may burst-drop events and the REST fallback may lag. There is no staleness check on the PM side in `smart_paper_trader.py` (only on the FV side).
- **Settlement proxy (FV at window close) approximates real settlement.** As analyzed above, this is wrong. The systematic error inflates settlement win rates during uptrends and deflates them during downtrends.
- **`statistics.stdev` on log-returns is stable with 3000 ticks.** True in general, but the buffer carries data across multiple 5-minute windows. During high-vol events, the stdev of a multi-window buffer underweights the current regime.

---

# Part 2 — Architecture Decision

## 2.1 What the Data Tells Us

The 100% sigma-at-floor finding has a clear implication: **every single trade in the dataset was noise.** The system was not trading information — it was sampling a broken probability function. The entry edge (mean 0.1547) is entirely an artifact of the floor, not genuine mispricing. The 118 stop losses are the consequence: positions entered on fake edge that then drift to their actual fair value.

The 60% settlement win rate on the 5 held-to-settlement positions is statistically meaningless (n=5), but the direction aligns with the TOS hypothesis: when positions are held to settlement, they win more than they lose. This does not validate TOS — it is insufficient data. It is consistent with TOS.

The dominant problem is clear and fixable. This is the correct order:

1. Fix sigma (eliminate the floor dependency, use intra-window BTC vol)
2. Run Experiment 1 and 2 from the report (signal quality audit + SL counterfactual)
3. Based on experiment outcomes, implement Architecture A (TOS) as the primary strategy
4. Build Architecture B (dual-leg arb) as a parallel secondary strategy
5. Layer Architecture C signal stack elements on top of TOS once TOS is validated

## 2.2 Architecture Decision Matrix

| Architecture | Implement Now? | Reason |
|---|---|---|
| **A — Terminal Oracle Sniper** | **YES** | Requires only: (1) real sigma, (2) late-window gate, (3) settlement-only exit. Minimal complexity. Highest probability of rapid validation. Matches data (settlement win rate direction). |
| **B — Dual-Leg Structural Arb** | **Parallel (low priority)** | Requires concurrent FOK execution (exchange.py changes), lower combined-ask threshold. Zero-prediction, but latency-sensitive. Build in Phase 3 while TOS is in paper trading. |
| **C — Regime Signal Stack** | **Later** | Requires 5 independent signal implementations, backtest infrastructure to validate, and a validated vol model first. Premature without experiment data. Add signal stack elements on top of TOS in Phase 4+. |

## 2.3 Prerequisites Before Any Implementation

These must be completed before writing production code:

**P1 (Blocker):** Fix `annualize_vol()` buffer scope. The PRICE_BUFFER must contain only intra-window ticks, not cross-window history. Alternatively: pass the window start timestamp to `fv_engine.py` and filter the price buffer to only use prices from the current window.

**P2 (Blocker):** Run Experiment 1 (signal-outcome correlation). After fixing sigma, re-run the paper trader for 48h and correlate BTC delta direction with settlement outcome. If Pearson correlation < 0.05, the FV model has no predictive power even with real sigma, and Architecture A becomes the only rational path.

**P3 (Validation):** Run Experiment 2 (SL counterfactual). From existing logs: for each SL exit, determine whether the market eventually settled at $1 or $0. This requires matching `smart_exits.jsonl` entries against PM resolution data. If > 40% of SL exits would have settled at $1, remove all mid-window exits for the TOS path.

**P4 (Infrastructure):** Add window-boundary cross-stream synchronization. The FV engine's `boundary_ts` must be matched against `pm_daemon.py`'s `market_ts` before any probability is applied. Mismatched windows must be dropped, not used.

## 2.4 Validation Gates (Highest Priority)

Do not proceed to Phase 2 implementation until:
- Sigma no longer hits the floor on > 20% of fills in a 24h paper run
- Pearson correlation between BTC delta direction and settlement outcome is measurable (even if weak)
- The paper trader can successfully paper-trade the TOS architecture (late-window only, settlement-only exit) for 24h without errors

---

# Part 3 — Component-by-Component Refactor Plan

## 3.1 `shared/math_utils.py` — Black-Scholes Core

**Current responsibility:** Pure mathematical functions: `norm_cdf`, `black_scholes_prob`, `annualize_vol`, `time_to_expiry_years`. No state, no I/O.

**Current problems:**
- `annualize_vol()` is correct as a function, but the caller (`fv_engine.py`) feeds it a buffer spanning multiple windows. The function itself is fine; the problem is the data it receives.
- No EWMA vol estimation (simple stdev of all log returns weights old and new data equally).
- No Parkinson/Garman-Klass estimators for potentially better intrabar estimation (lower priority).
- `time_to_expiry_years` uses wall-clock `time.time()` inside the function — creates implicit dependency on system time, making it untestable in replay scenarios.

**Required redesign:**
- Add `ewma_vol(prices, alpha, interval_s)` function: exponentially-weighted realized vol. This gives more weight to recent returns and is better suited for detecting regime changes mid-window.
- Refactor `time_to_expiry_years` to accept `now_ts` as a parameter (for replay compatibility): `time_to_expiry_years(end_ts_s, now_ts=None)`.
- Add `z_score(delta_pct, sigma_remaining)` helper: `delta_pct / sigma_remaining`. This is the key signal for Architecture A.
- Add `intra_window_vol(prices_with_timestamps, window_start_ts, interval_s)` that filters to only the current window's prices before computing vol.

**Interface changes:**
- `annualize_vol(prices, interval_s)` → keep existing signature, but add `intra_window_vol` as the preferred alternative
- `time_to_expiry_years(end_ts_s, now_ts=None)` — backward compatible (uses `time.time()` if `now_ts` is None)
- New: `ewma_vol(prices: Sequence[float], alpha: float, interval_s: float) -> float`
- New: `z_score(btc_delta_pct: float, sigma_annualized: float, t_remaining_s: float) -> float`
- New: `prob_from_z(z: float) -> float` — just `norm_cdf(z)`, named for readability

**Testing requirements:**
- All existing `test_math_utils.py` tests must continue to pass
- New test: `ewma_vol` gives higher weight to recent prices (verify with synthetic data where vol spikes at the end)
- New test: `z_score` at known inputs produces expected probabilities
- New test: `intra_window_vol` ignores prices before `window_start_ts`
- Property test: `black_scholes_prob(S, K, 1e-8, sigma) ≈ (1.0 if S > K else 0.0)`

## 3.2 `core/fv_engine.py` — Volatility Estimation & FV Model

**Current responsibility:** Subscribes to Binance BBO ticks, maintains rolling price buffer, estimates sigma, snaps strike K at window boundaries, computes `P(UP)` via Black-Scholes, publishes to FV_STREAM.

**Current problems (confirmed by data):**
- **Root cause:** `PRICE_BUFFER=3000` accumulates ticks across multiple windows. At ~10 ticks/s, 3000 ticks covers 5 minutes of Binance history. This is theoretically adequate but the vol calculation includes prices from the previous window, smoothing out intra-window movement. Empirically: 100% sigma at floor means the cross-window buffer is returning low vol that the floor then dominates.
- Scaled sigma floor (`MIN_SIGMA_FLOOR * sqrt(T/300)`) is the correct design, but at 100% floor usage it has zero effect — the floor is always active.
- The engine publishes on every Binance tick (~10/s). `smart_paper_trader.py` only consumes PM book ticks (~1/s). This is fine, but creates a large backlog in the FV_STREAM IPC buffer that could introduce latency spikes during reconnects.
- No window-aligned vol estimation. The first tick of a new window carries sigma derived partly from the previous window's BTC behavior.
- `_snap_strike()` correctly resets K at each boundary but the vol buffer is not cleared at the same time. The first 30s of a new window run with cross-window sigma until enough intra-window ticks accumulate.

**Required redesign:**

```python
class FVEngineV2:
    # Split buffer into two components:
    # 1. intra_window_prices: deque only for the CURRENT window, cleared at boundary
    # 2. cross_window_prices: long-horizon buffer for baseline vol comparison
    
    def _snap_strike(self, mid, now_ts):
        if boundary != self._current_boundary:
            # Clear intra-window buffer on boundary reset
            self._intra_window_prices.clear()
            self._strike = mid
            self._window_end_ts = boundary + MARKET_WINDOW_SECONDS
            self._current_boundary = boundary
    
    def _compute_sigma(self):
        # Primary: intra-window EWMA vol
        if len(self._intra_window_prices) >= MIN_BUFFER_FILL:
            intra_sigma = ewma_vol(
                self._intra_window_prices,
                alpha=EWMA_ALPHA,  # e.g. 0.94
                interval_s=avg_interval
            )
            return max(intra_sigma, self._dynamic_floor())
        # Fallback: cross-window buffer during warmup
        return self._cross_window_sigma()
    
    def _dynamic_floor(self):
        # Vol floor based on recent cross-window history
        # If cross-window sigma (30-min history) is 0.8 annualized,
        # the floor should be much lower than 0.50 annualized
        # The current fixed 0.50 floor is the bug.
        T_remaining = max(0.0, self._window_end_ts - time.time())
        scale = math.sqrt(T_remaining / MARKET_WINDOW_SECONDS)
        # Dynamic floor: use cross-window percentile
        # At p10 of recent vol distribution, not a fixed constant
        baseline_p10 = self._cross_window_vol_percentile(0.10)
        return baseline_p10 * scale  # typically much lower than 0.50
```

**Data model changes:**
- Add `intra_window_prices: deque` (cleared at each window boundary)
- Add `cross_window_prices: deque` (not cleared, longer history for baseline)
- Add `ewma_alpha: float` config parameter
- Published schema gains `intra_window_vol: float` and `cross_window_vol_baseline: float` for diagnostic

**Interface changes:**
- Published FV_STREAM schema: `[ts_ms, boundary_ts, prob_up, prob_down, sigma, btc_price, intra_vol, is_sigma_real]`
- `is_sigma_real: bool` — False when sigma is from floor/warmup, True when from intra-window data. **This is the critical gating signal for `smart_paper_trader.py`.**

**Performance concerns:**
- `statistics.stdev` on 3000 points takes ~2ms in Python. With 10 ticks/s this is fine (~2% CPU). With EWMA this drops to O(1) per tick.
- IPC socket HWM=1000. At 10 ticks/s the buffer fills in 100s if the consumer stalls. Set HWM lower (50) if backpressure is a concern.

**Testing requirements:**
- New test: after boundary snap, `intra_window_prices` is empty and sigma transitions to warmup mode
- New test: EWMA vol responds to vol spikes faster than simple stdev
- New test: `is_sigma_real` is False for the first `MIN_BUFFER_FILL` ticks of each window
- Integration test: run `fv_engine.py` against a synthetic tick stream with known vol and verify sigma is within 15% of ground truth

## 3.3 `cmd/binance_daemon.py` — Price Feed

**Current responsibility:** Binance SPOT WS → BINANCE_BBO ZMQ publication.

**Current problems:**
- No realized vol computation. The engine `fv_engine.py` receives only ticks, not derived statistics. This is by design, but it means `fv_engine.py` cannot use Binance's 5-minute Kline endpoint for a cleaner vol estimate.
- `STALE_TIMEOUT_S=5.0` triggers reconnection if no tick for 5s. Binance SPOT bookTicker fires every ~100ms normally; a 5s stale means a real connectivity problem. This is reasonable.
- Optional Chainlink cross-check is good but disabled by default. Should be enabled by default in production with a warning threshold.
- No BTC delta tracking from window K. The daemon publishes raw BTC price but does not compute `(btc_now - btc_K) / btc_K`. This could be added here or in `fv_engine.py`.

**Required additions (not full redesign):**
- Add `last_5min_realized_vol` as a shared state that `fv_engine.py` can query. This is the "real Binance vol" that the report recommends. Cleanest approach: compute it in the daemon and add to the ZMQ message.
- Extended BINANCE_BBO schema: `[ts_ms, bid, ask, btc_5min_realized_vol_annualized]`. The daemon maintains its own rolling 5-minute buffer (300s × 10 ticks/s = 3000 ticks), computes EWMA vol, and appends it.
- Enable Chainlink check by default with a configurable `CHAINLINK_WARNING_DELTA_USD=50` threshold.
- Add `btc_window_delta_pct` to the published message: `(mid - last_K_snap) / last_K_snap`. Requires the daemon to track K snaps, which currently only `fv_engine.py` does. Alternative: keep K computation in `fv_engine.py` and compute delta there.

**State management changes:**
- Add `_vol_buffer: deque` of recent log-returns for 5-minute EWMA vol computation
- Add `_vol_ewma: float` updated on each tick
- The `latest_mid` list becomes a more complete state object

**Testing requirements:**
- Unit test: EWMA vol computation convergence
- Integration test: published vol is within 20% of Binance's official 5-minute realized vol for the same window

## 3.4 `cmd/pm_daemon.py` — Polymarket Book Feed

**Current responsibility:** Gamma API market discovery + PM WebSocket book ingestion + `PM_BOOK` ZMQ publication.

**Current problems:**
- `_publish_book()` fires on a 1-second heartbeat even if the book hasn't changed. This creates 1 message/second minimum regardless of market activity. Not a problem at current scale but adds unnecessary load.
- `PM_STALE_TIMEOUT_S=10` only logs a warning, does not take action. In live trading, a 10-second stale orderbook is a correctness problem.
- Market transition detection relies on `condition_id` change. If the Gamma API returns a market before its WS feed is live (e.g. the market opened 2 seconds ago), the daemon tries to subscribe before the book is populated. The WS loop in `feed.py` handles reconnects but early messages may be missed.
- **The `market_ts` field in the published schema is critical** for cross-stream synchronization (matching FV to the right PM market), but `smart_paper_trader.py` does not yet use it for this purpose.

**Required redesign:**
- Add `market_ts` matching in `smart_paper_trader.py` (see 3.5 below) — the daemon is already emitting the right data.
- Add stale-book action: if PM book is stale > 10s, publish a `None` book to signal downstream consumers to pause entry evaluation. Don't just log a warning.
- Add book depth metrics to the published message: `bid_depth_up`, `bid_depth_down`, `ask_depth_up`, `ask_depth_down` (total USDC at best price level). This enables the liquidity checks in Architecture A without additional queries.
- Tighten heartbeat: only publish when book has changed OR every 2.0s (not every 1.0s). Reduces unnecessary wakeups in the paper trader.

**Extended PM_BOOK schema:**
```python
# Current: [ts_ms, market_id, bid_up, ask_up, bid_dn, ask_dn, market_ts, end_ts]
# Extended: [ts_ms, market_id, bid_up, ask_up, bid_dn, ask_dn, market_ts, end_ts,
#            liq_up, liq_dn, spread_up, spread_dn, book_stale: bool]
```

**Testing requirements:**
- Test: market transition clears stale book state within 1 heartbeat
- Test: `None` book is published when WS stale timeout fires
- Test: `market_ts` matches the Gamma API's `start_ts` field

## 3.5 `cmd/smart_paper_trader.py` — Signal + Position Engine

**Current responsibility:** Entry evaluation, position management, exit evaluation, settlement, JSONL persistence.

**Current problems (the most important component):**
- **Entry gating on `is_sigma_real` is absent.** Once `fv_engine.py` publishes a `is_sigma_real` flag, this component must gate all entries on it. Any entry while `is_sigma_real=False` is a noise trade.
- **FV-market window mismatch is unchecked.** `FVState.market_id` is `boundary_ts` (int) from `fv_engine.py`. `PMState.market_id` is `condition_id` (str) from `pm_daemon.py`. These are different types and not compared. The window-synchronization check requires matching `fv_engine._current_boundary` to `pm_daemon.market_ts`. This check is currently missing.
- **Settlement proxy is wrong.** The current implementation uses `self._fv.prob_up > 0.5` to determine the settlement winner. Correct approach: query the Gamma API for the actual resolution, or — for paper trading — use the `end_ts` and the BTC price at that timestamp from the Binance feed as a direct comparison to K.
- **The `Stats` class tracks `sigma_at_floor` but doesn't use it to halt entries.** This should be a hard gate, not just a counter.
- **`_check_exits` uses modular clock (`ts_ms % 300000`) for window timing.** This is fragile. The PM_BOOK now includes `end_ts` directly — use that.
- **The architecture flag `USE_TIME_AWARE_EXITS` as an env var creates hidden state.** The entry/exit logic should be configured by selecting an `ExitPolicy` class at startup.

**Required redesign for Architecture A (TOS):**

```python
class TOSEntryPolicy:
    """Terminal Oracle Sniper entry policy."""
    
    # Configuration
    LATE_WINDOW_ENTRY_START = 210   # seconds elapsed in window
    LATE_WINDOW_ENTRY_END   = 270   # seconds elapsed in window  
    MIN_Z_SCORE             = 1.28  # |z| > 1.28 → P > 0.90
    MIN_PROB_THRESHOLD      = 0.70  # min probability to enter
    MIN_EDGE_OVER_PM        = 0.05  # prob - pm_ask must exceed this
    MIN_LIQUIDITY_USDC      = 20.0  # PM liquidity at entry price
    
    def evaluate(
        self, 
        fv: FVState, 
        pm: PMState,
        is_sigma_real: bool,
    ) -> Optional[Tuple[str, float]]:  # (side, edge) or None
        
        if not is_sigma_real:
            return None  # never trade on floor sigma
        
        # Window timing gate — only enter in late window
        elapsed_s = pm.ts_ms / 1000 - pm.market_ts
        if not (self.LATE_WINDOW_ENTRY_START <= elapsed_s <= self.LATE_WINDOW_ENTRY_END):
            return None
        
        # Z-score gate — BTC must have moved decisively
        if abs(fv.z_score) < self.MIN_Z_SCORE:
            return None
        
        # Probability gate
        winning_side = "UP" if fv.prob_up > fv.prob_down else "DOWN"
        winning_prob = max(fv.prob_up, fv.prob_down)
        if winning_prob < self.MIN_PROB_THRESHOLD:
            return None
        
        # PM ask on winning side
        pm_ask = pm.ask_up if winning_side == "UP" else pm.ask_down
        if pm_ask is None:
            return None
        
        # Edge gate
        edge = winning_prob - pm_ask
        if edge < self.MIN_EDGE_OVER_PM:
            return None
        
        # Liquidity gate
        pm_liq = pm.liq_up if winning_side == "UP" else pm.liq_dn
        if pm_liq < self.MIN_LIQUIDITY_USDC:
            return None
        
        return (winning_side, edge)


class TOSExitPolicy:
    """TOS: hold to settlement only. No mid-window exits."""
    
    def evaluate(self, pos, bid, fv, remaining_s) -> Optional[str]:
        return None  # never exit mid-window in TOS
```

**State management changes:**
- `FVState` gains `z_score: float`, `intra_vol: float`, `is_sigma_real: bool`
- `PMState` gains `market_ts: int`, `end_ts: int`, `liq_up: float`, `liq_dn: float`, `spread_up: float`, `spread_dn: float`
- Settlement becomes oracle-based: add `_resolve_settlement(market_id, end_ts)` that queries Gamma for actual outcome
- Add `policy: EntryPolicy` and `exit_policy: ExitPolicy` dependency injection at startup
- Add `window_match_guard()`: skip PM tick if `fv.boundary_ts != pm.market_ts`

**Data model changes:**
```python
@dataclass
class EntryRecord:
    # Add fields:
    z_score: float = 0.0
    intra_vol: float = 0.0
    is_sigma_real: bool = False
    elapsed_s: float = 0.0    # seconds into window at fill time
    window_start_ts: int = 0
    window_end_ts: int = 0
```

**Testing requirements:**
- Unit test: `TOSEntryPolicy` blocks entries before t=210s
- Unit test: `TOSEntryPolicy` blocks entries when `is_sigma_real=False`
- Unit test: `TOSEntryPolicy` blocks entries when z-score < threshold
- Unit test: window mismatch guard drops tick when `fv.boundary_ts != pm.market_ts`
- Integration test: paper trader runs 24h with TOS policy without any assertion errors
- Statistical test: after 72h paper run, entry count per day is 5–20

## 3.6 `risk.py` — Risk Management

**Current responsibility:** Four circuit breakers: inventory imbalance, combined ask, spend cap, hourly loss.

**Current problems:**
- `RiskManager` is instantiated by `main.py/MarketRunner` but not by `smart_paper_trader.py`. The paper trading path has no active risk management beyond the position cap.
- `trading_halted` has no reset path. Once triggered, only a process restart can clear it.
- No per-window state reset. `current_spent` accumulates across windows but `max_spend_per_market` is per-window. Currently reset manually or not at all.
- No latency monitoring (fill latency, FV age at entry).
- No stale-data detection (PM book age).
- No kill switch accessible from outside the process (no signal handler beyond SIGINT).

**Required redesign:** See Part 8 (Risk System) for full design. Summary of minimal changes needed now:
- Integrate `RiskManager` into `smart_paper_trader.py`
- Add `reset_window()` method called on market transition
- Add `record_fv_age(ms)` and `record_pm_age(ms)` for latency tracking
- Add `check_data_freshness(fv_age_ms, pm_age_ms)` returning bool

## 3.7 `exchange.py` — Execution Engine

**Current responsibility:** Async wrapper for py-clob-client-v2. Limit orders, FOK, cancel, merge, redeem.

**Current problems:**
- Not used at all in the paper trading path. This is correct for now.
- For Architecture B (dual-leg arb), concurrent FOK is critical but not implemented. The current approach is sequential: place UP leg, then DOWN leg. If UP fills and DOWN fails, you have a naked directional position.
- No duplicate order detection. If the process crashes and restarts mid-order, it can double-post.
- No position reconciliation: on startup, `exchange.py` doesn't query existing open orders or positions from the CLOB.

**Required additions (for Architecture B):**
- `execute_concurrent_fok(token_up, token_down, ask_up, ask_down, shares)` — fires both FOK orders concurrently with `asyncio.gather`. If one fails, immediately fire a compensating sell on the successful leg.
- `get_open_positions() -> dict` — query CLOB for existing positions on startup to enable reconciliation.
- `get_open_orders() -> list` — for duplicate detection.

**Defer to Phase 3:** Don't touch `exchange.py` until TOS paper trading is validated.

## 3.8 `feed.py` — PM Orderbook

**Current responsibility:** `OrderBook` maintains bid/ask books for UP and DOWN, tracks PM-side vol. `MarketFeed` manages WS connection and REST fallback.

**Current problems:**
- Best-ask computation scans the entire book dict on every call: `min([p for p, s in self._book_up.items() if s > 0])`. For large books this is O(N). Use a `sortedcontainers.SortedList` or maintain a `heapq` for O(log N) best-bid/ask.
- `_push_mid()` uses `best_ask_up + best_ask_down / 2` as the "mid". This is the combined ask, not a proper bid-ask midpoint for either side. For Architecture C's `pm_price_momentum_signal`, a proper midpoint per side is needed: `(best_ask + best_bid) / 2`.
- No WS reconnection backoff — `asyncio.sleep(3)` is hardcoded. Should use exponential backoff with jitter.
- No depth tracking beyond the best level. Architecture A and B need `liq_up` (USDC at best ask) and `liq_dn`, which exist but scan the entire book.

**Required additions:**
- Replace inner dict scanning with `SortedList` for O(log N) best-price lookup (or maintain a simple heap)
- Add per-side midpoint: `mid_up = (best_ask_up + best_bid_up) / 2`
- Add L2 depth: sum of USDC at best 3 levels (for liquidity gating)
- Add book hash for change detection (avoid redundant publishes from `pm_daemon`)
- Add WS backoff with `BACKOFF_INIT=0.5, BACKOFF_MAX=30`
- Add `last_update_ts` timestamp per side for staleness detection

## 3.9 `gamma.py` — Market Discovery

**Current responsibility:** Targeted slug-based Gamma API fetch, market lifecycle validation, resolution polling.

**Current problems:**
- `get_next_tradable_market()` returns the next market by wall clock. It does not validate that the Chainlink oracle for the previous window has resolved. Entering a new window immediately after the previous one closes can create edge cases where the oracle hasn't settled.
- `safe_enter_market()` is called by `main.py` but not by the paper trader.
- No 15-minute market support. Currently hardcoded to `btc-updown-5m-{ts}` slug pattern.
- Resolution polling (`wait_for_resolution()`) blocks the event loop via `asyncio.sleep`. At `ORACLE_WAIT_SECONDS=320` this creates a 5-minute stall in the main loop during live trading.

**Required additions:**
- Add `MarketType` enum: `FIVE_MIN`, `FIFTEEN_MIN`, `ONE_HOUR`. Parameterize slug generation and window timing.
- Add `get_resolution(market_id)` non-blocking query for settlement outcome (needed for proper paper-trader settlement).
- Decouple resolution waiting from the main market loop using `asyncio.create_task`.

---

# Part 4 — Step-by-Step Implementation Phases

## Phase 0 — Data Analysis (Week 0, 2–3 days)

**Objective:** Validate assumptions from the report before writing any code.

**Tasks:**

**Task 0.1 — Signal-outcome correlation audit (Experiment 1)**
```python
# Script: tools/analyze_signal_quality.py
# Input: smart_fills.jsonl + PM resolution data
# For each fill, record:
#   - btc_delta_at_entry = (btc_price_at_fill - K) / K
#   - side = "UP" or "DOWN"
#   - predicted_side = "UP" if fv > 0.5 else "DOWN"
#   - settlement_outcome = actual $1 or $0 winner
# 
# Query settlement outcomes from Gamma API for each market_id in the fills log.
# 
# Compute:
#   - Pearson(btc_delta, settlement_is_up)
#   - Accuracy: fill's predicted_side == settlement winner
#   - Confusion matrix
```

Expected output: correlation near 0 (the signal is noise). Decision rule: if correlation < 0.05 AND prediction accuracy < 0.55, the FV model with floor sigma has zero predictive power. Proceed with TOS regardless. If accuracy > 0.55, there is some residual signal worth preserving.

**Task 0.2 — SL counterfactual replay (Experiment 2)**
```python
# Script: tools/sl_counterfactual.py
# Input: smart_exits.jsonl (SL exits only) + Gamma resolution data
# For each SL exit:
#   - Query Gamma API: what did market_id actually settle to?
#   - Record: would holding to settlement have been profitable?
# 
# Output:
#   - % of SL exits that would have won at settlement
#   - Average loss crystallized vs. what holding would have yielded
#   - Distribution of SL exit prices (confirmation that 0.110 avg is correct)
```

Expected output: > 40% of SL exits would have settled profitably. Decision rule: if > 40%, remove all mid-window stops for TOS path.

**Dependencies:** Gamma API access for resolution lookup. Both scripts should be in `tools/`.

**Risks:** Market resolution data may not be available for all old market IDs. Use what's available; statistical significance requires only ~30 resolved SL exits.

**Validation:** Both scripts produce deterministic output for the same input data.

**Rollback:** Not applicable — this is analysis only.

**Expected impact:** Confirms the architectural direction before any code is written. Eliminates the risk of building the wrong strategy.

---

## Phase 1 — Sigma Fix (Week 1, 3–4 days)

**Objective:** Make the FV engine produce real, intra-window volatility estimates. This is the single most impactful change possible.

**Components touched:** `core/fv_engine.py`, `shared/math_utils.py`, `cmd/binance_daemon.py` (optional)

**Tasks:**

**Task 1.1 — Add `intra_window_prices` buffer to FVEngine**
```python
# In FVEngine.__init__:
self._intra_window_prices: deque[float] = deque(maxlen=PRICE_BUFFER)

# In FVEngine._snap_strike (modify):
if boundary != self._current_boundary:
    self._intra_window_prices.clear()  # reset on window transition
    # ... existing K snap logic ...
    log.info("Window boundary: intra-window vol buffer cleared")

# In FVEngine._on_tick:
self._intra_window_prices.append(mid)  # after appending to _prices (cross-window)
```

**Task 1.2 — Add EWMA vol computation to `math_utils.py`**
```python
def ewma_vol(prices: Sequence[float], alpha: float = 0.94, interval_s: float = 0.1) -> float:
    """EWMA realized volatility (RiskMetrics method)."""
    prices = list(prices)
    if len(prices) < 3:
        return 0.0
    returns = [math.log(prices[i] / prices[i-1]) 
               for i in range(1, len(prices)) 
               if prices[i-1] > 0 and prices[i] > 0]
    if len(returns) < 2:
        return 0.0
    variance = returns[0] ** 2
    for r in returns[1:]:
        variance = alpha * variance + (1 - alpha) * r ** 2
    periods_per_year = _SECONDS_PER_YEAR / interval_s
    return math.sqrt(variance * periods_per_year)
```

**Task 1.3 — Switch FVEngine to use intra-window vol with dynamic floor**
```python
# In FVEngine._on_tick sigma computation section:
if len(self._intra_window_prices) >= MIN_BUFFER_FILL:
    intra_sigma = ewma_vol(self._intra_window_prices, alpha=EWMA_ALPHA, interval_s=avg_interval)
    is_sigma_real = intra_sigma > 0.001  # not trivially zero
else:
    intra_sigma = 0.0
    is_sigma_real = False

# Dynamic floor: much lower than fixed 0.50
# Use p10 of recent cross-window history or hard minimum 0.10 annualized
cross_sigma = annualize_vol(self._prices, interval_s=avg_interval)
dynamic_floor_base = max(cross_sigma * 0.30, 0.10)  # 30% of cross-window, min 10%
T_remaining_s = max(0.0, self._window_end_ts - now_ts)
scale = math.sqrt(T_remaining_s / MARKET_WINDOW_SECONDS)
dynamic_floor = dynamic_floor_base * scale

sigma = max(intra_sigma if is_sigma_real else dynamic_floor, dynamic_floor)

# Extend published schema to include is_sigma_real flag
msg = pack([timestamp_ms, self._current_boundary, prob_up, prob_down, sigma, mid, 
            intra_sigma, int(is_sigma_real)])
```

**Task 1.4 — Update FVState in `smart_paper_trader.py` to consume new schema**
```python
@dataclass
class FVState:
    ts_ms: int = 0
    boundary_ts: int = 0   # replaces market_id (was boundary_ts already)
    prob_up: float = 0.5
    prob_down: float = 0.5
    sigma: float = 0.0
    btc_price: float = 0.0
    intra_vol: float = 0.0       # NEW
    is_sigma_real: bool = False   # NEW

# In _on_fv():
if len(parsed) >= 8:
    ts_ms, boundary_ts, prob_up, prob_down, sigma, btc_price, intra_vol, is_real = parsed[:8]
    self._fv = FVState(..., intra_vol=intra_vol, is_sigma_real=bool(is_real))
```

**Task 1.5 — Add sigma-real gate to entry check**
```python
# In _check_entries():
if not self._fv.is_sigma_real:
    log.debug("SKIP entry — sigma not real (intra-window buffer warming up)")
    return
```

**Task 1.6 — Update `Stats.sigma_at_floor` to count `is_sigma_real=False` entries instead**
```python
# In _simulate_entry():
if not self._fv.is_sigma_real:
    self._stats.sigma_at_floor += 1  # repurpose: "sigma not from real vol"
```

**Dependencies:** None. This phase is self-contained.

**Risks:**
- EWMA alpha=0.94 may be inappropriate for 5-minute windows. The correct alpha depends on the desired half-life. At ~100ms tick intervals, alpha=0.94 gives a half-life of ~1.1 seconds — too fast. Use alpha=0.999 for ~100s half-life or higher. Test empirically.
- First 30s of each window will always have `is_sigma_real=False`. If TOS architecture enters at t=210–270s, this is fine (plenty of time for buffer warmup). But for Architecture C mid-window entries, this gate eliminates the first 30s anyway.
- Cross-window buffer still carries old data at boundary. Accept this — it's the fallback, not the primary.

**Validation approach:**
1. Run `fv_engine.py` for 10 minutes. Confirm `is_sigma_real` transitions to True within 30s of each window start.
2. Run the paper trader for 24h. Confirm `sigma_at_floor` counter drops from 261/261 to < 20%.
3. Inspect `smart_fills.jsonl` — sigma values should now vary continuously, not stick at 0.50.
4. If sigma still sticks: the Binance feed is in a genuinely low-vol period. Verify by checking Binance's actual 5-minute vol for the same period.

**Rollback:** Set `USE_INTRA_WINDOW_VOL=0` env var to fall back to existing cross-window buffer behavior.

**Expected impact:** This is the highest-impact single change possible. Once sigma is real, FV probabilities become meaningful, and the entire downstream pipeline becomes valid.

---

## Phase 2 — Architecture A (Terminal Oracle Sniper) Paper Trade (Week 2, 4–5 days)

**Objective:** Implement TOS entry policy + settlement-only exit in the paper trader. Run 72h paper trade. Measure win rate, expectancy, entry frequency.

**Components touched:** `cmd/smart_paper_trader.py`, `cmd/pm_daemon.py`, `gamma.py`

**Tasks:**

**Task 2.1 — Add `TOSEntryPolicy` class to `smart_paper_trader.py`**

As designed in Section 3.5. Key parameters (environment-variable driven):
```
TOS_ENTRY_START_S = 210    # seconds elapsed in window before entries allowed
TOS_ENTRY_END_S   = 270    # seconds elapsed in window after which entries blocked
TOS_MIN_PROB      = 0.70   # minimum BS probability to enter
TOS_MIN_EDGE      = 0.05   # minimum (prob - pm_ask) to enter
TOS_MIN_LIQUIDITY = 20.0   # minimum USDC at PM ask level
TOS_Z_THRESHOLD   = 1.04   # |z| > 1.04 → P > 0.85 
```

**Task 2.2 — Add `settlement_gate()` method**

For paper trading, settlement must query actual Gamma resolution, not use FV proxy. Add:
```python
async def _resolve_market_settlement(self, market_id: str) -> Optional[str]:
    """Query Gamma API for the actual settlement outcome. Returns 'UP', 'DOWN', or None."""
    # Use the MarketDiscovery session to query resolution
    # Cache results to avoid repeated queries
    # Fall back to FV proxy if resolution unavailable (and log a warning)
```

**Task 2.3 — Add `market_ts` window-match guard**

```python
# In _on_pm():
# Match FV boundary to PM market timestamp
fv_boundary = self._fv.boundary_ts   # seconds, from fv_engine
pm_market_ts = self._pm.market_ts    # seconds, from pm_daemon
if fv_boundary != pm_market_ts:
    # Windows are mismatched — FV is for a different window than PM book
    log.debug(
        "Window mismatch: FV boundary=%d PM market_ts=%d — skipping",
        fv_boundary, pm_market_ts
    )
    self._stats.window_mismatches += 1
    return
```

**Task 2.4 — Implement `TOSExitPolicy` (no mid-window exits)**

```python
# Replace the entire _check_exits() implementation with:
def _check_exits(self, market_id, bid_up, bid_dn, ts_ms):
    if self._exit_policy == "TOS":
        return  # TOS: hold to settlement, no mid-window exits
    # ... existing time-aware logic for non-TOS modes ...
```

**Task 2.5 — Fix settlement resolution**

```python
def _settle_market(self, market_id: str) -> None:
    """Settle positions using actual Gamma resolution (not FV proxy)."""
    # Query Gamma for outcome
    outcome = self._resolve_cache.get(market_id)
    if outcome is None:
        outcome = asyncio.run_until_complete(
            self._resolve_market_settlement(market_id)
        )
    
    if outcome == "UP":
        settlement = {"UP": 1.0, "DOWN": 0.0}
    elif outcome == "DOWN":
        settlement = {"UP": 0.0, "DOWN": 1.0}
    else:
        # Fallback to FV proxy with warning
        log.warning("Could not resolve %s — using FV proxy", market_id[:8])
        prob_up = self._fv.prob_up
        settlement = {"UP": (1.0 if prob_up > 0.5 else 0.0), 
                      "DOWN": (0.0 if prob_up > 0.5 else 1.0)}
```

**Task 2.6 — Update `EntryRecord` schema**

Add: `z_score`, `intra_vol`, `is_sigma_real`, `elapsed_s`, `window_end_ts` as required by the new FVState fields.

**Task 2.7 — Experiment 4 setup (15-minute market parallel test)**

Add `MARKET_TYPE=5m|15m` env var to `gamma.py`. When set to `15m`, generate `btc-updown-15m-{ts}` slugs instead. Run paper trader against both markets simultaneously (two separate paper trader instances, two separate JSONL files) for 48h.

**Dependencies:** Phase 1 must be complete (real sigma required for TOS to produce meaningful z-scores).

**Risks:**
- With `is_sigma_real` gating and TOS window restriction (t=210–270s), entry frequency may be very low (5–15 per day as predicted). If it's 0, the window timing or sigma warmup may be wrong. Debug by temporarily lowering `TOS_ENTRY_START_S` to 100 and checking entries.
- Settlement resolution API may be slow or rate-limited. Cache results and add a 2-second timeout.
- If the Gamma API returns the wrong winner (oracle dispute), settlement proxy will record incorrect outcomes. Acceptable for paper trading; must be verified for live.

**Validation approach:**
1. After 72h: entries/day should be 5–15.
2. Win rate (settlement wins / total settled) should be > 55%.
3. Expectancy per trade should be > $0.
4. Zero stop-loss exits (TOS has no SL).
5. `window_mismatches` counter should be < 5% of PM ticks.

**Rollback:** `ENTRY_POLICY=legacy` env var restores prior behavior.

**Expected impact:** If win rate is 60%+ and expectancy is positive, Architecture A is validated for live deployment at small size.

---

## Phase 3 — Architecture B (Dual-Leg Arb) Infrastructure (Week 3, 3 days)

**Objective:** Build concurrent FOK dual-leg execution. Paper trade for 24h to verify leg-fill atomicity.

**Components touched:** `exchange.py`, `cmd/smart_paper_trader.py` (arb mode), `config.py`

**Tasks:**

**Task 3.1 — `execute_concurrent_fok()` in `exchange.py`**

```python
async def execute_concurrent_fok(
    self,
    token_up:    str,
    token_down:  str,
    ask_up:      float,
    ask_down:    float,
    shares:      float,
    compensate:  bool = True,
) -> dict:
    """
    Fire both FOK legs simultaneously. 
    If one fills and the other fails, execute a compensating market sell.
    Returns status dict: {up_filled, down_filled, compensated, net_cost}
    """
    try:
        results = await asyncio.gather(
            self._place_fok(token_up, ask_up, shares, Side.BUY),
            self._place_fok(token_down, ask_down, shares, Side.BUY),
            return_exceptions=True
        )
    except Exception as e:
        log.error("Concurrent FOK exception: %s", e)
        return {"up_filled": False, "down_filled": False, "compensated": False}
    
    up_ok = not isinstance(results[0], Exception) and results[0].get("status") == "matched"
    dn_ok = not isinstance(results[1], Exception) and results[1].get("status") == "matched"
    
    if up_ok and dn_ok:
        return {"up_filled": True, "down_filled": True, "compensated": False}
    
    # Partial fill — compensate
    if up_ok and not dn_ok and compensate:
        await self._emergency_market_sell(token_up, shares)
        return {"up_filled": True, "down_filled": False, "compensated": True}
    if dn_ok and not up_ok and compensate:
        await self._emergency_market_sell(token_down, shares)
        return {"up_filled": False, "down_filled": True, "compensated": True}
    
    return {"up_filled": False, "down_filled": False, "compensated": False}
```

**Task 3.2 — Arb scanner in `smart_paper_trader.py`**

Add an `ArbScanner` that runs in parallel with the TOS strategy:
```python
async def _arb_scanner_loop(self):
    """Continuously monitor combined ask for structural arb opportunities."""
    while not stop_event.is_set():
        combined = self._pm.combined_ask()
        if combined and combined < ARB_TARGET_COMBINED:  # e.g. 0.96
            liq_up = self._pm.liq_up
            liq_dn = self._pm.liq_dn
            if liq_up and liq_dn:
                max_shares = min(liq_up, liq_dn, ARB_MAX_USDC / combined)
                if max_shares >= ARB_MIN_SHARES:
                    await self._simulate_arb_entry(combined, max_shares)
        await asyncio.sleep(0.05)  # 50ms poll
```

**Task 3.3 — Update `PM_BOOK` schema to include combined ask**

Add `combined_ask` as a pre-computed field from `pm_daemon.py`.

**Dependencies:** Phase 1 (sigma fix), Phase 2 (TOS paper trading infrastructure). Architecture B is additive — it runs alongside TOS, not instead of it.

**Validation approach:**
1. Run 24h paper trade with both TOS and arb scanner active.
2. Arb entries should show near-100% win rate when combined < 0.96.
3. Count days with 0 arb opportunities (expected: most days, since 0.96 is rare).

---

## Phase 4 — Signal Stack Elements (Week 4, 3–4 days)

**Objective:** Add Architecture C signal elements (BTC momentum persistence, orderbook imbalance) as optional entry filters on top of TOS. Measure impact on entry frequency and win rate.

**Components touched:** `cmd/smart_paper_trader.py`, `shared/math_utils.py`

**Tasks:**

**Task 4.1 — `SignalStack` class**

```python
class SignalStack:
    """Architecture C: require multiple signals to confirm before entry."""
    
    def btc_momentum_signal(self, btc_now, btc_30s_ago, btc_ref) -> Optional[str]:
        delta_now = (btc_now - btc_ref) / btc_ref
        delta_30s = (btc_30s_ago - btc_ref) / btc_ref
        if abs(delta_now) < 0.0004:  # < 0.04% move, no signal
            return None
        if sign(delta_now) != sign(delta_30s):  # no persistence
            return None
        return "UP" if delta_now > 0 else "DOWN"
    
    def orderbook_imbalance_signal(self, bid_depth_up, bid_depth_dn) -> Optional[str]:
        total = bid_depth_up + bid_depth_dn
        if total < 20.0:
            return None
        imbalance = (bid_depth_up - bid_depth_dn) / total
        if imbalance > 0.40:
            return "UP"
        if imbalance < -0.40:
            return "DOWN"
        return None
    
    def evaluate(self, fv, pm, btc_30s_ago) -> Optional[str]:
        signals = [
            self.btc_momentum_signal(fv.btc_price, btc_30s_ago, pm_K),
            self.orderbook_imbalance_signal(pm.liq_up, pm.liq_dn),
            # fv_signal: already baked into TOSEntryPolicy z-score check
        ]
        up = sum(1 for s in signals if s == "UP")
        dn = sum(1 for s in signals if s == "DOWN")
        if up >= 2:
            return "UP"
        if dn >= 2:
            return "DOWN"
        return None
```

**Task 4.2 — BTC price history buffer in `smart_paper_trader.py`**

Add a 60-second BTC price history deque for momentum computation:
```python
self._btc_history: deque[Tuple[float, float]] = deque(maxlen=600)  # (ts, price)
```

**Task 4.3 — Measure A/B impact**

Run two paper trader instances simultaneously:
- Instance A: TOS only (no signal stack)
- Instance B: TOS + signal stack (must satisfy `SignalStack.evaluate()`)

Compare entry frequency and win rate over 72h.

**Validation approach:** If Instance B has equal or better win rate with fewer entries, keep the signal stack. If win rate is worse (too many false negatives from conflicting signals), discard.

---

## Phase 5 — Live Deployment at Small Size (Week 5+)

**Conditions for proceeding to live:**
- Phase 2 paper trading win rate > 55% over ≥ 72h
- Phase 2 expectancy > $0 per trade
- Phase 2 entry frequency < 20/day
- Phase 1 sigma fix confirmed (< 20% entries at floor over 72h)
- Phase 0 experiments completed and documented
- Risk system fully integrated (all circuit breakers active)
- Settlement resolution uses Gamma API (not FV proxy)

**Tasks:**
- Replace `_simulate_entry()` with `exchange.execute_fok()` call
- Replace `_exit_position()` with `exchange.place_limit_sell()` or hold to settlement
- Integrate `RiskManager.check()` before every entry
- Add process health checks (see Part 8)
- Start at 10% of available capital per trade

---

# Part 5 — Metrics & Observability Design

## 5.1 Metrics Architecture

The current system uses JSONL files for post-hoc analysis and `print()` for terminal output. A proper observability system should add:

1. **Structured logging** (JSON lines to file, replacing mixed loguru/logging)
2. **In-process metrics counters** (extend `Stats` class)
3. **Periodic metrics snapshot** (emit to a metrics JSONL file every 60s)
4. **Alerting conditions** (configurable thresholds with SIGTERM to a monitoring process)

Defer Prometheus/Grafana to production hardening phase. For now, a structured `metrics.jsonl` file that can be tail-followed and graphed offline is sufficient.

## 5.2 Metric Definitions

**Signal quality metrics:**

| Metric | Computation | Target | Danger |
|---|---|---|---|
| `sigma_floor_rate` | `sigma_at_floor / total_fills` | < 0.10 | > 0.50: vol model broken |
| `intra_vol_mean` | Mean `intra_vol` across fills | 0.30–1.20 ann. | < 0.05: flat BTC session |
| `sigma_vs_actual_vol_error` | `|sigma_estimated - sigma_realized| / sigma_realized` | < 0.20 | > 0.50: model miscalibrated |
| `z_score_at_entry` | Distribution of |z| at fill time | Median > 1.0 | Median < 0.5: weak signal |
| `fv_vs_pm_correlation` | Pearson(prob_up_at_entry, settlement_winner) | > 0.15 | < 0.05: signal is noise |

**Trade quality metrics:**

| Metric | Computation | Target | Danger |
|---|---|---|---|
| `expectancy_per_trade` | `avg_win * win_rate - avg_loss * loss_rate` | > $0 | < -$0.10/trade for 20+ trades |
| `win_rate` | `(tp_exits + settlement_wins) / total_closed` | > 0.55 | < 0.40 over 30+ trades |
| `settlement_rate` | `settlement_exits / total_exits` | > 0.70 (TOS) | < 0.30: exits firing too early |
| `entries_per_window` | `total_entries / windows_observed` | < 2.0 | > 5: too aggressive |
| `edge_at_entry_mean` | Mean `fv - pm_ask` at fill | > 0.05 | < 0.02: entering on noise |
| `sl_recovery_rate` | SL exits that would have settled at $1 | < 0.30 | > 0.40: SL destroying value |

**Execution metrics:**

| Metric | Computation | Target | Danger |
|---|---|---|---|
| `fv_age_at_fill_p99` | 99th percentile FV age at entry | < 200ms | > 500ms: FV pipeline slow |
| `pm_book_age_mean` | Mean time since last PM book update | < 2s | > 10s: PM feed stale |
| `window_mismatch_rate` | `window_mismatches / pm_ticks` | < 0.01 | > 0.05: sync problem |
| `fill_cooldown_blocked_rate` | `cooldown_blocks / attempted_entries` | < 0.30 | > 0.70: churning |

**System health metrics:**

| Metric | Computation | Target | Danger |
|---|---|---|---|
| `binance_reconnects_per_hour` | Count WS reconnect events | < 1 | > 5: feed unstable |
| `pm_reconnects_per_hour` | Count PM WS reconnect events | < 2 | > 10: feed unstable |
| `zmq_drop_rate` | HWM drops logged by ZMQ | 0 | Any: consumer too slow |
| `chainlink_delta_usd` | Binance mid - Chainlink oracle | < $30 | > $50: oracle divergence |

## 5.3 Metrics Emission Points

```python
# In smart_paper_trader._simulate_entry():
emit_metric("entry", {
    "ts": ts_ms,
    "market_id": market_id[:8],
    "side": side,
    "z_score": fv.z_score,
    "edge": edge,
    "intra_vol": fv.intra_vol,
    "is_sigma_real": fv.is_sigma_real,
    "elapsed_s": elapsed_s,
    "pm_bid_depth": pm.liq_up if side == "UP" else pm.liq_dn,
})

# In smart_paper_trader._exit_position():
emit_metric("exit", {
    "ts": ts_ms,
    "reason": reason,
    "pnl": pnl,
    "hold_duration_s": hold_duration,
    "entry_price": avg_entry,
    "exit_price": exit_price,
})

# In fv_engine._on_tick() (every 5s):
emit_metric("fv_status", {
    "sigma": sigma,
    "intra_vol": intra_sigma,
    "is_sigma_real": is_sigma_real,
    "prob_up": prob_up,
    "t_remaining": T_remaining_s,
    "btc_delta_pct": btc_delta_pct,
})
```

## 5.4 Dashboard Design (Offline — Terminal)

```bash
# tools/dashboard.py — real-time terminal metrics display
# Tail metrics.jsonl and display:
# ┌─────────────────────────────────────────────────────┐
# │ BTC Bot Dashboard  2024-01-01 12:00:00              │
# ├──────────────────┬──────────────────────────────────┤
# │ Sigma            │ Trades                           │
# │ Intra: 0.82 ✓   │ Entries: 12  Win: 8/11  72.7%   │
# │ Cross: 0.71      │ Expectancy: +$0.23/trade        │
# │ Floor rate: 0%   │ SL exits: 0  (TOS mode)         │
# ├──────────────────┼──────────────────────────────────┤
# │ Signal           │ System                           │
# │ z_score: 1.84    │ FV age: 89ms  PM age: 340ms     │
# │ Edge: 0.072      │ Binance WS: OK                  │
# │ BTC Δ: +0.12%    │ PM WS: OK  Stale: 0             │
# └──────────────────┴──────────────────────────────────┘
```

## 5.5 Alerting Thresholds

```python
ALERT_CONDITIONS = {
    "sigma_floor_rate_gt_50pct": {
        "condition": lambda s: s.sigma_floor_rate > 0.50,
        "action": "log_warning + write_to_alert.log",
        "message": "sigma stuck at floor for >50% of recent fills — vol model broken",
    },
    "pm_book_stale_gt_30s": {
        "condition": lambda s: s.pm_book_age > 30,
        "action": "halt_entries + log_error",
        "message": "PM book stale >30s — do not enter new positions",
    },
    "hourly_loss_gt_threshold": {
        "condition": lambda s: s.hourly_pnl < -MAX_LOSS_PER_HOUR_USDC,
        "action": "halt_all + log_critical",
        "message": "Hourly loss limit breached — halting",
    },
    "chainlink_delta_gt_50": {
        "condition": lambda s: abs(s.chainlink_delta) > 50,
        "action": "log_warning",
        "message": "Binance/Chainlink divergence >$50 — oracle drift possible",
    },
}
```

---

# Part 6 — Backtesting & Replay Architecture

## 6.1 Design Principles

The replay system must be designed so that **strategy code does not know whether it is running live or in replay**. The same `smart_paper_trader.py` classes should work against both real ZMQ streams and a replay engine that injects historical events at the correct timestamps.

## 6.2 Historical Data Capture

**What to capture (add to production pipeline):**

```python
# tools/data_recorder.py — runs alongside the 4-process pipeline
# Subscribes to all three channels, writes events to a capture file

class DataRecorder:
    CHANNELS = [Channel.BINANCE_BBO, Channel.FV_STREAM, Channel.PM_BOOK]
    
    def record_event(self, channel: str, ts_ms: int, payload: bytes) -> None:
        line = {
            "channel": channel,
            "ts_ms": ts_ms,
            "data": base64.b64encode(payload).decode(),  # raw msgpack bytes
        }
        self._file.write(json.dumps(line) + "\n")
```

**Storage schema (one JSONL file per day):**
```
captures/2024-01-01.jsonl
  {"channel": "BINANCE_BBO", "ts_ms": 1704067200000, "data": "<b64>"}
  {"channel": "FV_STREAM",   "ts_ms": 1704067200100, "data": "<b64>"}
  {"channel": "PM_BOOK",     "ts_ms": 1704067200150, "data": "<b64>"}
```

**Storage requirements:** At ~10 Binance ticks/s + ~1 FV/s + ~1 PM/s, a single day of captures is approximately:
- Binance BBO: 10 ticks/s × 86400s × ~30 bytes/msg = ~25 MB/day
- FV_STREAM: ~1/s × 86400 × ~50 bytes = ~4 MB/day  
- PM_BOOK: ~1/s × 86400 × ~80 bytes = ~7 MB/day
- **Total: ~36 MB/day, ~1 GB/month**. This is manageable on local storage.

## 6.3 Replay Engine

```python
class ReplayEngine:
    """
    Reads a capture file and replays events through ZMQ sockets
    at the correct relative timing (or at maximum speed for offline backtesting).
    """
    
    def __init__(self, capture_path: Path, speed_multiplier: float = 0):
        # speed_multiplier=0 means "as fast as possible"
        # speed_multiplier=1.0 means real-time
        # speed_multiplier=10.0 means 10x speed
        self._capture_path = capture_path
        self._speed = speed_multiplier
        
        # Bind ZMQ publishers on the same addresses as the real daemons
        # but on different ports for replay isolation
        self._pubs = {
            "BINANCE_BBO": zmq_pub("ipc:///tmp/replay_binance.ipc"),
            "FV_STREAM":   zmq_pub("ipc:///tmp/replay_fv.ipc"),
            "PM_BOOK":     zmq_pub("ipc:///tmp/replay_pm.ipc"),
        }
    
    async def run(self, start_ts=None, end_ts=None):
        events = self._load_events(start_ts, end_ts)
        base_ts = events[0]["ts_ms"]
        replay_start = time.time()
        
        for event in events:
            # Calculate when to emit this event
            event_offset_ms = event["ts_ms"] - base_ts
            if self._speed > 0:
                target_elapsed = event_offset_ms / 1000.0 / self._speed
                actual_elapsed = time.time() - replay_start
                sleep_s = target_elapsed - actual_elapsed
                if sleep_s > 0:
                    await asyncio.sleep(sleep_s)
            
            # Emit to the correct ZMQ publisher
            pub = self._pubs[event["channel"]]
            raw = base64.b64decode(event["data"])
            pub.send(raw)
```

## 6.4 Deterministic Replay Requirements

For experiment reproducibility:
- Events must be replayed in original timestamp order (sort by `ts_ms` on load)
- All random number generation must be seeded from a fixed seed (none exists currently — add if needed)
- Configuration parameters must be recorded alongside the capture (snapshot `config.py` values at recording start)
- Settlement outcomes from the Gamma API must also be captured (add to `DataRecorder`)

## 6.5 Experiment Framework

```python
class ReplayExperiment:
    """Run a strategy configuration against a historical capture."""
    
    def __init__(self, capture_path, strategy_config):
        self.replay = ReplayEngine(capture_path, speed_multiplier=0)
        self.trader = SmartPaperTrader(config=strategy_config)  # dependency injection
    
    async def run(self) -> ExperimentResult:
        await asyncio.gather(
            self.replay.run(),
            self.trader.run(stop_event),
        )
        return ExperimentResult(
            metrics=self.trader._stats,
            config=self.strategy_config,
            capture_period=self.replay.period,
        )
    
    def compare(self, other: ExperimentResult) -> ComparisonReport:
        """Statistical comparison of two experiment results."""
        ...
```

## 6.6 Latency Simulation

For realistic backtesting, add configurable latency to the replay engine:
```python
REPLAY_LATENCY_CONFIG = {
    "fv_to_entry_latency_ms": 15,    # FV processing + entry decision
    "pm_book_to_entry_latency_ms": 5, # PM book processing
    "order_to_fill_latency_ms": 50,   # order placement + fill confirmation
    "pm_slippage_pct": 0.002,         # 0.2% slippage on FOK fill
}
```

---

# Part 7 — Experiment Implementation Plan

## Experiment 1 — Signal-Outcome Correlation Audit

**Status:** Can run immediately against existing logs.

**Implementation steps:**
```python
# tools/exp1_signal_audit.py

import json
import aiohttp
import asyncio
from pathlib import Path
from scipy.stats import pearsonr

fills = [json.loads(l) for l in Path("smart_fills.jsonl").read_text().splitlines() if l.strip()]

# Step 1: Fetch resolution for each unique market_id
async def fetch_resolution(session, market_id) -> Optional[str]:
    url = f"https://gamma-api.polymarket.com/markets?condition_id={market_id}"
    async with session.get(url) as r:
        data = await r.json()
        # Look for question.outcome or resolved field
        # Returns "YES" (UP wins) or "NO" (DOWN wins) or None

# Step 2: For each fill, compute:
#   btc_delta_at_fill = (btc_price - K)  ... need K from fv_engine logs
#   predicted_direction = "UP" if fv > 0.5 else "DOWN"
#   actual_winner = result from Gamma resolution

# Step 3: Correlation analysis
btc_deltas = [...]      # continuous variable
up_wins = [...]         # 1.0 if UP won, 0.0 if DOWN won

r, p_val = pearsonr(btc_deltas, up_wins)
print(f"Pearson r: {r:.4f}  p-value: {p_val:.4f}")
print(f"Signal accuracy: {accuracy:.1%}")
```

**Required datasets:** `smart_fills.jsonl`, Gamma API for resolution outcomes.

**Expected outputs:** Pearson correlation coefficient, p-value, confusion matrix, precision/recall for UP and DOWN direction prediction.

**Statistical methodology:**
- Minimum sample size for meaningful correlation: n=30 resolved trades
- Use Fisher z-transform for confidence interval on r
- Bonferroni correction if testing multiple timeframes

**Pass/fail criteria:**
- PASS: |r| > 0.10 AND p < 0.05 → the signal has measurable (if weak) predictive power. Proceed with fixing sigma and re-testing.
- FAIL: |r| < 0.05 OR p > 0.20 → the signal is noise. The FV model needs fundamental replacement before any further iteration.

**Visualization:** Scatter plot of BTC delta vs. settlement outcome. Heatmap of predicted vs. actual direction. Time-of-day heatmap of fill timing.

---

## Experiment 2 — SL Counterfactual Replay

**Status:** Can run immediately.

**Implementation steps:**
```python
# tools/exp2_sl_counterfactual.py

sl_exits = [e for e in exits if e["exit_reason"] in ("STOP_LOSS", "EMERGENCY_CUT")]

# Fetch actual settlement for each SL exit's market_id
# Compare: sl_exit["side"] == actual_winner → would have been profitable

counterfactual_win = 0
total_sl = len(sl_exits)

for exit in sl_exits:
    winner = fetch_resolution(exit["market_id"])
    if winner and winner == exit["side"]:
        counterfactual_win += 1
        would_have_profited += exit["shares"] * (1.0 - exit["avg_entry"])  # full payoff - cost
    else:
        confirmed_loss += exit["shares"] * exit["avg_entry"]  # cost lost

print(f"SL exits that would have won at settlement: {counterfactual_win}/{total_sl} = {counterfactual_win/total_sl:.0%}")
print(f"Value destroyed by SL: ${would_have_profited:.2f}")
print(f"Value saved by SL: ${confirmed_loss:.2f}")
print(f"Net SL impact: ${would_have_profited - confirmed_loss:+.2f}")
```

**Pass/fail criteria:**
- PASS: < 30% of SL exits would have won → the SL is doing its job. Keep it, but tune the threshold.
- FAIL: > 40% of SL exits would have won → the SL is destroying value. Remove all mid-window exits for the TOS path. This is the most likely outcome given the report's analysis and the current data showing avg SL exit price of 0.110 on avg entry of 0.184.

---

## Experiment 3 — Vol Model Accuracy

**Status:** Requires Phase 1 (sigma fix) to be complete first.

**Implementation steps:**
- After running the sigma-fixed paper trader for 48h, compare `intra_vol` from `smart_fills.jsonl` against the actual Binance 5-minute realized vol for the same windows.
- Binance 5-min klines provide `open, high, low, close` — compute Parkinson vol as `sqrt(ln(high/low)^2 / (4*ln(2)))` for each 5-min candle.
- Correlation between bot's `intra_vol` estimate and Parkinson vol should be > 0.7.

**Script:** `tools/exp3_vol_accuracy.py`

---

## Experiment 4 — Late-Entry Paper Trade Only

**Status:** Implement as part of Phase 2 (`TOS_ENTRY_START_S=210`).

**Parallel comparison design:**
- Run two instances of `smart_paper_trader.py` simultaneously:
  - `ENTRY_POLICY=legacy` (100s window age, no time gate)
  - `ENTRY_POLICY=TOS` (210s gate, settlement-only exit)
- Use separate `FILLS_PATH` and `EXITS_PATH` for each
- Run both for 72h and compare

**Expected outputs:** Entries/day, win rate, expectancy, edge at entry, sigma floor rate for both.

**Pass criteria for TOS:** win rate > 55% AND expectancy > $0 with n ≥ 30 settled trades.

---

## Experiment 5 — 15-Minute Market Comparison

**Status:** Implement with `MARKET_TYPE=15m` env var in Phase 2.

**Setup:**
```bash
# Instance 1: existing 5-minute paper trader
MARKET_TYPE=5m FILLS_PATH=fills_5m.jsonl EXITS_PATH=exits_5m.jsonl \
  python -m cmd.smart_paper_trader

# Instance 2: 15-minute market paper trader
MARKET_TYPE=15m FILLS_PATH=fills_15m.jsonl EXITS_PATH=exits_15m.jsonl \
  MARKET_WINDOW_SECONDS=900 MIN_WINDOW_AGE_S=300 \
  TOS_ENTRY_START_S=630 TOS_ENTRY_END_S=810 \
  python -m cmd.smart_paper_trader
```

**Pass criteria:** If 15-min win rate is > 5 percentage points above 5-min win rate over the same 72h period, switch primary trading venue before going live on 5-min markets.

---

# Part 8 — Risk Management & Safety Layer

## 8.1 Critical Failure Modes

| Failure Mode | Current Protection | Required Protection |
|---|---|---|
| Sigma stuck at floor → infinite noise entries | `sigma_at_floor` counter (soft warn only) | Hard entry gate on `is_sigma_real=False` |
| PM book stale → trading on old prices | 10s warning (log only) | Halt entries after 10s stale |
| FV/PM window mismatch → wrong probability | None | Hard gate on `boundary_ts == market_ts` |
| Runaway entries on single market | `MAX_SHARES_PER_SIDE` cap | Also: `MAX_ENTRIES_PER_WINDOW` limit |
| Duplicate orders on crash-restart | None | Startup position reconciliation |
| Chainlink oracle divergence | Optional check (disabled) | Enable by default, halt on > $100 delta |
| Process crash mid-trade | None | Position reconciliation on restart |
| Network partition (PM inaccessible) | PM WS reconnect loop | Add: entry halt if PM feed dark > 30s |
| Binance feed disconnect | Reconnect with backoff | Already implemented. Also: halt entries if Binance dark > 5s |

## 8.2 Enhanced `RiskManager`

```python
class RiskManagerV2(RiskManager):
    """Extended risk system for Phase 2+."""
    
    def __init__(self):
        super().__init__()
        self.pm_last_update_t: float = 0.0
        self.binance_last_update_t: float = 0.0
        self.entries_this_window: int = 0
        self.window_boundary_ts: int = 0
        self.sigma_real_entries: int = 0
        self.total_entries: int = 0
    
    def check_data_freshness(self, fv_age_ms: float, pm_age_ms: float) -> bool:
        """Return False if either feed is too stale to trust."""
        if fv_age_ms > 1000:  # FV older than 1 second
            log.warning("FV stale: %.0fms", fv_age_ms)
            return False
        if pm_age_ms > 10000:  # PM book older than 10 seconds  
            log.warning("PM book stale: %.0fms", pm_age_ms)
            return False
        return True
    
    def record_window_boundary(self, boundary_ts: int):
        """Called on each window transition."""
        if boundary_ts != self.window_boundary_ts:
            self.entries_this_window = 0
            self.current_spent = 0.0  # reset spend cap per window
            self.window_boundary_ts = boundary_ts
    
    def check_entry_allowed(self, is_sigma_real: bool) -> Tuple[bool, str]:
        """Multi-condition entry gate."""
        if self.trading_halted:
            return False, f"halted: {self.halt_reason}"
        if not is_sigma_real:
            return False, "sigma not real"
        if self.entries_this_window >= MAX_ENTRIES_PER_WINDOW:
            return False, f"window entry limit ({MAX_ENTRIES_PER_WINDOW}) reached"
        if self.current_spent >= MAX_SPEND_PER_MARKET:
            return False, "market spend cap reached"
        if self.hourly_pnl < -MAX_LOSS_PER_HOUR_USDC:
            self.halt("hourly loss limit")
            return False, "hourly loss limit"
        return True, "ok"
    
    def reset(self):
        """Manual reset (e.g. after acknowledging halt in production)."""
        self.trading_halted = False
        self.halt_reason = ""
        log.warning("RiskManager manually reset — verify all positions before trading")
```

## 8.3 Kill Switch Design

Add a `KILL_SWITCH_FILE` path (e.g. `/tmp/btcbot_halt`). Check for file existence on every PM tick:

```python
# In smart_paper_trader._on_pm():
if Path(KILL_SWITCH_FILE).exists():
    log.critical("Kill switch file detected — halting all entries")
    self._risk.halt("kill switch file")
    # In live mode: cancel all open orders
    return
```

This allows stopping the bot from outside the process without SIGKILL:
```bash
touch /tmp/btcbot_halt   # halt immediately
rm /tmp/btcbot_halt      # re-enable (requires manual risk.reset() in bot)
```

## 8.4 Position Reconciliation on Startup

```python
# In smart_paper_trader.__init__() — for live trading path:
async def _reconcile_positions(self):
    """On startup, check exchange for existing positions to avoid double-trading."""
    if LIVE_TRADING:
        open_positions = await self._exchange.get_open_positions()
        for pos in open_positions:
            # Reconstruct Position objects from exchange data
            self._positions[pos.market_id][pos.side] = Position(
                market_id=pos.market_id,
                side=pos.side,
                shares=pos.shares,
                cost=pos.avg_entry * pos.shares,  # approx cost
            )
        log.info("Reconciled %d open positions from exchange", len(open_positions))
```

## 8.5 Safety Invariants (Hard-coded, Non-configurable)

These must never be overridden by env vars:
1. **Never enter when `is_sigma_real=False`.** The vol model must be active.
2. **Never enter when PM book age > 30s.** Stale prices are worse than no prices.
3. **Never post a trade larger than `MAX_TAKER_FILL_USDC`.** Hard-coded in exchange.py.
4. **Never trade with combined ask > 1.02.** Already in `RiskManager.check()`.
5. **Hard stop if hourly loss > `MAX_LOSS_PER_HOUR_USDC`.** Already implemented.

## 8.6 Anomaly Detection

```python
# Add to metrics emission (every 60s):
anomalies = []

# 1. Entry rate spike
if entries_last_5min > 10:
    anomalies.append("entry_rate_spike")

# 2. Consistent SL cascade
sl_last_5_exits = sum(1 for e in recent_exits if e["exit_reason"] == "STOP_LOSS")
if sl_last_5_exits >= 4:
    anomalies.append("sl_cascade")

# 3. PM book extreme spread
if spread_up > 0.20 or spread_dn > 0.20:
    anomalies.append("extreme_spread")

# 4. BTC flash crash detection
if abs(btc_5min_delta_pct) > 0.02:  # > 2% BTC move in 5 min
    anomalies.append("btc_flash_event")
    # Consider: halt entries until vol stabilizes
```

---

# Part 9 — Production Deployment Architecture

## 9.1 Service Orchestration

```
# Production deployment: systemd units (not bash)
# 4 services: binance, fv, pm, trader

[Unit]
Name=btcbot-binance
Description=BTC Bot Binance Price Feed
After=network.target

[Service]
Type=simple
User=btcbot
WorkingDirectory=/opt/btcbot
ExecStart=/opt/btcbot/venv/bin/python -u -m cmd.binance_daemon
Restart=on-failure
RestartSec=5
StartLimitIntervalSec=300
StartLimitBurst=3
StandardOutput=journal
StandardError=journal
```

## 9.2 Process Startup Order & Health Checks

```bash
# Start order with health checks:
1. binance_daemon — wait for BINANCE_BBO to produce messages (timeout: 30s)
2. fv_engine — wait for FV_STREAM to produce messages (timeout: 30s)  
3. pm_daemon — wait for PM_BOOK to produce messages (timeout: 60s)
4. trader — check all three feeds are active before allowing entries

# Health check script: tools/health_check.py
# Returns exit code 0 if all feeds are active, 1 if any are stale
```

## 9.3 Structured Logging

Consolidate to a single logging system. Recommended: switch all modules to stdlib `logging` with JSON formatter:

```python
# shared/logging_config.py
import logging
import json

class JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "ts": record.created,
            "level": record.levelname,
            "module": record.name,
            "msg": record.getMessage(),
            "extra": getattr(record, "extra", {}),
        })

def setup_logging(service_name: str, log_dir: str = "logs"):
    handler = logging.handlers.TimedRotatingFileHandler(
        f"{log_dir}/{service_name}.log",
        when="midnight",
        backupCount=14,
    )
    handler.setFormatter(JSONFormatter())
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.DEBUG)
```

This enables `jq` filtering on log files and eventual shipping to a log aggregator.

## 9.4 Configuration Management

Current approach (env vars + dotenv) is correct. Additions needed:
- Add `CONFIG_VERSION` string to `.env` for tracking when config was last changed
- Add config snapshot to startup logs: emit all config values as a structured log event on process start
- Separate `.env.paper` and `.env.live` for paper vs. live configuration
- Never commit `.env` with credentials. Use `.env.example` with placeholder values.

## 9.5 Feature Flags

```python
# config.py additions:
FEATURE_TOS_ENTRY = bool(int(os.getenv("FEATURE_TOS_ENTRY", "0")))  # disabled by default
FEATURE_ARB_SCANNER = bool(int(os.getenv("FEATURE_ARB_SCANNER", "0")))
FEATURE_SIGNAL_STACK = bool(int(os.getenv("FEATURE_SIGNAL_STACK", "0")))
FEATURE_REAL_SETTLEMENT = bool(int(os.getenv("FEATURE_REAL_SETTLEMENT", "0")))
```

## 9.6 Paper/Live Separation

```python
# config.py:
TRADING_MODE = os.getenv("TRADING_MODE", "paper")  # "paper" or "live"

# smart_paper_trader.py:
if TRADING_MODE == "live":
    assert self._exchange is not None, "Live mode requires exchange connection"
    assert not self._exchange._read_only, "Live mode requires private key"
else:
    # Paper mode: simulate entries and exits, no real orders
```

## 9.7 Monitoring Stack (Target State)

For 24/7 operation:
- **Loki + Promtail** for log aggregation (lightweight alternative to ELK)
- **Prometheus + Grafana** for metrics (if metrics.jsonl approach is insufficient)
- **PagerDuty or ntfy.sh** for alerting (kill-switch conditions, hourly loss breach)
- **Uptime monitoring** (UptimeRobot or similar) to verify the 4 processes are running

For now (development): tail `metrics.jsonl` with the `tools/dashboard.py` script.

## 9.8 Secrets Handling

- Private key, CLOB API key must never appear in logs. `config.py` should redact them from any config dump.
- Use environment variables, not `.env` file, in production (inject via systemd `EnvironmentFile` directive)
- Rotate API keys after any suspected exposure

---

# Part 10 — Master TODO Tree

## Dependency Order

```
[BLOCKER] ── Phase 0: Data Analysis
│   ├── Task 0.1: Signal-outcome correlation (exp1_signal_audit.py) [2d]
│   └── Task 0.2: SL counterfactual replay (exp2_sl_counterfactual.py) [1d]
│       └── DECISION: which architecture to build
│
[BLOCKER] ── Phase 1: Sigma Fix (requires: none)
│   ├── Task 1.1: Add intra_window_prices buffer to FVEngine [0.5d]
│   ├── Task 1.2: Add ewma_vol() to math_utils.py [0.5d]
│   ├── Task 1.3: Switch FVEngine to use intra-window EWMA vol [1d]
│   ├── Task 1.4: Update FVState schema (is_sigma_real flag) [0.5d]
│   ├── Task 1.5: Add sigma-real gate to entry check [0.5d]
│   └── VALIDATION: 24h paper run — sigma_floor_rate < 20% [1d]
│
├── Phase 2: Architecture A — TOS (requires: Phase 1 complete)
│   ├── Task 2.1: TOSEntryPolicy class [1d]
│   ├── Task 2.2: Market_ts window-match guard [0.5d]
│   ├── Task 2.3: TOSExitPolicy (settlement-only) [0.5d]
│   ├── Task 2.4: Real settlement resolution (Gamma query) [1d]
│   ├── Task 2.5: Update EntryRecord schema [0.5d]
│   ├── Task 2.6: Experiment 4 setup (TOS vs. legacy paper trade) [0.5d]
│   ├── Task 2.7: Experiment 5 setup (15m market parallel test) [1d]
│   └── VALIDATION: 72h paper run — win rate > 55%, expectancy > $0 [3d]
│
├── Phase 1b: Risk System Upgrade (can run parallel to Phase 2)
│   ├── Task R1: RiskManagerV2 (data freshness, window reset) [1d]
│   ├── Task R2: Kill switch file detection [0.5d]
│   ├── Task R3: Position reconciliation on startup [1d]
│   └── Task R4: Anomaly detection in metrics emission [1d]
│
├── Phase 1c: Observability (can run parallel to Phase 2)
│   ├── Task O1: Unified JSON logging (replace loguru) [1d]
│   ├── Task O2: metrics.jsonl emission points [1d]
│   └── Task O3: tools/dashboard.py terminal display [1d]
│
├── Phase 1d: Data Capture for Replay (start immediately)
│   ├── Task D1: tools/data_recorder.py [1d]
│   └── Task D2: Capture settlement outcomes alongside market events [0.5d]
│
├── Phase 3: Architecture B — Dual-Leg Arb (requires: Phase 2 validated)
│   ├── Task 3.1: execute_concurrent_fok() in exchange.py [1.5d]
│   ├── Task 3.2: ArbScanner in smart_paper_trader.py [1d]
│   ├── Task 3.3: Extended PM_BOOK schema (liq depth, combined_ask) [0.5d]
│   └── VALIDATION: 24h paper run — arb entries 98%+ win rate [1d]
│
├── Phase 4: Signal Stack (requires: Phase 2 + Experiment 5 results)
│   ├── Task 4.1: SignalStack class [1d]
│   ├── Task 4.2: BTC 60s price history buffer [0.5d]
│   └── VALIDATION: A/B paper run TOS vs TOS+signals [3d]
│
├── Phase 5: Replay Infrastructure (can run parallel after Phase 1d starts)
│   ├── Task R1: ReplayEngine core [2d]
│   ├── Task R2: Experiment framework [1d]
│   └── Task R3: Latency simulation config [0.5d]
│
└── Phase 6: Live Deployment (requires: Phase 2 validated + Phase 1b+1c+1d complete)
    ├── Task L1: Systemd unit files [0.5d]
    ├── Task L2: .env.paper / .env.live separation [0.5d]
    ├── Task L3: Secrets hardening [0.5d]
    └── VALIDATION: 24h live at 10% capital, compare to paper [1d]
```

## Priority Summary

**Highest impact, do first:**
1. `exp1_signal_audit.py` and `exp2_sl_counterfactual.py` — validates the entire strategy direction before writing code (2–3 days, no coding required beyond analysis scripts)
2. Sigma fix (Phase 1) — the root cause of all current losses (3–4 days)
3. TOS paper trading (Phase 2) — first meaningful strategy test (4–5 days)

**High value, do in parallel:**
4. Data recorder (Phase 1d) — enables all future backtesting. Start immediately.
5. Risk system upgrade (Phase 1b) — required before live trading
6. Unified logging (Phase 1c) — required before making sense of 24h paper run data

**Do later:**
7. Architecture B (arb) — complementary, not critical path
8. Signal stack (Phase 4) — only if TOS validation shows room to improve
9. Replay infrastructure — valuable for long-term research, not blocking live

**Validate before proceeding to live:**
- Sigma floor rate < 20% in 24h paper run
- TOS win rate > 55% in 72h paper run with ≥ 30 settled trades
- Zero missing-risk-check code paths
- All experiment scripts producing documented results
- Real settlement resolution (not FV proxy) confirmed working

## Estimated Timeline

| Phase | Duration | Blocking? |
|---|---|---|
| Phase 0 (experiments) | 3 days | Blocks Phase 2 decision |
| Phase 1 (sigma fix) | 4 days + 1d validation | Blocks Phase 2 |
| Phase 1b/1c/1d (parallel) | 4 days | Blocks Phase 6 |
| Phase 2 (TOS paper) | 5 days + 3d validation | Blocks Phase 5/6 |
| Phase 3 (arb) | 3 days + 1d validation | Independent |
| Phase 4 (signal stack) | 4 days + 3d validation | Post-Phase 2 |
| Phase 5 (replay) | 3.5 days | Independent |
| Phase 6 (live) | 3 days + 1d validation | Post-Phase 2+1b+1c+1d |
| **Total** | **~5 weeks** | |

---

## Final Note

The diagnosis in `Report.md` is confirmed by the data: **all 261 fills used a broken sigma, producing no real signal.** The engineering path forward is clear. Phase 0 validates; Phase 1 fixes the root cause; Phase 2 tests the highest-probability strategy redesign. Every phase has explicit validation criteria and rollback paths. Do not skip the validation gates — each one prevents wasted work in the phase that follows.

The single most actionable item you can do today, before writing any code, is running `exp1_signal_audit.py` and `exp2_sl_counterfactual.py` against the existing logs. The results will tell you whether the TOS architecture is the right direction before you spend a week implementing it.
