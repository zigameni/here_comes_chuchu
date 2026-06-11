"""
strategies/tos_signal/strategy.py
──────────────────────────────────
TOS + Signal Stack strategy (Architecture C).

TOS_SIGNAL wraps a TOSStrategy and adds a two-signal confirmation gate:
  1. TOS base evaluation (timing, z-score, prob, edge, liquidity)
  2. BTC history availability check (30s lookback within 2s tolerance)
  3. Signal stack consensus:
       momentum  = btc_momentum_signal(btc_now, btc_30s_ago, K)
       imbalance = orderbook_imbalance_signal(pm)
       consensus = plurality vote
  4. Consensus must match the TOS direction (UP/DOWN)

If all four stages pass, the TOS EntrySignal is forwarded unchanged.
If any stage fails, [] is returned and the relevant counter is incremented.

BTC history is maintained internally via on_fv_update(), which is called
by SmartPaperTrader on every FV tick.  This makes the strategy self-contained
and independently testable without access to SmartPaperTrader state.

Diagnostics are split into two namespaces:
  "tos"          → TOSStrategy.get_diagnostics()  (entry gate counters)
  "signal_gate"  → TOSSignalStats.as_dict()        (signal gate counters)
"""

from __future__ import annotations

import logging
from collections import deque
from typing import Any, Dict, List, Optional

from strategies.base import BaseStrategy, EntrySignal, FVState, PMState, Position
from strategies.tos.strategy import TOSStrategy
from strategies.tos_signal.signal_stack import SignalStack
from strategies.tos_signal.stats import TOSSignalStats
from strategies.tos_signal.config import TOS_MAX_STRIKE_CROSSES, TOS_MIN_EFFICIENCY_RATIO

log = logging.getLogger(__name__)

# Maximum seconds between a history point and the 30s-ago target timestamp.
# Points older than this are considered too imprecise for momentum evaluation.
_BTC_HISTORY_TOLERANCE_S: float = 2.0

# BTC history downsampling rate: keep at most one point per second.
# maxlen=600 then covers 600s = 10min (2 full windows).
_BTC_HISTORY_SAMPLE_S: float = 1.0
_BTC_HISTORY_MAXLEN:   int   = 600


class TOSSignalStrategy(BaseStrategy):
    """
    TOS entry gate + Architecture C signal confirmation.

    Call on_fv_update(fv) on every FV tick to maintain BTC price history.
    Call evaluate_entry(fv, pm) on every PM tick to get entry decisions.
    """

    def __init__(self) -> None:
        # Inner TOS strategy for base evaluation
        self._tos = TOSStrategy()

        # Signal infrastructure
        self._signal_stack = SignalStack()
        self._btc_history: deque[tuple[float, float]] = deque(maxlen=_BTC_HISTORY_MAXLEN)

        # Per-window signal gate stats
        self._stats = TOSSignalStats()

        # Chaos tracking variables
        self._strike_crosses: int = 0
        self._path_length: float = 0.0
        self._first_price: Optional[float] = None
        self._prev_side: Optional[str] = None
        self._prev_tick_price: Optional[float] = None

    @property
    def name(self) -> str:
        return "TOS_SIGNAL"

    # ── BaseStrategy interface ─────────────────────────────────────────────────

    def on_fv_update(self, fv: FVState) -> None:
        """
        Maintain a downsampled BTC price history for the momentum signal.

        Downsampled to 1 Hz so maxlen=600 covers 10 minutes regardless of
        the underlying FV tick rate (~110 Hz from Binance BBO).  Without
        downsampling, the deque would only cover ~5.5s of lookback — far
        too short for the 30s momentum window.
        """
        ts_s = fv.ts_ms / 1000.0
        btc_price = float(fv.btc_price)
        
        # ── Chaos Metrics Update ──
        if self._first_price is None:
            self._first_price = btc_price
            
        if self._prev_tick_price is not None:
            self._path_length += abs(btc_price - self._prev_tick_price)
            
        self._prev_tick_price = btc_price
        
        if btc_price > fv.strike:
            side = "UP"
        elif btc_price < fv.strike:
            side = "DOWN"
        else:
            side = self._prev_side
            
        if self._prev_side is not None and side != self._prev_side:
            self._strike_crosses += 1
            
        self._prev_side = side

        # ── Downsampled BTC History Update ──
        if not self._btc_history or (ts_s - self._btc_history[-1][0]) >= _BTC_HISTORY_SAMPLE_S:
            self._btc_history.append((ts_s, btc_price))

    def evaluate_entry(self, fv: FVState, pm: PMState) -> List[EntrySignal]:
        """
        Evaluate TOS_SIGNAL entry criteria.

        Stage 1: TOS base gate.
        Stage 2: BTC history availability.
        Stage 3: Signal stack consensus.
        Stage 4: Consensus direction matches TOS direction.

        Returns [EntrySignal] on full pass, [] on any failure.
        """
        # ── Stage 1: TOS base evaluation ─────────────────────────────────────
        tos_signals = self._tos.evaluate_entry(fv, pm)
        if not tos_signals:
            return []  # TOS rejected — don't count as a signal candidate

        tos_signal = tos_signals[0]  # TOS returns at most one signal per tick
        self._stats.tos_candidates += 1

        # ── Stage 1.5: Chaos Checks ──────────────────────────────────────────
        net_movement = abs(fv.btc_price - self._first_price) if self._first_price is not None else 0.0
        efficiency_ratio = (net_movement / self._path_length) if self._path_length > 0 else 1.0

        if self._strike_crosses > TOS_MAX_STRIKE_CROSSES:
            self._stats.rejected_chaos += 1
            log.debug(
                "TOS_SIGNAL REJECT %s — chaotic market (%d crosses > %d limit)",
                tos_signal.side, self._strike_crosses, TOS_MAX_STRIKE_CROSSES
            )
            return []

        if efficiency_ratio < TOS_MIN_EFFICIENCY_RATIO:
            self._stats.rejected_chaos += 1
            log.debug(
                "TOS_SIGNAL REJECT %s — chaotic market (eff_ratio %.3f < %.3f limit)",
                tos_signal.side, efficiency_ratio, TOS_MIN_EFFICIENCY_RATIO
            )
            return []

        # ── Stage 2: BTC history availability ────────────────────────────────
        now_s      = pm.ts_ms / 1000.0
        target_ts  = now_s - 30.0
        btc_30s_ago    = 0.0
        closest_diff   = 99_999.0

        for hist_ts, hist_btc in self._btc_history:
            diff = abs(hist_ts - target_ts)
            if diff < closest_diff:
                closest_diff = diff
                btc_30s_ago  = hist_btc

        if closest_diff > _BTC_HISTORY_TOLERANCE_S:
            self._stats.rejected_btc_history += 1
            log.debug(
                "TOS_SIGNAL REJECT %s — btc_history_not_ready "
                "(closest=%.1fs diff, need <=%.1fs)",
                tos_signal.side, closest_diff, _BTC_HISTORY_TOLERANCE_S,
            )
            return []

        # ── Stage 3: individual signal evaluation ────────────────────────────
        mom_sig   = self._signal_stack.btc_momentum_signal(fv.btc_price, btc_30s_ago, fv.strike)
        imb_sig   = self._signal_stack.orderbook_imbalance_signal(pm)
        consensus = self._signal_stack.evaluate(fv, pm, btc_30s_ago)

        # Per-signal None counters (independent of consensus outcome)
        if mom_sig is None:
            self._stats.rejected_momentum += 1
        if imb_sig is None:
            self._stats.rejected_imbalance += 1

        log.debug(
            "TOS_SIGNAL EVAL %s — momentum=%s  imbalance=%s  consensus=%s  "
            "BTC=%.2f  btc_30s=%.2f  K=%.2f  delta=%.4f%%  liq_up=%.1f  liq_dn=%.1f",
            tos_signal.side,
            mom_sig, imb_sig, consensus,
            fv.btc_price, btc_30s_ago, fv.strike,
            abs(fv.btc_price - fv.strike) / fv.strike * 100 if fv.strike > 0 else 0.0,
            pm.liq_up, pm.liq_down,
        )

        # ── Stage 4: consensus present and aligned with TOS ──────────────────
        if consensus is None:
            if mom_sig is not None or imb_sig is not None:
                # At least one signal fired but they actively conflict
                self._stats.rejected_disagreement += 1
                log.debug(
                    "TOS_SIGNAL REJECT %s — signal_stack_disagreement "
                    "(momentum=%s, imbalance=%s)",
                    tos_signal.side, mom_sig, imb_sig,
                )
            else:
                # Neither signal had a view
                self._stats.rejected_consensus += 1
                log.debug(
                    "TOS_SIGNAL REJECT %s — signal_stack_no_consensus (both None)",
                    tos_signal.side,
                )
            return []

        if consensus != tos_signal.side:
            self._stats.rejected_disagreement += 1
            log.debug(
                "TOS_SIGNAL REJECT %s — signal_direction_mismatch  "
                "consensus=%s but TOS wants %s",
                tos_signal.side, consensus, tos_signal.side,
            )
            return []

        # All stages cleared
        self._stats.accepted_signal += 1
        log.debug(
            "TOS_SIGNAL ACCEPT %s — momentum=%s  imbalance=%s  "
            "consensus=%s  edge=%.4f  z=%.3f",
            tos_signal.side, mom_sig, imb_sig,
            consensus, tos_signal.edge, tos_signal.z_score,
        )

        return [tos_signal]

    def evaluate_exit(
        self,
        pos:   Position,
        fv:    FVState,
        pm:    PMState,
        ts_ms: int,
    ) -> Optional[str]:
        """TOS_SIGNAL holds every position to settlement — no mid-window exits."""
        return None

    def reset_for_market(self) -> None:
        """Reset all per-window counters on window transition."""
        self._tos.reset_for_market()
        self._stats.reset_for_market()
        self._strike_crosses = 0
        self._path_length = 0.0
        self._first_price = None
        self._prev_side = None
        self._prev_tick_price = None

    def get_diagnostics(self) -> Dict[str, Any]:
        """
        Return diagnostics for both the TOS gate and the signal gate.

        Structure:
          {
            "strategy":     "TOS_SIGNAL",
            "tos":          { ... TOSStrategy diagnostics ... },
            "signal_gate":  { ... TOSSignalStats ... }
          }

        The "signal_gate" namespace answers:
          • How many candidates reached the signal gate this window?
          • Which signal/gate caused the most rejections?
          • Were accepted_signal > 0? (non-zero = at least one entry fired)
        """
        return {
            "strategy":    "TOS_SIGNAL",
            "tos":         self._tos.get_diagnostics(),
            "signal_gate": self._stats.as_dict(),
        }
