import logging
import time

log = logging.getLogger("risk")
from config import (
    MAX_SPEND_PER_MARKET, MAX_INVENTORY_IMBALANCE,
    COMBINED_ASK_STOP, MAX_TAKER_FILL_USDC, MAX_LOSS_PER_HOUR_USDC,
    MIN_ARB_SHARES, MAX_ENTRIES_PER_WINDOW,
)


class RiskManager:
    def __init__(self):
        self.current_spent:   float = 0.0
        self.hourly_pnl:      float = 0.0
        self.hour_start_ms:   int   = 0
        self.trading_halted:  bool  = False
        self.halt_reason:     str   = ""

    def update_time(self, ts_ms: int):
        if self.hour_start_ms == 0:
            self.hour_start_ms = ts_ms
        if ts_ms - self.hour_start_ms >= 3600_000:
            self.hourly_pnl = 0.0
            self.hour_start_ms = ts_ms
            if self.trading_halted and "hourly loss" in self.halt_reason.lower():
                self.trading_halted = False
                self.halt_reason = ""
                log.warning("Hourly loss halt expired; trading resumed for new hour")

    def halt(self, reason: str):
        if not self.trading_halted:
            log.error(f"🚨 CIRCUIT BREAKER TRIGGERED: {reason}")
        self.trading_halted = True
        self.halt_reason    = reason

    def check(self, up_balance: float, down_balance: float, combined_ask: float) -> bool:
        """Return True if all circuit breakers pass; halt + return False otherwise."""
        if self.trading_halted:
            return False

        imbalance = abs(up_balance - down_balance)

        if imbalance > MAX_INVENTORY_IMBALANCE * 2:
            self.halt(f"Inventory imbalance ${imbalance:.2f} > ${MAX_INVENTORY_IMBALANCE * 2:.2f}")
            return False

        if combined_ask > COMBINED_ASK_STOP:
            self.halt(f"Combined ask {combined_ask:.4f} > {COMBINED_ASK_STOP}")
            return False

        if self.current_spent >= MAX_SPEND_PER_MARKET:
            log.warning(f"Spend cap reached: ${self.current_spent:.2f}")
            return False

        if self.hourly_pnl < -MAX_LOSS_PER_HOUR_USDC:
            self.halt(f"Hourly loss ${-self.hourly_pnl:.2f} exceeds limit ${MAX_LOSS_PER_HOUR_USDC:.2f}")
            return False

        return True

    def max_arb_shares(
        self,
        liq_up:     float,
        liq_down:   float,
        ask_up:     float,
        ask_down:   float,
        combined:   float,
    ) -> float:
        remaining_budget = MAX_SPEND_PER_MARKET - self.current_spent
        max_by_liq_up    = min(liq_up,   MAX_TAKER_FILL_USDC / ask_up)
        max_by_liq_down  = min(liq_down, MAX_TAKER_FILL_USDC / ask_down)
        max_by_budget    = remaining_budget / combined
        return min(max_by_liq_up, max_by_liq_down, max_by_budget)

    def record_entry(self, cost_usdc: float) -> None:
        """Record a new entry: track committed spend but do NOT touch hourly_pnl.
        
        hourly_pnl must only reflect *realized* (settled) outcomes so the circuit
        breaker does not fire on open positions whose cost happens to exceed the
        hourly limit before the market resolves.
        """
        self.current_spent += cost_usdc

    def record_settlement(self, cost_usdc: float, gross_return: float) -> None:
        """Record a settlement: realized PnL = gross_return - cost_usdc.
        
        This is the only place hourly_pnl is updated. A loss settlement drives it
        negative; a win settlement keeps it positive. The circuit breaker in
        check_entry_allowed() reads hourly_pnl to enforce MAX_LOSS_PER_HOUR_USDC.
        """
        self.hourly_pnl += gross_return - cost_usdc

    def record_trade(self, cost_usdc: float, gross_return: float) -> None:
        """Legacy combined call — kept for backward compat with old callers.
        
        New code should call record_entry() at entry and record_settlement() at
        exit so that hourly_pnl only tracks realized losses.
        """
        self.current_spent += cost_usdc
        self.hourly_pnl    += gross_return - cost_usdc

    def status(self) -> dict:
        return {
            "spent":         self.current_spent,
            "hourly_pnl":    self.hourly_pnl,
            "halted":        self.trading_halted,
            "halt_reason":   self.halt_reason,
        }


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
            log.warning(f"FV stale: {fv_age_ms:.0f}ms")
            return False
        if pm_age_ms > 10000:  # PM book older than 10 seconds  
            log.warning(f"PM book stale: {pm_age_ms:.0f}ms")
            return False
        return True
    
    def record_window_boundary(self, boundary_ts: int):
        """Called on each window transition."""
        if boundary_ts != self.window_boundary_ts:
            self.entries_this_window = 0
            self.current_spent = 0.0  # reset spend cap per window
            self.window_boundary_ts = boundary_ts
    
    def check_entry_allowed(self, is_sigma_real: bool) -> tuple[bool, str]:
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
