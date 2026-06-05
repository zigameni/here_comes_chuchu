from dataclasses import dataclass, field
from loguru import logger


@dataclass
class PnLTracker:
    market_id:     str   = ""
    total_spent:   float = 0.0
    merged_usdc:   float = 0.0
    redeemed_usdc: float = 0.0
    up_balance:    float = 0.0
    down_balance:  float = 0.0
    gross_up:      float = 0.0
    gross_down:    float = 0.0
    arb_count:     int   = 0
    merge_count:   int   = 0

    @property
    def net_pnl(self) -> float:
        return self.merged_usdc + self.redeemed_usdc - self.total_spent

    @property
    def matched_pairs(self) -> float:
        return min(self.up_balance, self.down_balance)

    def record_arb(self, shares: float, cost_up: float, cost_down: float):
        total = cost_up + cost_down
        self.total_spent  += total
        self.up_balance   += shares
        self.down_balance += shares
        self.gross_up     += shares
        self.gross_down   += shares
        self.arb_count    += 1
        logger.info(
            f"💰 ARB #{self.arb_count}: {shares:.2f} shares — "
            f"cost ${total:.2f} — projected profit ${shares - total:.4f}"
        )

    def record_limit_fill(self, side: str, shares: float, cost: float):
        self.total_spent += cost
        if side == "UP":
            self.up_balance += shares
            self.gross_up += shares
        else:
            self.down_balance += shares
            self.gross_down += shares


    def record_merge(self, pairs: float):
        self.up_balance   -= pairs
        self.down_balance -= pairs
        self.merged_usdc  += pairs
        self.merge_count  += 1
        logger.info(f"🔄 Merged {pairs:.2f} pairs → locked ${pairs:.2f} USDC")

    def record_redeem(self, amount: float):
        self.redeemed_usdc += amount
        logger.info(f"💵 Redeemed ${amount:.2f} USDC")

    def print_summary(self):
        logger.info("=" * 55)
        logger.info("📊  FINAL PnL STATEMENT")
        logger.info("=" * 55)
        logger.info(f"  Total spent:      -${self.total_spent:>10.2f}")
        logger.info(f"  Merged USDC:      +${self.merged_usdc:>10.2f}")
        logger.info(f"  Redeemed USDC:    +${self.redeemed_usdc:>10.2f}")
        logger.info("-" * 55)
        pnl = self.net_pnl
        sign = "+" if pnl >= 0 else ""
        logger.info(f"  NET PnL:          {sign}${pnl:>10.2f}")
        logger.info("=" * 55)
        logger.info(f"  Arb trades: {self.arb_count}   Merges: {self.merge_count}")
