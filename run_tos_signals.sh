#!/usr/bin/env bash
# Run the Phase 4 Signal Stack paper trader.
# Assumes run_tos_standalone.sh is already running (daemons are active).

set -euo pipefail

export ENTRY_POLICY="TOS_SIGNAL"
export EXIT_POLICY="TOS"
export FILLS_PATH="fills_signals.jsonl"
export EXITS_PATH="exits_signals.jsonl"

# --- Optimized Variables ---
export TOS_ENTRY_START_S=190
export TOS_ENTRY_END_S=260
export TOS_MIN_PROB=0.7
export TOS_MIN_EDGE=0.12
export TOS_MIN_LIQUIDITY=50
export TOS_Z_THRESHOLD=0.3
export TOS_MIN_VARIANCE_RATIO=1.9

export MIN_EDGE_THRESHOLD=0.02
export MIN_ENTRY_ASK=0.09
export FV_ENTRY_MAX=0.95
export FV_ENTRY_MIN=0.13
export FV_STALE_MS=500
export MIN_WINDOW_AGE_S=20
export FILL_COOLDOWN_MS=14000

export EARLY_HIGH_CONFIDENCE_BID=0.9
export LATE_WINDOW_SECONDS=120
export LATE_SL_FLOOR=0.05
export LATE_TP_BID=0.92
export EMERGENCY_SECONDS=90
export EMERGENCY_CUT_PRICE=0.21
export EMERGENCY_FV_CONFIRM=0.4
export EMERGENCY_TP_BID=0.85

export MERTON_DISTANCE_GATE=2.25
export OFI_IMBALANCE_GATE=30
export SIGNAL_MIN_LIQUIDITY=50
# -------------------------

export LOGDIR="/tmp/btc_phase4_signals"
export PYTHON="${PYTHON:-./venv/bin/python}"

mkdir -p "$LOGDIR"
logfile="$LOGDIR/smart_paper_trader_signals.log"

echo "Starting Phase 4 TOS_SIGNAL trader..."
echo "Fills: $FILLS_PATH"
echo "Exits: $EXITS_PATH"
echo "Log:   $logfile"

"$PYTHON" -u -m cmd.smart_paper_trader 2>&1 | tee "$logfile"
