#!/usr/bin/env bash
# Run the Phase 4 Signal Stack paper trader.
# Assumes run_tos_standalone.sh is already running (daemons are active).

set -euo pipefail

export ENTRY_POLICY="TOS_SIGNAL"
export EXIT_POLICY="TOS"
export FILLS_PATH="fills_signals.jsonl"
export EXITS_PATH="exits_signals.jsonl"
export LOGDIR="/tmp/btc_phase4_signals"
export PYTHON="${PYTHON:-./venv/bin/python}"

mkdir -p "$LOGDIR"
logfile="$LOGDIR/smart_paper_trader_signals.log"

echo "Starting Phase 4 TOS_SIGNAL trader..."
echo "Fills: $FILLS_PATH"
echo "Exits: $EXITS_PATH"
echo "Log:   $logfile"

"$PYTHON" -u -m cmd.smart_paper_trader 2>&1 | tee "$logfile"
