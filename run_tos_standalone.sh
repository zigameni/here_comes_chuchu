#!/usr/bin/env bash
# Run the TOS Phase 3.5 stack WITH the data feeds (no legacy trader).
#
# Usage:
#   ./run_tos_standalone.sh          # start full market-data stack + TOS trader
#   ./run_tos_standalone.sh stop     # stop this TOS stack
#   ./run_tos_standalone.sh logs     # follow this TOS stack's logs
#   ./run_tos_standalone.sh analyze  # summarize fills_tos.jsonl/exits_tos.jsonl

set -euo pipefail

export ENTRY_POLICY="${ENTRY_POLICY:-TOS}"
export EXIT_POLICY="${EXIT_POLICY:-TOS}"
export FILLS_PATH="${FILLS_PATH:-fills_tos.jsonl}"
export EXITS_PATH="${EXITS_PATH:-exits_tos.jsonl}"
export PIDFILE="${PIDFILE:-.phase35_tos.pids}"
export LOGDIR="${LOGDIR:-/tmp/btc_phase35_tos}"

# Enable Phase 3 Dual-Leg Arb Scanner for validation run
export ARB_ENABLED="${ARB_ENABLED:-1}"

if [ "${1:-}" == "stop" ]; then
    if [ -f "${PIDFILE}_recorder" ]; then
        while read pid; do
            kill "$pid" 2>/dev/null || true
        done < "${PIDFILE}_recorder"
        rm -f "${PIDFILE}_recorder"
    fi
fi

if [ "${1:-}" != "stop" ] && [ "${1:-}" != "logs" ] && [ "${1:-}" != "analyze" ]; then
    export PYTHON="${PYTHON:-./venv/bin/python}"
    mkdir -p "${LOGDIR}"
    echo "Starting DataRecorder..."
    "$PYTHON" -u -m tools.data_recorder > "${LOGDIR}/data_recorder.log" 2>&1 &
    echo $! > "${PIDFILE}_recorder"
fi

exec ./run_phase35.sh "${@}"
