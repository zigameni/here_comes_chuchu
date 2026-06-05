#!/usr/bin/env bash
# run_replay.sh
# Deterministic backtest engine. Replays a capture file and routes data
# to the strategy via isolated ZMQ IPC sockets.

set -e

# Change to the directory of this script
cd "$(dirname "$0")"

# Ensure venv is active
if [[ -z "${VIRTUAL_ENV}" ]]; then
    if [[ -f "venv/bin/activate" ]]; then
        source venv/bin/activate
    elif [[ -f "venv/Scripts/activate" ]]; then
        source venv/Scripts/activate
    else
        echo "Error: Could not find virtual environment. Please create one or activate it."
        exit 1
    fi
fi

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <capture_file.jsonl> [-s speed_multiplier]"
    echo "Example: $0 captures/2026-06-01.jsonl -s 10.0"
    exit 1
fi

CAPTURE_FILE="$1"
shift

# Default speed 0 (max speed)
SPEED="0"
if [[ $# -ge 2 ]] && [[ "$1" == "-s" ]]; then
    SPEED="$2"
fi

if [[ ! -f "$CAPTURE_FILE" ]]; then
    echo "Error: Capture file '$CAPTURE_FILE' not found."
    exit 1
fi

echo "=========================================================="
echo " Starting Replay Engine"
echo " Capture File: $CAPTURE_FILE"
echo " Speed:        ${SPEED}x"
echo "=========================================================="

export REPLAY_MODE="1"

export PYTHONHASHSEED=0

export FILLS_PATH="replay_fills.jsonl"
export EXITS_PATH="replay_exits.jsonl"
export ENTRY_POLICY="TOS"
export EXIT_POLICY="TOS"

echo "Starting Paper Trader in background (logs to replay_trader.log)..."
# Clear old logs/metrics
rm -f "$FILLS_PATH" "$EXITS_PATH" replay_trader.log replay_metrics.jsonl

python -u -m cmd.smart_paper_trader > replay_trader.log 2>&1 &
TRADER_PID=$!

echo "Starting Replay Engine playback..."
python -m tools.replay_engine "$CAPTURE_FILE" --speed "$SPEED"

echo "Playback complete. Shutting down paper trader..."
kill -SIGTERM $TRADER_PID || true
wait $TRADER_PID 2>/dev/null || true

echo "Replay finished!"
echo "- Fills:   $FILLS_PATH"
echo "- Exits:   $EXITS_PATH"
echo "- Metrics: replay_metrics.jsonl"
