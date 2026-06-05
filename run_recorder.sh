#!/usr/bin/env bash
# run_recorder.sh
# Starts the ZMQ data recorder to capture live market events into JSONL files.

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

echo "Starting BTC Bot Data Recorder..."
python -m tools.data_recorder
