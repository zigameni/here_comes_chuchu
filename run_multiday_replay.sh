#!/usr/bin/env bash
# run_multiday_replay.sh
# ──────────────────────────────────────────────────────────────────────────────
# Runs the replay engine for the TOS_SIGNAL strategy across multiple capture
# files in a single continuous session.
#
# All selected capture files are merged into one temp JSONL (sorted by
# ts_ms then file order — the same ordering replay_engine uses internally),
# so the trader process runs uninterrupted across day boundaries instead of
# restarting between files.
#
# Usage:
#   ./run_multiday_replay.sh                         # replay all captures/
#   ./run_multiday_replay.sh captures/2026-06-08.jsonl captures/2026-06-09.jsonl
#   ./run_multiday_replay.sh -s 50                   # 50x speed on all files

set -euo pipefail
cd "$(dirname "$0")"

# ── Activate venv ─────────────────────────────────────────────────────────────
if [[ -z "${VIRTUAL_ENV:-}" ]]; then
    if [[ -f "venv/bin/activate" ]]; then
        source venv/bin/activate
    elif [[ -f "venv/Scripts/activate" ]]; then
        source venv/Scripts/activate
    else
        echo "Error: Could not find virtual environment."
        exit 1
    fi
fi

# ── Parse args ────────────────────────────────────────────────────────────────
SPEED="0"
CAPTURE_FILES=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -s|--speed)
            SPEED="$2"; shift 2 ;;
        *.jsonl)
            CAPTURE_FILES+=("$1"); shift ;;
        *)
            echo "Unknown argument: $1"
            echo "Usage: $0 [-s speed] [capture_file.jsonl ...]"
            exit 1 ;;
    esac
done

# Default: all files in captures/ sorted by name (chronological)
if [[ ${#CAPTURE_FILES[@]} -eq 0 ]]; then
    while IFS= read -r -d '' f; do
        CAPTURE_FILES+=("$f")
    done < <(find captures -maxdepth 1 -name "*.jsonl" -print0 | sort -z)
fi

if [[ ${#CAPTURE_FILES[@]} -eq 0 ]]; then
    echo "Error: No capture files found in captures/"
    exit 1
fi

# ── Output directory ──────────────────────────────────────────────────────────
OUTDIR="replay_multiday_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$OUTDIR"

MERGED_CAPTURE="$OUTDIR/merged_capture.jsonl"
FILLS="$OUTDIR/fills_merged.jsonl"
EXITS="$OUTDIR/exits_merged.jsonl"
TRADER_LOG="$OUTDIR/trader_merged.log"

# Clean up the temp merged file on exit (normal or error)
trap 'rm -f "$MERGED_CAPTURE"' EXIT

echo "══════════════════════════════════════════════════════"
echo "  Multi-Day Replay  →  TOS_SIGNAL Strategy"
echo "  Speed:    ${SPEED}x (0 = max)"
echo "  Output:   $OUTDIR/"
echo "  Days:     ${#CAPTURE_FILES[@]}"
echo "══════════════════════════════════════════════════════"
echo ""

# ── Merge capture files into a single sorted JSONL ───────────────────────────
echo "  Merging ${#CAPTURE_FILES[@]} capture file(s) → $MERGED_CAPTURE"

# Build a Python list literal of the file paths for the inline script
FILES_PY="["
for f in "${CAPTURE_FILES[@]}"; do
    FILES_PY+="'${f}',"
done
FILES_PY+="]"

python3 - "$MERGED_CAPTURE" <<PYEOF
import sys, json

merged_path = sys.argv[1]
capture_files = ${FILES_PY}

events = []
for file_path in capture_files:
    with open(file_path, "r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh):
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
                # Carry file-order position for stable sort within same ts_ms.
                # This mirrors the sort key in ReplayEngine._load_events().
                ev["_line_no"] = line_no
                ev["_src_file"] = file_path
                events.append(ev)
            except Exception as e:
                print(f"  Warning: skipping malformed line in {file_path}: {e}", flush=True)

# Sort by ts_ms first, then by original file order (stable across day boundaries)
events.sort(key=lambda x: (x["ts_ms"], x["_line_no"]))

written = 0
with open(merged_path, "w", encoding="utf-8") as out:
    for ev in events:
        # Strip internal sort helpers before writing
        ev.pop("_line_no", None)
        ev.pop("_src_file", None)
        out.write(json.dumps(ev) + "\n")
        written += 1

print(f"  Merged {written:,} events from {len(capture_files)} file(s).", flush=True)
PYEOF

echo ""

# ── Single continuous replay ──────────────────────────────────────────────────
echo "──────────────────────────────────────────────────────"
echo "  Replaying merged capture as one session"
echo "  Fills:  $FILLS"
echo "  Exits:  $EXITS"
echo "──────────────────────────────────────────────────────"

export REPLAY_MODE="1"
export PYTHONHASHSEED=0
export ENTRY_POLICY="TOS_SIGNAL"
export EXIT_POLICY="TOS"
export FILLS_PATH="$FILLS"
export EXITS_PATH="$EXITS"

# Start the paper trader in the background
rm -f "$TRADER_LOG"
python -u -m cmd.smart_paper_trader > "$TRADER_LOG" 2>&1 &
TRADER_PID=$!

# Run the replay engine (blocks until the merged capture is fully played back)
set +e
python -m tools.replay_engine "$MERGED_CAPTURE" --speed "$SPEED"
REPLAY_EXIT=$?
set -e

# Shut down the paper trader cleanly
kill -SIGTERM "$TRADER_PID" 2>/dev/null || true
wait "$TRADER_PID" 2>/dev/null || true

if [[ $REPLAY_EXIT -ne 0 ]]; then
    echo ""
    echo "  ⚠  Replay engine exited with code $REPLAY_EXIT — no results to summarise."
    exit $REPLAY_EXIT
fi

echo "  ✓ Replay complete."
echo ""

# ── Aggregate summary ─────────────────────────────────────────────────────────
echo ""
echo "══════════════════════════════════════════════════════"
echo "  Aggregate Summary — TOS_SIGNAL Replay"
echo "══════════════════════════════════════════════════════"

python3 - "$FILLS" "$EXITS" <<'PYEOF'
import sys, json
from pathlib import Path

fills_path, exits_path = sys.argv[1], sys.argv[2]

def load(p):
    path = Path(p)
    if not path.exists():
        return []
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]

fills = load(fills_path)
exits = load(exits_path)

cost      = sum(f.get("cost", 0)     for f in fills)
proceeds  = sum(e.get("proceeds", 0) for e in exits)
wins      = sum(1 for e in exits if e.get("pnl", 0) > 0)
losses    = sum(1 for e in exits if e.get("pnl", 0) < 0)
net       = proceeds - cost
wr        = (wins / len(exits) * 100) if exits else 0.0

header = f"{'Fills':>6} {'Exits':>6} {'Wins':>6} {'Losses':>7} {'Cost':>12} {'Proceeds':>12} {'Net PnL':>12} {'WinRate':>8}"
print(header)
print("─" * len(header))
print(f"{len(fills):>6} {len(exits):>6} {wins:>6} {losses:>7} "
      f"${cost:>10.2f} ${proceeds:>10.2f} ${net:>+10.2f} {wr:>7.1f}%")
PYEOF

echo ""
echo "Fills/exits saved in: $OUTDIR/"
echo "══════════════════════════════════════════════════════"
