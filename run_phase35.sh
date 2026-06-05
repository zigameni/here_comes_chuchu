#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────
#  Phase 3.5 launcher — Smart Paper Trader
#
#  Usage:
#    chmod +x run_phase35.sh
#    ./run_phase35.sh          # start all daemons + follow logs
#    ./run_phase35.sh stop     # kill everything
#    ./run_phase35.sh logs     # re-attach to logs
#    ./run_phase35.sh analyze  # print fills + exits summary
# ─────────────────────────────────────────────────────────────────

PIDFILE="${PIDFILE:-.phase35.pids}"
LOGDIR="${LOGDIR:-/tmp/btc_phase35}"
PYTHON="${PYTHON:-./venv/bin/python}"

# ── Stop mode ────────────────────────────────────────────────────
if [[ "${1:-}" == "stop" ]]; then
    if [[ ! -f "$PIDFILE" ]]; then
        echo "No PID file found — nothing to stop."
        exit 0
    fi
    echo "Stopping all Phase 3.5 processes..."
    while read -r pid name; do
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" && echo "  killed $name (pid $pid)"
        else
            echo "  $name (pid $pid) already gone"
        fi
    done < "$PIDFILE"
    rm -f "$PIDFILE"
    echo "Done."
    exit 0
fi

# ── Logs-only mode ───────────────────────────────────────────────
if [[ "${1:-}" == "logs" ]]; then
    tail -f "$LOGDIR"/binance_daemon.log \
            "$LOGDIR"/fv_engine.log \
            "$LOGDIR"/pm_daemon.log \
            "$LOGDIR"/smart_paper_trader.log
    exit 0
fi

# ── Analyze mode — summarize fills and exits ─────────────────────
if [[ "${1:-}" == "analyze" ]]; then
    FILLS="${FILLS_PATH:-smart_fills.jsonl}"
    EXITS="${EXITS_PATH:-smart_exits.jsonl}"

    echo ""
    echo "════ Phase 3.5 Analysis ════════════════════════════════"
    echo ""

    if [[ ! -f "$FILLS" ]]; then
        echo "  No fills file found at $FILLS — has the trader run yet?"
        exit 1
    fi

    "$PYTHON" - <<'PYEOF'
import json, os, sys
from collections import defaultdict
from pathlib import Path

fills_path = Path(os.getenv("FILLS_PATH", "smart_fills.jsonl"))
exits_path = Path(os.getenv("EXITS_PATH", "smart_exits.jsonl"))

fills, exits = [], []
if fills_path.exists():
    fills = [json.loads(l) for l in fills_path.read_text().splitlines() if l.strip()]
if exits_path.exists():
    exits = [json.loads(l) for l in exits_path.read_text().splitlines() if l.strip()]

print(f"  Entries: {len(fills)}")
print(f"  Exits:   {len(exits)}")

if not exits:
    print("  No exits yet — keep running.")
    sys.exit(0)

total_cost     = sum(f["cost"] for f in fills)
total_proceeds = sum(e["proceeds"] for e in exits)
net            = total_proceeds - total_cost

by_reason = defaultdict(list)
for e in exits:
    by_reason[e["exit_reason"]].append(e["pnl"])

print(f"\n  Total cost:     ${total_cost:.2f}")
print(f"  Total proceeds: ${total_proceeds:.2f}")
print(f"  Net P&L:        ${net:+.2f}  ({net/total_cost*100:+.1f}% ROI)" if total_cost else "")

print(f"\n  Exit breakdown:")
for reason, pnls in sorted(by_reason.items()):
    avg   = sum(pnls) / len(pnls)
    wins  = sum(1 for p in pnls if p > 0)
    print(f"    {reason:<12}  count={len(pnls):3}  avg_pnl={avg:+.4f}  wins={wins}/{len(pnls)}")

# Sigma floor check
sigma_floor_val = float(os.getenv("MIN_SIGMA_FLOOR", "0.50"))
sigma_at_floor = sum(1 for f in fills if f["sigma"] <= sigma_floor_val + 0.0001)
print(f"\n  Sigma floor ({sigma_floor_val:.2f}): {sigma_at_floor}/{len(fills)} ({sigma_at_floor/len(fills)*100:.0f}%)")
if sigma_at_floor / max(len(fills), 1) > 0.8:
    print("  ⚠ WARNING: sigma stuck at floor >80% of fills.")
    print("    The ring buffer may not have enough data yet, or")
    print("    BTC is genuinely very low volatility in this window.")
    print("    Consider extending PRICE_BUFFER in .env (current default: 3000).")

# FV age at fill
ages = [f["fv_age_ms"] for f in fills if "fv_age_ms" in f]
if ages:
    print(f"\n  FV age at fill: min={min(ages)}ms  max={max(ages)}ms  mean={sum(ages)/len(ages):.0f}ms")
    old = sum(1 for a in ages if a > 500)
    print(f"  FV age >500ms:  {old}/{len(ages)} ({old/len(ages)*100:.0f}%)")
    if old / max(len(ages), 1) > 0.05:
        print("  ⚠ WARNING: more than 5% of entries had stale FV (>500ms).")
        print("    Check FV_STALE_MS setting and FV engine throughput.")

print("")
PYEOF
    exit 0
fi

# ── Sanity checks ────────────────────────────────────────────────
if [[ ! -f "config.py" ]]; then
    echo "ERROR: Run this from the BTC-Bot root (where config.py lives)."
    exit 1
fi
if [[ ! -f "$PYTHON" ]]; then
    echo "ERROR: $PYTHON not found. Activate your venv first."
    exit 1
fi

rm -f "$PIDFILE"

mkdir -p "$LOGDIR" || { echo "ERROR: Cannot create $LOGDIR/"; exit 1; }

for _d in binance_daemon fv_engine pm_daemon smart_paper_trader; do
    touch "$LOGDIR/${_d}.log" || {
        echo "ERROR: Cannot create $LOGDIR/${_d}.log"
        exit 1
    }
done
unset _d

# ── Launch one process ───────────────────────────────────────────
launch() {
    local name="$1"
    local module="$2"
    local logfile="$LOGDIR/${name}.log"

    {
        echo "════════════════════════════════════════"
        echo "  $name  started $(date '+%Y-%m-%d %H:%M:%S')"
        echo "════════════════════════════════════════"
    } > "$logfile"

    (
        "$PYTHON" -u -m "$module" 2>&1
        echo ""
        echo "════ PROCESS EXITED — code=$? — $(date '+%H:%M:%S') ════"
    ) >> "$logfile" &

    local pid=$!
    echo "$pid $name" >> "$PIDFILE"
    echo "  [+] $name   pid=$pid   →  $logfile"
}

# ── Start sequence ───────────────────────────────────────────────
echo ""
echo "Starting Phase 3.5 — Smart Paper Trader..."
echo ""

launch "binance_daemon"     "cmd.binance_daemon"
echo "      waiting 6s for Binance feed to connect..."
sleep 6

launch "fv_engine"          "core.fv_engine"
echo "      waiting 5s for FV engine to warm up..."
sleep 5

launch "pm_daemon"          "cmd.pm_daemon"
echo "      waiting 5s for PM daemon to find a market..."
sleep 5

launch "smart_paper_trader" "cmd.smart_paper_trader"

echo ""
echo "════════════════════════════════════════════════════════"
echo "  All 4 processes running.  PIDs in $PIDFILE"
echo ""
echo "  Fills: ${FILLS_PATH:-smart_fills.jsonl}"
echo "  Exits: ${EXITS_PATH:-smart_exits.jsonl}"
echo ""
echo "  Stop:    ./run_phase35.sh stop"
echo "  Logs:    ./run_phase35.sh logs"
echo "  Analyze: ./run_phase35.sh analyze"
echo "════════════════════════════════════════════════════════"
echo ""
sleep 1

tail -f "$LOGDIR/binance_daemon.log" \
        "$LOGDIR/fv_engine.log" \
        "$LOGDIR/pm_daemon.log" \
        "$LOGDIR/smart_paper_trader.log"
