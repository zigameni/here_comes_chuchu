#!/usr/bin/env python3
"""
tools/compare_runs.py
──────────────────────
Compares the output of a live paper-trading run against a replay run.
Calculates PnL, Win Rates, and matches trades by market_id and side 
to report any divergence in execution timing, price, or strategy output.
"""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

def load_jsonl(path: str):
    p = Path(path)
    if not p.exists():
        return []
    with open(p, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]

def ts_span(rows):
    ts_values = [r.get("ts_ms") for r in rows if r.get("ts_ms")]
    if not ts_values:
        return None
    return min(ts_values), max(ts_values)

def fmt_span(span):
    if span is None:
        return "n/a"
    start, end = span
    start_s = datetime.fromtimestamp(start / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    end_s = datetime.fromtimestamp(end / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return f"{start_s} UTC -> {end_s} UTC"

def calc_pnl(fills, exits):
    total_cost = sum(f.get("cost", 0) for f in fills)
    total_proceeds = sum(e.get("proceeds", 0) for e in exits)
    net_pnl = total_proceeds - total_cost

    settlement_exits = [e for e in exits if e.get("exit_reason") == "SETTLEMENT"]
    early_exits = [e for e in exits if e.get("exit_reason") != "SETTLEMENT"]
    
    wins = sum(1 for e in exits if e.get("pnl", 0) > 0)
    losses = sum(1 for e in exits if e.get("pnl", 0) < 0)
    draws = sum(1 for e in exits if e.get("pnl", 0) == 0)
    
    return {
        "cost": total_cost,
        "proceeds": total_proceeds,
        "net": net_pnl,
        "wins": wins,
        "losses": losses,
        "draws": draws,
        "settlement_exits": len(settlement_exits),
        "early_exits": len(early_exits),
        "total_exits": len(exits)
    }

def main():
    live_fills_path = "fills_tos.jsonl"
    live_exits_path = "exits_tos.jsonl"
    replay_fills_path = "replay_fills.jsonl"
    replay_exits_path = "replay_exits.jsonl"

    live_fills = load_jsonl(live_fills_path)
    live_exits = load_jsonl(live_exits_path)
    replay_fills = load_jsonl(replay_fills_path)
    replay_exits = load_jsonl(replay_exits_path)

    live_stats = calc_pnl(live_fills, live_exits)
    replay_stats = calc_pnl(replay_fills, replay_exits)

    print("══════════════════════════════════════════════════════")
    print("  Run Comparison: Live (TOS) vs Replay")
    print("══════════════════════════════════════════════════════\n")

    live_span = ts_span(live_fills + live_exits)
    replay_span = ts_span(replay_fills + replay_exits)
    print(f"Live span:   {fmt_span(live_span)}")
    print(f"Replay span: {fmt_span(replay_span)}")
    if live_span and replay_span and (live_span[1] < replay_span[0] or replay_span[1] < live_span[0]):
        print("WARNING: live and replay output time ranges do not overlap.\n")
    else:
        print("")

    print(f"{'Metric':<25} | {'Live (TOS)':<15} | {'Replay':<15} | {'Delta':<15}")
    print("-" * 78)
    print(f"{'Fills':<25} | {len(live_fills):<15} | {len(replay_fills):<15} | {len(live_fills) - len(replay_fills):<+15}")
    print(f"{'Terminal Records':<25} | {len(live_exits):<15} | {len(replay_exits):<15} | {len(live_exits) - len(replay_exits):<+15}")
    print(f"{'Settlement Records':<25} | {live_stats['settlement_exits']:<15} | {replay_stats['settlement_exits']:<15} | {live_stats['settlement_exits'] - replay_stats['settlement_exits']:<+15}")
    print(f"{'Early Sell Records':<25} | {live_stats['early_exits']:<15} | {replay_stats['early_exits']:<15} | {live_stats['early_exits'] - replay_stats['early_exits']:<+15}")
    print("-" * 78)
    print(f"{'Total Cost':<25} | ${live_stats['cost']:<14.2f} | ${replay_stats['cost']:<14.2f} | ${live_stats['cost'] - replay_stats['cost']:<+14.2f}")
    print(f"{'Total Proceeds':<25} | ${live_stats['proceeds']:<14.2f} | ${replay_stats['proceeds']:<14.2f} | ${live_stats['proceeds'] - replay_stats['proceeds']:<+14.2f}")
    print(f"{'Net PnL':<25} | ${live_stats['net']:<+14.2f} | ${replay_stats['net']:<+14.2f} | ${live_stats['net'] - replay_stats['net']:<+14.2f}")
    print("-" * 78)
    print(f"{'Winning Exits':<25} | {live_stats['wins']:<15} | {replay_stats['wins']:<15} | {live_stats['wins'] - replay_stats['wins']:<+15}")
    print(f"{'Losing Exits':<25} | {live_stats['losses']:<15} | {replay_stats['losses']:<15} | {live_stats['losses'] - replay_stats['losses']:<+15}")
    
    live_wr = (live_stats['wins'] / live_stats['total_exits'] * 100) if live_stats['total_exits'] > 0 else 0.0
    replay_wr = (replay_stats['wins'] / replay_stats['total_exits'] * 100) if replay_stats['total_exits'] > 0 else 0.0
        
    print(f"{'Win Rate (Exits)':<25} | {live_wr:<14.1f}% | {replay_wr:<14.1f}% | {live_wr - replay_wr:<+14.1f}%")
    if live_stats["early_exits"] == 0 and live_stats["settlement_exits"] > 0:
        print("Note: Live TOS terminal records are all SETTLEMENT records; no early sells were recorded.")
    print("══════════════════════════════════════════════════════\n")

    # Index by (market_id, side) -> list of fills
    live_index = defaultdict(list)
    for f in live_fills:
        live_index[(f.get("market_id"), f.get("side"))].append(f)
        
    replay_index = defaultdict(list)
    for f in replay_fills:
        replay_index[(f.get("market_id"), f.get("side"))].append(f)

    common_keys = set(live_index.keys()).intersection(set(replay_index.keys()))
    live_only_keys = set(live_index.keys()) - set(replay_index.keys())
    replay_only_keys = set(replay_index.keys()) - set(live_index.keys())

    print(f"Matching unique positions (market + side):")
    print(f"  Common:      {len(common_keys)}")
    print(f"  Live Only:   {len(live_only_keys)}")
    print(f"  Replay Only: {len(replay_only_keys)}")

    if len(common_keys) > 0:
        print("\nExecution Divergence Analysis (for common positions):")
        ts_diffs = []
        price_diffs = []
        fv_diffs = []
        
        for key in common_keys:
            lf = live_index[key][0]
            rf = replay_index[key][0]
            
            ts_diffs.append(abs(lf.get("ts_ms", 0) - rf.get("ts_ms", 0)))
            price_diffs.append(abs(lf.get("ask", 0) - rf.get("ask", 0)))
            fv_diffs.append(abs(lf.get("fv", 0) - rf.get("fv", 0)))
            
        print(f"  Avg Entry Time Divergence:  {sum(ts_diffs)/len(ts_diffs):.1f} ms")
        print(f"  Max Entry Time Divergence:  {max(ts_diffs):.1f} ms")
        print(f"  Avg Entry Price Divergence: {sum(price_diffs)/len(price_diffs):.5f}")
        print(f"  Max Entry Price Divergence: {max(price_diffs):.5f}")
        print(f"  Avg FV Divergence:          {sum(fv_diffs)/len(fv_diffs):.5f}")
        print(f"  Max FV Divergence:          {max(fv_diffs):.5f}")

        if max(ts_diffs) == 0 and max(price_diffs) == 0:
            print("\n✅ PERFECT MATCH: Replay execution exactly matches live execution for common positions!")
        else:
            print("\n⚠ DIVERGENCE DETECTED: Replay execution differs from live execution.")
            print("This can happen if:  ")
            print("1. Strategy config was changed since the live run.")
            print("2. Live data captures lost some ticks or had different WS latency.")
            print("3. Time-dependent logic (e.g. wall-clock checks instead of data timestamps).")

    if len(live_only_keys) > 0 and len(replay_fills) > 0:
        print("\nNote: There are live entries that didn't happen in replay.")
        print("This is normal if your capture file covers a smaller time window than the live run.")

if __name__ == "__main__":
    main()
