#!/usr/bin/env python3
"""
tools/analyze_latency.py
────────────────────────
Analyzes feed latency and websocket stutter from .jsonl capture files.
"""

import json
import base64
import msgpack
import statistics
import argparse
import sys
from pathlib import Path

def analyze_latency(filepath: str):
    fv_delays = []
    pm_delays = []
    fv_intervals = []
    pm_intervals = []
    
    last_fv_ts = None
    last_pm_ts = None

    print(f"Analyzing {filepath}...")
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip(): continue
                try:
                    obj = json.loads(line)
                    channel = obj.get("channel", "")
                    
                    # Capture files usually record a local receipt timestamp at the root
                    local_ts = obj.get("local_ts") or obj.get("recv_ts") or obj.get("timestamp")
                    
                    if "data" not in obj:
                        continue
                        
                    raw = base64.b64decode(obj["data"])
                    parsed = msgpack.unpackb(raw, raw=False)
                    
                    if not parsed or len(parsed) == 0:
                        continue
                        
                    # exchange_ts_ms is the first element in both FV_STREAM and PM_BOOK schemas
                    exchange_ts_ms = parsed[0]
                    
                    if "fv_stream" in channel:
                        if local_ts:
                            # Normalize seconds to milliseconds if necessary
                            if local_ts < 20000000000: local_ts *= 1000
                            fv_delays.append(local_ts - exchange_ts_ms)
                            
                        if last_fv_ts and exchange_ts_ms >= last_fv_ts:
                            fv_intervals.append(exchange_ts_ms - last_fv_ts)
                        last_fv_ts = exchange_ts_ms
                        
                    elif "pm_book" in channel:
                        if local_ts:
                            if local_ts < 20000000000: local_ts *= 1000
                            pm_delays.append(local_ts - exchange_ts_ms)
                            
                        if last_pm_ts and exchange_ts_ms >= last_pm_ts:
                            pm_intervals.append(exchange_ts_ms - last_pm_ts)
                        last_pm_ts = exchange_ts_ms
                        
                except Exception:
                    continue
    except FileNotFoundError:
        print(f"Error: File '{filepath}' not found.")
        sys.exit(1)

    # Helper to print statistical summaries
    def print_stats(name: str, data: list):
        if not data:
            print(f"No data for {name}")
            return
            
        # Filter negative anomalies (usually caused by clock sync issues)
        valid_data = [x for x in data if x >= 0]
        if not valid_data:
            print(f"No valid positive data for {name}")
            return
            
        mean_val = statistics.mean(valid_data)
        median_val = statistics.median(valid_data)
        max_val = max(valid_data)
        
        print(f"--- {name} ---")
        print(f"  Count:  {len(valid_data)}")
        print(f"  Mean:   {mean_val:.2f} ms")
        print(f"  Median: {median_val:.2f} ms")
        print(f"  Max:    {max_val:.2f} ms")
        
        if len(valid_data) > 1:
            quantiles = statistics.quantiles(valid_data, n=100)
            print(f"  95th %: {quantiles[94]:.2f} ms")
            print(f"  99th %: {quantiles[98]:.2f} ms")
        print()

    print("\n=== Execution Latency Profile ===")
    
    if fv_delays or pm_delays:
        print("\n1. Feed Desync (Local Receipt vs Exchange Generation Time)")
        print("   If Mean > 50ms, your physical distance from the exchange is killing the edge.")
        print_stats("FV Stream Latency (Binance -> You)", fv_delays)
        print_stats("PM Book Latency (Polymarket -> You)", pm_delays)
    else:
        print("\n1. Feed Desync: No local timestamps found in the JSON wrapper to calculate desync.")

    print("\n2. Tick Intervals (Time between consecutive exchange ticks)")
    print("   If the 99th percentile spikes > 500ms, your websocket stream is stuttering.")
    print_stats("FV Tick Intervals", fv_intervals)
    print_stats("PM Tick Intervals", pm_intervals)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze execution latency from a capture file.")
    parser.add_argument("file", help="Path to the .jsonl capture file")
    args = parser.parse_args()
    
    analyze_latency(args.file)
