import sys
import json
import base64
import msgpack
import argparse
from collections import defaultdict
from datetime import datetime

def analyze_chaos(filepaths):
    # market_id -> list of fv_stream ticks
    markets = defaultdict(list)
    
    print(f"Reading files: {filepaths}")
    for filepath in filepaths:
        with open(filepath, "r") as f:
            for line in f:
                obj = json.loads(line)
                if "fv_stream.ipc" not in obj.get("channel", ""):
                    continue
                
                raw = base64.b64decode(obj["data"])
                parsed = msgpack.unpackb(raw, raw=False)
                
                # Format: [ts_ms, boundary_ts, prob_up, prob_down, sigma, btc_price, intra_vol, is_sigma_real, strike]
                if len(parsed) < 9:
                    continue
                
                ts_ms = parsed[0]
                boundary_ts = parsed[1]
                sigma = parsed[4]
                btc_price = parsed[5]
                intra_vol = parsed[6]
                strike = parsed[8]
                
                markets[boundary_ts].append({
                    "ts_ms": ts_ms,
                    "btc_price": btc_price,
                    "strike": strike,
                    "intra_vol": intra_vol,
                    "sigma": sigma
                })
    
    print(f"Finished reading. Found {len(markets)} distinct markets.")
    
    # Calculate metrics per market
    results = []
    for boundary_ts, ticks in markets.items():
        if not ticks:
            continue
            
        ticks.sort(key=lambda x: x["ts_ms"])
        
        crosses = 0
        path_length = 0.0
        sum_intra_vol = 0.0
        
        first_price = ticks[0]["btc_price"]
        last_price = ticks[-1]["btc_price"]
        net_movement = abs(last_price - first_price)
        
        prev_side = None
        for i, tick in enumerate(ticks):
            sum_intra_vol += tick["intra_vol"]
            
            # Strike cross logic
            if tick["btc_price"] > tick["strike"]:
                side = "UP"
            elif tick["btc_price"] < tick["strike"]:
                side = "DOWN"
            else:
                side = prev_side
                
            if prev_side is not None and side != prev_side:
                crosses += 1
            prev_side = side
            
            # Path length logic
            if i > 0:
                path_length += abs(tick["btc_price"] - ticks[i-1]["btc_price"])
                
        avg_intra_vol = sum_intra_vol / len(ticks)
        efficiency_ratio = (net_movement / path_length) if path_length > 0 else 1.0
        
        results.append({
            "boundary_ts": boundary_ts,
            "window": datetime.fromtimestamp(boundary_ts).strftime('%Y-%m-%d %H:%M:%S'),
            "ticks": len(ticks),
            "crosses": crosses,
            "efficiency_ratio": efficiency_ratio,
            "avg_intra_vol": avg_intra_vol,
            "path_length": path_length,
            "net_movement": net_movement
        })
        
    results.sort(key=lambda x: x["boundary_ts"])
    
    # Group into hourly buckets to see trends over the days
    hourly = defaultdict(lambda: {"markets": 0, "crosses": 0, "efficiency_ratio": 0.0, "avg_intra_vol": 0.0})
    for r in results:
        hour = datetime.fromtimestamp(r["boundary_ts"]).strftime('%Y-%m-%d %H:00')
        hourly[hour]["markets"] += 1
        hourly[hour]["crosses"] += r["crosses"]
        hourly[hour]["efficiency_ratio"] += r["efficiency_ratio"]
        hourly[hour]["avg_intra_vol"] += r["avg_intra_vol"]
        
    print("\n=== Hourly Aggregates ===")
    print(f"{'Hour':<16} | {'Mkts':>4} | {'Avg Crosses':>11} | {'Avg Eff Ratio':>13} | {'Avg IntraVol':>12}")
    print("-" * 66)
    
    for hour in sorted(hourly.keys()):
        h = hourly[hour]
        m = h["markets"]
        if m == 0: continue
        avg_crosses = h["crosses"] / m
        avg_er = h["efficiency_ratio"] / m
        avg_vol = h["avg_intra_vol"] / m
        print(f"{hour:<16} | {m:>4} | {avg_crosses:>11.1f} | {avg_er:>13.3f} | {avg_vol:>12.4f}")
        
    print("\n=== Top 20 Most Chaotic Markets (by Strike Crosses) ===")
    results.sort(key=lambda x: x["crosses"], reverse=True)
    print(f"{'Market Window':<20} | {'Crosses':>7} | {'Eff Ratio':>9} | {'Avg Vol':>7} | {'Path Len':>8} | {'Net Move':>8}")
    print("-" * 71)
    for r in results[:20]:
        print(f"{r['window']:<20} | {r['crosses']:>7} | {r['efficiency_ratio']:>9.3f} | {r['avg_intra_vol']:>7.3f} | {r['path_length']:>8.1f} | {r['net_movement']:>8.1f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze market chaos from captures.")
    parser.add_argument("files", nargs="+", help="JSONL capture files")
    args = parser.parse_args()
    
    analyze_chaos(args.files)
