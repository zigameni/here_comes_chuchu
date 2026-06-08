#!/usr/bin/env python3
"""
tools/analyze_run.py
────────────────────
Deep analysis of live paper-trading fills (entries) and exits.

Sections:
  1. Overview & Summary
  2. PnL Distribution
  3. Side Breakdown (UP vs DOWN)
  4. Entry Quality (Edge, FV Age, Z-Score, Timing, Sigma)
  5. Market-Level Analysis (multi-fills, open positions)
  6. Edge → PnL Correlation
  7. BTC Price Context
  8. Streak & Drawdown Analysis
  9. Top Winners & Losers
  10. Data-Driven Improvement Suggestions
"""

import json
import math
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

# ── I/O ───────────────────────────────────────────────────────────────────────

FILLS_PATH = "fills_tos.jsonl"
EXITS_PATH = "exits_tos.jsonl"


def load_jsonl(path: str):
    p = Path(path)
    if not p.exists():
        print(f"[WARN] File not found: {path}")
        return []
    rows = []
    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError as err:
                    print(f"[WARN] Skipping malformed line in {path}: {err}")
    return rows


# ── Math helpers ──────────────────────────────────────────────────────────────

def mean(lst):
    return sum(lst) / len(lst) if lst else 0.0

def median(lst):
    if not lst:
        return 0.0
    s = sorted(lst)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0

def stdev(lst):
    if len(lst) < 2:
        return 0.0
    m = mean(lst)
    return math.sqrt(sum((x - m) ** 2 for x in lst) / (len(lst) - 1))

def percentile(lst, p):
    if not lst:
        return 0.0
    s = sorted(lst)
    idx = (p / 100.0) * (len(s) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (idx - lo)

def pearson(xs, ys):
    """Return Pearson r for two equal-length lists, or None if not computable."""
    if len(xs) != len(ys) or len(xs) < 3:
        return None
    mx, my = mean(xs), mean(ys)
    cov = mean([(x - mx) * (y - my) for x, y in zip(xs, ys)])
    sx, sy = stdev(xs), stdev(ys)
    if sx == 0 or sy == 0:
        return None
    return cov / (sx * sy)

def pct(n, total):
    return (n / total * 100) if total > 0 else 0.0


# ── Formatting helpers ────────────────────────────────────────────────────────

def ts_str(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def sep(char="═", n=72):
    print(char * n)

def header(title):
    print()
    sep()
    print(f"  {title}")
    sep()

def subheader(title):
    pad = max(0, 62 - len(title))
    print(f"\n── {title} " + "─" * pad)

def hist_ascii(values, bins=10, width=38, label=None):
    """Print a compact horizontal ASCII histogram."""
    if not values:
        return
    mn, mx = min(values), max(values)
    if mn == mx:
        print(f"  (all values = {mn:.5f})")
        return
    bin_size = (mx - mn) / bins
    counts = [0] * bins
    for v in values:
        idx = min(int((v - mn) / bin_size), bins - 1)
        counts[idx] += 1
    max_c = max(counts) or 1
    if label:
        print(f"  {label}")
    for i, c in enumerate(counts):
        lo = mn + i * bin_size
        hi = lo + bin_size
        bar = "█" * round(c / max_c * width)
        print(f"  [{lo:+8.4f}, {hi:+8.4f})  {bar:<{width}}  {c:>4}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    fills = load_jsonl(FILLS_PATH)
    exits = load_jsonl(EXITS_PATH)

    if not fills and not exits:
        print("No data found. Check FILLS_PATH / EXITS_PATH.")
        return

    # Pre-compute common groupings
    fills_by_key   = defaultdict(list)   # (market_id, side) -> [fill, ...]
    fills_by_mid   = defaultdict(list)   # market_id -> [fill, ...]
    for f in fills:
        fills_by_key[(f.get("market_id"), f.get("side"))].append(f)
        fills_by_mid[f.get("market_id")].append(f)

    exits_by_key   = defaultdict(list)   # (market_id, side) -> [exit, ...]
    exits_by_mid   = defaultdict(list)   # market_id -> [exit, ...]
    for e in exits:
        exits_by_key[(e.get("market_id"), e.get("side"))].append(e)
        exits_by_mid[e.get("market_id")].append(e)

    wins   = [e for e in exits if e.get("pnl", 0) > 0]
    losses = [e for e in exits if e.get("pnl", 0) < 0]
    draws  = [e for e in exits if e.get("pnl", 0) == 0]

    settlement_exits = [e for e in exits if e.get("exit_reason") == "SETTLEMENT"]
    early_exits      = [e for e in exits if e.get("exit_reason") != "SETTLEMENT"]

    total_cost     = sum(f.get("cost", 0) for f in fills)
    total_proceeds = sum(e.get("proceeds", 0) for e in exits)
    net_pnl        = total_proceeds - total_cost
    gross_wins     = sum(e["pnl"] for e in wins)
    gross_losses   = abs(sum(e["pnl"] for e in losses))
    profit_factor  = gross_wins / gross_losses if gross_losses > 0 else float("inf")
    wr             = pct(len(wins), len(exits))

    pnls     = [e.get("pnl", 0) for e in exits]
    pnl_pcts = [e.get("pnl_pct", 0) * 100 for e in exits]

    edges    = [f.get("edge", 0)      for f in fills]
    fv_ages  = [f.get("fv_age_ms", 0) for f in fills]
    z_scores = [f.get("z_score", 0)   for f in fills]
    sigmas   = [f.get("sigma", 0)     for f in fills]
    asks     = [f.get("ask", 0)       for f in fills]
    btc_px   = [f.get("btc_price", 0) for f in fills if "btc_price" in f]

    # ─────────────────────────────────────────────────────────────────────────
    # 1. OVERVIEW
    # ─────────────────────────────────────────────────────────────────────────
    header("1. OVERVIEW & SUMMARY")

    all_ts = [r["ts_ms"] for r in fills + exits if "ts_ms" in r]
    if all_ts:
        span_h = (max(all_ts) - min(all_ts)) / 3_600_000
        print(f"\n  Period:    {ts_str(min(all_ts))}  →  {ts_str(max(all_ts))}")
        print(f"  Duration:  {span_h:.2f} hours")

    print(f"\n  {'Metric':<30} {'Value'}")
    print(f"  {'-'*52}")
    print(f"  {'Total Fills (Entries):':<30} {len(fills)}")
    print(f"  {'Total Exits:':<30} {len(exits)}")
    print(f"  {'Settlement Exits:':<30} {len(settlement_exits)}  ({pct(len(settlement_exits), len(exits)):.0f}%)")
    print(f"  {'Early-Sell Exits:':<30} {len(early_exits)}  ({pct(len(early_exits), len(exits)):.0f}%)")
    print(f"  {'Capital Deployed (fills):':<30} ${total_cost:.2f}")
    print(f"  {'Total Proceeds (exits):':<30} ${total_proceeds:.2f}")
    print(f"  {'Net PnL:':<30} ${net_pnl:+.2f}")
    print(f"  {'Win Rate:':<30} {wr:.1f}%  ({len(wins)}W / {len(losses)}L / {len(draws)}D)")
    print(f"  {'Profit Factor:':<30} {profit_factor:.3f}  (gross wins ÷ gross losses)")
    print(f"  {'Avg PnL per Exit:':<30} ${mean(pnls):+.4f}")
    print(f"  {'Avg PnL% per Exit:':<30} {mean(pnl_pcts):+.2f}%")
    if len(exits) > 0:
        print(f"  {'Avg Wins:':<30} ${mean([e['pnl'] for e in wins]):+.4f}" if wins else "")
        print(f"  {'Avg Loss:':<30} ${mean([e['pnl'] for e in losses]):+.4f}" if losses else "")

    # Open positions: fills that have no corresponding exit
    filled_keys = set(fills_by_key.keys())
    exited_keys = set(exits_by_key.keys())
    open_keys   = filled_keys - exited_keys
    open_fills  = [f for k in open_keys for f in fills_by_key[k]]
    open_cost   = sum(f.get("cost", 0) for f in open_fills)
    print(f"\n  {'Open Positions (no exit yet):':<30} {len(open_keys)} positions  (${open_cost:.2f} at risk)")

    # ─────────────────────────────────────────────────────────────────────────
    # 2. PnL DISTRIBUTION
    # ─────────────────────────────────────────────────────────────────────────
    header("2. PnL DISTRIBUTION")

    if pnls:
        print(f"\n  {'Metric':<25} {'Abs ($)':>14}  {'% of Cost':>10}")
        print(f"  {'-'*53}")
        for label, fn in [
            ("Best Exit",       lambda: (max(pnls),                 max(pnl_pcts))),
            ("Worst Exit",      lambda: (min(pnls),                 min(pnl_pcts))),
            ("Mean",            lambda: (mean(pnls),                mean(pnl_pcts))),
            ("Median",          lambda: (median(pnls),              median(pnl_pcts))),
            ("Std Dev",         lambda: (stdev(pnls),               stdev(pnl_pcts))),
            ("P5  (tail loss)", lambda: (percentile(pnls, 5),       percentile(pnl_pcts, 5))),
            ("P25",             lambda: (percentile(pnls, 25),      percentile(pnl_pcts, 25))),
            ("P75",             lambda: (percentile(pnls, 75),      percentile(pnl_pcts, 75))),
            ("P95 (tail gain)", lambda: (percentile(pnls, 95),      percentile(pnl_pcts, 95))),
        ]:
            av, bv = fn()
            print(f"  {label:<25} ${av:>+13.4f}  {bv:>+9.2f}%")

        print()
        hist_ascii(pnls, bins=12, width=36, label="PnL ($) Histogram")

        if wins and losses:
            rr = abs(mean([e["pnl"] for e in wins]) / mean([e["pnl"] for e in losses]))
            print(f"\n  Win/Loss Ratio (avg sizes):  {rr:.3f}")

        # Kelly Criterion
        if wins and losses and len(exits) > 0:
            w       = len(wins) / len(exits)
            avg_win = mean([e["pnl"] for e in wins])
            avg_loss= abs(mean([e["pnl"] for e in losses]))
            if avg_loss > 0:
                b     = avg_win / avg_loss
                kelly = w - (1 - w) / b
                print(f"  Kelly Fraction:              {kelly * 100:+.2f}%")
                if kelly < 0:
                    print("    ⚠  Negative Kelly → strategy has negative expected value")
                elif kelly < 0.05:
                    print("    ⚠  Kelly < 5% → edge is very thin, be conservative with sizing")
                else:
                    print(f"    ✅  Kelly positive → optimal fractional bet ≈ {kelly*100:.1f}% of bankroll")

    # ─────────────────────────────────────────────────────────────────────────
    # 3. SIDE BREAKDOWN
    # ─────────────────────────────────────────────────────────────────────────
    header("3. SIDE BREAKDOWN  (UP vs DOWN)")

    for side in ["UP", "DOWN"]:
        sf  = [f for f in fills  if f.get("side") == side]
        se  = [e for e in exits  if e.get("side") == side]
        sw  = [e for e in se     if e.get("pnl", 0) > 0]
        sl  = [e for e in se     if e.get("pnl", 0) < 0]
        print(f"\n  ▸ {side}")
        print(f"    Fills:            {len(sf):<6}  ({pct(len(sf), len(fills)):.0f}% of all fills)")
        print(f"    Exits:            {len(se):<6}  ({pct(len(se), len(exits)):.0f}% of all exits)")
        print(f"    Capital Deployed: ${sum(f['cost'] for f in sf):.2f}")
        print(f"    Net PnL:          ${sum(e['pnl'] for e in se):+.2f}")
        print(f"    Win Rate:         {pct(len(sw), len(se)):.1f}%  ({len(sw)}W / {len(sl)}L)")
        if se:
            print(f"    Avg PnL/Exit:     ${mean([e['pnl'] for e in se]):+.4f}")
        if sf:
            print(f"    Avg Ask Price:    {mean([f['ask'] for f in sf]):.4f}")
            print(f"    Avg Edge:         {mean([f['edge'] for f in sf]):.5f}")
            print(f"    Avg Z-Score:      {mean([f.get('z_score', 0) for f in sf]):+.4f}")

    # Are both sides open at the same time in the same market? (directional hedge)
    hedged_markets = set()
    for mid in fills_by_mid:
        sides_in_market = {f.get("side") for f in fills_by_mid[mid]}
        if "UP" in sides_in_market and "DOWN" in sides_in_market:
            hedged_markets.add(mid)
    if hedged_markets:
        print(f"\n  ⚠  Markets with both UP & DOWN fills simultaneously: {len(hedged_markets)}")
        print("    (These are effectively hedged/delta-neutral positions)")

    # ─────────────────────────────────────────────────────────────────────────
    # 4. ENTRY QUALITY
    # ─────────────────────────────────────────────────────────────────────────
    header("4. ENTRY QUALITY ANALYSIS")

    subheader("Edge at Entry  (fv − ask)")
    print(f"  Mean:     {mean(edges):.5f}")
    print(f"  Median:   {median(edges):.5f}")
    print(f"  Std Dev:  {stdev(edges):.5f}")
    print(f"  Min:      {min(edges):.5f}   Max: {max(edges):.5f}")
    print(f"\n  Edge Distribution:")
    print(f"  {'Range':<15} {'Count':>7}  {'%':>7}")
    buckets = [(0.00,0.05,"0.00–0.05"),(0.05,0.10,"0.05–0.10"),
               (0.10,0.15,"0.10–0.15"),(0.15,0.20,"0.15–0.20"),
               (0.20,0.30,"0.20–0.30"),(0.30,1.00,"0.30+")]
    for lo, hi, lbl in buckets:
        n = sum(1 for e in edges if lo <= e < hi)
        bar = "█" * round(pct(n, len(edges)) / 2.5) if len(edges) > 0 else ""
        print(f"  {lbl:<15} {n:>7}  {pct(n, len(edges)):>6.1f}%  {bar}")

    print()
    hist_ascii(edges, bins=10, width=36, label="Edge Histogram")

    subheader("FV Age at Entry  (staleness of fair value)")
    print(f"  Mean:     {mean(fv_ages):.1f} ms")
    print(f"  Median:   {median(fv_ages):.1f} ms")
    print(f"  P95:      {percentile(fv_ages, 95):.1f} ms")
    print(f"  Max:      {max(fv_ages):.1f} ms")
    stale_500  = sum(1 for a in fv_ages if a > 500)
    stale_1000 = sum(1 for a in fv_ages if a > 1000)
    print(f"  FV Age > 500ms:   {stale_500:>4}  ({pct(stale_500, len(fv_ages)):.1f}%)")
    print(f"  FV Age > 1000ms:  {stale_1000:>4}  ({pct(stale_1000, len(fv_ages)):.1f}%)")
    print()
    hist_ascii(fv_ages, bins=10, width=36, label="FV Age (ms) Histogram")

    subheader("Z-Score at Entry")
    print(f"  Mean:       {mean(z_scores):+.4f}  (systematic directional bias if far from 0)")
    print(f"  Median:     {median(z_scores):+.4f}")
    print(f"  Avg |z|:    {mean([abs(z) for z in z_scores]):.4f}")
    print(f"  Min:        {min(z_scores):+.4f}   Max: {max(z_scores):+.4f}")
    high_z = sum(1 for z in z_scores if abs(z) > 2.0)
    print(f"  |z| > 2.0:  {high_z}  ({pct(high_z, len(z_scores)):.1f}%) — strong-conviction entries")
    low_z  = sum(1 for z in z_scores if abs(z) < 0.5)
    print(f"  |z| < 0.5:  {low_z}  ({pct(low_z, len(z_scores)):.1f}%) — low-conviction entries")
    print()
    hist_ascii(z_scores, bins=12, width=36, label="Z-Score Histogram")

    subheader("Entry Timing Within Window  (elapsed_s)")
    time_fracs = []
    for f in fills:
        ws = f.get("window_start_ts", 0)
        we = f.get("window_end_ts", 0)
        dur = we - ws
        if dur > 0:
            time_fracs.append(f.get("elapsed_s", 0) / dur)

    if time_fracs:
        print(f"  Avg entry position in window: {mean(time_fracs)*100:.1f}%")
        q1  = sum(1 for t in time_fracs if t < 0.25)
        q2  = sum(1 for t in time_fracs if 0.25 <= t < 0.50)
        q3  = sum(1 for t in time_fracs if 0.50 <= t < 0.75)
        q4  = sum(1 for t in time_fracs if t >= 0.75)
        print(f"  Q1 (0–25%):   {q1:>4}  ({pct(q1, len(time_fracs)):.1f}%)")
        print(f"  Q2 (25–50%):  {q2:>4}  ({pct(q2, len(time_fracs)):.1f}%)")
        print(f"  Q3 (50–75%):  {q3:>4}  ({pct(q3, len(time_fracs)):.1f}%)")
        print(f"  Q4 (75–100%): {q4:>4}  ({pct(q4, len(time_fracs)):.1f}%)")

    subheader("Intra-Window Volatility (Sigma)")
    print(f"  Mean:   {mean(sigmas):.5f}")
    print(f"  Median: {median(sigmas):.5f}")
    print(f"  Min:    {min(sigmas):.5f}   Max: {max(sigmas):.5f}")
    high_sigma = sum(1 for s in sigmas if s > 0.30)
    print(f"  Sigma > 0.30: {high_sigma}  ({pct(high_sigma, len(sigmas)):.1f}%) — high-vol entries")

    subheader("Ask Price Distribution")
    print(f"  Mean:   {mean(asks):.4f}")
    print(f"  Median: {median(asks):.4f}")
    print(f"  Min:    {min(asks):.4f}   Max: {max(asks):.4f}")
    # Near-certainty entries (ask > 0.85) and deep underdog entries (ask < 0.30)
    near_cert = sum(1 for a in asks if a > 0.85)
    underdog  = sum(1 for a in asks if a < 0.30)
    print(f"  Ask > 0.85 (near-certain):  {near_cert}  ({pct(near_cert, len(asks)):.1f}%)")
    print(f"  Ask < 0.30 (deep underdog): {underdog}  ({pct(underdog, len(asks)):.1f}%)")

    # ─────────────────────────────────────────────────────────────────────────
    # 5. MARKET-LEVEL ANALYSIS
    # ─────────────────────────────────────────────────────────────────────────
    header("5. MARKET-LEVEL ANALYSIS")

    fills_per_mkt = [len(v) for v in fills_by_mid.values()]
    multi_fill    = {mid: fs for mid, fs in fills_by_mid.items() if len(fs) > 1}
    both_sides    = {mid for mid in fills_by_mid
                     if len({f.get("side") for f in fills_by_mid[mid]}) > 1}

    print(f"\n  Unique markets w/ fills:   {len(fills_by_mid)}")
    print(f"  Unique markets w/ exits:   {len(exits_by_mid)}")
    print(f"  Avg fills per market:      {mean(fills_per_mkt):.2f}")
    print(f"  Max fills per market:      {max(fills_per_mkt) if fills_per_mkt else 0}")
    print(f"  Markets with >1 fill:      {len(multi_fill)}  ({pct(len(multi_fill), len(fills_by_mid)):.1f}%)")
    print(f"  Markets with both sides:   {len(both_sides)}")

    if multi_fill:
        print(f"\n  Heaviest Multi-Fill Markets (top 8 by count):")
        print(f"  {'Market (truncated)':<22} {'N':>4} {'Sides':>12} {'Avg Ask':>9} {'Cost':>9}")
        for mid, fs in sorted(multi_fill.items(), key=lambda x: -len(x[1]))[:8]:
            sides = "/".join(sorted(set(f.get("side","?") for f in fs)))
            print(f"  {mid[:20]:<22} {len(fs):>4} {sides:>12} "
                  f"{mean([f['ask'] for f in fs]):>9.4f} ${sum(f['cost'] for f in fs):>8.2f}")

    # Open positions
    open_pos = []
    for key in open_keys:
        fs = fills_by_key[key]
        open_pos.append({
            "market_id": key[0],
            "side": key[1],
            "n_fills": len(fs),
            "cost": sum(f["cost"] for f in fs),
            "avg_ask": mean([f["ask"] for f in fs]),
            "avg_edge": mean([f["edge"] for f in fs]),
            "latest_ts": max(f["ts_ms"] for f in fs),
        })
    if open_pos:
        print(f"\n  Open Positions (fills without a matching exit):")
        print(f"  {'Market (truncated)':<22} {'Side':>5} {'N':>4} {'Avg Ask':>9} {'Avg Edge':>9} {'Cost':>9}")
        for pos in sorted(open_pos, key=lambda x: -x["cost"])[:10]:
            print(f"  {pos['market_id'][:20]:<22} {pos['side']:>5} {pos['n_fills']:>4} "
                  f"{pos['avg_ask']:>9.4f} {pos['avg_edge']:>9.5f} ${pos['cost']:>8.2f}")
        if len(open_pos) > 10:
            print(f"  ... and {len(open_pos) - 10} more")

    # ─────────────────────────────────────────────────────────────────────────
    # 6. EDGE → PnL CORRELATION
    # ─────────────────────────────────────────────────────────────────────────
    header("6. EDGE → PnL CORRELATION  (matched markets)")

    common_mids = set(fills_by_mid.keys()) & set(exits_by_mid.keys())
    print(f"\n  Markets with both fills & exits: {len(common_mids)}")

    if len(common_mids) >= 3:
        edge_pnl = []
        for mid in common_mids:
            avg_edge = mean([f["edge"] for f in fills_by_mid[mid]])
            tot_pnl  = sum(e["pnl"] for e in exits_by_mid[mid])
            edge_pnl.append((avg_edge, tot_pnl))

        edges_m = [p[0] for p in edge_pnl]
        pnls_m  = [p[1] for p in edge_pnl]
        r = pearson(edges_m, pnls_m)
        if r is not None:
            print(f"  Pearson r (edge vs PnL):  {r:+.4f}")
            if   r >  0.40: print("  ✅  Positive correlation — higher edge → better outcome")
            elif r >  0.10: print("  ~~  Weak positive correlation")
            elif r < -0.10: print("  ⚠   Negative/weak correlation — edge estimate may be noisy")
            else:           print("  ~~  No meaningful linear correlation detected")

        wins_m    = [p[0] for p in edge_pnl if p[1] > 0]
        losses_m  = [p[0] for p in edge_pnl if p[1] < 0]
        print(f"\n  Avg edge on winning markets:  {mean(wins_m):.5f}" if wins_m else "")
        print(f"  Avg edge on losing markets:   {mean(losses_m):.5f}" if losses_m else "")

        # FV age vs PnL
        fvage_pnl = []
        for mid in common_mids:
            avg_fv_age = mean([f.get("fv_age_ms", 0) for f in fills_by_mid[mid]])
            tot_pnl    = sum(e["pnl"] for e in exits_by_mid[mid])
            fvage_pnl.append((avg_fv_age, tot_pnl))
        r_fv = pearson([p[0] for p in fvage_pnl], [p[1] for p in fvage_pnl])
        if r_fv is not None:
            print(f"\n  Pearson r (fv_age_ms vs PnL):  {r_fv:+.4f}")
            if r_fv < -0.15:
                print("  ⚠   FV staleness hurts — fresher FV entries tend to outperform")
            else:
                print("  ~~  FV age doesn't strongly predict outcome in this dataset")

        # Z-score magnitude vs PnL
        zscore_pnl = []
        for mid in common_mids:
            avg_abs_z = mean([abs(f.get("z_score", 0)) for f in fills_by_mid[mid]])
            tot_pnl   = sum(e["pnl"] for e in exits_by_mid[mid])
            zscore_pnl.append((avg_abs_z, tot_pnl))
        r_z = pearson([p[0] for p in zscore_pnl], [p[1] for p in zscore_pnl])
        if r_z is not None:
            print(f"  Pearson r (|z-score| vs PnL):  {r_z:+.4f}")
            if r_z > 0.15:
                print("  ✅  Higher |z| at entry tends to produce better PnL")
            elif r_z < -0.15:
                print("  ⚠   High-z entries underperform — possible mean-reversion failure")
    else:
        print("  (Not enough matched markets for correlation analysis)")

    # ─────────────────────────────────────────────────────────────────────────
    # 7. BTC PRICE CONTEXT
    # ─────────────────────────────────────────────────────────────────────────
    header("7. BTC PRICE CONTEXT  (at entry)")

    if btc_px:
        px_range = max(btc_px) - min(btc_px)
        print(f"\n  Min:    ${min(btc_px):>12,.2f}")
        print(f"  Max:    ${max(btc_px):>12,.2f}")
        print(f"  Mean:   ${mean(btc_px):>12,.2f}")
        print(f"  Range:  ${px_range:>12,.2f}  ({px_range / mean(btc_px) * 100:.2f}% swing across session)")

        up_btc   = [f["btc_price"] for f in fills if f.get("side") == "UP"   and "btc_price" in f]
        down_btc = [f["btc_price"] for f in fills if f.get("side") == "DOWN" and "btc_price" in f]
        if up_btc and down_btc:
            print(f"\n  Avg BTC price on UP   fills: ${mean(up_btc):>12,.2f}")
            print(f"  Avg BTC price on DOWN fills: ${mean(down_btc):>12,.2f}")
            diff = mean(up_btc) - mean(down_btc)
            if abs(diff) > 100:
                direction = "higher" if diff > 0 else "lower"
                print(f"  ▸ UP entries taken at ${abs(diff):,.0f} {direction} BTC price than DOWN entries")

        # BTC price vs PnL on matched markets
        btc_pnl = []
        for mid in (set(fills_by_mid.keys()) & set(exits_by_mid.keys())):
            avg_btc = mean([f.get("btc_price", 0) for f in fills_by_mid[mid] if "btc_price" in f])
            tot_pnl = sum(e["pnl"] for e in exits_by_mid[mid])
            if avg_btc > 0:
                btc_pnl.append((avg_btc, tot_pnl))
        r_btc = pearson([p[0] for p in btc_pnl], [p[1] for p in btc_pnl])
        if r_btc is not None:
            print(f"\n  Pearson r (BTC price vs PnL):  {r_btc:+.4f}")
            if abs(r_btc) > 0.2:
                print("  ▸ BTC price level has some correlation with outcome — check regime sensitivity")

    # ─────────────────────────────────────────────────────────────────────────
    # 8. STREAK & DRAWDOWN ANALYSIS
    # ─────────────────────────────────────────────────────────────────────────
    header("8. STREAK & DRAWDOWN ANALYSIS")

    if exits:
        sorted_exits = sorted(exits, key=lambda e: e.get("ts_ms", 0))
        outcomes = [1 if e.get("pnl", 0) > 0 else (-1 if e.get("pnl", 0) < 0 else 0)
                    for e in sorted_exits]

        max_win_streak = max_loss_streak = cur_win = cur_loss = 0
        for o in outcomes:
            if o > 0:
                cur_win += 1; cur_loss = 0
            elif o < 0:
                cur_loss += 1; cur_win = 0
            else:
                cur_win = cur_loss = 0
            max_win_streak  = max(max_win_streak,  cur_win)
            max_loss_streak = max(max_loss_streak, cur_loss)

        print(f"\n  Max Consecutive Wins:   {max_win_streak}")
        print(f"  Max Consecutive Losses: {max_loss_streak}")

        # Cumulative PnL curve & drawdown
        cum_pnl = []
        running = 0.0
        for e in sorted_exits:
            running += e.get("pnl", 0)
            cum_pnl.append(running)

        peak   = cum_pnl[0]
        max_dd = 0.0
        dd_end_idx = 0
        for i, v in enumerate(cum_pnl):
            peak = max(peak, v)
            dd = peak - v
            if dd > max_dd:
                max_dd = dd
                dd_end_idx = i

        print(f"  Max Drawdown:           ${max_dd:.4f}  (peak-to-trough in chronological exit order)")
        print(f"  Final Cumulative PnL:   ${cum_pnl[-1]:+.4f}")

        # Drawdown as % of peak
        if max_dd > 0 and max(cum_pnl) > 0:
            print(f"  Max DD % of peak:       {max_dd / max(cum_pnl) * 100:.1f}%")

        # Recovery factor
        if max_dd > 0:
            recovery = cum_pnl[-1] / max_dd
            print(f"  Recovery Factor:        {recovery:.3f}  (total PnL ÷ max drawdown)")

        # Mini equity curve (sparkline)
        if len(cum_pnl) > 1:
            mn_eq, mx_eq = min(cum_pnl), max(cum_pnl)
            steps = min(60, len(cum_pnl))
            idxs  = [int(i * (len(cum_pnl) - 1) / (steps - 1)) for i in range(steps)]
            BLOCKS = " ▁▂▃▄▅▆▇█"
            rng    = mx_eq - mn_eq or 1
            spark  = "".join(BLOCKS[min(8, int((cum_pnl[i] - mn_eq) / rng * 8))] for i in idxs)
            print(f"\n  Equity Curve (left→right, chronological exits):")
            print(f"  ${mn_eq:+.2f} ┤{spark}├ ${mx_eq:+.2f}")

    # ─────────────────────────────────────────────────────────────────────────
    # 9. TOP WINNERS & LOSERS
    # ─────────────────────────────────────────────────────────────────────────
    header("9. TOP WINNERS & LOSERS")

    by_pnl = sorted(exits, key=lambda e: e.get("pnl", 0))

    def print_exit_table(label, rows):
        print(f"\n  {label}")
        print(f"  {'Market (truncated)':<22} {'Side':>5} {'Entry':>7} {'Exit':>7} {'PnL':>9} {'PnL%':>7} {'Reason'}")
        for e in rows:
            print(f"  {e.get('market_id','')[:20]:<22} "
                  f"{e.get('side',''):>5} "
                  f"{e.get('avg_entry', 0):>7.4f} "
                  f"{e.get('exit_price', 0):>7.4f} "
                  f"${e.get('pnl', 0):>+8.2f} "
                  f"{e.get('pnl_pct', 0)*100:>+6.1f}% "
                  f"{e.get('exit_reason','')}")

    print_exit_table("Top 5 Losses:", by_pnl[:5])
    print_exit_table("Top 5 Wins:",   by_pnl[-5:][::-1])

    # ─────────────────────────────────────────────────────────────────────────
    # 10. DATA-DRIVEN SUGGESTIONS
    # ─────────────────────────────────────────────────────────────────────────
    header("10. DATA-DRIVEN IMPROVEMENT SUGGESTIONS")

    suggestions = []

    # PnL/EV
    if net_pnl < 0:
        suggestions.append(
            f"⚠  Net PnL is negative (${net_pnl:.2f}). Overall, the strategy lost money this session.")
    if profit_factor < 1.0:
        suggestions.append(
            f"⚠  Profit Factor {profit_factor:.3f} < 1.0 — gross losses exceed gross wins. "
            "Raise minimum edge or reduce exposure on low-edge trades.")

    # Kelly
    if wins and losses and len(exits) > 0:
        w       = len(wins) / len(exits)
        avg_win = mean([e["pnl"] for e in wins])
        avg_loss= abs(mean([e["pnl"] for e in losses]))
        if avg_loss > 0:
            kelly = w - (1 - w) / (avg_win / avg_loss)
            if kelly < 0:
                suggestions.append(
                    "⚠  Kelly Fraction is negative — strategy has negative EV. "
                    "Revisit model calibration before sizing up.")
            elif 0 < kelly < 0.05:
                suggestions.append(
                    f"⚠  Kelly Fraction is only {kelly*100:.1f}%. Edge is very thin — consider "
                    "raising the minimum edge threshold (e.g., to 0.10+) to be more selective.")

    # FV age
    if fv_ages:
        avg_fv_age = mean(fv_ages)
        stale_pct  = pct(stale_500, len(fv_ages))
        if avg_fv_age > 300:
            suggestions.append(
                f"⚠  Mean FV age at entry is {avg_fv_age:.0f}ms — relatively stale. "
                "Add a filter: skip entries where fv_age_ms > 250ms.")
        if stale_pct > 10:
            suggestions.append(
                f"⚠  {stale_pct:.1f}% of entries have FV age > 500ms. "
                "These use potentially outdated model output — consider hard-capping at 500ms.")

    # Edge
    if edges and mean(edges) < 0.10:
        suggestions.append(
            f"⚠  Mean edge is {mean(edges):.4f}, which is low. "
            "Raising the edge floor to ≥ 0.10 may improve signal quality at the cost of fewer fills.")

    # Z-score
    low_z_pct = pct(sum(1 for z in z_scores if abs(z) < 0.5), len(z_scores))
    if low_z_pct > 20:
        suggestions.append(
            f"⚠  {low_z_pct:.0f}% of entries have |z| < 0.5 (low conviction). "
            "Consider requiring |z_score| > 0.75 or 1.0 for entry.")

    # Side imbalance
    up_n   = sum(1 for f in fills if f.get("side") == "UP")
    down_n = sum(1 for f in fills if f.get("side") == "DOWN")
    if up_n > 0 and down_n > 0:
        ratio = max(up_n, down_n) / min(up_n, down_n)
        if ratio > 2.5:
            dom = "UP" if up_n > down_n else "DOWN"
            suggestions.append(
                f"⚠  Strong side imbalance: {up_n} UP vs {down_n} DOWN ({ratio:.1f}x). "
                f"Bot is heavily biased toward {dom} — verify this matches your market view.")

    # Both-sides hedging
    if hedged_markets:
        suggestions.append(
            f"ℹ  {len(hedged_markets)} markets have both UP and DOWN fills — "
            "evaluate whether the hedged positions cancel edge or provide genuine diversification.")

    # Multi-fill sizing
    if multi_fill and len(multi_fill) > len(fills_by_mid) * 0.2:
        suggestions.append(
            f"ℹ  {pct(len(multi_fill), len(fills_by_mid)):.0f}% of markets have multiple fills "
            "(average-in pattern). Check whether multi-fill markets out- or underperform single-fill ones.")

    # Open positions
    if open_pos:
        suggestions.append(
            f"ℹ  {len(open_pos)} open positions (${open_cost:.2f} total) have no recorded exit. "
            "Ensure exits are captured or settlements have been processed.")

    # Max loss streak
    if max_loss_streak >= 5:
        suggestions.append(
            f"⚠  Max loss streak is {max_loss_streak}. Consider a circuit-breaker that pauses trading "
            "after N consecutive losses to prevent runaway drawdown.")

    for i, s in enumerate(suggestions, 1):
        print(f"\n  {i:>2}. {s}")

    if not suggestions:
        print("\n  ✅  No major issues found — metrics look healthy for this session.")

    sep()
    print("  Analysis complete.")
    sep()
    print()


if __name__ == "__main__":
    main()