#!/usr/bin/env python3
"""
Test a single optimizer config against capture files.

Examples:
  # Single day
  python -m tools.run_single --run-id abc12345 --date 2026-06-06

  # Date range
  python -m tools.run_single --run-id abc12345 --start 2026-06-05 --end 2026-06-09

  # All available captures
  python -m tools.run_single --run-id abc12345 --all

  # Keep fills/exits for further inspection
  python -m tools.run_single --run-id abc12345 --date 2026-06-06 --save-fills

  # Point at stage2 results instead of the default
  python -m tools.run_single --run-id abc12345 --all \
      --results-csv optimization_results/optimization_results_stage2.csv
"""
import argparse
import csv
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.parameter_registry import PARAMETERS, get_fixed_env
from tools.replay_engine import run_backtest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("run_single")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _merge_captures(files: list[Path], output_path: Path) -> None:
    """Merge and time-sort a list of capture JSONL files into one."""
    events = []
    for f in files:
        with open(f, "r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh):
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                    ev["_line_no"] = line_no
                    events.append(ev)
                except Exception:
                    pass

    events.sort(key=lambda x: (x["ts_ms"], x["_line_no"]))

    with open(output_path, "w", encoding="utf-8") as out:
        for ev in events:
            ev.pop("_line_no", None)
            out.write(json.dumps(ev) + "\n")


def _print_results(run_id: str, label: str, params: dict, metrics: dict) -> None:
    width = 54
    print()
    print("=" * width)
    print(f"  run_id  : {run_id}")
    print(f"  captures: {label}")
    print("-" * width)
    print("  PARAMETERS")
    for k, v in params.items():
        print(f"    {k:<28} {v}")
    print("-" * width)
    print("  METRICS")
    skip = {"error"}
    for k, v in metrics.items():
        if k in skip:
            continue
        if isinstance(v, float):
            print(f"    {k:<28} {v:.4f}")
        else:
            print(f"    {k:<28} {v}")
    if metrics.get("error"):
        print(f"  ERROR: {metrics['error']}")
    print("=" * width)
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Replay a single optimizer config against capture data."
    )
    parser.add_argument(
        "--run-id", required=True,
        help="run_id to look up in the results CSV.",
    )
    parser.add_argument(
        "--results-csv",
        default="optimization_results/optimization_results.csv",
        help="Path to the optimization results CSV (default: optimization_results/optimization_results.csv).",
    )
    parser.add_argument(
        "--captures-dir",
        default="captures/",
        help="Directory containing YYYY-MM-DD.jsonl capture files.",
    )
    parser.add_argument(
        "--output-dir",
        default="/tmp/run_single",
        help="Temp directory for merged captures (cleaned up after run).",
    )
    parser.add_argument(
        "--save-fills", action="store_true",
        help="Keep fills/exits JSONL files in --output-dir after the run.",
    )

    # Capture selection — mutually exclusive
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument(
        "--date",
        help="Single capture date, e.g. 2026-06-06.",
    )
    scope.add_argument(
        "--all", dest="use_all", action="store_true",
        help="Use every capture file in --captures-dir.",
    )
    scope.add_argument(
        "--start",
        help="Start date for a range (inclusive). Requires --end.",
    )

    parser.add_argument(
        "--end",
        help="End date for a range (inclusive). Required with --start.",
    )

    args = parser.parse_args()

    if args.start and not args.end:
        parser.error("--end is required when using --start.")

    # ------------------------------------------------------------------
    # Load config from CSV
    # ------------------------------------------------------------------
    results_csv = Path(args.results_csv)
    if not results_csv.exists():
        log.error(f"Results CSV not found: {results_csv}")
        return 1

    with open(results_csv) as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)

    row = next((r for r in all_rows if r.get("run_id") == args.run_id), None)
    if row is None:
        log.error(f"run_id '{args.run_id}' not found in {results_csv}.")
        sample = [r["run_id"] for r in all_rows[:10]]
        log.info(f"First 10 run_ids available: {sample}")
        return 1

    param_keys = list(PARAMETERS.keys())
    params: dict = {}
    for k in param_keys:
        if k in row and row[k] != "":
            try:
                params[k] = (
                    float(row[k]) if PARAMETERS[k]["type"] == "float"
                    else int(row[k])
                )
            except (ValueError, KeyError):
                pass

    log.info(f"Loaded config run_id={args.run_id} ({len(params)} params)")

    # ------------------------------------------------------------------
    # Resolve capture files
    # ------------------------------------------------------------------
    cap_dir = Path(args.captures_dir)
    all_files = sorted(cap_dir.glob("*.jsonl"))

    if args.date:
        capture_files = [f for f in all_files if f.stem == args.date]
        label = args.date
    elif args.use_all:
        capture_files = all_files
        label = "all"
    else:
        capture_files = [
            f for f in all_files if args.start <= f.stem <= args.end
        ]
        label = f"{args.start}_to_{args.end}"

    if not capture_files:
        log.error(f"No capture files found for selection '{label}'.")
        log.info(f"Available: {[f.name for f in all_files]}")
        return 1

    log.info(f"Using {len(capture_files)} capture file(s): {[f.name for f in capture_files]}")

    # ------------------------------------------------------------------
    # Merge + run
    # ------------------------------------------------------------------
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    safe_label = label.replace("/", "-")
    merged = out_dir / f"merged_{args.run_id}_{safe_label}.jsonl"
    fills  = out_dir / f"fills_{args.run_id}_{safe_label}.jsonl"
    exits  = out_dir / f"exits_{args.run_id}_{safe_label}.jsonl"

    if len(capture_files) == 1 and not args.use_all:
        # Skip merge for a single file — just link directly
        log.info("Single file — skipping merge step.")
        merged = capture_files[0]
        cleanup_merged = False
    else:
        log.info("Merging capture files...")
        _merge_captures(capture_files, merged)
        cleanup_merged = True

    log.info("Running backtest...")
    metrics = run_backtest(
        capture_path=merged,
        env_overrides={**get_fixed_env(), **params},
        fills_path=fills,
        exits_path=exits,
        speed=0.0,
    )

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    if cleanup_merged:
        merged.unlink(missing_ok=True)

    if not args.save_fills:
        fills.unlink(missing_ok=True)
        exits.unlink(missing_ok=True)
    else:
        log.info(f"Fills : {fills}")
        log.info(f"Exits : {exits}")

    # ------------------------------------------------------------------
    # Print results
    # ------------------------------------------------------------------
    _print_results(args.run_id, label, params, metrics)

    return 0 if not metrics.get("error") else 1


if __name__ == "__main__":
    sys.exit(main())
