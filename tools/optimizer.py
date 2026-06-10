#!/usr/bin/env python3
import argparse
import csv
import datetime
import json
import logging
import os
import random
import sys
import uuid
from pathlib import Path

# Ensure we can import from the project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.parameter_registry import PARAMETERS, get_fixed_env, sample_random, grid_points, suggest_optuna
from tools.replay_engine import run_backtest

try:
    import optuna
    optuna.logging.set_verbosity(optuna.logging.WARNING)
except ImportError:
    optuna = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("optimizer")

def _merge_captures(files: list[Path], output_path: Path):
    events = []
    for f in files:
        with open(f, "r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh):
                line = line.strip()
                if not line: continue
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

def _write_best_results(input_csv: Path, output_csv: Path, top_n: int = 20):
    if not input_csv.exists():
        return
    with open(input_csv, "r") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    valid_rows = [r for r in rows if r.get("passed_validation") == "True"]
    
    def sort_key(r):
        return (
            float(r.get("val_sharpe", 0) or 0),
            float(r.get("val_net_pnl", 0) or 0),
            -float(r.get("val_drawdown", 0) or 0)
        )
        
    valid_rows.sort(key=sort_key, reverse=True)
    top_rows = valid_rows[:top_n]
    
    if not top_rows:
        return
        
    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(top_rows)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["random", "grid", "bayesian"], required=True)
    parser.add_argument("--n-iter", type=int, default=50)
    parser.add_argument("--filter-start", type=str, default="")
    parser.add_argument("--filter-end", type=str, default="")
    parser.add_argument("--train-start", type=str, required=True)
    parser.add_argument("--train-end", type=str, required=True)
    parser.add_argument("--val-start", type=str, required=True)
    parser.add_argument("--val-end", type=str, required=True)
    parser.add_argument("--captures-dir", type=str, required=True)
    parser.add_argument("--output-dir", type=str, required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--min-train-sharpe", type=float, default=0.0)

    args = parser.parse_args()

    if args.mode == "bayesian" and optuna is None:
        log.error("Optuna is required for bayesian mode. Please 'pip install optuna'.")
        return

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    cap_dir = Path(args.captures_dir)
    
    def date_from_filename(p: Path):
        return p.stem
    
    all_files = sorted([f for f in cap_dir.glob("*.jsonl")])
    train_files = [f for f in all_files if args.train_start <= date_from_filename(f) <= args.train_end]
    val_files = [f for f in all_files if args.val_start <= date_from_filename(f) <= args.val_end]
    filter_files = []
    if args.filter_start and args.filter_end:
        filter_files = [f for f in all_files if args.filter_start <= date_from_filename(f) <= args.filter_end]
    
    if not train_files:
        log.error("No train files found.")
        return
    if not val_files:
        log.error("No val files found.")
        return
        
    log.info(f"Train files: {[f.name for f in train_files]}")
    log.info(f"Val files: {[f.name for f in val_files]}")
    if filter_files:
        log.info(f"Filter files: {[f.name for f in filter_files]}")
    
    train_merged = out_dir / "train_merged.jsonl"
    val_merged = out_dir / "val_merged.jsonl"
    filter_merged = out_dir / "filter_merged.jsonl"
    
    log.info("Merging train files...")
    _merge_captures(train_files, train_merged)
    log.info("Merging val files...")
    _merge_captures(val_files, val_merged)
    if filter_files:
        log.info("Merging filter files...")
        _merge_captures(filter_files, filter_merged)
    
    rng = random.Random(args.seed)
    
    csv_path = out_dir / "optimization_results.csv"
    param_keys = list(PARAMETERS.keys())
    
    fieldnames = [
        "run_id", "timestamp", "mode", "seed"
    ] + param_keys + [
        "filter_net_pnl", "filter_roi", "filter_win_rate", "filter_sharpe", "filter_drawdown", "filter_trades", "train_skipped",
        "train_net_pnl", "train_roi", "train_win_rate", "train_sharpe", "train_drawdown", "train_trades",
        "val_net_pnl", "val_roi", "val_win_rate", "val_sharpe", "val_drawdown", "val_trades",
        "val_skipped", "passed_validation", "error"
    ]
    
    if not csv_path.exists():
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

    def evaluate_params(params, run_id, timestamp, mode, seed_val):
        row = {
            "run_id": run_id,
            "timestamp": timestamp,
            "mode": mode,
            "seed": seed_val,
            "train_skipped": False,
            "val_skipped": False,
            "passed_validation": False,
            "error": "",
            **params
        }

        # Stage 1: Filter
        if filter_files:
            log.info(f"[{run_id}] Stage 1: Running filter backtest...")
            f_fills = out_dir / f"filter_fills_{run_id}.jsonl"
            f_exits = out_dir / f"filter_exits_{run_id}.jsonl"
            
            f_metrics = run_backtest(
                capture_path=filter_merged,
                env_overrides={**get_fixed_env(), **params},
                fills_path=f_fills,
                exits_path=f_exits,
                speed=0.0
            )
            
            if f_fills.exists(): f_fills.unlink()
            if f_exits.exists(): f_exits.unlink()
            
            row.update({
                "filter_net_pnl": f_metrics["net_pnl"],
                "filter_roi": f_metrics["roi"],
                "filter_win_rate": f_metrics["win_rate"],
                "filter_sharpe": f_metrics["sharpe"],
                "filter_drawdown": f_metrics["max_drawdown"],
                "filter_trades": f_metrics["total_trades"]
            })
            
            if f_metrics["error"] is not None:
                log.warning(f"[{run_id}] Filter run failed: {f_metrics['error']}")
                row["error"] = f_metrics["error"]
                row["train_skipped"] = True
                row["val_skipped"] = True
                return row
                
            # If the strategy bleeds money on the worst day (net_pnl <= 0), skip.
            # Using net_pnl perfectly covers 0 trades (pnl=0), 1 trade that loses, and multiple trades.
            if f_metrics["net_pnl"] <= 0.0:
                log.info(f"[{run_id}] Failed filter stage (pnl=${f_metrics['net_pnl']}, trades={f_metrics['total_trades']}, sharpe={f_metrics['sharpe']}). Skipping train/val.")
                row["train_skipped"] = True
                row["val_skipped"] = True
                return row
            else:
                log.info(f"[{run_id}] PASSED filter stage! (pnl=${f_metrics['net_pnl']}, sharpe={f_metrics['sharpe']}, trades={f_metrics['total_trades']})")

        # Stage 2: Train
        log.info(f"[{run_id}] Stage 2: Running train backtest...")
        train_fills = out_dir / f"train_fills_{run_id}.jsonl"
        train_exits = out_dir / f"train_exits_{run_id}.jsonl"
        
        train_metrics = run_backtest(
            capture_path=train_merged,
            env_overrides={**get_fixed_env(), **params},
            fills_path=train_fills,
            exits_path=train_exits,
            speed=0.0
        )
        
        if train_fills.exists(): train_fills.unlink()
        if train_exits.exists(): train_exits.unlink()
        
        row.update({
            "train_net_pnl": train_metrics["net_pnl"],
            "train_roi": train_metrics["roi"],
            "train_win_rate": train_metrics["win_rate"],
            "train_sharpe": train_metrics["sharpe"],
            "train_drawdown": train_metrics["max_drawdown"],
            "train_trades": train_metrics["total_trades"]
        })
        
        if train_metrics["error"] is not None:
            log.warning(f"[{run_id}] Train run failed: {train_metrics['error']}")
            row["error"] = train_metrics["error"]
            row["val_skipped"] = True
            return row
            
        if train_metrics["sharpe"] < args.min_train_sharpe:
            log.info(f"[{run_id}] Skipping validation (train_sharpe={train_metrics['sharpe']} < {args.min_train_sharpe})")
            row["val_skipped"] = True
            return row

        # Stage 3: Val
        log.info(f"[{run_id}] Stage 3: Running val backtest...")
        val_fills = out_dir / f"val_fills_{run_id}.jsonl"
        val_exits = out_dir / f"val_exits_{run_id}.jsonl"
        
        val_metrics = run_backtest(
            capture_path=val_merged,
            env_overrides={**get_fixed_env(), **params},
            fills_path=val_fills,
            exits_path=val_exits,
            speed=0.0
        )
        
        if val_fills.exists(): val_fills.unlink()
        if val_exits.exists(): val_exits.unlink()
        
        row.update({
            "val_net_pnl": val_metrics["net_pnl"],
            "val_roi": val_metrics["roi"],
            "val_win_rate": val_metrics["win_rate"],
            "val_sharpe": val_metrics["sharpe"],
            "val_drawdown": val_metrics["max_drawdown"],
            "val_trades": val_metrics["total_trades"]
        })
        
        if val_metrics["error"] is not None:
            log.warning(f"[{run_id}] Val run failed: {val_metrics['error']}")
            row["error"] = val_metrics["error"]
            return row

        # Final Evaluation
        passed = True
        if val_metrics["sharpe"] <= 0.3: passed = False
        if val_metrics["win_rate"] <= 0.50: passed = False
        if val_metrics["net_pnl"] <= 0: passed = False
        
        if train_metrics["sharpe"] > 0:
            decay = (train_metrics["sharpe"] - val_metrics["sharpe"]) / train_metrics["sharpe"]
            if decay >= 0.50:
                passed = False
        else:
            passed = False
            
        row["passed_validation"] = passed
        log.info(f"[{run_id}] Validation passed: {passed}")
        
        return row

    def save_row(row):
        with open(csv_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow({k: row.get(k, "") for k in fieldnames})

    # Execution paths
    if args.mode in ["random", "grid"]:
        grid_iterator = grid_points() if args.mode == "grid" else None
        
        for i in range(args.n_iter):
            log.info(f"--- Iteration {i+1}/{args.n_iter} ---")
            if args.mode == "random":
                params = sample_random(rng)
            else:
                try:
                    params = next(grid_iterator)
                except StopIteration:
                    log.info("Grid search exhausted.")
                    break
                    
            run_id = str(uuid.uuid4())[:8]
            timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
            
            row = evaluate_params(params, run_id, timestamp, args.mode, args.seed if args.mode == "random" else "")
            save_row(row)

    elif args.mode == "bayesian":
        def objective(trial):
            run_id = str(uuid.uuid4())[:8]
            timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
            log.info(f"--- Optuna Trial {trial.number} ({run_id}) ---")
            
            params = suggest_optuna(trial)
            row = evaluate_params(params, run_id, timestamp, "bayesian", "")
            save_row(row)
            
            # What does Optuna optimize?
            # We want it to maximize Train Sharpe. If Train Sharpe wasn't reached, 
            # we give it Filter Sharpe (if available) so it can still learn from the failure.
            # If both failed/errored, return a very bad score.
            if row.get("error"):
                return -999.0
            
            if row.get("train_skipped"):
                return row.get("filter_sharpe", -99.0)
            
            return row.get("train_sharpe", -99.0)

        study = optuna.create_study(direction="maximize")
        log.info(f"Starting Bayesian Optimization for {args.n_iter} trials...")
        study.optimize(objective, n_trials=args.n_iter)
        
        log.info("Optuna Optimization Complete.")
        log.info(f"Best trial: {study.best_trial.number} with train_sharpe: {study.best_trial.value}")

    # Finalize
    best_csv_path = out_dir / "best_results.csv"
    _write_best_results(csv_path, best_csv_path)
    
    if train_merged.exists(): train_merged.unlink()
    if val_merged.exists(): val_merged.unlink()
    if filter_files and filter_merged.exists(): filter_merged.unlink()

if __name__ == "__main__":
    main()
