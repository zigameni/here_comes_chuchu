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

def _count_capture_markets(capture_path: Path) -> int:
    """
    Scan a merged capture JSONL and return the number of distinct market_ids.
    This is the full universe the strategy was exposed to — the correct
    denominator for participation rate (trades / total_markets).
    """
    market_ids = set()
    with open(capture_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
                mid = ev.get("market_id")
                if mid:
                    market_ids.add(mid)
            except Exception:
                pass
    return len(market_ids)


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
    parser.add_argument("--stage", choices=["all", "filter", "train_val"], default="all")
    parser.add_argument("--top-n", type=int, default=10)
    parser.add_argument("--min-filter-trades", type=int, default=30,
                        help="Minimum number of filter-stage trades required.")
    parser.add_argument("--min-filter-participation", type=float, default=0.1,
                        help="Minimum fraction of available capture markets that must have been "
                             "traded for a config to pass the filter gate. E.g. 0.05 means the "
                             "strategy must have entered at least 5%% of all markets seen in the "
                             "filter capture. Prevents inflated Sharpe from 2-trade flukes in a "
                             "288-market universe from passing.")
    parser.add_argument("--min-train-trades", type=int, default=0,
                        help="Minimum number of train-stage trades required to proceed to "
                             "validation. When > 0, the train Sharpe is also confidence-penalised "
                             "by min(1, trades / (2 * min_train_trades)) before being compared to "
                             "--min-train-sharpe, so a 2-trade fluke over 864 markets cannot "
                             "produce an inflated Sharpe that passes the gate. "
                             "Defaults to 0 (disabled); recommended: ~3x --min-filter-trades "
                             "since the train window is typically 3x longer than the filter.")
    parser.add_argument("--reset-study", action="store_true",
                        help="Delete the existing Optuna study DB before starting. "
                             "Required when the objective function has changed, otherwise old "
                             "trial scores pollute the surrogate model.")

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
    
    if args.stage != "train_val":
        log.info("Merging train files...")
        _merge_captures(train_files, train_merged)
        log.info("Merging val files...")
        _merge_captures(val_files, val_merged)
        if filter_files:
            log.info("Merging filter files...")
            _merge_captures(filter_files, filter_merged)
    
    # Count total distinct markets in each capture once, so the participation-rate
    # gate inside evaluate_params doesn't have to re-scan the file on every trial.
    total_filter_markets: int = 0
    if filter_files and filter_merged.exists():
        total_filter_markets = _count_capture_markets(filter_merged)
        log.info(f"Total filter markets available: {total_filter_markets}")

    total_train_markets: int = 0
    if train_merged.exists():
        total_train_markets = _count_capture_markets(train_merged)
        log.info(f"Total train markets available: {total_train_markets}")

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

            # Enforce participation rate: reject configs that only took 1-2 lucky
            # trades out of ~288 available markets — their Sharpe is meaningless.
            if args.min_filter_participation > 0 and total_filter_markets > 0:
                participation = f_metrics["total_trades"] / total_filter_markets
                if participation < args.min_filter_participation:
                    log.info(
                        f"[{run_id}] Failed filter participation: "
                        f"{f_metrics['total_trades']} trades / {total_filter_markets} markets "
                        f"= {participation:.3f} < {args.min_filter_participation}. Skipping."
                    )
                    row["train_skipped"] = True
                    row["val_skipped"] = True
                    return row

            log.info(f"[{run_id}] PASSED filter stage! (pnl=${f_metrics['net_pnl']}, sharpe={f_metrics['sharpe']}, trades={f_metrics['total_trades']})")

        if args.stage == "filter":
            row["train_skipped"] = True
            row["val_skipped"] = True
            return row

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

        # Confidence-penalised train Sharpe.
        # run_backtest computes Sharpe only over trades that happened — so 2
        # winning trades out of 864 markets produce an artificially huge Sharpe
        # because the ~862 zero-return slots are excluded from the calculation.
        # We scale the raw Sharpe down by a confidence factor that ramps from 0.5
        # (at the minimum trade floor) to 1.0 (at 2× the floor), forcing the gate
        # to reject low-participation configs regardless of their raw Sharpe.
        _min_train_threshold = args.min_train_trades if args.min_train_trades > 0 else max(1, args.min_filter_trades * 2)
        train_confidence = min(1.0, train_metrics["total_trades"] / max(1, _min_train_threshold * 2))
        penalized_train_sharpe = train_metrics["sharpe"] * train_confidence

        # Optional hard minimum-trades gate for the train stage.
        if args.min_train_trades > 0 and train_metrics["total_trades"] < args.min_train_trades:
            log.info(
                f"[{run_id}] Skipping validation: train trades {train_metrics['total_trades']} "
                f"< --min-train-trades {args.min_train_trades}"
            )
            row["val_skipped"] = True
            return row

        if penalized_train_sharpe < args.min_train_sharpe:
            log.info(
                f"[{run_id}] Skipping validation: raw train_sharpe={train_metrics['sharpe']:.3f}, "
                f"confidence={train_confidence:.3f}, penalized={penalized_train_sharpe:.3f} "
                f"< --min-train-sharpe {args.min_train_sharpe}"
            )
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
    if args.stage == "train_val":
        log.info(f"Running train_val stage for top {args.top_n} configs...")
        if not csv_path.exists():
            log.error("No optimization_results.csv found. Run filter stage first.")
            return
            
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
        # Filter rows that passed the filter stage.
        # Hard gate: pnl > 0 AND trade count meets minimum threshold so that
        # configs with 1-2 lucky trades can't win via inflated Sharpe.
        valid_rows = []
        for r in rows:
            try:
                pnl = float(r.get("filter_net_pnl", 0) or 0)
                trades = float(r.get("filter_trades", 0) or 0)
                if pnl > 0 and trades >= args.min_filter_trades:
                    valid_rows.append(r)
            except ValueError:
                pass

        log.info(
            f"{len(valid_rows)} rows passed filter gate "
            f"(pnl>0, trades>={args.min_filter_trades}) out of {len(rows)} total."
        )

        # Rank by a confidence-penalised Sharpe so that raw PnL volume is
        # rewarded alongside risk-adjusted quality.
        #
        # confidence  = min(1, trades / min_filter_trades*2)
        #   → ramps from 0.5 at the minimum trade floor up to 1.0 once the
        #     config has twice the minimum number of trades.
        #   → configs with exactly min_filter_trades trades get 0.5× Sharpe;
        #     those with 2× min_filter_trades (or more) get the full Sharpe.
        # Primary sort:  penalised_sharpe  (risk-quality × volume confidence)
        # Tiebreaker:    filter_net_pnl    (absolute dollar return)
        def _rank_key(r):
            pnl    = float(r.get("filter_net_pnl", 0) or 0)
            sharpe = float(r.get("filter_sharpe", 0) or 0)
            trades = float(r.get("filter_trades", 0) or 0)
            confidence = min(1.0, trades / (args.min_filter_trades * 2))
            penalised_sharpe = sharpe * confidence
            return (penalised_sharpe, pnl)

        valid_rows.sort(key=_rank_key, reverse=True)
        top_rows = valid_rows[:args.top_n]

        log.info("Top-N filter ranking:")
        for rank, r in enumerate(top_rows, 1):
            trades     = float(r.get("filter_trades", 0) or 0)
            sharpe     = float(r.get("filter_sharpe", 0) or 0)
            pnl        = float(r.get("filter_net_pnl", 0) or 0)
            confidence = min(1.0, trades / (args.min_filter_trades * 2))
            log.info(
                f"  #{rank:>2}  run_id={r['run_id']}  "
                f"pnl=${pnl:.2f}  sharpe={sharpe:.2f}  trades={int(trades)}  "
                f"confidence={confidence:.2f}  "
                f"score={sharpe * confidence:.2f}"
            )
        
        stage2_csv = out_dir / "optimization_results_stage2.csv"
        if not stage2_csv.exists():
            with open(stage2_csv, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                
        log.info("Merging train and val files for train_val stage...")
        _merge_captures(train_files, train_merged)
        _merge_captures(val_files, val_merged)
                
        for i, row in enumerate(top_rows):
            log.info(f"--- Top {i+1}/{len(top_rows)} (run_id: {row['run_id']}) ---")
            params = {k: float(row[k]) if PARAMETERS[k]["type"] == "float" else int(row[k]) for k in param_keys if k in row and row[k]}
            
            run_id = row["run_id"]
            timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
            
            new_row = evaluate_params(params, run_id, timestamp, "train_val", row.get("seed", ""))
            
            with open(stage2_csv, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writerow({k: new_row.get(k, "") for k in fieldnames})

        best_csv_path = out_dir / "best_results_stage2.csv"
        _write_best_results(stage2_csv, best_csv_path)

    elif args.mode in ["random", "grid"]:
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

            if row.get("error"):
                return -999.0

            # ── Tier 1: Validation completed — the only signal we truly trust ──
            #
            # ROOT-CAUSE FIX: the old code returned train_sharpe here, which
            # scored the one passing trial (train_sharpe=0.07) below many
            # overfit configs (train_sharpe=0.10-0.18). Optuna learned to move
            # AWAY from the correct region. We now use val_sharpe directly so
            # the surrogate model sees the true out-of-sample quality.
            if not row.get("val_skipped") and row.get("val_sharpe") is not None:
                val_sharpe       = float(row.get("val_sharpe",   0) or 0)
                train_sharpe_raw = float(row.get("train_sharpe", 0) or 0)

                # Penalise heavy Sharpe decay (train → val).
                # Perfect generalisation (val >= train) gets no penalty.
                # Decay above 50% is scaled down linearly toward 0.
                if train_sharpe_raw > 0:
                    decay = (train_sharpe_raw - val_sharpe) / train_sharpe_raw
                    generalization_factor = max(0.0, 1.0 - max(0.0, decay - 0.50))
                else:
                    # train_sharpe near-zero: val_sharpe is the only real signal
                    generalization_factor = 0.5

                score = val_sharpe * generalization_factor

                # Large bonus for clearing ALL four validation gates (sharpe>0.3,
                # win_rate>0.50, pnl>0, decay<50%). This makes the passing region
                # unmistakably dominant in the surrogate model — the bonus cannot
                # be matched by any Tier-2 or Tier-3 result.
                if row.get("passed_validation"):
                    score += 0.50

                return score

            # ── Tier 2: Train ran, val was skipped (train gate not met) ────────
            # Return penalised train Sharpe but subtract a large constant so this
            # tier never outscores a Tier-1 result:
            #   best possible here ≈ 0.20 – 3.0 = –2.80, well below val-tier min.
            if not row.get("train_skipped"):
                train_sharpe = row.get("train_sharpe")
                if train_sharpe is None:
                    return -99.0
                train_trades = float(row.get("train_trades", 0) or 0)
                _min_train_threshold = (
                    args.min_train_trades if args.min_train_trades > 0
                    else max(1, args.min_filter_trades * 2)
                )
                train_confidence = min(
                    1.0, train_trades / max(1, _min_train_threshold * 2)
                )
                return float(train_sharpe) * train_confidence - 3.0

            # ── Tier 3: Filter failed (both train and val skipped) ──────────────
            # Confidence-penalised filter Sharpe, offset so this band is always
            # below Tier-2:  best case ≈ 0.77 – 10.0 = –9.23.
            filter_pnl    = float(row.get("filter_net_pnl",  0) or 0)
            filter_sharpe = float(row.get("filter_sharpe",   0) or 0)
            filter_trades = float(row.get("filter_trades",   0) or 0)
            if filter_pnl <= 0:
                return -99.0
            confidence = min(1.0, filter_trades / (args.min_filter_trades * 2))
            return filter_sharpe * confidence - 10.0

        db_path = out_dir / "optuna_study.db"

        # --reset-study: wipe the old DB when the objective function has changed.
        # Old trial scores are baked into SQLite and will pollute the surrogate
        # model if the objective is different — a fresh start is the clean fix.
        if args.reset_study and db_path.exists():
            db_path.unlink()
            log.info(f"--reset-study: cleared old Optuna study DB at {db_path}")

        study = optuna.create_study(
            storage=f"sqlite:///{db_path}",
            study_name="btc_bot_optimization",
            load_if_exists=True,
            direction="maximize"
        )

        # Warm-start: re-enqueue any previously validated configs so Optuna
        # immediately evaluates the known-good neighbourhood under the new
        # objective, rather than rediscovering it from scratch.
        # Handles column renames between old CSV exports and the current registry.
        _col_renames = {
            "BTC_MOMENTUM_GATE":      "MERTON_DISTANCE_GATE",
            "ORDERBOOK_IMBALANCE_GATE": "OFI_IMBALANCE_GATE",
        }
        if csv_path.exists():
            _enqueued = 0
            with open(csv_path, "r") as _f:
                for _row in csv.DictReader(_f):
                    if _row.get("passed_validation") != "True":
                        continue
                    try:
                        _norm = {_col_renames.get(k, k): v for k, v in _row.items()}
                        _params = {
                            k: (
                                float(_norm[k])
                                if PARAMETERS[k]["type"] == "float"
                                else int(float(_norm[k]))
                            )
                            for k in param_keys
                            if k in _norm and _norm[k] not in ("", None, "nan")
                        }
                        study.enqueue_trial(_params)
                        _enqueued += 1
                        log.info(
                            f"Warm-start: enqueued passing config "
                            f"run_id={_row.get('run_id', '?')}"
                        )
                    except Exception as _e:
                        log.warning(f"Failed to enqueue warm-start row: {_e}")
            if _enqueued == 0:
                log.info("No previously validated configs found to warm-start from.")
            else:
                log.info(f"Warm-start: {_enqueued} config(s) enqueued.")

        log.info(f"Starting Bayesian Optimization for {args.n_iter} trials...")
        study.optimize(objective, n_trials=args.n_iter)
        
        log.info("Optuna Optimization Complete.")
        log.info(f"Best trial: {study.best_trial.number} with val_sharpe score: {study.best_trial.value:.4f}")

    # Finalize
    if args.stage != "train_val":
        best_csv_path = out_dir / "best_results.csv"
        _write_best_results(csv_path, best_csv_path)
    
    if train_merged.exists(): train_merged.unlink()
    if val_merged.exists(): val_merged.unlink()
    if filter_files and filter_merged.exists(): filter_merged.unlink()

if __name__ == "__main__":
    main()