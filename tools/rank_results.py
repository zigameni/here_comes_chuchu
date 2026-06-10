#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path

def main():
    parser = argparse.ArgumentParser(description="Rank optimization results and output the top valid runs.")
    parser.add_argument("--input", type=str, required=True, help="Input optimization_results.csv path")
    parser.add_argument("--output", type=str, required=True, help="Output best_results.csv path")
    parser.add_argument("--top", type=int, default=20, help="Number of top results to keep")
    
    args = parser.parse_args()
    
    input_csv = Path(args.input)
    output_csv = Path(args.output)
    
    if not input_csv.exists():
        print(f"Error: Input file {input_csv} does not exist.")
        return
        
    with open(input_csv, "r") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
        
    valid_rows = [r for r in rows if r.get("passed_validation") == "True"]
    
    def sort_key(r):
        return (
            float(r.get("val_sharpe", 0) or 0),
            float(r.get("val_net_pnl", 0) or 0),
            -float(r.get("val_drawdown", 0) or 0)
        )
        
    valid_rows.sort(key=sort_key, reverse=True)
    top_rows = valid_rows[:args.top]
    
    if not top_rows:
        print("No valid runs found. Output file will be empty but with headers.")
        
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(top_rows)
        
    print(f"Wrote {len(top_rows)} top results to {output_csv}")

if __name__ == "__main__":
    main()
