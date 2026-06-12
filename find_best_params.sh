#!/bin/bash
source venv/bin/activate
mkdir -p optimization_results

nohup python -m tools.optimizer \
  --stage all \
  --mode bayesian \
  --n-iter 150 \
  --filter-start 2026-06-06 \
  --filter-end 2026-06-06 \
  --train-start 2026-06-05 \
  --train-end 2026-06-07 \
  --val-start 2026-06-08 \
  --val-end 2026-06-09 \
  --captures-dir captures/ \
  --output-dir optimization_results/ \
  > optimization_results/optimizer.log 2>&1 &