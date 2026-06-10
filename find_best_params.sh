# Activate the virtual environment
source venv/bin/activate

# Create the output directory
mkdir -p optimization_results

# Run the optimizer in the background (using nohup) so it won't die if you close the terminal
nohup python -m tools.optimizer \
  --mode bayesian \
  --n-iter 100 \
  --filter-start 2026-06-06 \
  --filter-end 2026-06-06 \
  --train-start 2026-06-05 \
  --train-end 2026-06-07 \
  --val-start 2026-06-08 \
  --val-end 2026-06-09 \
  --captures-dir captures/ \
  --output-dir optimization_results/ \
  > optimization_results/optimizer.log 2>&1 &
