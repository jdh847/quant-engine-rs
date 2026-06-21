#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

cargo run -- readiness \
  --config config/bot.toml \
  --output-dir outputs_rust/readiness \
  --train-ratio 0.70 \
  --min-history-days 252 \
  --min-oos-days 60

echo "wrote: outputs_rust/readiness/readiness_report.json"
