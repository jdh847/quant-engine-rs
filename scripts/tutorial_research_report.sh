#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

cargo run --bin research_report -- \
  --config config/bot.toml \
  --output-dir outputs_rust/research_report \
  --train-days 10 \
  --test-days 4 \
  --short-windows 3 \
  --long-windows 7 \
  --vol-windows 5 \
  --top-ns 1 \
  --min-momentums 0.001 \
  --portfolio-methods risk_parity \
  --factor-decay-horizons 1,3,5 \
  --regime-vol-window 5 \
  --regime-fast-window 3 \
  --regime-slow-window 7

echo "wrote: outputs_rust/research_report/research_report.md"
echo "wrote: outputs_rust/research_report/research_report.html"
echo "wrote: outputs_rust/research_report/research_report.json"
