#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/pqbot_factoric}"

engine_cmd=(cargo run --quiet --bin private_quant_bot --)
if [ -n "${QUANT_ENGINE_BIN:-}" ]; then
  engine_cmd=("$QUANT_ENGINE_BIN")
fi

mkdir -p outputs_rust

output_jsonl="outputs_rust/factor_ic_us_long.jsonl"
output_text="outputs_rust/factor_ic_us_long.txt"

"${engine_cmd[@]}" factor-ic \
  --config config/bot_us_long.toml \
  --output-path "$output_jsonl" \
  --market US | tee "$output_text"

python3 scripts/us_factor_ic_diagnostics.py \
  --config config/bot_us_long.toml \
  --output-dir outputs_rust/research_us_long_factor_ic

echo "US long-sample factor IC completed:"
echo "  $output_jsonl"
echo "  $output_text"
echo "  outputs_rust/research_us_long_factor_ic/factor_ic_daily_us.csv"
echo "  outputs_rust/research_us_long_factor_ic/factor_ic_rolling_us.csv"
echo "  outputs_rust/research_us_long_factor_ic/factor_ic_regime_us.csv"
echo "  outputs_rust/research_us_long_factor_ic/factor_ic_summary_us.md"
