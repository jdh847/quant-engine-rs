#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

engine_args=()
if [ -n "${QUANT_ENGINE_BIN:-}" ]; then
  engine_args=(--engine-bin "$QUANT_ENGINE_BIN")
elif [ -x "/tmp/quant_engine_rs_target_check/debug/quant-engine-rs" ]; then
  engine_args=(--engine-bin "/tmp/quant_engine_rs_target_check/debug/quant-engine-rs")
elif [ -x "target/debug/quant-engine-rs" ]; then
  engine_args=(--engine-bin "target/debug/quant-engine-rs")
fi

if [ ! -f "config/bot_us.toml" ]; then
  python3 scripts/refresh_real_historical_data.py \
    --markets US \
    --us-universe-file scripts/us_universe_template.csv \
    --us-max-symbols 20 \
    --output-dir data_real_us \
    --config-output config/bot_us.toml \
    --force
fi

python3 scripts/us_factor_decomposition.py \
  --config config/bot_us.toml \
  --output-dir outputs_rust/research_us_factor_decomp \
  "${engine_args[@]}"

echo "US factor decomposition completed:"
echo "  outputs_rust/research_us_factor_decomp/factor_decomposition_us.csv"
echo "  outputs_rust/research_us_factor_decomp/factor_decomposition_us.md"
