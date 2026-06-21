#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

engine_cmd=(cargo run --)
if [ -n "${QUANT_ENGINE_BIN:-}" ]; then
  engine_cmd=("$QUANT_ENGINE_BIN")
elif [ -x "/tmp/quant_engine_rs_target_check/debug/quant-engine-rs" ]; then
  engine_cmd=("/tmp/quant_engine_rs_target_check/debug/quant-engine-rs")
elif [ -x "target/debug/quant-engine-rs" ]; then
  engine_cmd=("target/debug/quant-engine-rs")
fi

python3 scripts/refresh_real_historical_data.py \
  --markets US \
  --us-universe-file scripts/us_universe_template.csv \
  --us-max-symbols 20 \
  --output-dir data_real_us \
  --config-output config/bot_us.toml \
  --force

"${engine_cmd[@]}" readiness \
  --config config/bot_us.toml \
  --output-dir outputs_rust/readiness_us \
  --train-ratio 0.70 \
  --min-history-days 252 \
  --min-oos-days 60

"${engine_cmd[@]}" research \
  --config config/bot_us.toml \
  --output-dir outputs_rust/research_us \
  --markets US \
  --strategy-plugins layered_multi_factor,momentum_guard \
  --portfolio-methods risk_parity,hrp

decomp_engine_args=()
regime_engine_args=()
decay_engine_args=()
if [ "${engine_cmd[0]}" != "cargo" ]; then
  decomp_engine_args=(--engine-bin "${engine_cmd[0]}")
  regime_engine_args=(--engine-bin "${engine_cmd[0]}")
  decay_engine_args=(--engine-bin "${engine_cmd[0]}")
fi

python3 scripts/us_factor_decomposition.py \
  --config config/bot_us.toml \
  --output-dir outputs_rust/research_us_factor_decomp \
  "${decomp_engine_args[@]}"

python3 scripts/us_regime_split.py \
  --config config/bot_us.toml \
  --output-dir outputs_rust/research_us_regime_split \
  "${regime_engine_args[@]}"

python3 scripts/us_factor_decay.py \
  --config config/bot_us.toml \
  --output-dir outputs_rust/research_us_factor_decay \
  "${decay_engine_args[@]}"

echo "US-focused refresh/readiness/research/decomposition/regime-split/factor-decay completed."
