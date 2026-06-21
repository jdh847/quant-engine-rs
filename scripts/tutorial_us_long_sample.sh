#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

engine_cmd=(cargo run --)
compare_cmd=(cargo run --quiet --bin compare --)
if [ -n "${QUANT_ENGINE_BIN:-}" ]; then
  engine_cmd=("$QUANT_ENGINE_BIN")
elif [ -x "/tmp/quant_engine_rs_target_check/debug/quant-engine-rs" ]; then
  engine_cmd=("/tmp/quant_engine_rs_target_check/debug/quant-engine-rs")
elif [ -x "target/debug/quant-engine-rs" ]; then
  engine_cmd=("target/debug/quant-engine-rs")
fi

if [ -n "${QUANT_COMPARE_BIN:-}" ]; then
  compare_cmd=("$QUANT_COMPARE_BIN")
elif [ -x "/tmp/quant_engine_rs_target_check/debug/compare" ]; then
  compare_cmd=("/tmp/quant_engine_rs_target_check/debug/compare")
elif [ -x "target/debug/compare" ]; then
  compare_cmd=("target/debug/compare")
fi

decomp_engine_args=()
regime_engine_args=()
decay_engine_args=()
if [ "${engine_cmd[0]}" != "cargo" ]; then
  decomp_engine_args=(--engine-bin "${engine_cmd[0]}")
  regime_engine_args=(--engine-bin "${engine_cmd[0]}")
  decay_engine_args=(--engine-bin "${engine_cmd[0]}")
fi

python3 scripts/refresh_real_historical_data.py \
  --markets US \
  --us-universe-file scripts/us_universe_extended.csv \
  --us-max-symbols 50 \
  --start-date 2018-01-02 \
  --end-date 2026-04-08 \
  --output-dir data_real_us_long \
  --config-output config/bot_us_long.toml \
  --force

"${engine_cmd[@]}" readiness \
  --config config/bot_us_long.toml \
  --output-dir outputs_rust/readiness_us_long \
  --train-ratio 0.70 \
  --min-history-days 252 \
  --min-oos-days 80

"${engine_cmd[@]}" run \
  --config config/bot_us_long.toml \
  --output-dir outputs_rust/run_us_long_baseline

"${engine_cmd[@]}" research \
  --config config/bot_us_long.toml \
  --output-dir outputs_rust/research_us_long \
  --markets US \
  --short-windows 3,4 \
  --long-windows 7,9 \
  --vol-windows 5 \
  --top-ns 1,2 \
  --min-momentums=-0.01,0.0 \
  --strategy-plugins layered_multi_factor,momentum_guard \
  --portfolio-methods risk_parity,hrp

python3 scripts/us_route_defaults.py \
  --leaderboard outputs_rust/research_us_long/research_leaderboard.csv \
  --config-in config/bot_us_long.toml \
  --config-out config/bot_us_long_tuned.toml \
  --summary-out outputs_rust/research_us_long/route_update_summary.txt

"${engine_cmd[@]}" run \
  --config config/bot_us_long_tuned.toml \
  --output-dir outputs_rust/run_us_long_tuned

"${compare_cmd[@]}" \
  --baseline-dir outputs_rust/run_us_long_baseline \
  --candidate-dir outputs_rust/run_us_long_tuned \
  --output-dir outputs_rust/compare_us_long_route

python3 scripts/us_route_decision.py \
  --baseline-summary outputs_rust/run_us_long_baseline/summary.txt \
  --candidate-summary outputs_rust/run_us_long_tuned/summary.txt \
  --candidate-config config/bot_us_long_tuned.toml \
  --target-configs config/bot.toml,config/bot_us.toml \
  --output outputs_rust/compare_us_long_route/route_decision_us.txt

python3 scripts/us_factor_decomposition.py \
  --config config/bot_us_long.toml \
  --output-dir outputs_rust/research_us_long_factor_decomp \
  "${decomp_engine_args[@]}"

python3 scripts/us_regime_split.py \
  --config config/bot_us_long.toml \
  --output-dir outputs_rust/research_us_long_regime_split \
  "${regime_engine_args[@]}"

python3 scripts/us_factor_decay.py \
  --config config/bot_us_long.toml \
  --output-dir outputs_rust/research_us_long_factor_decay \
  "${decay_engine_args[@]}"

echo "US long-sample pipeline completed:"
echo "  outputs_rust/readiness_us_long"
echo "  outputs_rust/research_us_long"
echo "  outputs_rust/run_us_long_baseline"
echo "  outputs_rust/run_us_long_tuned"
echo "  outputs_rust/compare_us_long_route/compare_report.html"
echo "  outputs_rust/compare_us_long_route/route_decision_us.txt"
echo "  outputs_rust/research_us_long_factor_decomp/factor_decomposition_us.csv"
echo "  outputs_rust/research_us_long_regime_split/regime_split_us.csv"
echo "  outputs_rust/research_us_long_factor_decay/factor_decay_us.csv"
