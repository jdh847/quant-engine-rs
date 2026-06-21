#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/pqbot_relative_value}"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}"

OUT_ROOT="${1:-outputs_rust/relative_value_evidence}"
CONFIG_DIR="$OUT_ROOT/configs"
mkdir -p "$CONFIG_DIR"

engine_cmd=(cargo run --quiet --bin private_quant_bot --)
compare_cmd=(cargo run --quiet --bin compare --)
if [ -n "${QUANT_ENGINE_BIN:-}" ]; then
  engine_cmd=("$QUANT_ENGINE_BIN")
fi
if [ -n "${QUANT_COMPARE_BIN:-}" ]; then
  compare_cmd=("$QUANT_COMPARE_BIN")
fi

MAIN_LONG="$CONFIG_DIR/bot_us_long.toml"
CAND_LONG="$CONFIG_DIR/bot_us_relative_value.toml"
MAIN_HIGH_COST="$CONFIG_DIR/bot_us_long_high_cost.toml"
CAND_HIGH_COST="$CONFIG_DIR/bot_us_relative_value_high_cost.toml"
MAIN_RECENT="$CONFIG_DIR/bot_us_long_recent.toml"
CAND_RECENT="$CONFIG_DIR/bot_us_relative_value_recent.toml"

cp config/bot_us_long.toml "$MAIN_LONG"
cp config/bot_us_relative_value.toml "$CAND_LONG"
cp config/bot_us_long.toml "$MAIN_HIGH_COST"
cp config/bot_us_relative_value.toml "$CAND_HIGH_COST"
cp config/bot_us_long.toml "$MAIN_RECENT"
cp config/bot_us_relative_value.toml "$CAND_RECENT"

patch_high_cost() {
  local path="$1"
  perl -0pi -e 's/commission_bps = 1\.5/commission_bps = 5.0/g; s/slippage_bps = 3\.0/slippage_bps = 10.0/g; s/commission_bps = 0\.8/commission_bps = 5.0/g; s/slippage_bps = 2\.0/slippage_bps = 10.0/g' "$path"
}

patch_recent_us_data() {
  local path="$1"
  perl -0pi -e 's#data_file = "data_real_us_long/us_equities\.csv"#data_file = "data_real_recent/us_equities.csv"#g; s#industry_file = "data_real_us_long/us_industries\.csv"#industry_file = "data_real_recent/us_industries.csv"#g' "$path"
}

patch_high_cost "$MAIN_HIGH_COST"
patch_high_cost "$CAND_HIGH_COST"
patch_recent_us_data "$MAIN_RECENT"
patch_recent_us_data "$CAND_RECENT"

run_case() {
  local label="$1"
  local config="$2"
  local out_dir="$OUT_ROOT/$label"
  "${engine_cmd[@]}" run --config "$config" --output-dir "$out_dir"
}

compare_case() {
  local label="$1"
  local baseline="$2"
  local candidate="$3"
  "${compare_cmd[@]}" \
    --baseline-dir "$OUT_ROOT/$baseline" \
    --candidate-dir "$OUT_ROOT/$candidate" \
    --output-dir "$OUT_ROOT/compare_$label"
}

run_case long_main "$MAIN_LONG"
run_case long_candidate "$CAND_LONG"
compare_case long long_main long_candidate

run_case high_cost_main "$MAIN_HIGH_COST"
run_case high_cost_candidate "$CAND_HIGH_COST"
compare_case high_cost high_cost_main high_cost_candidate

run_case recent_main "$MAIN_RECENT"
run_case recent_candidate "$CAND_RECENT"
compare_case recent recent_main recent_candidate

echo "US relative-value evidence completed -> $OUT_ROOT"
