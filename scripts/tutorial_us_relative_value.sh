#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/pqbot_relative_value}"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}"

engine_cmd=(cargo run --quiet --bin private_quant_bot --)
if [ -n "${QUANT_ENGINE_BIN:-}" ]; then
  engine_cmd=("$QUANT_ENGINE_BIN")
fi

OUT_DIR="${1:-outputs_rust/run_us_relative_value}"

"${engine_cmd[@]}" run \
  --config config/bot_us_relative_value.toml \
  --output-dir "$OUT_DIR"

"${engine_cmd[@]}" dashboard \
  --output-dir "$OUT_DIR"

echo "industry_relative_reversion candidate run completed -> $OUT_DIR"
