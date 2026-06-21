#!/usr/bin/env bash
# B: Relative-value cost/turnover stress test.
#
# Runs momentum_guard (baseline) and industry_relative_reversion (candidate)
# on the US long sample at 1x / 2x / 5x transaction cost, then tabulates
# PnL / Sharpe / maxDD / trades so we can see the PnL-vs-cost decay curve and
# find the break-even cost for the high-turnover candidate.
#
# Cost is scaled on the per-market US override (commission_bps, slippage_bps),
# which is what the engine actually charges for US fills.
set -euo pipefail
cd "$(dirname "$0")/.."

BIN="${QUANT_ENGINE_BIN:-target/debug/private_quant_bot}"
OUT="outputs_rust/b_cost_stress"
# Generated configs MUST live inside the project tree: the data_file paths in
# the base configs are relative and resolve against the config's project root,
# so a /tmp config would look for /tmp/data_real_us_long and fail.
TMP="$OUT/_configs"
mkdir -p "$OUT" "$TMP"

# strategy_label : base_config
declare -a STRATS=(
  "momentum_guard:config/bot_us_long.toml"
  "relative_value:config/bot_us_relative_value.toml"
)
# cost_label : commission_bps : slippage_bps
declare -a COSTS=(
  "1x:0.8:2.0"
  "2x:1.6:4.0"
  "5x:4.0:10.0"
)

gen_cfg() {  # base_cfg comm slip outpath
  local base="$1" comm="$2" slip="$3" out="$4"
  # Only the markets.US override carries 0.8 / 2.0; global [execution] is 1.5/3.0.
  sed -e "s/^commission_bps = 0.8$/commission_bps = $comm/" \
      -e "s/^slippage_bps = 2.0$/slippage_bps = $slip/" \
      "$base" > "$out"
}

extract() {  # summary.txt (key=value%, no spaces) -> "pnl_ratio sharpe max_dd trades"
  local f="$1"
  awk -F= '
    function v(x){ gsub(/%/,"",x); return x }
    /^pnl_ratio=/    { pnl=v($2) }
    /^sharpe=/       { shp=v($2) }
    /^max_drawdown=/ { dd=v($2) }
    /^trades=/       { tr=v($2) }
    END { printf "%s %s %s %s", pnl, shp, dd, tr }
  ' "$f"
}

printf "%-16s %-5s %12s %10s %10s %10s\n" "strategy" "cost" "pnl_ratio" "sharpe" "max_dd" "trades"
echo "--------------------------------------------------------------------------"
RESULTS="$OUT/results.tsv"
: > "$RESULTS"
for s in "${STRATS[@]}"; do
  slabel="${s%%:*}"; sbase="${s##*:}"
  for c in "${COSTS[@]}"; do
    clabel="$(echo "$c" | cut -d: -f1)"
    comm="$(echo "$c" | cut -d: -f2)"
    slip="$(echo "$c" | cut -d: -f3)"
    cfg="$TMP/${slabel}_${clabel}.toml"
    rundir="$OUT/${slabel}_${clabel}"
    gen_cfg "$sbase" "$comm" "$slip" "$cfg"
    "$BIN" run --config "$cfg" --output-dir "$rundir" --skip-validate-data >/dev/null 2>&1
    metrics="$(extract "$rundir/summary.txt")"
    read -r pnl shp dd tr <<< "$metrics"
    printf "%-16s %-5s %12s %10s %10s %10s\n" "$slabel" "$clabel" "$pnl" "$shp" "$dd" "$tr"
    printf "%s\t%s\t%s\t%s\t%s\t%s\n" "$slabel" "$clabel" "$pnl" "$shp" "$dd" "$tr" >> "$RESULTS"
  done
done
echo "--------------------------------------------------------------------------"
echo "Wrote: $RESULTS"
