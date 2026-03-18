#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_BASE="${ROOT}/outputs_rust_compare_example"
BASELINE="${OUT_BASE}/baseline"
CANDIDATE="${OUT_BASE}/candidate"
REPORT="${OUT_BASE}/report"

rm -rf "${OUT_BASE}"
mkdir -p "${BASELINE}" "${CANDIDATE}" "${REPORT}"

cat > "${BASELINE}/summary.txt" <<'EOF'
total_return_pct: 4.20
annualized_vol_pct: 11.30
max_drawdown_pct: -5.80
sharpe: 0.91
turnover_pct: 18.00
EOF

cat > "${CANDIDATE}/summary.txt" <<'EOF'
total_return_pct: 6.05
annualized_vol_pct: 10.40
max_drawdown_pct: -4.10
sharpe: 1.18
turnover_pct: 14.50
EOF

cat > "${BASELINE}/audit_snapshot.json" <<'EOF'
{
  "command": "run",
  "paper_only": true,
  "base_currency": "USD",
  "portfolio_method": "risk_parity",
  "strategy_plugin": "baseline_alpha",
  "stats": {
    "market_count": 3,
    "tradable_count": 36,
    "blocked_count": 0,
    "filtered_count": 2
  },
  "markets": {
    "US": { "data_sha256": "us-base", "industry_sha256": "us-ind-base" },
    "JP": { "data_sha256": "jp-base" }
  }
}
EOF

cat > "${CANDIDATE}/audit_snapshot.json" <<'EOF'
{
  "command": "run",
  "paper_only": true,
  "base_currency": "USD",
  "portfolio_method": "hrp",
  "strategy_plugin": "candidate_alpha",
  "stats": {
    "market_count": 3,
    "tradable_count": 42,
    "blocked_count": 0,
    "filtered_count": 1
  },
  "markets": {
    "US": { "data_sha256": "us-cand", "industry_sha256": "us-ind-cand" },
    "JP": { "data_sha256": "jp-base" }
  }
}
EOF

cat > "${BASELINE}/data_quality_report.csv" <<'EOF'
market,status,duplicate_rows,bad_close_rows,bad_volume_rows,return_outliers,large_gaps,non_trading_day_rows
US,PASS,0,0,0,1,0,0
JP,WARN,0,0,0,0,1,0
EOF

cat > "${CANDIDATE}/data_quality_report.csv" <<'EOF'
market,status,duplicate_rows,bad_close_rows,bad_volume_rows,return_outliers,large_gaps,non_trading_day_rows
US,PASS,0,0,0,0,0,0
JP,PASS,0,0,0,0,0,0
EOF

cd "${ROOT}"
cargo run --quiet --bin compare -- \
  --baseline-dir "${BASELINE}" \
  --candidate-dir "${CANDIDATE}" \
  --output-dir "${REPORT}"

echo "Open ${REPORT}/compare_report.html"
