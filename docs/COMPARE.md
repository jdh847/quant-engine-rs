# Compare Two Runs

The `compare` binary turns two run output directories into a single diff bundle:

- `compare_report.md`
- `compare_report.html`
- `compare_report.json`

It compares three layers:

- portfolio summary metrics from `summary.txt`
- reproducibility/audit metadata from `audit_snapshot.json`
- data quality aggregates from `data_quality_report.csv`

Example:

```bash
cargo run --bin compare -- \
  --baseline-dir outputs_rust/run_a \
  --candidate-dir outputs_rust/run_b \
  --output-dir outputs_rust/compare_a_vs_b
```

Quick demo without market data:

```bash
bash scripts/compare_example.sh
```

This is useful when you are:

- iterating on alpha versions and want a clean before/after report
- checking whether a config change altered turnover, drawdown, or audit hashes
- packaging a candidate run for review before sharing a bundle
