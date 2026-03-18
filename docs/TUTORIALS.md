# Tutorials (Zero To Results)

All commands are paper-only.

## 1) Demo + Dashboard (Fastest)

```bash
cargo run -- demo --config config/bot.toml --lang en
open "$(cat outputs_rust/demo/LATEST_DASHBOARD.txt)"
```

Expected:

- a new folder under `outputs_rust/demo/run_<timestamp>/`
- `dashboard.html` with KPI cards + trades/rejections/factors panels
- by default, `validate-data` is run and dashboard shows Data Quality (use `--skip-validate-data` to disable)
- `config_used_redacted.toml` (best-effort redaction; avoid putting secrets in configs)

Committed HTML example:

- `docs/examples/dashboard_sample.html`

## 2) Serve Dashboard Over localhost (Avoid file:// fetch issues)

Some browsers block `fetch('./*.csv')` under `file://`.

```bash
cargo run -- serve --root outputs_rust/demo --bind 127.0.0.1:0 --lang en
```

Open the printed URL (it redirects `/` to the latest `dashboard.html`).

If your environment refuses to bind a local port (e.g. `Operation not permitted`), you can still read the dashboard by opening the HTML directly:

```bash
open "$(cat outputs_rust/demo/LATEST_DASHBOARD.txt)"
```

In that case, live refresh may show `fallback` depending on the browser's `file://` restrictions.

## 3) Research Leaderboard (Cross-Market)

```bash
cargo run -- research \
  --config config/bot.toml \
  --output-dir outputs_rust/research \
  --markets US,A,JP \
  --strategy-plugins layered_multi_factor,momentum_guard \
  --portfolio-methods risk_parity,hrp \
  --lang en
```

Expected:

- `outputs_rust/research/research_leaderboard.csv`
- `outputs_rust/research/research_leaderboard.md`

## 4) Data Quality Check (Credibility Gate)

`run` / `demo` already run this by default. You can also run it manually:

```bash
cargo run -- validate-data --config config/bot.toml --output-dir outputs_rust/data_quality --return-outlier-threshold 0.35 --gap-days-threshold 10
```

Expected:

- `outputs_rust/data_quality/data_quality_report.csv`
- `outputs_rust/data_quality/data_quality_summary.txt`

## 5) Research Report (Walk-Forward + Regime Split + Factor Decay)

```bash
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
```

Expected:

- `outputs_rust/research_report/research_report.md`
- `outputs_rust/research_report/research_report.html`
- `outputs_rust/research_report/research_report.json`
- `outputs_rust/research_report/factor_decay.csv`
- `outputs_rust/research_report/regime_split.csv`
- `outputs_rust/research_report/walk_forward_deep_dive.csv`

## 6) Generate a Reproducible Synthetic Dataset (No External Data)

```bash
cargo run -- gen-synth-data --output-dir data_synth --seed 42 --us-symbols 12 --a-symbols 12 --jp-symbols 12 --force
```

Then update `config/bot.toml` `markets.*.data_file` paths to point at `data_synth/*.csv`.

## 7) Create a Shareable Run Bundle

```bash
cargo run -- bundle --output-dir outputs_rust
```

Expected:

- `outputs_rust/run_bundle_<timestamp>.tar.gz`

Verify:

```bash
cargo run -- bundle-verify --bundle-path outputs_rust/run_bundle_<timestamp>.tar.gz
```

Extract:

```bash
cargo run -- bundle-extract --bundle-path outputs_rust/run_bundle_<timestamp>.tar.gz --output-dir outputs_rust/unpacked --force
```

## Scripts

Runnable helpers:

- `scripts/tutorial_demo.sh`
- `scripts/tutorial_research.sh`
- `scripts/tutorial_optimize.sh`
- `scripts/tutorial_serve.sh`
