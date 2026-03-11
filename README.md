# Private Quant Bot (Rust, Paper-Only)

Personal multi-market quant bot in Rust for paper trading only.

Docs:
- English: [README.md](README.md)
- 中文: [README.zh-CN.md](README.zh-CN.md)
- 日本語: [README.ja.md](README.ja.md)
- Data & credibility: `docs/DATA.md`
- Tutorials: `docs/TUTORIALS.md`
- Paper-only security model: `docs/SECURITY_PAPER_ONLY.md`
- Plugin SDK: `docs/PLUGIN_SDK.md`
- Open-source release checklist: `docs/OPEN_SOURCE_RELEASE_CHECKLIST.md`

## Core guarantees

- Rust implementation (`cargo run`, `cargo test`)
- Paper-only execution path (`broker.paper_only = true`)
- Multi-market setup: US / A-share / JP
- Strategy/risk/execution decoupled (inspired by top Rust quant architectures)

## Safety & legal

- Paper-only by default. This is not a live trading system.
- Not financial advice.
- If you ever pasted a token (PAT) or broker credential in any chat/log: revoke and rotate it immediately.

License: MIT OR Apache-2.0 (see [LICENSE](LICENSE)).

## GitHub inspirations

- [barter-rs](https://github.com/barter-rs/barter-rs): componentized trading engine design
- [NautilusTrader](https://github.com/nautechsystems/nautilus_trader): strategy/risk/execution separation and production mindset
- [rust_bt](https://github.com/jensnesten/rust_bt): strategy/backtest iteration workflow

Detailed mapping: `docs/GITHUB_LEARNINGS.md`

## Features implemented

- Strategy: layered multi-factor alpha (`momentum + mean reversion + low-vol + volume`)
  - cross-sectional winsorization (de-extreme)
  - two-stage ranking (layered scoring)
  - industry-neutralized final score
- Strategy plugin registry:
  - `layered_multi_factor` (default)
  - `momentum_guard` (momentum/volatility with trend guard)
  - configurable via `strategy.strategy_plugin` or CLI `--strategy-plugin`
  - scaffold helper: `cargo run -- scaffold-plugin --id your_plugin --output-dir plugins`
- Portfolio construction: `risk_parity` or `hrp` branch, blended with alpha, plus turnover constraint
- Regime-aware risk budget scaling (target volatility clamp with floor/ceiling)
- Unified risk: gross cap, symbol cap, daily loss lock
- Cross-market currency exposure control (USD/CNY/JPY net limits in base currency)
- Optional live FX adapter (fallback to static `fx_to_base` on network/provider failure)
  - supports refresh interval and failure cooldown to avoid repeated failed requests
- Market rules:
  - A-share `T+1` sell constraint
  - A-share limit-up/down day guardrails
  - US/JP/CN holiday calendar service
- Execution adapters:
  - `sim` paper broker
  - `ibkr_paper` lifecycle adapter (create/submit/ack/fill/reconcile/cancel)
- Market-aware transaction costs (commission/slippage/sell tax/min fee per market)
- Streaming-style event replay export (`replay` command)
- Walk-forward optimization with grid search
- Cross-market research command with leaderboard export
- Professional metrics: CAGR / Sharpe / Sortino / Calmar / Win Rate / Profit Factor
- Reproducible benchmark suite with dataset manifest hashing
- Intuitive dashboard UI (`dashboard.html`) generated from run outputs
- Robustness assessment (walk-forward OOS stability + PBO proxy + deflated Sharpe proxy)
- Data quality validator (duplicates, invalid rows, return outliers, date-order/gap checks)
- Paper daemon loop (cycle scheduler + state snapshots + drawdown alerts)
- Experiment tracking and run registry (CSV + JSON + top-run Markdown)
- Terminal control center with live-refresh status (`control-center`)
- Public leaderboard builder (`leaderboard`) combining registry + benchmark + research
- Strategy Plugin SDK generator + validator (`sdk-init` / `sdk-check`)
- SDK runtime auto-registration (`sdk-register`) so plugin id can be used directly in `--strategy-plugin`

## Quick start

```bash
# one-command demo (creates outputs_rust/demo/run_<timestamp>/dashboard.html)
cargo run -- demo --config config/bot.toml --lang en

# macOS: open the latest demo dashboard
open "$(cat outputs_rust/demo/LATEST_DASHBOARD.txt)"

# If your browser blocks `file://` fetch requests, serve via localhost:
cargo run -- serve --root outputs_rust/demo --bind 127.0.0.1:8787 --lang en
# then open http://127.0.0.1:8787/

# standard run (writes to outputs_rust/)
cargo run -- run --config config/bot.toml --output-dir outputs_rust --lang en

# optional: force strategy plugin for this run
cargo run -- run --config config/bot.toml --output-dir outputs_rust --strategy-plugin momentum_guard
```

Supported language tags: `en` / `zh` / `ja`.

Sanity check your setup:

```bash
cargo run -- doctor --config config/bot.toml
```

Generates:

- `outputs_rust/equity_curve.csv`
- `outputs_rust/trades.csv`
- `outputs_rust/rejections.csv`
- `outputs_rust/summary.txt`
- `outputs_rust/dashboard.html`
- `outputs_rust/factor_attribution.csv`
- `outputs_rust/factor_attribution_summary.txt`
- `outputs_rust/robustness/robustness_folds.csv` (via `robustness`)
- `outputs_rust/data_quality_report.csv` / `data_quality_summary.txt` (via `run`/`demo` default `validate-data`)
- `outputs_rust/daemon/paper_daemon_state.json` (via `paper-daemon`)
- `outputs_rust/run_registry.csv` / `run_registry.json` / `run_registry_top.md` (auto-updated by each command)
- `outputs_rust/leaderboard/leaderboard_public.csv` / `.md` / `.html` (via `leaderboard`)

Disable automatic data validation with `--skip-validate-data` (you can still run `validate-data` manually to any output dir, e.g. `outputs_rust/data_quality/`).

`summary.txt` includes:

- return metrics: `pnl_ratio`, `cagr`
- risk metrics: `max_drawdown`, `sharpe`, `sortino`, `calmar`
- quality metrics: `daily_win_rate`, `profit_factor`

## Walk-forward optimization

```bash
cargo run -- optimize \
  --config config/bot.toml \
  --output-dir outputs_rust/optimize \
  --train-days 12 \
  --test-days 5 \
  --short-windows 3,4,5 \
  --long-windows 7,9,11 \
  --vol-windows 5,7 \
  --top-ns 1,2 \
  --min-momentums=-0.01,0.0,0.01 \
  --strategy-plugins layered_multi_factor,momentum_guard \
  --portfolio-methods risk_parity,hrp
```

Generates:

- `outputs_rust/optimize/walk_forward_folds.csv`
- `outputs_rust/optimize/walk_forward_summary.txt`

## Cross-market research leaderboard

```bash
cargo run -- research \
  --config config/bot.toml \
  --output-dir outputs_rust/research \
  --markets US,A,JP \
  --short-windows 3,4,5 \
  --long-windows 7,9,11 \
  --vol-windows 5,7 \
  --top-ns 1,2 \
  --min-momentums=-0.01,0.0,0.01 \
  --strategy-plugins layered_multi_factor,momentum_guard \
  --portfolio-methods risk_parity,hrp
```

Generates:

- `outputs_rust/research/research_leaderboard.csv`
- `outputs_rust/research/research_leaderboard.md`

## Dashboard only

```bash
cargo run -- dashboard --output-dir outputs_rust --lang zh
```

## Run Bundle (Shareable Artifact)

Create a single `.tar.gz` containing the key run artifacts plus SHA256 manifest:

```bash
cargo run -- bundle --output-dir outputs_rust
```

Verify a bundle:

```bash
cargo run -- bundle-verify --bundle-path outputs_rust/run_bundle_<timestamp>.tar.gz
```

Extract a bundle to a directory (verifies first):

```bash
cargo run -- bundle-extract --bundle-path outputs_rust/run_bundle_<timestamp>.tar.gz --output-dir outputs_rust/unpacked --force
```

## Strategy plugin catalog

```bash
cargo run -- plugins
```

Generate plugin scaffold:

```bash
cargo run -- scaffold-plugin --id value_quality --output-dir plugins
```

Generate SDK plugin package:

```bash
cargo run -- sdk-init --id alpha_world --output-dir plugins_sdk
```

Validate SDK package structure:

```bash
cargo run -- sdk-check --package-dir plugins_sdk/alpha_world
```

Register SDK plugin into runtime registry:

```bash
cargo run -- sdk-register --package-dir plugins_sdk/alpha_world --name "Alpha World"
```

## Benchmark suite

```bash
cargo run -- benchmark \
  --config config/bot.toml \
  --output-dir outputs_rust/benchmark \
  --strategy-plugins layered_multi_factor,momentum_guard \
  --portfolio-methods risk_parity,hrp
```

Generates:

- `outputs_rust/benchmark/baseline_results.csv`
- `outputs_rust/benchmark/baseline_report.md`
- `outputs_rust/benchmark/dataset_manifest.csv`

## Robustness assessment

```bash
cargo run -- robustness \
  --config config/bot.toml \
  --output-dir outputs_rust/robustness \
  --train-days 12 \
  --test-days 5 \
  --short-windows 3,4,5 \
  --long-windows 7,9,11 \
  --vol-windows 5,7 \
  --top-ns 1,2 \
  --min-momentums=-0.01,0.0,0.01 \
  --strategy-plugins layered_multi_factor,momentum_guard \
  --portfolio-methods risk_parity,hrp
```

## Data quality checks

```bash
cargo run -- validate-data \
  --config config/bot.toml \
  --output-dir outputs_rust/data_quality \
  --return-outlier-threshold 0.35 \
  --gap-days-threshold 10
```

## Paper daemon

```bash
cargo run -- paper-daemon \
  --config config/bot.toml \
  --output-dir outputs_rust/daemon \
  --cycles 3 \
  --sleep-secs 1 \
  --alert-drawdown-ratio 0.03
```

## Run registry

```bash
cargo run -- registry --output-dir outputs_rust --top 20
```

## Control center (TUI)

```bash
cargo run -- control-center --output-dir outputs_rust --refresh-secs 2 --cycles 30
```

## Public leaderboard

```bash
cargo run -- leaderboard --output-dir outputs_rust --top 50
```

## Event replay export

```bash
cargo run -- replay \
  --config config/bot.toml \
  --output-dir outputs_rust/replay
```

Generates:

- `outputs_rust/replay/event_replay.csv`
- `outputs_rust/replay/event_replay_summary.txt`

## Safety

- `broker.mode = "sim"` by default.
- `ibkr_paper` mode keeps local paper simulation as source-of-truth and writes lifecycle logs.
- Keep `paper_only = true` to avoid accidental live-trading behavior.

## Open-source plan

- Contributor guide: `CONTRIBUTING.md`
- Long-term milestone plan: `ROADMAP.md`
- Chinese docs: `README.zh-CN.md`
- Japanese docs: `README.ja.md`
- Community standards: `CODE_OF_CONDUCT.md`, `SECURITY.md`
- Auto-published research site: `.github/workflows/pages.yml`
  - includes interactive benchmark/research explorers on Pages index
