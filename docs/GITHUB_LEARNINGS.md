# Rust Quant GitHub Learnings Applied

Date: 2026-02-27

## Reference repos

- https://github.com/barter-rs/barter-rs
- https://github.com/nautechsystems/nautilus_trader
- https://github.com/jensnesten/rust_bt
- https://github.com/HourglassDevTeam/Hourglass

## Architecture patterns applied

- Pluggable boundaries: `strategy`, `market rules`, `risk`, `execution`, `engine`.
- Strategy registry pattern (`strategy_plugin`) with runtime switch (`layered_multi_factor` / `momentum_guard`).
- Public plugin discoverability via `cargo run -- plugins`.
- Contributor acceleration via `cargo run -- scaffold-plugin`.
- Event-loop style execution in `engine` with deterministic state transitions.
- Shared code path for backtest/paper simulation and broker bridge mode.
- Broker abstraction to switch between `sim` and `ibkr_paper` without touching strategy.
- Broker lifecycle journaling (submit/ack/fill/reconcile/cancel) inspired by production-grade execution engines.
- Exchange calendar service with market-specific holiday closures.

## Strategy patterns applied

- Trend + momentum gate (common baseline in open-source quant repos).
- Volatility-adjusted cross-sectional scoring (`score = momentum / volatility`).
- `top_n` ranking for capital concentration and simpler portfolio control.
- Parameter search plus walk-forward and research leaderboard workflow for reproducible strategy iteration.
- Benchmark matrix now evaluates scenarios across plugin/method combinations for fair comparisons.
- Factor attribution export (`factor_attribution.csv`) helps explain alpha composition over time.
- Robustness report adds walk-forward OOS stability / PBO proxy / deflated Sharpe proxy.
- Data quality report enforces practical dataset checks before strategy claims.
- Run registry (`run_registry.csv/json/md`) tracks experiment lineage and comparable scores across commands.
- Terminal control center + public leaderboard mirror open-source quant projects' ops visibility patterns.
- Plugin SDK workflow (`sdk-init`/`sdk-check`) mirrors extensibility patterns from mature quant frameworks.
- Added `sdk-register` to bridge SDK package metadata into runtime plugin registry (`config/sdk_plugins.toml`).

## Safety patterns applied

- Paper-only broker switch by config.
- A-share microstructure guardrails (`T+1`, limit-up/down guard).
- Centralized pre-trade checks before execution.

## Why this matters

- Faster iteration: strategy tuning without modifying broker code.
- Easier community contribution: contributors can add new plugins without touching execution/risk paths.
- Lower regression risk: market rules and risk checks are explicit modules.
- Straight path to production paper bridge while keeping local replay reproducible.
- Reproducible benchmark outputs via dataset hashing and baseline reports.
