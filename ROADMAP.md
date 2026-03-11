# Roadmap (Toward a 10k-star Rust Quant Bot)

## Phase 1: Research-grade Paper System (now)

- [x] Multi-market paper engine (US/A/JP)
- [x] Strategy/risk/execution modular architecture
- [x] A-share T+1 and limit guardrails
- [x] Walk-forward optimization
- [x] Research leaderboard and intuitive dashboard

## Phase 2: Broker and Data Depth

- [x] IBKR paper adapter full order lifecycle (ack/fill/cancel reconciliation)
- [x] Real market calendars and holiday services per exchange
- [x] Streaming data adapter and event replay engine
- [x] More realistic transaction cost model per market

## Phase 3: Alpha & Portfolio

- [x] Multi-factor library (momentum/quality/volatility/mean-reversion)
- [x] Regime detection and dynamic risk budget
- [x] Portfolio optimizer (risk parity / HRP / turnover constraints)
- [x] Cross-market currency exposure controls

## Phase 4: Platform

- [x] TUI control center and web dashboard with live refresh
- [x] Experiment tracking and run registry
- [x] Strategy plugin SDK
- [x] Public benchmark suite and leaderboard
