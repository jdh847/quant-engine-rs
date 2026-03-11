# Contributing

## Development

1. Install Rust stable.
2. Run checks before PR:

```bash
cargo fmt
cargo clippy --all-targets --all-features -- -D warnings
cargo test
cargo run -- run --config config/bot.toml --output-dir outputs_rust
cargo run -- benchmark --config config/bot.toml --output-dir outputs_rust/benchmark
cargo run -- research --config config/bot.toml --output-dir outputs_rust/research
```

## Design principles

- Keep the system paper-only by default.
- Keep strategy, risk, execution, and market rules decoupled.
- Add tests for every market microstructure rule.
- Favor deterministic backtests and reproducible outputs.

## PR scope

- One feature area per PR (strategy, risk, execution adapter, research, UI).
- Include output artifacts or snapshots when behavior changes.
