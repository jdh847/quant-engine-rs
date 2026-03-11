# Open Source Release Checklist

This checklist is for making the repo safe and boring to publish.

## Safety

- Confirm `broker.paper_only` is enforced by config validation.
- Confirm network paths are gated behind env hard-switches.
- `PQBOT_ALLOW_NETWORK=1` is required for any network calls.
- `PQBOT_ALLOW_IBKR_PAPER=1` is required for `ibkr_paper`.
- Run `./scripts/secret_scan.sh`.
- If any token/password was ever pasted in a chat/log: revoke and rotate it.

## Data & Backtest Credibility

- Ensure data sources and licensing are documented in `docs/DATA.md`.
- Ensure corporate actions are handled (prefer `adj_close` when available).
- Document/handle:
  - trading sessions and holiday calendars
  - suspensions / limit-up/down / volume=0 days
  - survivorship and delisting assumptions
- Produce a reproducible run:
  - `cargo test` passes (includes stability golden test)
  - `cargo run -- demo ...` produces a deterministic `summary.txt` and `audit_snapshot.json`

## Docs & Demo

- `README.md` quick-start works on a fresh machine.
- Tutorials in `docs/TUTORIALS.md` all run end-to-end.
- A dashboard HTML artifact exists and matches docs screenshots/examples.

## Quality Gate

- `cargo fmt --check`
- `cargo clippy -D warnings`
- `cargo test`
- GitHub Actions CI is green

## Publish

- If you want to flip the repo to public: `./scripts/make_repo_public.sh <owner/repo>`
