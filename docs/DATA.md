# Data & Backtest Credibility

This project is built to make research reproducible and avoid "mystery backtests". The bundled dataset is small and synthetic; for real research you must plug in your own data pipeline.

## Data Sources / Licensing

This repo ships only a tiny synthetic sample dataset under `data/`.
For larger local experiments without external data, use `gen-synth-data` to generate a reproducible synthetic dataset.

If you fetch or generate real market data:

- verify the provider's licensing/terms
- do not commit or redistribute vendor data unless you have explicit rights
- document your data lineage (provider, timestamps, adjustments, symbol mappings)

`scripts/fetch_stooq_daily.sh` is provided as a **template** only. It is not an endorsed "official" source.

## Data Schema

Market bars (CSV):

- Columns: `date,symbol,close,volume`
- `date`: `YYYY-MM-DD` (exchange date)
- `close`: expected to be **adjusted close** for corporate actions
- Optional: `adj_close` (if present, it overrides `close`)
- `volume`: >= 0 (0 is treated as halted/illiquid)

Industry map (CSV):

- Columns: `symbol,industry`

## Corporate Actions (Splits/Dividends)

The engine currently treats `Bar.close` as the *price series used for returns/volatility*.

- If you want split/dividend correctness, feed **adjusted close**.
- If your provider gives both, write `adj_close` and keep `close` as raw close.

## Halts / Suspensions / Missing Bars

This repo is intentionally conservative:

- If a symbol has **no bar** on a date, or `close <= 0`, orders are rejected as a data gap.
- If `volume <= 0`, orders are rejected as halted/illiquid.

See: `src/market.rs` rules.

## A-Share Rules (Simplified)

Built-in guardrails:

- `T+1` sell constraint (same-day sells rejected)
- limit-up/limit-down day guardrails (based on previous close and ~10% band)

See: `src/market.rs`.

## Trading Sessions & Holidays

Backtests are daily-bar based. The calendar is a small hard-coded holiday set for 2025-2026:

- US: NYSE-like closures
- JP: TSE-like closures
- A: China A-share closures

See: `src/calendar.rs`.

If you run outside the covered dates, you should extend the holiday lists or replace the calendar with a proper service.

The `validate-data` command also flags rows that fall on non-trading days (weekends/known holidays) as a `WARN`.

### Custom Holiday Files (Per Market)

You can extend the built-in holiday list per market using `markets.<M>.holiday_file` in `config/bot.toml`.

Format: one date per line (`YYYY-MM-DD`). Blank lines and lines starting with `#` are ignored.

Example:

```toml
[markets.US]
holiday_file = "calendar/us_holidays.txt"
```

## Reproducibility: Dataset Manifest

You can generate a machine-readable manifest with file hashes and date ranges:

```bash
cargo run -- dataset-manifest --config config/bot.toml --output-path data/DATASET_MANIFEST.json
```

This helps you tie results to:

- exact `config` hash
- exact `data` file hashes
- dataset date coverage
