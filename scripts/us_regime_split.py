#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import subprocess
from pathlib import Path


REGIMES = [
    ("2018_bear", "2018-01-02", "2018-12-31"),
    ("2019_rebound", "2019-01-02", "2019-12-31"),
    ("2020_covid", "2020-01-02", "2020-12-31"),
    ("2021_risk_on", "2021-01-04", "2021-12-31"),
    ("2022_rate_hike", "2022-01-03", "2022-12-30"),
    ("2023_2024_ai", "2023-01-03", "2024-12-31"),
    ("2025_2026_recent", "2025-01-02", "2026-12-31"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run fixed US regime-split backtests and export a summary."
    )
    parser.add_argument("--config", default="config/bot_us.toml")
    parser.add_argument("--output-dir", default="outputs_rust/research_us_regime_split")
    parser.add_argument("--engine-bin", default="")
    parser.add_argument(
        "--min-trading-days",
        type=int,
        default=80,
        help="skip regime buckets with fewer than this many distinct dates",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base_config = Path(args.config)
    if not base_config.exists():
        raise SystemExit(f"config not found: {base_config}")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    config_text = base_config.read_text(encoding="utf-8")
    data_path = resolve_path(base_config.parent, extract_us_data_file(config_text))
    if not data_path.exists():
        raise SystemExit(f"US data file not found: {data_path}")

    rows = load_csv_rows(data_path)
    all_dates = unique_dates(rows, data_path)
    all_start = all_dates[0]
    all_end = all_dates[-1]

    records: list[dict[str, object]] = []
    for name, start_raw, end_raw in REGIMES:
        start = max(parse_date(start_raw), all_start)
        end = min(parse_date(end_raw), all_end)
        if start > end:
            continue

        sliced = [row for row in rows if start <= parse_date(row["date"]) <= end]
        trading_days = len({row["date"] for row in sliced})
        if trading_days < args.min_trading_days:
            continue

        regime_dir = output_dir / name
        regime_dir.mkdir(parents=True, exist_ok=True)
        regime_csv = regime_dir / "us_equities.csv"
        write_market_csv(regime_csv, sliced)

        patched = patch_market_data_file(config_text, str(regime_csv))
        regime_cfg = regime_dir / "config.toml"
        regime_cfg.write_text(patched, encoding="utf-8")

        run_command(build_run_cmd(args.engine_bin, regime_cfg, regime_dir))
        summary = parse_kv_file(regime_dir / "summary.txt")
        record = {
            "regime": name,
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "trading_days": trading_days,
            "score": score_from_summary(summary),
            "pnl_ratio": parse_float(summary.get("pnl_ratio")),
            "max_drawdown": parse_float(summary.get("max_drawdown")),
            "sharpe": parse_float(summary.get("sharpe")),
            "sortino": parse_float(summary.get("sortino")),
            "calmar": parse_float(summary.get("calmar")),
            "trades": int(parse_float(summary.get("trades")) or 0.0),
            "rejections": int(parse_float(summary.get("rejections")) or 0.0),
        }
        records.append(record)

    if not records:
        raise SystemExit("no regime buckets produced usable results")

    summary_csv = output_dir / "regime_split_us.csv"
    summary_md = output_dir / "regime_split_us.md"
    write_summary(records, summary_csv, summary_md)
    print(f"regime split completed | rows={len(records)} csv={summary_csv}")
    return 0


def extract_us_data_file(config_text: str) -> str:
    section = ""
    for line in config_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            section = stripped[1:-1].strip()
            continue
        if section == "markets.US" and stripped.startswith("data_file"):
            _, value = stripped.split("=", 1)
            return value.strip().strip('"')
    raise SystemExit("failed to locate [markets.US].data_file in config")


def resolve_path(base_dir: Path, raw: str) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path
    return (base_dir.parent / path).resolve()


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    return list(csv.DictReader(path.open("r", encoding="utf-8")))


def unique_dates(rows: list[dict[str, str]], path: Path) -> list[dt.date]:
    dates = sorted({parse_date(row["date"]) for row in rows})
    if not dates:
        raise SystemExit(f"no trading dates in {path}")
    return dates


def write_market_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["date", "symbol", "close", "adj_close", "volume"])
        for row in rows:
            writer.writerow(
                [
                    row.get("date", ""),
                    row.get("symbol", ""),
                    row.get("close", ""),
                    row.get("adj_close", row.get("close", "")),
                    row.get("volume", ""),
                ]
            )


def patch_market_data_file(text: str, new_path: str) -> str:
    lines = text.splitlines(keepends=True)
    out: list[str] = []
    section = ""
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            section = stripped[1:-1].strip()
            out.append(line)
            continue
        if section == "markets.US" and stripped.startswith("data_file"):
            out.append(f'data_file = "{new_path}"\n')
            continue
        out.append(line)
    return "".join(out)


def build_run_cmd(engine_bin: str, config_path: Path, output_dir: Path) -> list[str]:
    tail = [
        "run",
        "--config",
        str(config_path),
        "--output-dir",
        str(output_dir),
    ]
    if engine_bin:
        return [engine_bin, *tail]
    return ["cargo", "run", "--", *tail]


def run_command(cmd: list[str]) -> None:
    proc = subprocess.run(cmd, text=True, capture_output=True)
    if proc.returncode != 0:
        raise SystemExit(
            "command failed:\n"
            + " ".join(cmd)
            + "\nstdout:\n"
            + proc.stdout
            + "\nstderr:\n"
            + proc.stderr
        )


def parse_kv_file(path: Path) -> dict[str, str]:
    if not path.exists():
        raise SystemExit(f"missing summary file: {path}")
    out: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = value.strip()
    return out


def parse_float(raw: str | None) -> float:
    if raw is None:
        return 0.0
    text = str(raw).strip().rstrip("%")
    if not text:
        return 0.0
    try:
        value = float(text)
    except ValueError:
        return 0.0
    if str(raw).strip().endswith("%"):
        return value / 100.0
    return value


def score_from_summary(summary: dict[str, str]) -> float:
    pnl_ratio = parse_float(summary.get("pnl_ratio"))
    max_drawdown = parse_float(summary.get("max_drawdown"))
    sharpe = parse_float(summary.get("sharpe"))
    sortino = parse_float(summary.get("sortino"))
    calmar = parse_float(summary.get("calmar"))
    return pnl_ratio + sharpe * 0.12 + sortino * 0.06 + calmar * 0.04 - max_drawdown * 0.9


def write_summary(rows: list[dict[str, object]], csv_path: Path, md_path: Path) -> None:
    rows = sorted(rows, key=lambda row: parse_date(str(row["start_date"])))
    headers = [
        "regime",
        "start_date",
        "end_date",
        "trading_days",
        "score",
        "pnl_ratio",
        "max_drawdown",
        "sharpe",
        "sortino",
        "calmar",
        "trades",
        "rejections",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    best = max(rows, key=lambda row: float(row["score"]))
    worst = min(rows, key=lambda row: float(row["score"]))
    lines = [
        "# US Regime Split",
        "",
        f"best_regime={best['regime']} score={float(best['score']):.6f} pnl={float(best['pnl_ratio']) * 100.0:.2f}% sharpe={float(best['sharpe']):.4f}",
        f"worst_regime={worst['regime']} score={float(worst['score']):.6f} pnl={float(worst['pnl_ratio']) * 100.0:.2f}% sharpe={float(worst['sharpe']):.4f}",
        "",
        "| Regime | Days | Score | PnL | MaxDD | Sharpe | Trades |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            "| {regime} | {days} | {score:.6f} | {pnl:.6f} | {dd:.6f} | {sharpe:.6f} | {trades} |".format(
                regime=row["regime"],
                days=int(row["trading_days"]),
                score=float(row["score"]),
                pnl=float(row["pnl_ratio"]),
                dd=float(row["max_drawdown"]),
                sharpe=float(row["sharpe"]),
                trades=int(row["trades"]),
            )
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_date(text: str) -> dt.date:
    return dt.datetime.strptime(text, "%Y-%m-%d").date()


if __name__ == "__main__":
    raise SystemExit(main())
