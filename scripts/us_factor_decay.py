#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import subprocess
from pathlib import Path


FACTOR_PROFILES = [
    ("all_factors", 0.45, 0.20, 0.25, 0.10),
    ("momentum_only", 1.00, 0.00, 0.00, 0.00),
    ("mean_reversion_only", 0.00, 1.00, 0.00, 0.00),
    ("low_vol_only", 0.00, 0.00, 1.00, 0.00),
    ("volume_only", 0.00, 0.00, 0.00, 1.00),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Measure US factor profile decay across chronological windows."
    )
    parser.add_argument("--config", default="config/bot_us.toml")
    parser.add_argument("--output-dir", default="outputs_rust/research_us_factor_decay")
    parser.add_argument("--engine-bin", default="")
    parser.add_argument(
        "--window-days",
        type=int,
        default=126,
        help="trading days per evaluation window",
    )
    parser.add_argument(
        "--min-windows",
        type=int,
        default=3,
        help="minimum number of windows required to emit a summary",
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
    dates = unique_dates(rows, data_path)
    windows = build_windows(dates, args.window_days)
    if len(windows) < args.min_windows:
        raise SystemExit(
            f"not enough windows for factor decay: got {len(windows)}, need {args.min_windows}"
        )

    detail_records: list[dict[str, object]] = []
    for profile, w_m, w_r, w_lv, w_v in FACTOR_PROFILES:
        for idx, (start, end, window_dates) in enumerate(windows, start=1):
            window_rows = [
                row for row in rows if start <= parse_date(row["date"]) <= end
            ]
            window_dir = output_dir / f"{profile}_window_{idx:02d}"
            window_dir.mkdir(parents=True, exist_ok=True)
            window_csv = window_dir / "us_equities.csv"
            write_market_csv(window_csv, window_rows)

            patched = patch_config(
                config_text,
                str(window_csv),
                {
                    "strategy_plugin": '"layered_multi_factor"',
                    "portfolio_method": '"risk_parity"',
                    "factor_momentum_weight": f"{w_m:.2f}",
                    "factor_mean_reversion_weight": f"{w_r:.2f}",
                    "factor_low_vol_weight": f"{w_lv:.2f}",
                    "factor_volume_weight": f"{w_v:.2f}",
                },
            )
            window_cfg = window_dir / "config.toml"
            window_cfg.write_text(patched, encoding="utf-8")

            run_command(build_run_cmd(args.engine_bin, window_cfg, window_dir))
            summary = parse_kv_file(window_dir / "summary.txt")
            detail_records.append(
                {
                    "profile": profile,
                    "window_index": idx,
                    "start_date": start.isoformat(),
                    "end_date": end.isoformat(),
                    "trading_days": len(window_dates),
                    "score": score_from_summary(summary),
                    "pnl_ratio": parse_float(summary.get("pnl_ratio")),
                    "max_drawdown": parse_float(summary.get("max_drawdown")),
                    "sharpe": parse_float(summary.get("sharpe")),
                    "trades": int(parse_float(summary.get("trades")) or 0.0),
                }
            )

    summary_records = summarize_decay(detail_records)
    detail_csv = output_dir / "factor_decay_windows_us.csv"
    summary_csv = output_dir / "factor_decay_us.csv"
    summary_md = output_dir / "factor_decay_us.md"
    write_detail(detail_records, detail_csv)
    write_summary(summary_records, summary_csv, summary_md)
    print(f"factor decay completed | rows={len(summary_records)} csv={summary_csv}")
    return 0


def summarize_decay(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    by_profile: dict[str, list[dict[str, object]]] = {}
    for row in rows:
        by_profile.setdefault(str(row["profile"]), []).append(row)

    out: list[dict[str, object]] = []
    for profile, profile_rows in by_profile.items():
        profile_rows.sort(key=lambda row: int(row["window_index"]))
        scores = [float(row["score"]) for row in profile_rows]
        early_count = max(1, len(scores) // 2)
        early = scores[:early_count]
        late = scores[-early_count:]
        early_avg = sum(early) / len(early)
        late_avg = sum(late) / len(late)
        decay_delta = late_avg - early_avg
        decay_ratio = late_avg / early_avg if abs(early_avg) > 1e-9 else 0.0
        slope = linear_slope(scores)
        latest = profile_rows[-1]
        for candidate in reversed(profile_rows):
            if (
                abs(float(candidate["score"])) > 1e-9
                or abs(float(candidate["pnl_ratio"])) > 1e-9
                or abs(float(candidate["sharpe"])) > 1e-9
            ):
                latest = candidate
                break
        out.append(
            {
                "profile": profile,
                "windows": len(profile_rows),
                "avg_score_early": early_avg,
                "avg_score_late": late_avg,
                "decay_delta": decay_delta,
                "decay_ratio": decay_ratio,
                "trend_slope": slope,
                "latest_score": float(latest["score"]),
                "latest_pnl_ratio": float(latest["pnl_ratio"]),
                "latest_sharpe": float(latest["sharpe"]),
            }
        )
    out.sort(key=lambda row: float(row["decay_delta"]), reverse=True)
    return out


def linear_slope(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    xs = list(range(len(values)))
    mean_x = sum(xs) / len(xs)
    mean_y = sum(values) / len(values)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, values))
    denominator = sum((x - mean_x) ** 2 for x in xs)
    if denominator == 0:
        return 0.0
    return numerator / denominator


def build_windows(
    dates: list[dt.date], window_days: int
) -> list[tuple[dt.date, dt.date, list[dt.date]]]:
    windows: list[tuple[dt.date, dt.date, list[dt.date]]] = []
    start = 0
    while start + window_days <= len(dates):
        window_dates = dates[start : start + window_days]
        windows.append((window_dates[0], window_dates[-1], window_dates))
        start += window_days
    return windows


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


def patch_config(text: str, data_file: str, strategy_updates: dict[str, str]) -> str:
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
            out.append(f'data_file = "{data_file}"\n')
            continue
        if section == "strategy":
            replaced = False
            for key, value in strategy_updates.items():
                if stripped.startswith(f"{key} ="):
                    out.append(f"{key} = {value}\n")
                    replaced = True
                    break
            if replaced:
                continue
        if section == "strategy.market_routing.US":
            if stripped.startswith("strategy_plugin ="):
                out.append('strategy_plugin = "layered_multi_factor"\n')
                continue
            if stripped.startswith("portfolio_method ="):
                out.append('portfolio_method = "risk_parity"\n')
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


def write_detail(rows: list[dict[str, object]], path: Path) -> None:
    headers = [
        "profile",
        "window_index",
        "start_date",
        "end_date",
        "trading_days",
        "score",
        "pnl_ratio",
        "max_drawdown",
        "sharpe",
        "trades",
    ]
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_summary(rows: list[dict[str, object]], csv_path: Path, md_path: Path) -> None:
    headers = [
        "profile",
        "windows",
        "avg_score_early",
        "avg_score_late",
        "decay_delta",
        "decay_ratio",
        "trend_slope",
        "latest_score",
        "latest_pnl_ratio",
        "latest_sharpe",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    best = max(rows, key=lambda row: float(row["decay_delta"]))
    worst = min(rows, key=lambda row: float(row["decay_delta"]))
    lines = [
        "# US Factor Decay",
        "",
        f"best_decay_profile={best['profile']} delta={float(best['decay_delta']):.6f} latest_sharpe={float(best['latest_sharpe']):.4f}",
        f"worst_decay_profile={worst['profile']} delta={float(worst['decay_delta']):.6f} latest_sharpe={float(worst['latest_sharpe']):.4f}",
        "",
        "| Profile | Windows | Early Score | Late Score | Decay Delta | Trend Slope | Latest Sharpe |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            "| {profile} | {windows} | {early:.6f} | {late:.6f} | {delta:.6f} | {slope:.6f} | {sharpe:.6f} |".format(
                profile=row["profile"],
                windows=int(row["windows"]),
                early=float(row["avg_score_early"]),
                late=float(row["avg_score_late"]),
                delta=float(row["decay_delta"]),
                slope=float(row["trend_slope"]),
                sharpe=float(row["latest_sharpe"]),
            )
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_date(text: str) -> dt.date:
    return dt.datetime.strptime(text, "%Y-%m-%d").date()


if __name__ == "__main__":
    raise SystemExit(main())
