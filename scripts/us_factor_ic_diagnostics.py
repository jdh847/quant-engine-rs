#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import math
from collections import defaultdict, deque
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

FACTOR_ORDER = ["momentum", "mean_reversion", "low_vol", "volume"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute daily, rolling, and regime-split factor IC diagnostics for US data."
    )
    parser.add_argument("--config", default="config/bot_us_long.toml")
    parser.add_argument(
        "--output-dir", default="outputs_rust/research_us_long_factor_ic"
    )
    parser.add_argument(
        "--rolling-days",
        type=int,
        default=126,
        help="trading days per rolling IC window",
    )
    parser.add_argument(
        "--min-days",
        type=int,
        default=60,
        help="minimum daily IC observations required for a rolling/regime report row",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config)
    if not config_path.exists():
        raise SystemExit(f"config not found: {config_path}")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    config_text = config_path.read_text(encoding="utf-8")
    data_path = resolve_path(config_path.parent, extract_us_data_file(config_text))
    if not data_path.exists():
        raise SystemExit(f"US data file not found: {data_path}")

    windows = extract_windows(config_text)
    bars_by_date = load_bars_by_date(data_path)
    daily_rows = build_daily_ic_rows(bars_by_date, windows)
    if not daily_rows:
        raise SystemExit("no daily IC rows produced")

    rolling_rows = summarize_rolling(daily_rows, args.rolling_days, args.min_days)
    regime_rows = summarize_regimes(daily_rows, args.min_days)

    daily_csv = output_dir / "factor_ic_daily_us.csv"
    rolling_csv = output_dir / "factor_ic_rolling_us.csv"
    regime_csv = output_dir / "factor_ic_regime_us.csv"
    summary_md = output_dir / "factor_ic_summary_us.md"

    write_csv(
        daily_csv,
        [
            "date",
            "factor",
            "ic",
            "symbols",
        ],
        daily_rows,
    )
    write_csv(
        rolling_csv,
        [
            "factor",
            "window_index",
            "start_date",
            "end_date",
            "n_days",
            "mean_ic",
            "std_ic",
            "ic_ir",
            "t_stat",
            "positive_ratio",
        ],
        rolling_rows,
    )
    write_csv(
        regime_csv,
        [
            "regime",
            "factor",
            "start_date",
            "end_date",
            "n_days",
            "mean_ic",
            "std_ic",
            "ic_ir",
            "t_stat",
            "positive_ratio",
        ],
        regime_rows,
    )
    write_summary(summary_md, daily_rows, rolling_rows, regime_rows)
    print(
        "factor ic diagnostics completed | "
        f"daily={len(daily_rows)} rolling={len(rolling_rows)} regime={len(regime_rows)} "
        f"dir={output_dir}"
    )
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


def extract_windows(config_text: str) -> dict[str, int]:
    wanted = {
        "long_window": 7,
        "vol_window": 5,
        "mean_reversion_window": 3,
        "volume_window": 5,
    }
    section = ""
    for line in config_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            section = stripped[1:-1].strip()
            continue
        if section != "strategy" or "=" not in stripped:
            continue
        key, value = [part.strip() for part in stripped.split("=", 1)]
        if key in wanted:
            try:
                wanted[key] = int(value.split("#", 1)[0].strip().strip('"'))
            except ValueError:
                pass
    return wanted


def resolve_path(base_dir: Path, raw: str) -> Path:
    path = Path(raw)
    if path.is_absolute():
        return path
    return (base_dir.parent / path).resolve()


def parse_date(raw: str) -> dt.date:
    return dt.date.fromisoformat(raw)


def load_bars_by_date(path: Path) -> dict[dt.date, list[dict[str, float | str]]]:
    rows = defaultdict(list)
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            date = parse_date(row["date"])
            close_raw = row.get("adj_close") or row.get("close") or ""
            volume_raw = row.get("volume") or ""
            try:
                close = float(close_raw)
                volume = float(volume_raw)
            except ValueError:
                continue
            if not math.isfinite(close) or close <= 0 or not math.isfinite(volume):
                continue
            rows[date].append(
                {
                    "symbol": row.get("symbol", ""),
                    "close": close,
                    "volume": volume,
                }
            )
    return dict(sorted(rows.items(), key=lambda item: item[0]))


def build_daily_ic_rows(
    bars_by_date: dict[dt.date, list[dict[str, float | str]]],
    windows: dict[str, int],
) -> list[dict[str, object]]:
    history: dict[str, dict[str, deque[float]]] = defaultdict(
        lambda: {"closes": deque(), "volumes": deque()}
    )
    day_factors: dict[dt.date, dict[str, dict[str, float]]] = {}
    day_closes: dict[dt.date, dict[str, float]] = {}
    max_len = (
        max(
            windows["long_window"],
            windows["vol_window"] + 1,
            windows["mean_reversion_window"] + 1,
            windows["volume_window"],
        )
        + 1
    )

    for date, day_bars in bars_by_date.items():
        factors_today: dict[str, dict[str, float]] = defaultdict(dict)
        closes_today: dict[str, float] = {}
        for bar in day_bars:
            symbol = str(bar["symbol"])
            close = float(bar["close"])
            volume = float(bar["volume"])
            symbol_history = history[symbol]
            factors = compute_factors(symbol_history, close, volume, windows)
            if factors is not None:
                momentum, mean_reversion, low_vol, volume_signal = factors
                factors_today["momentum"][symbol] = momentum
                factors_today["mean_reversion"][symbol] = mean_reversion
                factors_today["low_vol"][symbol] = low_vol
                factors_today["volume"][symbol] = volume_signal
            closes_today[symbol] = close
            symbol_history["closes"].append(close)
            symbol_history["volumes"].append(volume)
            while len(symbol_history["closes"]) > max_len:
                symbol_history["closes"].popleft()
            while len(symbol_history["volumes"]) > max_len:
                symbol_history["volumes"].popleft()
        if factors_today:
            day_factors[date] = dict(factors_today)
        day_closes[date] = closes_today

    dates = sorted(day_closes.keys())
    rows: list[dict[str, object]] = []
    for idx, date in enumerate(dates[:-1]):
        next_date = dates[idx + 1]
        factors = day_factors.get(date)
        if not factors:
            continue
        closes_today = day_closes[date]
        closes_next = day_closes[next_date]
        forward_returns = {}
        for symbol, today_close in closes_today.items():
            next_close = closes_next.get(symbol)
            if next_close is None or today_close <= 0:
                continue
            forward_returns[symbol] = next_close / today_close - 1.0
        for factor in FACTOR_ORDER:
            ic, symbols = daily_ic(factors.get(factor, {}), forward_returns)
            if ic is None:
                continue
            rows.append(
                {
                    "date": date.isoformat(),
                    "factor": factor,
                    "ic": ic,
                    "symbols": symbols,
                }
            )
    return rows


def compute_factors(
    history: dict[str, deque[float]],
    current_close: float,
    current_volume: float,
    windows: dict[str, int],
) -> tuple[float, float, float, float] | None:
    closes = history["closes"]
    volumes = history["volumes"]
    needed = max(
        windows["long_window"],
        windows["vol_window"] + 1,
        windows["mean_reversion_window"] + 1,
    )
    if len(closes) < needed or len(volumes) < windows["volume_window"]:
        return None

    closes_list = list(closes)
    long_base = closes_list[len(closes_list) - windows["long_window"]]
    mr_base = closes_list[len(closes_list) - windows["mean_reversion_window"]]
    if long_base <= 0 or mr_base <= 0:
        return None

    momentum = current_close / long_base - 1.0
    mean_reversion = -(current_close / mr_base - 1.0)

    vol_start = len(closes_list) - windows["vol_window"]
    returns = []
    for i in range(vol_start, len(closes_list) - 1):
        if closes_list[i] <= 0:
            return None
        returns.append(closes_list[i + 1] / closes_list[i] - 1.0)
    returns.append(current_close / closes_list[-1] - 1.0)
    vol = stddev(returns)
    low_vol = -max(vol, 1e-6)

    volume_slice = list(volumes)[len(volumes) - windows["volume_window"] :]
    avg_vol = sum(volume_slice) / len(volume_slice)
    volume_signal = current_volume / avg_vol - 1.0 if avg_vol > 0 else 0.0

    return momentum, mean_reversion, low_vol, volume_signal


def stddev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def daily_ic(
    factor_values: dict[str, float],
    forward_returns: dict[str, float],
) -> tuple[float | None, int]:
    pairs = [
        (value, forward_returns[symbol])
        for symbol, value in factor_values.items()
        if symbol in forward_returns
        and math.isfinite(value)
        and math.isfinite(forward_returns[symbol])
    ]
    if len(pairs) < 2:
        return None, len(pairs)
    xs = [pair[0] for pair in pairs]
    ys = [pair[1] for pair in pairs]
    return spearman_rank_correlation(xs, ys), len(pairs)


def spearman_rank_correlation(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) != len(ys) or len(xs) < 2:
        return None
    x_ranks = fractional_ranks(xs)
    y_ranks = fractional_ranks(ys)
    return pearson_correlation(x_ranks, y_ranks)


def fractional_ranks(values: list[float]) -> list[float]:
    indexed = list(enumerate(values))
    indexed.sort(key=lambda item: item[1])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(indexed):
        j = i + 1
        while j < len(indexed) and indexed[j][1] == indexed[i][1]:
            j += 1
        avg_rank = (i + 1 + j) / 2.0
        for k in range(i, j):
            ranks[indexed[k][0]] = avg_rank
        i = j
    return ranks


def pearson_correlation(xs: list[float], ys: list[float]) -> float | None:
    if len(xs) != len(ys) or len(xs) < 2:
        return None
    mean_x = sum(xs) / len(xs)
    mean_y = sum(ys) / len(ys)
    cov = 0.0
    var_x = 0.0
    var_y = 0.0
    for x, y in zip(xs, ys):
        dx = x - mean_x
        dy = y - mean_y
        cov += dx * dy
        var_x += dx * dx
        var_y += dy * dy
    if var_x < 1e-12 or var_y < 1e-12:
        return None
    return cov / (math.sqrt(var_x) * math.sqrt(var_y))


def summarize_rolling(
    daily_rows: list[dict[str, object]], rolling_days: int, min_days: int
) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in daily_rows:
        grouped[str(row["factor"])].append(row)

    output: list[dict[str, object]] = []
    for factor, rows in grouped.items():
        rows.sort(key=lambda row: row["date"])
        for idx, start in enumerate(range(0, len(rows), rolling_days), start=1):
            window = rows[start : start + rolling_days]
            if len(window) < min_days:
                continue
            metrics = summarize_ic_values([float(row["ic"]) for row in window])
            if metrics is None:
                continue
            output.append(
                {
                    "factor": factor,
                    "window_index": idx,
                    "start_date": window[0]["date"],
                    "end_date": window[-1]["date"],
                    "n_days": len(window),
                    **metrics,
                }
            )
    return sorted(output, key=lambda row: (row["factor"], row["window_index"]))


def summarize_regimes(
    daily_rows: list[dict[str, object]], min_days: int
) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in daily_rows:
        grouped[str(row["factor"])].append(row)

    output: list[dict[str, object]] = []
    for factor, rows in grouped.items():
        rows.sort(key=lambda row: row["date"])
        for regime, start_raw, end_raw in REGIMES:
            start = parse_date(start_raw)
            end = parse_date(end_raw)
            window = [
                row for row in rows if start <= parse_date(str(row["date"])) <= end
            ]
            if len(window) < min_days:
                continue
            metrics = summarize_ic_values([float(row["ic"]) for row in window])
            if metrics is None:
                continue
            output.append(
                {
                    "regime": regime,
                    "factor": factor,
                    "start_date": start.isoformat(),
                    "end_date": end.isoformat(),
                    "n_days": len(window),
                    **metrics,
                }
            )
    return sorted(output, key=lambda row: (row["regime"], row["factor"]))


def summarize_ic_values(values: list[float]) -> dict[str, float] | None:
    if len(values) < 2:
        return None
    mean_ic = sum(values) / len(values)
    variance = (
        sum((value - mean_ic) ** 2 for value in values) / (len(values) - 1)
        if len(values) > 1
        else 0.0
    )
    std_ic = math.sqrt(variance)
    ic_ir = mean_ic / std_ic if std_ic > 1e-12 else 0.0
    t_stat = ic_ir * math.sqrt(len(values))
    positive_ratio = sum(1 for value in values if value > 0) / len(values)
    return {
        "mean_ic": mean_ic,
        "std_ic": std_ic,
        "ic_ir": ic_ir,
        "t_stat": t_stat,
        "positive_ratio": positive_ratio,
    }


def write_csv(path: Path, headers: list[str], rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_summary(
    path: Path,
    daily_rows: list[dict[str, object]],
    rolling_rows: list[dict[str, object]],
    regime_rows: list[dict[str, object]],
) -> None:
    best_overall = best_metric(rolling_rows, "ic_ir")
    worst_overall = min_metric(rolling_rows, "ic_ir")
    best_regime = best_metric(regime_rows, "ic_ir")
    lines = [
        "# US Factor IC Diagnostics",
        "",
        f"daily_rows={len(daily_rows)}",
        f"rolling_rows={len(rolling_rows)}",
        f"regime_rows={len(regime_rows)}",
        "",
    ]
    if best_overall:
        lines.append(
            "best_rolling="
            f"{best_overall['factor']} "
            f"window={best_overall['window_index']} "
            f"ic_ir={float(best_overall['ic_ir']):.4f} "
            f"t={float(best_overall['t_stat']):.2f}"
        )
    if worst_overall:
        lines.append(
            "worst_rolling="
            f"{worst_overall['factor']} "
            f"window={worst_overall['window_index']} "
            f"ic_ir={float(worst_overall['ic_ir']):.4f} "
            f"t={float(worst_overall['t_stat']):.2f}"
        )
    if best_regime:
        lines.append(
            "best_regime="
            f"{best_regime['regime']} "
            f"{best_regime['factor']} "
            f"ic_ir={float(best_regime['ic_ir']):.4f} "
            f"t={float(best_regime['t_stat']):.2f}"
        )
    lines.extend(
        [
            "",
            "| Factor | Best Rolling IC IR | Worst Rolling IC IR | Best Regime |",
            "| --- | ---: | ---: | --- |",
        ]
    )
    for factor in FACTOR_ORDER:
        factor_rolls = [row for row in rolling_rows if row["factor"] == factor]
        factor_regimes = [row for row in regime_rows if row["factor"] == factor]
        best_roll = best_metric(factor_rolls, "ic_ir")
        worst_roll = min_metric(factor_rolls, "ic_ir")
        best_regime_factor = best_metric(factor_regimes, "ic_ir")
        lines.append(
            "| "
            f"{factor} | "
            f"{format_roll(best_roll)} | "
            f"{format_roll(worst_roll)} | "
            f"{format_regime(best_regime_factor)} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def best_metric(rows: list[dict[str, object]], key: str) -> dict[str, object] | None:
    if not rows:
        return None
    return max(rows, key=lambda row: float(row[key]))


def min_metric(rows: list[dict[str, object]], key: str) -> dict[str, object] | None:
    if not rows:
        return None
    return min(rows, key=lambda row: float(row[key]))


def format_roll(row: dict[str, object] | None) -> str:
    if not row:
        return "-"
    return (
        f"{float(row['ic_ir']):.3f} "
        f"(w{row['window_index']} {row['start_date']}->{row['end_date']})"
    )


def format_regime(row: dict[str, object] | None) -> str:
    if not row:
        return "-"
    return f"{row['regime']} {float(row['ic_ir']):.3f}"


if __name__ == "__main__":
    raise SystemExit(main())
