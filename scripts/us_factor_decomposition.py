#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
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
        description="Run US factor decomposition research on layered_multi_factor."
    )
    parser.add_argument("--config", default="config/bot_us.toml")
    parser.add_argument("--output-dir", default="outputs_rust/research_us_factor_decomp")
    parser.add_argument("--engine-bin", default="")
    parser.add_argument("--short-window", type=int, default=3)
    parser.add_argument("--long-window", type=int, default=7)
    parser.add_argument("--vol-window", type=int, default=5)
    parser.add_argument("--top-n", type=int, default=1)
    parser.add_argument("--min-momentums", default="-0.01,0.0")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    base_config = Path(args.config)
    if not base_config.exists():
        raise SystemExit(f"config not found: {base_config}")

    records: list[dict[str, object]] = []
    for profile, w_m, w_r, w_lv, w_v in FACTOR_PROFILES:
        profile_dir = out_dir / profile
        profile_dir.mkdir(parents=True, exist_ok=True)
        profile_cfg = profile_dir / "config.toml"

        cfg_text = patch_strategy_block(
            base_config.read_text(encoding="utf-8"),
            {
                "strategy_plugin": '"layered_multi_factor"',
                "portfolio_method": '"risk_parity"',
                "factor_momentum_weight": f"{w_m:.2f}",
                "factor_mean_reversion_weight": f"{w_r:.2f}",
                "factor_low_vol_weight": f"{w_lv:.2f}",
                "factor_volume_weight": f"{w_v:.2f}",
            },
        )
        profile_cfg.write_text(cfg_text, encoding="utf-8")

        cmd = build_research_cmd(
            args.engine_bin,
            profile_cfg,
            profile_dir,
            args.short_window,
            args.long_window,
            args.vol_window,
            args.top_n,
            args.min_momentums,
        )
        run_command(cmd)

        row = load_us_top_row(profile_dir / "research_leaderboard.csv")
        row["profile"] = profile
        row["factor_momentum_weight"] = w_m
        row["factor_mean_reversion_weight"] = w_r
        row["factor_low_vol_weight"] = w_lv
        row["factor_volume_weight"] = w_v
        records.append(row)

    summary_csv = out_dir / "factor_decomposition_us.csv"
    summary_md = out_dir / "factor_decomposition_us.md"
    write_summary(records, summary_csv, summary_md)

    print(f"factor decomposition completed | rows={len(records)} csv={summary_csv}")
    return 0


def patch_strategy_block(text: str, updates: dict[str, str]) -> str:
    lines = text.splitlines(keepends=True)
    out: list[str] = []
    in_strategy = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            in_strategy = stripped == "[strategy]"
            out.append(line)
            continue
        if in_strategy:
            replaced = False
            for key, value in updates.items():
                if stripped.startswith(f"{key} ="):
                    out.append(f"{key} = {value}\n")
                    replaced = True
                    break
            if replaced:
                continue
        out.append(line)
    return "".join(out)


def build_research_cmd(
    engine_bin: str,
    config_path: Path,
    output_dir: Path,
    short_window: int,
    long_window: int,
    vol_window: int,
    top_n: int,
    min_momentums: str,
) -> list[str]:
    tail = [
        "research",
        "--config",
        str(config_path),
        "--output-dir",
        str(output_dir),
        "--markets",
        "US",
        "--short-windows",
        str(short_window),
        "--long-windows",
        str(long_window),
        "--vol-windows",
        str(vol_window),
        "--top-ns",
        str(top_n),
        "--min-momentums",
        min_momentums,
        "--strategy-plugins",
        "layered_multi_factor",
        "--portfolio-methods",
        "risk_parity",
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


def load_us_top_row(path: Path) -> dict[str, object]:
    if not path.exists():
        raise SystemExit(f"missing research leaderboard: {path}")
    rows = list(csv.DictReader(path.open("r", encoding="utf-8")))
    us_rows = [row for row in rows if row.get("scenario") == "US"]
    target = us_rows[0] if us_rows else rows[0]
    return {
        "scenario": target.get("scenario", ""),
        "score": float(target.get("score", "0") or 0.0),
        "pnl_ratio": float(target.get("pnl_ratio", "0") or 0.0),
        "max_drawdown": float(target.get("max_drawdown", "0") or 0.0),
        "sharpe": float(target.get("sharpe", "0") or 0.0),
        "sortino": float(target.get("sortino", "0") or 0.0),
        "calmar": float(target.get("calmar", "0") or 0.0),
        "daily_win_rate": float(target.get("daily_win_rate", "0") or 0.0),
        "profit_factor": float(target.get("profit_factor", "0") or 0.0),
        "trades": int(target.get("trades", "0") or 0),
        "rejections": int(target.get("rejections", "0") or 0),
    }


def write_summary(rows: list[dict[str, object]], csv_path: Path, md_path: Path) -> None:
    baseline = next((r for r in rows if r["profile"] == "all_factors"), None)
    if baseline is None:
        raise SystemExit("all_factors baseline is missing")

    headers = [
        "profile",
        "scenario",
        "factor_momentum_weight",
        "factor_mean_reversion_weight",
        "factor_low_vol_weight",
        "factor_volume_weight",
        "score",
        "pnl_ratio",
        "max_drawdown",
        "sharpe",
        "sortino",
        "calmar",
        "daily_win_rate",
        "profit_factor",
        "trades",
        "rejections",
        "delta_score_vs_all",
        "delta_pnl_vs_all",
        "delta_sharpe_vs_all",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            row = dict(row)
            row["delta_score_vs_all"] = float(row["score"]) - float(baseline["score"])
            row["delta_pnl_vs_all"] = float(row["pnl_ratio"]) - float(baseline["pnl_ratio"])
            row["delta_sharpe_vs_all"] = float(row["sharpe"]) - float(baseline["sharpe"])
            writer.writerow(row)

    lines = [
        "# US Factor Decomposition",
        "",
        "| Profile | Score | PnL | MaxDD | Sharpe | Trades | Delta Score vs All |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        ds = float(row["score"]) - float(baseline["score"])
        lines.append(
            "| {profile} | {score:.6f} | {pnl:.6f} | {dd:.6f} | {sharpe:.6f} | {trades} | {ds:.6f} |".format(
                profile=row["profile"],
                score=float(row["score"]),
                pnl=float(row["pnl_ratio"]),
                dd=float(row["max_drawdown"]),
                sharpe=float(row["sharpe"]),
                trades=int(row["trades"]),
                ds=ds,
            )
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
