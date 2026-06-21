#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Update config defaults from the top US research row."
    )
    parser.add_argument("--leaderboard", required=True)
    parser.add_argument("--config-in", required=True)
    parser.add_argument("--config-out", required=True)
    parser.add_argument("--summary-out", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    leaderboard = Path(args.leaderboard)
    config_in = Path(args.config_in)
    config_out = Path(args.config_out)
    summary_out = Path(args.summary_out) if args.summary_out else None

    rows = list(csv.DictReader(leaderboard.open("r", encoding="utf-8")))
    us_rows = [row for row in rows if (row.get("scenario") or "").upper() == "US"]
    if not us_rows:
        raise SystemExit(f"no US rows found in {leaderboard}")
    top = us_rows[0]

    patched = patch_config(
        config_in.read_text(encoding="utf-8"),
        {
            "strategy_plugin": quote(top["strategy_plugin"]),
            "short_window": top["short_window"],
            "long_window": top["long_window"],
            "vol_window": top["vol_window"],
            "top_n": top["top_n"],
            "min_momentum": top["min_momentum"],
            "portfolio_method": quote(top["portfolio_method"]),
        },
        {
            "strategy_plugin": quote(top["strategy_plugin"]),
            "portfolio_method": quote(top["portfolio_method"]),
        },
    )
    config_out.write_text(patched, encoding="utf-8")

    if summary_out is not None:
        summary_out.write_text(
            "\n".join(
                [
                    f"scenario={top['scenario']}",
                    f"strategy_plugin={top['strategy_plugin']}",
                    f"portfolio_method={top['portfolio_method']}",
                    f"short_window={top['short_window']}",
                    f"long_window={top['long_window']}",
                    f"vol_window={top['vol_window']}",
                    f"top_n={top['top_n']}",
                    f"min_momentum={top['min_momentum']}",
                    f"score={top['score']}",
                    f"pnl_ratio={top['pnl_ratio']}",
                    f"sharpe={top['sharpe']}",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

    print(
        f"updated defaults | plugin={top['strategy_plugin']} method={top['portfolio_method']} short={top['short_window']} long={top['long_window']} vol={top['vol_window']} top_n={top['top_n']} min_momentum={top['min_momentum']}"
    )
    return 0


def patch_config(
    text: str, strategy_updates: dict[str, str], routing_updates: dict[str, str]
) -> str:
    lines = text.splitlines(keepends=True)
    out: list[str] = []
    section = ""
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            section = stripped[1:-1].strip()
            out.append(line)
            continue
        if section == "strategy":
            replaced = replace_line(stripped, strategy_updates)
            if replaced is not None:
                out.append(replaced)
                continue
        if section == "strategy.market_routing.US":
            replaced = replace_line(stripped, routing_updates)
            if replaced is not None:
                out.append(replaced)
                continue
        out.append(line)
    return "".join(out)


def replace_line(stripped: str, updates: dict[str, str]) -> str | None:
    for key, value in updates.items():
        if stripped.startswith(f"{key} ="):
            return f"{key} = {value}\n"
    return None


def quote(value: str) -> str:
    return '"' + value + '"'


if __name__ == "__main__":
    raise SystemExit(main())
