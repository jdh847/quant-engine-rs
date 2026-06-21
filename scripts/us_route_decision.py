#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Decide whether a tuned US route should replace current defaults."
    )
    parser.add_argument("--baseline-summary", required=True)
    parser.add_argument("--candidate-summary", required=True)
    parser.add_argument("--candidate-config", required=True)
    parser.add_argument("--target-configs", required=True)
    parser.add_argument("--output", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    baseline = parse_summary(Path(args.baseline_summary))
    candidate = parse_summary(Path(args.candidate_summary))
    candidate_config = Path(args.candidate_config)
    target_configs = [Path(part) for part in args.target_configs.split(",") if part.strip()]
    output = Path(args.output)

    baseline_score = score(baseline)
    candidate_score = score(candidate)
    promote = candidate_score > baseline_score

    lines = [
        f"baseline_score={baseline_score:.6f}",
        f"candidate_score={candidate_score:.6f}",
        f"baseline_pnl_ratio={baseline.get('pnl_ratio', 0.0):.6f}",
        f"candidate_pnl_ratio={candidate.get('pnl_ratio', 0.0):.6f}",
        f"baseline_max_drawdown={baseline.get('max_drawdown', 0.0):.6f}",
        f"candidate_max_drawdown={candidate.get('max_drawdown', 0.0):.6f}",
        f"decision={'promote_candidate' if promote else 'keep_current_defaults'}",
    ]

    if promote:
        for target in target_configs:
            shutil.copyfile(candidate_config, target)
        lines.append("targets_updated=" + ",".join(str(p) for p in target_configs))
    else:
        lines.append("targets_updated=")

    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(lines[-2])
    return 0


def parse_summary(path: Path) -> dict[str, float]:
    out: dict[str, float] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        out[key.strip()] = parse_float(value.strip())
    return out


def parse_float(raw: str) -> float:
    text = raw.strip().rstrip("%")
    if not text:
        return 0.0
    try:
        value = float(text)
    except ValueError:
        return 0.0
    if raw.strip().endswith("%"):
        return value / 100.0
    return value


def score(summary: dict[str, float]) -> float:
    return (
        summary.get("pnl_ratio", 0.0)
        + summary.get("sharpe", 0.0) * 0.12
        + summary.get("sortino", 0.0) * 0.06
        + summary.get("calmar", 0.0) * 0.04
        - summary.get("max_drawdown", 0.0) * 0.9
    )


if __name__ == "__main__":
    raise SystemExit(main())
