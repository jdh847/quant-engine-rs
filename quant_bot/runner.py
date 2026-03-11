from __future__ import annotations

import argparse
import csv
from pathlib import Path

from quant_bot.config import load_config
from quant_bot.data import CSVDataPortal
from quant_bot.engine import QuantBotEngine
from quant_bot.execution import PaperBroker
from quant_bot.risk import UnifiedRiskManager
from quant_bot.strategy import MomentumTrendStrategy


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run private quant bot paper simulation")
    parser.add_argument(
        "--config",
        default="config/bot.toml",
        help="Path to bot configuration file",
    )
    parser.add_argument(
        "--output-dir",
        default="outputs",
        help="Directory for run artifacts",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    cfg = load_config(args.config)

    data_portal = CSVDataPortal(
        market_files={name: mkt.data_file for name, mkt in cfg.markets.items()}
    )
    strategy = MomentumTrendStrategy(cfg.strategy)
    risk = UnifiedRiskManager(cfg.risk)
    broker = PaperBroker(
        starting_cash=cfg.start.starting_capital,
        commission_bps=cfg.execution.commission_bps,
        slippage_bps=cfg.execution.slippage_bps,
    )

    engine = QuantBotEngine(
        config=cfg,
        data_portal=data_portal,
        strategy=strategy,
        risk=risk,
        broker=broker,
    )
    result = engine.run()

    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    write_equity(output_dir / "equity_curve.csv", result.equity_curve)
    write_trades(output_dir / "trades.csv", result.trades)
    write_rejections(output_dir / "rejections.csv", result.rejections)
    write_summary(output_dir / "summary.txt", result)

    if result.equity_curve:
        last = result.equity_curve[-1]
        print(
            "run completed | "
            f"dates={len(result.equity_curve)} trades={len(result.trades)} "
            f"rejections={len(result.rejections)} final_equity={last.equity:.2f}"
        )
    else:
        print("run completed with no data")


def write_equity(path: Path, points: list) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["date", "equity", "cash", "gross_exposure", "net_exposure"])
        for point in points:
            writer.writerow(
                [
                    point.date.isoformat(),
                    f"{point.equity:.4f}",
                    f"{point.cash:.4f}",
                    f"{point.gross_exposure:.4f}",
                    f"{point.net_exposure:.4f}",
                ]
            )


def write_trades(path: Path, trades: list) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["date", "market", "symbol", "side", "qty", "price", "fees"])
        for trade in trades:
            writer.writerow(
                [
                    trade.date.isoformat(),
                    trade.market,
                    trade.symbol,
                    trade.side.value,
                    trade.qty,
                    f"{trade.price:.4f}",
                    f"{trade.fees:.4f}",
                ]
            )


def write_rejections(path: Path, rows: list) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["date", "market", "symbol", "side", "qty", "reason"])
        for row in rows:
            writer.writerow(
                [
                    row.date.isoformat(),
                    row.market,
                    row.symbol,
                    row.side.value,
                    row.qty,
                    row.reason,
                ]
            )


def write_summary(path: Path, result) -> None:
    if not result.equity_curve:
        path.write_text("no data\n", encoding="utf-8")
        return

    start = result.equity_curve[0].equity
    end = result.equity_curve[-1].equity
    pnl = end - start
    pnl_ratio = pnl / start if start else 0.0

    max_equity = start
    max_drawdown = 0.0
    for point in result.equity_curve:
        max_equity = max(max_equity, point.equity)
        drawdown = (max_equity - point.equity) / max_equity if max_equity else 0.0
        max_drawdown = max(max_drawdown, drawdown)

    lines = [
        f"start_equity={start:.2f}",
        f"end_equity={end:.2f}",
        f"pnl={pnl:.2f}",
        f"pnl_ratio={pnl_ratio:.4%}",
        f"max_drawdown={max_drawdown:.4%}",
        f"trades={len(result.trades)}",
        f"rejections={len(result.rejections)}",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
