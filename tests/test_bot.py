from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from quant_bot.config import load_config
from quant_bot.data import CSVDataPortal
from quant_bot.engine import QuantBotEngine
from quant_bot.execution import PaperBroker
from quant_bot.risk import UnifiedRiskManager
from quant_bot.strategy import MomentumTrendStrategy


ROOT = Path(__file__).resolve().parents[1]


class QuantBotTests(unittest.TestCase):
    def test_load_config(self) -> None:
        cfg = load_config(ROOT / "config" / "bot.toml")
        self.assertEqual(cfg.start.starting_capital, 1000000)
        self.assertEqual(set(cfg.markets.keys()), {"US", "A", "JP"})

    def test_engine_run_generates_outputs(self) -> None:
        cfg = load_config(ROOT / "config" / "bot.toml")
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

        result = QuantBotEngine(
            config=cfg,
            data_portal=data_portal,
            strategy=strategy,
            risk=risk,
            broker=broker,
        ).run()

        self.assertGreater(len(result.equity_curve), 0)
        self.assertGreater(len(result.trades), 0)

        with tempfile.TemporaryDirectory() as tmp_dir:
            out = Path(tmp_dir) / "run.txt"
            out.write_text(f"equity_points={len(result.equity_curve)}\n", encoding="utf-8")
            self.assertIn("equity_points=", out.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
