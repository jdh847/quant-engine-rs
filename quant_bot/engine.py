from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from quant_bot.config import BotConfig
from quant_bot.data import CSVDataPortal
from quant_bot.execution import PaperBroker
from quant_bot.models import Bar, EquityPoint, Order, RiskRejection, Side, Trade
from quant_bot.risk import UnifiedRiskManager
from quant_bot.strategy import MomentumTrendStrategy


@dataclass(frozen=True)
class RunResult:
    equity_curve: list[EquityPoint]
    trades: list[Trade]
    rejections: list[RiskRejection]


class QuantBotEngine:
    def __init__(
        self,
        config: BotConfig,
        data_portal: CSVDataPortal,
        strategy: MomentumTrendStrategy,
        risk: UnifiedRiskManager,
        broker: PaperBroker,
    ) -> None:
        self._cfg = config
        self._data = data_portal
        self._strategy = strategy
        self._risk = risk
        self._broker = broker

    def run(self) -> RunResult:
        equity_curve: list[EquityPoint] = []
        trades: list[Trade] = []
        rejections: list[RiskRejection] = []
        prices: dict[tuple[str, str], float] = {}

        for bar_date in self._data.trading_dates():
            equity_before = self._broker.equity(prices)
            self._risk.start_day(equity_before)

            for market_name, market_cfg in self._cfg.markets.items():
                bars = self._data.bars_for(bar_date, market_name)
                if not bars:
                    continue

                for bar in bars:
                    prices[(bar.market, bar.symbol)] = bar.close

                equity_now = self._broker.equity(prices)
                market_budget = equity_now * market_cfg.allocation
                target_notionals = self._strategy.target_notionals(bars, market_budget)
                proposed_orders = self._orders_from_targets(
                    bar_date=bar_date,
                    bars=bars,
                    target_notionals=target_notionals,
                    lot_size=market_cfg.lot_size,
                    min_trade_notional=market_cfg.min_trade_notional,
                )

                accepted, blocked = self._risk.filter_orders(
                    orders=proposed_orders,
                    broker=self._broker,
                    prices=prices,
                    equity=equity_now,
                )
                rejections.extend(blocked)
                trades.extend(self._broker.execute_orders(accepted, prices))

            equity_after = self._broker.equity(prices)
            equity_curve.append(
                EquityPoint(
                    date=bar_date,
                    equity=equity_after,
                    cash=self._broker.cash,
                    gross_exposure=self._broker.gross_exposure(prices),
                    net_exposure=self._broker.net_exposure(prices),
                )
            )

        return RunResult(equity_curve=equity_curve, trades=trades, rejections=rejections)

    def _orders_from_targets(
        self,
        bar_date: date,
        bars: list[Bar],
        target_notionals: dict[str, float],
        lot_size: int,
        min_trade_notional: float,
    ) -> list[Order]:
        orders: list[Order] = []

        for bar in bars:
            target_notional = target_notionals.get(bar.symbol, 0.0)
            raw_target_qty = int(target_notional / bar.close)
            target_qty = (raw_target_qty // lot_size) * lot_size
            pos = self._broker.get_position(bar.market, bar.symbol)
            delta = target_qty - pos.qty
            delta_notional = abs(delta * bar.close)
            if delta != 0 and delta_notional < min_trade_notional:
                continue

            if delta > 0:
                orders.append(
                    Order(
                        date=bar_date,
                        market=bar.market,
                        symbol=bar.symbol,
                        side=Side.BUY,
                        qty=delta,
                    )
                )
            elif delta < 0:
                orders.append(
                    Order(
                        date=bar_date,
                        market=bar.market,
                        symbol=bar.symbol,
                        side=Side.SELL,
                        qty=abs(delta),
                    )
                )

        return orders
