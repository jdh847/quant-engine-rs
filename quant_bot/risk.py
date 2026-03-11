from __future__ import annotations

from quant_bot.config import RiskConfig
from quant_bot.execution import PaperBroker
from quant_bot.models import Order, RiskRejection, Side


class UnifiedRiskManager:
    def __init__(self, config: RiskConfig) -> None:
        self._cfg = config
        self._start_day_equity = 0.0
        self._daily_locked = False

    def start_day(self, equity: float) -> None:
        self._start_day_equity = equity
        self._daily_locked = False

    def _update_daily_lock(self, current_equity: float) -> None:
        if self._start_day_equity <= 0:
            return
        drawdown_ratio = 1.0 - (current_equity / self._start_day_equity)
        if drawdown_ratio >= self._cfg.daily_loss_limit_ratio:
            self._daily_locked = True

    def filter_orders(
        self,
        orders: list[Order],
        broker: PaperBroker,
        prices: dict[tuple[str, str], float],
        equity: float,
    ) -> tuple[list[Order], list[RiskRejection]]:
        accepted: list[Order] = []
        rejected: list[RiskRejection] = []

        self._update_daily_lock(current_equity=equity)
        gross_limit = self._cfg.max_gross_exposure_ratio * equity
        symbol_limit = self._cfg.max_symbol_weight * equity

        for order in orders:
            key = (order.market, order.symbol)
            px = prices[key]
            pos = broker.get_position(order.market, order.symbol)

            if self._daily_locked and order.side == Side.BUY:
                rejected.append(
                    RiskRejection(
                        date=order.date,
                        market=order.market,
                        symbol=order.symbol,
                        side=order.side,
                        qty=order.qty,
                        reason="daily loss lock: only reducing trades allowed",
                    )
                )
                continue

            delta = order.qty if order.side == Side.BUY else -order.qty
            projected_qty = max(0, pos.qty + delta)
            projected_symbol_notional = abs(projected_qty * px)
            if projected_symbol_notional > symbol_limit:
                rejected.append(
                    RiskRejection(
                        date=order.date,
                        market=order.market,
                        symbol=order.symbol,
                        side=order.side,
                        qty=order.qty,
                        reason="symbol weight breach",
                    )
                )
                continue

            projected_gross = broker.projected_gross_after_order(order, prices)
            if projected_gross > gross_limit:
                rejected.append(
                    RiskRejection(
                        date=order.date,
                        market=order.market,
                        symbol=order.symbol,
                        side=order.side,
                        qty=order.qty,
                        reason="gross exposure breach",
                    )
                )
                continue

            accepted.append(order)

        return accepted, rejected
