from __future__ import annotations

from dataclasses import dataclass

from quant_bot.models import Order, Position, Side, Trade


@dataclass
class PaperBroker:
    starting_cash: float
    commission_bps: float
    slippage_bps: float

    def __post_init__(self) -> None:
        self.cash = float(self.starting_cash)
        self.positions: dict[tuple[str, str], Position] = {}

    def get_position(self, market: str, symbol: str) -> Position:
        key = (market, symbol)
        if key not in self.positions:
            self.positions[key] = Position(market=market, symbol=symbol)
        return self.positions[key]

    def _fill_price(self, close: float, side: Side) -> float:
        slip = self.slippage_bps / 10000.0
        if side == Side.BUY:
            return close * (1.0 + slip)
        return close * (1.0 - slip)

    def _fees(self, notional: float) -> float:
        return notional * (self.commission_bps / 10000.0)

    def projected_gross_after_order(
        self,
        order: Order,
        prices: dict[tuple[str, str], float],
    ) -> float:
        current = self.gross_exposure(prices)
        key = (order.market, order.symbol)
        price = prices[key]
        pos = self.get_position(order.market, order.symbol)
        current_symbol = abs(pos.qty * price)

        delta = order.qty if order.side == Side.BUY else -order.qty
        projected_qty = max(0, pos.qty + delta)
        projected_symbol = abs(projected_qty * price)

        return current - current_symbol + projected_symbol

    def execute_orders(
        self,
        orders: list[Order],
        close_prices: dict[tuple[str, str], float],
    ) -> list[Trade]:
        trades: list[Trade] = []

        for order in orders:
            key = (order.market, order.symbol)
            close = close_prices[key]
            fill = self._fill_price(close, order.side)
            pos = self.get_position(order.market, order.symbol)

            if order.side == Side.BUY:
                notional = fill * order.qty
                fees = self._fees(notional)
                total = notional + fees
                if total > self.cash:
                    continue

                new_qty = pos.qty + order.qty
                if new_qty > 0:
                    pos.avg_price = ((pos.avg_price * pos.qty) + notional) / new_qty
                pos.qty = new_qty
                self.cash -= total
                trades.append(
                    Trade(
                        date=order.date,
                        market=order.market,
                        symbol=order.symbol,
                        side=order.side,
                        qty=order.qty,
                        price=fill,
                        fees=fees,
                    )
                )
            else:
                sell_qty = min(order.qty, pos.qty)
                if sell_qty <= 0:
                    continue

                notional = fill * sell_qty
                fees = self._fees(notional)
                pos.qty -= sell_qty
                if pos.qty == 0:
                    pos.avg_price = 0.0
                self.cash += notional - fees
                trades.append(
                    Trade(
                        date=order.date,
                        market=order.market,
                        symbol=order.symbol,
                        side=order.side,
                        qty=sell_qty,
                        price=fill,
                        fees=fees,
                    )
                )

        return trades

    def equity(self, prices: dict[tuple[str, str], float]) -> float:
        return self.cash + self.net_exposure(prices)

    def gross_exposure(self, prices: dict[tuple[str, str], float]) -> float:
        total = 0.0
        for key, pos in self.positions.items():
            if key in prices:
                total += abs(pos.qty * prices[key])
        return total

    def net_exposure(self, prices: dict[tuple[str, str], float]) -> float:
        total = 0.0
        for key, pos in self.positions.items():
            if key in prices:
                total += pos.qty * prices[key]
        return total
