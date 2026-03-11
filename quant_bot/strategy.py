from __future__ import annotations

from collections import defaultdict, deque
from statistics import fmean

from quant_bot.config import StrategyConfig
from quant_bot.models import Bar


class MomentumTrendStrategy:
    def __init__(self, config: StrategyConfig) -> None:
        self._cfg = config
        self._history: dict[tuple[str, str], deque[float]] = defaultdict(
            lambda: deque(maxlen=self._cfg.long_window)
        )

    def on_bar(self, bar: Bar) -> int:
        key = (bar.market, bar.symbol)
        history = self._history[key]
        history.append(bar.close)
        if len(history) < self._cfg.long_window:
            return 0

        short_ma = fmean(list(history)[-self._cfg.short_window :])
        long_ma = fmean(history)
        momentum = bar.close / history[0] - 1.0

        return int(short_ma > long_ma and momentum >= self._cfg.min_momentum)

    def target_notionals(self, bars: list[Bar], market_budget: float) -> dict[str, float]:
        decisions: dict[str, int] = {}
        for bar in bars:
            decisions[bar.symbol] = self.on_bar(bar)

        active = [symbol for symbol, signal in decisions.items() if signal == 1]
        targets = {bar.symbol: 0.0 for bar in bars}
        if not active:
            return targets

        per_symbol = market_budget / len(active)
        for symbol in active:
            targets[symbol] = per_symbol
        return targets
