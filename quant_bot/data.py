from __future__ import annotations

from collections import defaultdict
import csv
from datetime import date
from pathlib import Path

from quant_bot.models import Bar


class CSVDataPortal:
    def __init__(self, market_files: dict[str, Path]) -> None:
        self._bars_by_day: dict[date, dict[str, list[Bar]]] = defaultdict(lambda: defaultdict(list))
        self._all_dates: set[date] = set()
        self._load_files(market_files)

    def _load_files(self, market_files: dict[str, Path]) -> None:
        for market, path in market_files.items():
            with path.open("r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    bar_date = date.fromisoformat(row["date"])
                    bar = Bar(
                        date=bar_date,
                        market=market,
                        symbol=row["symbol"],
                        close=float(row["close"]),
                        volume=float(row["volume"]),
                    )
                    self._bars_by_day[bar_date][market].append(bar)
                    self._all_dates.add(bar_date)

        for bar_date in self._all_dates:
            for market in self._bars_by_day[bar_date]:
                self._bars_by_day[bar_date][market].sort(key=lambda item: item.symbol)

    def trading_dates(self) -> list[date]:
        return sorted(self._all_dates)

    def bars_for(self, bar_date: date, market: str) -> list[Bar]:
        return list(self._bars_by_day.get(bar_date, {}).get(market, []))
