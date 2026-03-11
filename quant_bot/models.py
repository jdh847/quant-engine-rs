from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from enum import Enum


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True)
class Bar:
    date: date
    market: str
    symbol: str
    close: float
    volume: float


@dataclass(frozen=True)
class Order:
    date: date
    market: str
    symbol: str
    side: Side
    qty: int


@dataclass
class Position:
    market: str
    symbol: str
    qty: int = 0
    avg_price: float = 0.0


@dataclass(frozen=True)
class Trade:
    date: date
    market: str
    symbol: str
    side: Side
    qty: int
    price: float
    fees: float


@dataclass(frozen=True)
class RiskRejection:
    date: date
    market: str
    symbol: str
    side: Side
    qty: int
    reason: str


@dataclass(frozen=True)
class EquityPoint:
    date: date
    equity: float
    cash: float
    gross_exposure: float
    net_exposure: float
