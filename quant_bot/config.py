from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class StartConfig:
    starting_capital: float
    base_currency: str


@dataclass(frozen=True)
class StrategyConfig:
    short_window: int
    long_window: int
    min_momentum: float


@dataclass(frozen=True)
class RiskConfig:
    max_gross_exposure_ratio: float
    max_symbol_weight: float
    daily_loss_limit_ratio: float


@dataclass(frozen=True)
class ExecutionConfig:
    commission_bps: float
    slippage_bps: float


@dataclass(frozen=True)
class MarketConfig:
    name: str
    allocation: float
    data_file: Path
    lot_size: int
    min_trade_notional: float


@dataclass(frozen=True)
class BotConfig:
    start: StartConfig
    strategy: StrategyConfig
    risk: RiskConfig
    execution: ExecutionConfig
    markets: dict[str, MarketConfig]


def load_config(path: str | Path) -> BotConfig:
    config_path = Path(path).expanduser().resolve()
    payload = _parse_simple_toml(config_path.read_text(encoding="utf-8"))

    start_cfg = payload["start"]
    strategy_cfg = payload["strategy"]
    risk_cfg = payload["risk"]
    execution_cfg = payload["execution"]
    market_cfg = payload["markets"]

    markets: dict[str, MarketConfig] = {}
    for market_name, market_data in market_cfg.items():
        markets[market_name] = MarketConfig(
            name=market_name,
            allocation=float(market_data["allocation"]),
            data_file=(config_path.parent.parent / market_data["data_file"]).resolve(),
            lot_size=int(market_data["lot_size"]),
            min_trade_notional=float(market_data.get("min_trade_notional", 0.0)),
        )
        if markets[market_name].lot_size <= 0:
            raise ValueError(f"lot_size must be positive for market {market_name}")

    allocation_sum = sum(item.allocation for item in markets.values())
    if abs(allocation_sum - 1.0) > 1e-6:
        raise ValueError(f"market allocations must sum to 1.0, got {allocation_sum}")

    strategy = StrategyConfig(
        short_window=int(strategy_cfg["short_window"]),
        long_window=int(strategy_cfg["long_window"]),
        min_momentum=float(strategy_cfg["min_momentum"]),
    )
    if strategy.short_window >= strategy.long_window:
        raise ValueError("strategy short_window must be smaller than long_window")

    return BotConfig(
        start=StartConfig(
            starting_capital=float(start_cfg["starting_capital"]),
            base_currency=str(start_cfg["base_currency"]),
        ),
        strategy=strategy,
        risk=RiskConfig(
            max_gross_exposure_ratio=float(risk_cfg["max_gross_exposure_ratio"]),
            max_symbol_weight=float(risk_cfg["max_symbol_weight"]),
            daily_loss_limit_ratio=float(risk_cfg["daily_loss_limit_ratio"]),
        ),
        execution=ExecutionConfig(
            commission_bps=float(execution_cfg["commission_bps"]),
            slippage_bps=float(execution_cfg["slippage_bps"]),
        ),
        markets=markets,
    )


def _parse_simple_toml(text: str) -> dict[str, Any]:
    root: dict[str, Any] = {}
    current: dict[str, Any] = root

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("[") and line.endswith("]"):
            section_name = line[1:-1].strip()
            current = root
            for part in section_name.split("."):
                current = current.setdefault(part, {})
            continue

        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        current[key.strip()] = _parse_toml_value(value.strip())

    return root


def _parse_toml_value(raw: str) -> Any:
    if raw.startswith('"') and raw.endswith('"'):
        return raw[1:-1]

    lowered = raw.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False

    try:
        if any(token in raw for token in (".", "e", "E")):
            return float(raw)
        return int(raw)
    except ValueError:
        return raw
