#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import argparse
import csv
import datetime as dt
import json
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen


US_SYMBOLS = [
    "AAPL",
    "MSFT",
    "NVDA",
    "AMZN",
    "GOOGL",
    "META",
    "JPM",
    "XOM",
    "LLY",
    "COST",
    "UNH",
    "HD",
]

A_SHARE_SYMBOLS = [
    "600519",
    "000001",
    "300750",
    "601318",
    "000333",
    "600036",
    "002594",
    "600276",
    "000858",
    "601899",
    "600030",
    "600887",
]

JP_SYMBOLS = [
    "7203",
    "6758",
    "9984",
    "8306",
    "8035",
    "6501",
    "9432",
    "6861",
    "4063",
    "6098",
    "7974",
    "7267",
]

DEFAULT_MARKET_ALLOCATIONS = {
    "US": 0.50,
    "A": 0.30,
    "JP": 0.20,
}

SOURCE_LABEL_DESCRIPTION = {
    "yfinance": "Yahoo Finance chart endpoint",
    "eastmoney": "Eastmoney kline endpoint compatible with AKShare's `stock_zh_a_hist`",
}


@dataclass(frozen=True)
class MarketSpec:
    name: str
    symbols: list[str]
    output_csv: Path
    output_industries: Path
    source_label: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch real historical bars into the repo's CSV schema."
    )
    parser.add_argument("--output-dir", default="data_real")
    parser.add_argument("--config-output", default="config/bot_real.toml")
    parser.add_argument(
        "--markets",
        default="US,A,JP",
        help="comma-separated markets to refresh (US,A,JP)",
    )
    parser.add_argument(
        "--us-universe-file",
        default="",
        help="optional CSV file with columns `symbol,industry` for US universe",
    )
    parser.add_argument(
        "--us-max-symbols",
        type=int,
        default=0,
        help="optional cap for US symbols after loading defaults/file (0 = no cap)",
    )
    parser.add_argument(
        "--manifest-output",
        default="",
        help="write a machine-readable lineage manifest",
    )
    parser.add_argument("--start-date", default="2024-01-02")
    parser.add_argument("--end-date", default="2026-04-01")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start = parse_date(args.start_date)
    end = parse_date(args.end_date)
    if start > end:
        raise SystemExit("start-date must be <= end-date")
    if args.us_max_symbols < 0:
        raise SystemExit("us-max-symbols must be >= 0")

    out_dir = Path(args.output_dir)
    config_path = Path(args.config_output)
    markets = parse_markets(args.markets)
    manifest_path = (
        Path(args.manifest_output)
        if args.manifest_output
        else out_dir / "DATASET_MANIFEST.json"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    config_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    us_symbols = list(US_SYMBOLS)
    us_industries = industry_map("US")
    us_meta: dict[str, object] = {}
    if args.us_universe_file:
        universe_path = Path(args.us_universe_file)
        us_symbols, us_industries = load_us_universe_file(universe_path)
        us_meta["universe_file"] = str(universe_path)
        us_meta["universe_sha256"] = sha256_file(universe_path)

    if args.us_max_symbols > 0:
        us_symbols = us_symbols[: args.us_max_symbols]
        us_industries = {symbol: us_industries.get(symbol, "Other") for symbol in us_symbols}
        us_meta["us_max_symbols"] = args.us_max_symbols

    market_specs_all = {
        "US": MarketSpec(
            name="US",
            symbols=us_symbols,
            output_csv=out_dir / "us_equities.csv",
            output_industries=out_dir / "us_industries.csv",
            source_label="yfinance",
        ),
        "A": MarketSpec(
            name="A",
            symbols=A_SHARE_SYMBOLS,
            output_csv=out_dir / "a_share.csv",
            output_industries=out_dir / "a_industries.csv",
            source_label="eastmoney",
        ),
        "JP": MarketSpec(
            name="JP",
            symbols=JP_SYMBOLS,
            output_csv=out_dir / "jp_equities.csv",
            output_industries=out_dir / "jp_industries.csv",
            source_label="yfinance",
        ),
    }
    market_industries_all = {
        "US": us_industries,
        "A": industry_map("A"),
        "JP": industry_map("JP"),
    }
    market_meta_all = {
        "US": us_meta,
        "A": {},
        "JP": {},
    }
    specs = [market_specs_all[name] for name in markets]

    if not args.force:
        existing = [
            p
            for p in [config_path, manifest_path]
            if p.exists()
        ]
        existing.extend(
            p
            for spec in specs
            for p in [spec.output_csv, spec.output_industries]
            if p.exists()
        )
        if existing:
            raise SystemExit(
                "refusing to overwrite existing files; pass --force to refresh real historical data"
            )

    generated = []
    for spec in specs:
        print(f"fetching {spec.name} via {spec.source_label}")
        rows = fetch_market_rows(spec.name, spec.symbols, start, end)
        write_market_csv(spec.output_csv, rows)
        write_industry_csv(
            spec.output_industries,
            spec.symbols,
            market_industries_all.get(spec.name, {}),
        )
        generated.append((spec.name, len(rows)))

    write_readme(out_dir, start, end, specs, generated)
    write_manifest_json(
        manifest_path,
        start,
        end,
        specs,
        generated,
        market_meta_all,
    )
    write_config_file(config_path, out_dir, markets)

    print("done")
    for market, rows in generated:
        print(f"{market}: {rows} rows")
    print(f"manifest: {manifest_path}")
    print(f"config: {config_path}")
    return 0


def fetch_market_rows(
    market: str, symbols: list[str], start: dt.date, end: dt.date
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for symbol in symbols:
        if market == "A":
            symbol_rows = fetch_a_share(symbol, start, end)
        else:
            symbol_rows = fetch_yahoo(symbol, market, start, end)
        rows.extend(symbol_rows)
        time.sleep(0.15)
    rows.sort(key=lambda row: (row["date"], row["symbol"]))
    return rows


def fetch_yahoo(symbol: str, market: str, start: dt.date, end: dt.date) -> list[dict[str, object]]:
    yf = load_yfinance()
    yahoo_symbol = yahoo_symbol_for(symbol, market)
    df = yf.download(
        yahoo_symbol,
        start=start.isoformat(),
        end=(end + dt.timedelta(days=1)).isoformat(),
        interval="1d",
        auto_adjust=False,
        progress=False,
        threads=False,
    )
    if df.empty:
        raise RuntimeError(f"no Yahoo data returned for {yahoo_symbol}")
    if getattr(df.columns, "nlevels", 1) > 1:
        try:
            df = df.xs(yahoo_symbol, axis=1, level=1)
        except Exception:
            df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]

    out: list[dict[str, object]] = []
    for day, row in df.iterrows():
        if hasattr(day, "date"):
            day = day.date()
        if not isinstance(day, dt.date):
            continue
        if day < start or day > end:
            continue
        close = parse_float(row.get("Close"))
        volume = parse_float(row.get("Volume"))
        adj_close = parse_float(row.get("Adj Close"))
        if close is None or volume is None:
            continue
        if adj_close is None:
            adj_close = close
        out.append(
            {
                "date": day.isoformat(),
                "symbol": symbol,
                "close": round(close, 4),
                "adj_close": round(adj_close, 4),
                "volume": round(volume, 2),
            }
        )
    if not out:
        raise RuntimeError(f"no Yahoo data returned for {yahoo_symbol}")
    return out


def load_yfinance():
    try:
        import yfinance as yf  # type: ignore
    except ImportError as exc:  # pragma: no cover - dependency message only
        raise RuntimeError(
            "yfinance is required for US/JP real-history refresh. "
            "Install it with `pip install yfinance` and try again."
        ) from exc
    return yf


def fetch_a_share(symbol: str, start: dt.date, end: dt.date) -> list[dict[str, object]]:
    secid = f"{a_share_exchange_id(symbol)}.{symbol}"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": "101",
        "fqt": "1",
        "secid": secid,
        "beg": start.strftime("%Y%m%d"),
        "end": end.strftime("%Y%m%d"),
        "_": str(int(time.time() * 1000)),
    }
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get?" + urlencode(params)
    payload = json.loads(http_get_text(url))
    klines = payload.get("data", {}).get("klines") or []
    out: list[dict[str, object]] = []
    for raw in klines:
        parts = raw.split(",")
        if len(parts) < 6:
            continue
        day = parse_date(parts[0])
        if day < start or day > end:
            continue
        open_ = parse_float(parts[1])
        close = parse_float(parts[2])
        high = parse_float(parts[3])
        low = parse_float(parts[4])
        volume = parse_float(parts[5])
        if close is None or volume is None:
            continue
        # Keep close/adj_close aligned because we fetch the adjusted Eastmoney series.
        out.append(
            {
                "date": day.isoformat(),
                "symbol": symbol,
                "close": round(close, 4),
                "adj_close": round(close, 4),
                "volume": round(volume, 2),
                "_open": open_,
                "_high": high,
                "_low": low,
            }
        )
    if not out:
        raise RuntimeError(f"no Eastmoney data returned for {symbol} ({secid})")
    return out


def write_market_csv(path: Path, rows: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["date", "symbol", "close", "adj_close", "volume"])
        for row in rows:
            writer.writerow(
                [
                    row["date"],
                    row["symbol"],
                    row["close"],
                    row["adj_close"],
                    row["volume"],
                ]
            )


def write_industry_csv(path: Path, symbols: list[str], industries: dict[str, str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["symbol", "industry"])
        for symbol in symbols:
            writer.writerow([symbol, industries.get(symbol, "Other")])


def write_readme(
    out_dir: Path,
    start: dt.date,
    end: dt.date,
    specs: list[MarketSpec],
    generated: list[tuple[str, int]],
) -> None:
    source_descriptions = []
    seen_sources: set[str] = set()
    for spec in specs:
        if spec.source_label in seen_sources:
            continue
        seen_sources.add(spec.source_label)
        source_descriptions.append(
            f"- {spec.name}: {SOURCE_LABEL_DESCRIPTION.get(spec.source_label, spec.source_label)}"
        )

    lines = [
        "# Real Historical Dataset",
        "",
        "Generated by `scripts/refresh_real_historical_data.py`.",
        "",
        "Sources:",
        *source_descriptions,
        "",
        "Schema:",
        "- `date,symbol,close,adj_close,volume`",
        "- `DATASET_MANIFEST.json` records hashes and source lineage for the refresh.",
        "",
        f"Date range: {start.isoformat()} .. {end.isoformat()}",
        "",
        "Markets:",
    ]
    for market, rows in generated:
        lines.append(f"- {market}: {rows} rows")
    (out_dir / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_manifest_json(
    path: Path,
    start: dt.date,
    end: dt.date,
    specs: list[MarketSpec],
    generated: list[tuple[str, int]],
    market_meta_all: dict[str, dict[str, object]],
) -> None:
    generated_map = dict(generated)
    payload = {
        "generated_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source_script": "scripts/refresh_real_historical_data.py",
        "date_range": {
            "start": start.isoformat(),
            "end": end.isoformat(),
        },
        "markets": [],
    }
    for spec in specs:
        csv_sha256 = sha256_file(spec.output_csv)
        industries_sha256 = sha256_file(spec.output_industries)
        payload["markets"].append(
            {
                "market": spec.name,
                "source_label": spec.source_label,
                "symbols": list(spec.symbols),
                "rows": generated_map.get(spec.name, 0),
                "output_csv": str(spec.output_csv),
                "output_industries": str(spec.output_industries),
                "csv_sha256": csv_sha256,
                "industries_sha256": industries_sha256,
                **market_meta_all.get(spec.name, {}),
            }
        )
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def yahoo_symbol_for(symbol: str, market: str) -> str:
    if market == "US":
        # Yahoo uses '-' for US tickers with share classes (e.g. BRK.B -> BRK-B).
        return symbol.replace(".", "-")
    if market == "JP":
        return f"{symbol}.T"
    raise ValueError(f"unsupported yahoo market: {market}")


def write_config_file(config_path: Path, data_root: Path, markets: list[str]) -> None:
    src = Path("config/bot.toml").read_text(encoding="utf-8")
    out = src.replace('data/us_equities.csv', f'{data_root.name}/us_equities.csv')
    out = out.replace('data/a_share.csv', f'{data_root.name}/a_share.csv')
    out = out.replace('data/jp_equities.csv', f'{data_root.name}/jp_equities.csv')
    out = out.replace('data/us_industries.csv', f'{data_root.name}/us_industries.csv')
    out = out.replace('data/a_industries.csv', f'{data_root.name}/a_industries.csv')
    out = out.replace('data/jp_industries.csv', f'{data_root.name}/jp_industries.csv')
    out = out.replace('min_fee = 5.0\n\n[markets.JP]', 'min_fee = 5.0\n\ngap_days_threshold = 15\n\n[markets.JP]')
    out = filter_config_to_markets(out, markets)
    out = normalize_market_allocations(out, markets)
    config_path.write_text(out, encoding="utf-8")


def parse_markets(raw: str) -> list[str]:
    aliases = {
        "US": "US",
        "A": "A",
        "CN": "A",
        "A_SHARE": "A",
        "ASHARE": "A",
        "JP": "JP",
        "JP_STOCK": "JP",
    }
    out: list[str] = []
    for part in raw.split(","):
        token = part.strip().upper()
        if not token:
            continue
        market = aliases.get(token)
        if market is None:
            raise SystemExit(f"unsupported market token: {part!r}; allowed: US,A,JP")
        if market not in out:
            out.append(market)
    if not out:
        raise SystemExit("at least one market must be selected")
    return out


def filter_config_to_markets(text: str, markets: list[str]) -> str:
    keep = set(markets)
    out_lines: list[str] = []
    keep_section = True
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            section = stripped[1:-1].strip()
            keep_section = True
            if section.startswith("markets."):
                market = section.split(".", 1)[1]
                keep_section = market in keep
            elif section.startswith("strategy.market_routing."):
                market = section.split(".", 2)[2]
                keep_section = market in keep
        if keep_section:
            out_lines.append(line)
    return "".join(out_lines)


def normalize_market_allocations(text: str, markets: list[str]) -> str:
    base = [DEFAULT_MARKET_ALLOCATIONS.get(m, 0.0) for m in markets]
    total = sum(base)
    if total <= 0:
        return text
    normalized = {
        m: DEFAULT_MARKET_ALLOCATIONS.get(m, 0.0) / total
        for m in markets
    }

    out_lines: list[str] = []
    current_market = ""
    for line in text.splitlines(keepends=True):
        stripped = line.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            section = stripped[1:-1].strip()
            if section.startswith("markets."):
                current_market = section.split(".", 1)[1]
            else:
                current_market = ""
        if current_market and stripped.startswith("allocation"):
            out_lines.append(f"allocation = {normalized[current_market]:.2f}\n")
        else:
            out_lines.append(line)
    return "".join(out_lines)


def load_us_universe_file(path: Path) -> tuple[list[str], dict[str, str]]:
    if not path.exists():
        raise SystemExit(f"US universe file not found: {path}")

    rows = list(csv.reader(path.open("r", encoding="utf-8")))
    if not rows:
        raise SystemExit(f"US universe file is empty: {path}")

    start_idx = 0
    first_col = rows[0][0].strip().lower() if rows[0] else ""
    if first_col in {"symbol", "ticker"}:
        start_idx = 1

    symbols: list[str] = []
    industries: dict[str, str] = {}
    seen: set[str] = set()
    for row in rows[start_idx:]:
        if not row:
            continue
        raw_symbol = row[0].strip().upper()
        if not raw_symbol or raw_symbol.startswith("#"):
            continue
        if raw_symbol in seen:
            continue
        seen.add(raw_symbol)
        symbols.append(raw_symbol)
        industry = row[1].strip() if len(row) > 1 and row[1].strip() else "Other"
        industries[raw_symbol] = industry

    if not symbols:
        raise SystemExit(f"US universe file contains no symbols: {path}")
    return symbols, industries


def industry_map(market: str) -> dict[str, str]:
    if market == "US":
        return {
            "AAPL": "Technology",
            "MSFT": "Technology",
            "NVDA": "Technology",
            "AMZN": "ConsumerDiscretionary",
            "GOOGL": "CommunicationServices",
            "META": "CommunicationServices",
            "JPM": "Financials",
            "XOM": "Energy",
            "LLY": "Healthcare",
            "COST": "ConsumerStaples",
            "UNH": "Healthcare",
            "HD": "ConsumerDiscretionary",
        }
    if market == "A":
        return {
            "600519": "ConsumerStaples",
            "000001": "Financials",
            "300750": "Industrials",
            "601318": "Financials",
            "000333": "ConsumerDiscretionary",
            "600036": "Financials",
            "002594": "Industrials",
            "600276": "Healthcare",
            "000858": "ConsumerStaples",
            "601899": "Materials",
            "600030": "Financials",
            "600887": "ConsumerStaples",
        }
    return {
        "7203": "Automotive",
        "6758": "Technology",
        "9984": "CommunicationServices",
        "8306": "Financials",
        "8035": "Technology",
        "6501": "Industrials",
        "9432": "CommunicationServices",
        "6861": "Technology",
        "4063": "Materials",
        "6098": "Industrials",
        "7974": "ConsumerDiscretionary",
        "7267": "Automotive",
    }


def a_share_exchange_id(symbol: str) -> str:
    return "1" if symbol.startswith("6") else "0"


def http_get_text(url: str) -> str:
    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0 Safari/537.36",
            "Accept": "*/*",
        },
    )
    with urlopen(req, timeout=30) as resp:
        raw = resp.read()
    return raw.decode("utf-8", errors="replace")


def parse_float(raw: object) -> float | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_date(text: str) -> dt.date:
    text = text.strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"invalid date: {text}")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
