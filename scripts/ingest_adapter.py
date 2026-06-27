#!/usr/bin/env python3
"""Survivorship-free ingestion adapter (P1 skeleton).

Turns a pluggable, survivorship-aware data Source into the CSV files the Rust
engine consumes, baking point-in-time correctness into the output:

  {market}_equities.csv    date,symbol,close,adj_close,volume   (PIT-masked bars)
  {market}_delistings.csv  market,symbol,delist_date,terminal_price,reason
  {market}_industries.csv  symbol,industry
  DATASET_MANIFEST.json    lineage + a SURVIVORSHIP AUDIT (proves real churn)

Why this exists: the current refresh pipeline (yfinance + a hand-picked list of
today's winners) is survivorship-biased -- 0 delistings over 8 years. This
adapter instead consumes a Source that knows point-in-time universe membership
and delistings, masks each symbol's bars to the dates it was actually a tradeable
member, and emits a delistings feed so the engine realizes terminal P&L (via the
delisting force-liquidation hook). The audit in the manifest is the proof: it
reports how many names delisted / entered / left, which must be > 0.

The engine needs NO change beyond the already-landed delisting hook.

Run the offline self-test (no API key needed):
    python3 scripts/ingest_adapter.py --self-test

Generate a synthetic survivorship-free dataset the engine can read:
    python3 scripts/ingest_adapter.py --source synthetic --out-dir data_synth_pit \
        --market JP --start 2021-01-04 --end 2024-01-01
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import math
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path


# ─────────────────────────── data model ───────────────────────────
@dataclass(frozen=True)
class Bar:
    date: dt.date
    symbol: str
    close: float
    adj_close: float
    volume: float


@dataclass(frozen=True)
class DelistingEvent:
    date: dt.date          # last trading day; the engine liquidates on this date
    symbol: str
    terminal_price: float  # settlement per share (bankruptcy ~0, M&A cash-out, last trade)
    reason: str


@dataclass(frozen=True)
class MembershipInterval:
    """A symbol is a valid, tradeable universe member on [start, end] (inclusive).
    For a delisted name, `end` is the delisting date. A symbol may have multiple
    intervals (re-added after removal)."""
    symbol: str
    start: dt.date
    end: dt.date


# ─────────────────────────── source interface ───────────────────────────
class Source(ABC):
    """A survivorship-aware data source. Concrete sources own all knowledge of
    point-in-time membership, price adjustment, and delistings."""

    market: str = "?"

    @abstractmethod
    def universe_intervals(self, start: dt.date, end: dt.date) -> list[MembershipInterval]:
        """Point-in-time membership intervals clipped to [start, end]. MUST be
        as-of-date correct: a name added on D+1 must not appear on D."""

    @abstractmethod
    def fetch_ohlcv(self, symbol: str, start: dt.date, end: dt.date) -> list[Bar]:
        """Raw bars with split/dividend-adjusted `adj_close` already populated."""

    @abstractmethod
    def delistings(self, start: dt.date, end: dt.date) -> list[DelistingEvent]:
        """Delisting events whose date falls in [start, end]."""

    @abstractmethod
    def industry_of(self, symbol: str) -> str:
        ...


# ─────────────────────────── pipeline ───────────────────────────
@dataclass
class IngestResult:
    market: str
    equities_path: Path
    delistings_path: Path
    industries_path: Path
    manifest_path: Path
    audit: dict


def _weekdays(start: dt.date, end: dt.date) -> list[dt.date]:
    out, d = [], start
    while d <= end:
        if d.weekday() < 5:  # Mon..Fri
            out.append(d)
        d += dt.timedelta(days=1)
    return out


def _covered(date: dt.date, intervals: list[MembershipInterval]) -> bool:
    return any(iv.start <= date <= iv.end for iv in intervals)


def run_pipeline(source: Source, start: dt.date, end: dt.date, out_dir: Path) -> IngestResult:
    out_dir.mkdir(parents=True, exist_ok=True)
    market = source.market

    intervals = source.universe_intervals(start, end)
    by_symbol: dict[str, list[MembershipInterval]] = {}
    for iv in intervals:
        by_symbol.setdefault(iv.symbol, []).append(iv)

    delistings = [e for e in source.delistings(start, end) if start <= e.date <= end]
    delist_by_symbol = {e.symbol: e for e in delistings}

    # ② fetch + ③ PIT-mask each symbol to its membership window(s).
    kept: list[Bar] = []
    for symbol, ivs in by_symbol.items():
        sym_start = min(iv.start for iv in ivs)
        sym_end = max(iv.end for iv in ivs)
        for bar in source.fetch_ohlcv(symbol, sym_start, sym_end):
            if start <= bar.date <= end and _covered(bar.date, ivs):
                kept.append(bar)

    kept.sort(key=lambda b: (b.date, b.symbol))

    # ④ emit.
    equities_path = out_dir / f"{_csv_stem(market)}_equities.csv"
    # Single `delistings.csv` per data dir (rows carry a `market` column, so one
    # file covers all markets) — exactly the name the engine auto-loads.
    delistings_path = out_dir / "delistings.csv"
    industries_path = out_dir / f"{_csv_stem(market)}_industries.csv"
    manifest_path = out_dir / "DATASET_MANIFEST.json"

    _write_equities(equities_path, kept)
    _write_delistings(delistings_path, market, delistings)
    _write_industries(industries_path, source, sorted(by_symbol))

    audit = survivorship_audit(start, end, by_symbol, delistings, kept)
    _write_manifest(
        manifest_path, market, start, end,
        source=type(source).__name__,
        outputs=[equities_path, delistings_path, industries_path],
        audit=audit,
    )

    return IngestResult(market, equities_path, delistings_path,
                        industries_path, manifest_path, audit)


def survivorship_audit(start, end, by_symbol, delistings, kept_bars) -> dict:
    """The proof the dataset is NOT survivor-only: it must show real churn."""
    n_symbols = len(by_symbol)
    n_delisted = len(delistings)
    entered_after_start = sum(
        1 for ivs in by_symbol.values() if min(iv.start for iv in ivs) > start
    )
    # A name "left" if its last membership ends before the window end (removed or
    # delisted), as opposed to surviving to the end.
    left_before_end = sum(
        1 for ivs in by_symbol.values() if max(iv.end for iv in ivs) < end
    )
    full_span = sum(
        1 for ivs in by_symbol.values()
        if min(iv.start for iv in ivs) <= start and max(iv.end for iv in ivs) >= end
    )
    return {
        "window": {"start": start.isoformat(), "end": end.isoformat()},
        "symbols_ever": n_symbols,
        "delisted_in_window": n_delisted,
        "entered_after_start": entered_after_start,
        "left_before_end": left_before_end,
        "survived_full_span": full_span,
        "bars": len(kept_bars),
        # Hard signal: 0 delistings + everyone full-span == survivorship bias.
        "looks_survivorship_free": n_delisted > 0 and full_span < n_symbols,
    }


def _csv_stem(market: str) -> str:
    return {"US": "us", "A": "a_share", "JP": "jp"}.get(market, market.lower())


def _write_equities(path: Path, bars: list[Bar]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["date", "symbol", "close", "adj_close", "volume"])
        for b in bars:
            w.writerow([b.date.isoformat(), b.symbol,
                        round(b.close, 4), round(b.adj_close, 4), round(b.volume, 2)])


def _write_delistings(path: Path, market: str, events: list[DelistingEvent]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["market", "symbol", "delist_date", "terminal_price", "reason"])
        for e in sorted(events, key=lambda x: (x.date, x.symbol)):
            w.writerow([market, e.symbol, e.date.isoformat(),
                        round(e.terminal_price, 4), e.reason])


def _write_industries(path: Path, source: Source, symbols: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["symbol", "industry"])
        for s in symbols:
            w.writerow([s, source.industry_of(s) or "Other"])


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_manifest(path, market, start, end, source, outputs, audit) -> None:
    payload = {
        "source_adapter": "scripts/ingest_adapter.py",
        "source": source,
        "market": market,
        "date_range": {"start": start.isoformat(), "end": end.isoformat()},
        "survivorship_audit": audit,
        "outputs": [
            {"path": str(p), "sha256": _sha256(p), "bytes": p.stat().st_size}
            for p in outputs
        ],
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


# ─────────────────────────── synthetic source (no key) ───────────────────────────
class SyntheticSource(Source):
    """Deterministic, survivorship-aware synthetic market with REAL churn:
    survivors, late IPOs, bankruptcies (terminal 0) and M&A cash-outs. Lets the
    whole pipeline + audit be validated offline, no API key. Prices are a
    deterministic drift+sine per symbol (no RNG, fully reproducible)."""

    INDUSTRIES = ["Technology", "Financials", "Industrials", "Healthcare"]

    def __init__(self, market: str = "JP", n_survivors: int = 18,
                 n_late_ipo: int = 4, n_delist: int = 6):
        self.market = market
        self._n = (n_survivors, n_late_ipo, n_delist)
        self._intervals: list[MembershipInterval] = []
        self._delistings: list[DelistingEvent] = []
        self._industry: dict[str, str] = {}
        self._built_for: tuple[dt.date, dt.date] | None = None

    # deterministic per-symbol seed in [0,1)
    @staticmethod
    def _seed(symbol: str) -> float:
        h = hashlib.sha256(symbol.encode()).digest()
        return int.from_bytes(h[:4], "big") / 0xFFFFFFFF

    def _build(self, start: dt.date, end: dt.date) -> None:
        if self._built_for == (start, end):
            return
        self._intervals, self._delistings, self._industry = [], [], {}
        days = _weekdays(start, end)
        n_surv, n_ipo, n_del = self._n
        idx = 0

        def add(sym, s, e):
            self._intervals.append(MembershipInterval(sym, s, e))
            self._industry[sym] = self.INDUSTRIES[idx % len(self.INDUSTRIES)]

        for i in range(n_surv):
            add(f"SURV{i:02d}", start, end)
            idx += 1
        for i in range(n_ipo):
            ipo = days[len(days) // 4 + i]  # IPO ~25% into the window
            add(f"IPO{i:02d}", ipo, end)
            idx += 1
        for i in range(n_del):
            dday = days[len(days) // 2 + i * 3]  # delist around mid-window
            sym = f"DEL{i:02d}"
            add(sym, start, dday)
            # alternate bankruptcy (terminal 0) and cash-out (terminal > 0)
            if i % 2 == 0:
                self._delistings.append(DelistingEvent(dday, sym, 0.0, "bankruptcy"))
            else:
                self._delistings.append(DelistingEvent(dday, sym, 7.5, "acquired"))
            idx += 1
        self._built_for = (start, end)

    def universe_intervals(self, start, end):
        self._build(start, end)
        return list(self._intervals)

    def delistings(self, start, end):
        self._build(start, end)
        return list(self._delistings)

    def industry_of(self, symbol):
        return self._industry.get(symbol, "Other")

    def fetch_ohlcv(self, symbol, start, end):
        s = self._seed(symbol)
        drift = -0.0004 + 0.001 * s          # per-day drift, some up some down
        amp = 0.01 + 0.02 * s                 # sine amplitude
        freq = 0.05 + 0.1 * s
        price = 20.0 + 80.0 * s               # starting price 20..100

        # A bankruptcy name (terminal 0) should DECLINE into delisting -- that is
        # exactly the falling-knife laggard a reversion strategy buys, then gets
        # wiped on by the terminal-0 liquidation. Model the last ~40 trading days
        # as a steady drawdown so the survivorship trap is real, not escapable at
        # a high last price.
        bankruptcy = next(
            (e for e in self._delistings
             if e.symbol == symbol and e.terminal_price == 0.0), None
        )
        days = _weekdays(start, end)
        decay_start = None
        if bankruptcy is not None and bankruptcy.date in days:
            decay_start = max(0, days.index(bankruptcy.date) - 40)

        bars = []
        for t, day in enumerate(days):
            r = drift + amp * math.sin(t * freq)
            if decay_start is not None and t >= decay_start:
                r -= 0.05  # ~5%/day bleed into bankruptcy
            price = max(0.05, price * (1.0 + r))
            vol = 1_000_000 * (0.5 + s)
            bars.append(Bar(day, symbol, price, price, vol))
        return bars


# ─────────────────────────── J-Quants source (needs free API key) ───────────────────────────
class JQuantsSource(Source):
    """Japan, JPX official J-Quants API (free tier = 12-week delayed, fine for
    backtest). SKELETON: implements the interface shape + a disk cache design;
    the network calls are TODO until a free API key is provided.

    Free account: https://jpx-jquants.com/en  (V2 = API-key auth since 2025-12).
    Endpoints to wire:
      - /v2/listed/info     -> universe membership + delisting flags (PIT)
      - /v2/prices/daily_quotes (with AdjustmentClose) -> OHLCV + adjusted price
    Caching: store each raw JSON response under <cache>/jquants/<endpoint>/<key>.json
    so re-runs are free and lineage is reproducible (hash the cache into manifest).
    """

    market = "JP"

    def __init__(self, api_key: str | None, cache_dir: Path = Path("data_cache/jquants")):
        self.api_key = api_key
        self.cache_dir = cache_dir
        if not api_key:
            raise SystemExit(
                "JQuantsSource needs a free J-Quants API key.\n"
                "Register at https://jpx-jquants.com/en , then re-run with "
                "--api-key <KEY> (or set JQUANTS_API_KEY). The pipeline, PIT "
                "masking, delisting emission and audit are already validated via "
                "--self-test; only the fetch layer below is left to wire."
            )

    def _todo(self, what: str):
        raise NotImplementedError(
            f"J-Quants {what} fetch not wired yet. Implement the cached GET against "
            f"the V2 endpoint (see class docstring). Key is present, so this is the "
            f"only remaining work to run on real Japanese data."
        )

    def universe_intervals(self, start, end):
        self._todo("universe (/v2/listed/info)")

    def fetch_ohlcv(self, symbol, start, end):
        self._todo("prices (/v2/prices/daily_quotes)")

    def delistings(self, start, end):
        self._todo("delistings (/v2/listed/info delist flags)")

    def industry_of(self, symbol):
        return "Other"


# ─────────────────────────── CLI ───────────────────────────
def _parse_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def build_source(name: str, market: str, api_key: str | None) -> Source:
    if name == "synthetic":
        return SyntheticSource(market=market)
    if name == "jquants":
        return JQuantsSource(api_key=api_key)
    raise SystemExit(f"unknown source '{name}'; choose synthetic|jquants")


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", default="synthetic", help="synthetic|jquants")
    p.add_argument("--market", default="JP")
    p.add_argument("--out-dir", default="data_synth_pit")
    p.add_argument("--start", default="2021-01-04")
    p.add_argument("--end", default="2024-01-01")
    p.add_argument("--api-key", default=None)
    p.add_argument("--self-test", action="store_true",
                   help="run the offline pipeline validation and exit")
    args = p.parse_args(argv)

    if args.self_test:
        return self_test()

    import os
    api_key = args.api_key or os.environ.get("JQUANTS_API_KEY")
    source = build_source(args.source, args.market, api_key)
    res = run_pipeline(source, _parse_date(args.start), _parse_date(args.end),
                       Path(args.out_dir))
    print(f"wrote {res.equities_path}")
    print(f"wrote {res.delistings_path}")
    print(f"wrote {res.industries_path}")
    print("survivorship audit:")
    print(json.dumps(res.audit, indent=2))
    if not res.audit["looks_survivorship_free"]:
        print("WARNING: audit does not look survivorship-free (no churn/delistings)")
    return 0


# ─────────────────────────── offline self-test ───────────────────────────
def self_test() -> int:
    import tempfile

    start, end = dt.date(2021, 1, 4), dt.date(2023, 1, 1)
    src = SyntheticSource(market="JP")
    with tempfile.TemporaryDirectory() as tmp:
        res = run_pipeline(src, start, end, Path(tmp))
        a = res.audit

        # 1) audit shows real churn (the whole point).
        assert a["delisted_in_window"] > 0, a
        assert a["entered_after_start"] > 0, a
        assert a["left_before_end"] >= a["delisted_in_window"], a
        assert a["survived_full_span"] < a["symbols_ever"], a
        assert a["looks_survivorship_free"] is True, a

        # 2) PIT mask: a delisted name has NO bars after its delist date.
        delist = src.delistings(start, end)[0]
        rows = list(csv.DictReader(res.equities_path.open()))
        after = [r for r in rows
                 if r["symbol"] == delist.symbol and r["date"] > delist.date.isoformat()]
        assert not after, f"{delist.symbol} has bars after delisting {delist.date}: {after[:3]}"

        # 3) a late-IPO name has NO bars before its first membership date.
        ivs = {iv.symbol: iv for iv in src.universe_intervals(start, end)}
        ipo_sym = next(s for s in ivs if s.startswith("IPO"))
        ipo_start = ivs[ipo_sym].start
        before = [r for r in rows
                  if r["symbol"] == ipo_sym and r["date"] < ipo_start.isoformat()]
        assert not before, f"{ipo_sym} has bars before IPO {ipo_start}"

        # 4) delistings.csv matches the engine's expected schema/header.
        dl_header = res.delistings_path.open().readline().strip()
        assert dl_header == "market,symbol,delist_date,terminal_price,reason", dl_header
        dl_rows = list(csv.DictReader(res.delistings_path.open()))
        assert len(dl_rows) == a["delisted_in_window"]
        assert any(float(r["terminal_price"]) == 0.0 for r in dl_rows), "expect a bankruptcy@0"

        # 5) equities header matches engine schema.
        eq_header = res.equities_path.open().readline().strip()
        assert eq_header == "date,symbol,close,adj_close,volume", eq_header

        print("self-test OK")
        print(json.dumps(a, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
