use std::collections::HashMap;

use chrono::NaiveDate;
use serde::Serialize;

use crate::execution::ExecutionAdapter;
use crate::model::PriceMap;

/// A point-in-time snapshot of broker state for reconciliation.
#[derive(Debug, Clone, Serialize)]
pub struct ReconcileSnapshot {
    pub date: NaiveDate,
    pub source: String,
    pub cash: f64,
    pub equity: f64,
    pub gross_exposure: f64,
    pub positions: HashMap<(String, String), i64>,
}

impl ReconcileSnapshot {
    /// Capture the current state of any `ExecutionAdapter`.
    pub fn capture<E: ExecutionAdapter>(
        broker: &E,
        prices: &PriceMap,
        date: NaiveDate,
        source: &str,
        symbols: &[(String, String)],
    ) -> Self {
        let mut positions = HashMap::new();
        for (market, symbol) in symbols {
            let qty = broker.position_qty(market, symbol);
            if qty != 0 {
                positions.insert((market.clone(), symbol.clone()), qty);
            }
        }
        Self {
            date,
            source: source.to_string(),
            cash: broker.cash(),
            equity: broker.equity(prices),
            gross_exposure: broker.gross_exposure(prices),
            positions,
        }
    }
}

/// A single position mismatch between two snapshots.
#[derive(Debug, Clone, Serialize)]
pub struct PositionDrift {
    pub market: String,
    pub symbol: String,
    pub expected_qty: i64,
    pub actual_qty: i64,
    pub drift_qty: i64,
}

/// The comparison between an expected (internal) and actual (broker) snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct ReconcileReport {
    pub date: NaiveDate,
    pub expected_source: String,
    pub actual_source: String,
    pub cash_expected: f64,
    pub cash_actual: f64,
    pub cash_drift: f64,
    pub equity_expected: f64,
    pub equity_actual: f64,
    pub equity_drift: f64,
    pub equity_drift_bps: f64,
    pub gross_expected: f64,
    pub gross_actual: f64,
    pub gross_drift: f64,
    pub position_drifts: Vec<PositionDrift>,
    pub clean: bool,
}

impl ReconcileReport {
    /// Compare two snapshots and produce a drift report.
    pub fn diff(expected: &ReconcileSnapshot, actual: &ReconcileSnapshot) -> Self {
        let cash_drift = actual.cash - expected.cash;
        let equity_drift = actual.equity - expected.equity;
        let equity_drift_bps = if expected.equity.abs() > 1e-12 {
            (equity_drift / expected.equity) * 10_000.0
        } else {
            0.0
        };
        let gross_drift = actual.gross_exposure - expected.gross_exposure;

        // Collect all symbols from both snapshots.
        let mut all_symbols: Vec<(String, String)> = expected.positions.keys().cloned().collect();
        for key in actual.positions.keys() {
            if !expected.positions.contains_key(key) {
                all_symbols.push(key.clone());
            }
        }
        all_symbols.sort();

        let mut position_drifts = Vec::new();
        for (market, symbol) in &all_symbols {
            let exp_qty = expected
                .positions
                .get(&(market.clone(), symbol.clone()))
                .copied()
                .unwrap_or(0);
            let act_qty = actual
                .positions
                .get(&(market.clone(), symbol.clone()))
                .copied()
                .unwrap_or(0);
            if exp_qty != act_qty {
                position_drifts.push(PositionDrift {
                    market: market.clone(),
                    symbol: symbol.clone(),
                    expected_qty: exp_qty,
                    actual_qty: act_qty,
                    drift_qty: act_qty - exp_qty,
                });
            }
        }

        let clean =
            position_drifts.is_empty() && cash_drift.abs() < 0.01 && equity_drift.abs() < 0.01;

        Self {
            date: expected.date,
            expected_source: expected.source.clone(),
            actual_source: actual.source.clone(),
            cash_expected: expected.cash,
            cash_actual: actual.cash,
            cash_drift,
            equity_expected: expected.equity,
            equity_actual: actual.equity,
            equity_drift,
            equity_drift_bps,
            gross_expected: expected.gross_exposure,
            gross_actual: actual.gross_exposure,
            gross_drift,
            position_drifts,
            clean,
        }
    }

    /// Write report as a single JSON line to the given writer.
    pub fn write_jsonl<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let line = serde_json::to_string(self).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?;
        writeln!(writer, "{line}")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::NaiveDate;

    use crate::execution::PaperBroker;
    use crate::model::{Order, PriceMap, Side};

    use super::{ReconcileReport, ReconcileSnapshot};

    fn date() -> NaiveDate {
        NaiveDate::from_ymd_opt(2025, 1, 10).expect("valid")
    }

    fn prices() -> PriceMap {
        let mut p = PriceMap::new();
        p.insert(("US".to_string(), "AAPL".to_string()), 200.0);
        p.insert(("US".to_string(), "MSFT".to_string()), 400.0);
        p
    }

    fn symbols() -> Vec<(String, String)> {
        vec![
            ("US".to_string(), "AAPL".to_string()),
            ("US".to_string(), "MSFT".to_string()),
        ]
    }

    // ── Identical brokers produce clean report ────────────────
    #[test]
    fn identical_snapshots_are_clean() {
        let mut broker = PaperBroker::new(100_000.0, 0.0, 0.0);
        let p = prices();
        let buy = Order {
            date: date(),
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Buy,
            qty: 50,
        };
        broker.execute_orders(&[buy], &p);

        let snap = ReconcileSnapshot::capture(&broker, &p, date(), "internal", &symbols());
        let report = ReconcileReport::diff(&snap, &snap);

        assert!(report.clean);
        assert!(report.position_drifts.is_empty());
        assert!(report.cash_drift.abs() < 0.01);
        assert!(report.equity_drift.abs() < 0.01);
    }

    // ── Position drift detected ───────────────────────────────
    #[test]
    fn position_drift_detected() {
        let p = prices();
        let syms = symbols();

        // "internal" broker: holds 50 AAPL
        let mut internal = PaperBroker::new(100_000.0, 0.0, 0.0);
        let buy = Order {
            date: date(),
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Buy,
            qty: 50,
        };
        internal.execute_orders(&[buy], &p);

        // "broker" side: holds only 48 AAPL (simulating partial fill drift)
        let mut actual = PaperBroker::new(100_000.0, 0.0, 0.0);
        let buy_less = Order {
            date: date(),
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Buy,
            qty: 48,
        };
        actual.execute_orders(&[buy_less], &p);

        let snap_int = ReconcileSnapshot::capture(&internal, &p, date(), "internal", &syms);
        let snap_act = ReconcileSnapshot::capture(&actual, &p, date(), "ibkr_paper", &syms);
        let report = ReconcileReport::diff(&snap_int, &snap_act);

        assert!(!report.clean);
        assert_eq!(report.position_drifts.len(), 1);
        assert_eq!(report.position_drifts[0].expected_qty, 50);
        assert_eq!(report.position_drifts[0].actual_qty, 48);
        assert_eq!(report.position_drifts[0].drift_qty, -2);
    }

    // ── Cash drift detected ───────────────────────────────────
    #[test]
    fn cash_drift_detected() {
        let p = prices();
        let syms = symbols();

        let mut internal = PaperBroker::new(100_000.0, 0.0, 0.0);
        let mut actual = PaperBroker::new(99_950.0, 0.0, 0.0); // $50 less

        // both buy same qty
        let buy = Order {
            date: date(),
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Buy,
            qty: 10,
        };
        internal.execute_orders(&[buy.clone()], &p);
        actual.execute_orders(&[buy], &p);

        let snap_int = ReconcileSnapshot::capture(&internal, &p, date(), "internal", &syms);
        let snap_act = ReconcileSnapshot::capture(&actual, &p, date(), "ibkr_paper", &syms);
        let report = ReconcileReport::diff(&snap_int, &snap_act);

        assert!(!report.clean);
        assert!((report.cash_drift - (-50.0)).abs() < 0.01);
    }

    // ── Equity drift in bps ───────────────────────────────────
    #[test]
    fn equity_drift_bps_is_correct() {
        let _p = prices();

        let snap_int = ReconcileSnapshot {
            date: date(),
            source: "internal".to_string(),
            cash: 100_000.0,
            equity: 100_000.0,
            gross_exposure: 0.0,
            positions: HashMap::new(),
        };
        let snap_act = ReconcileSnapshot {
            date: date(),
            source: "ibkr".to_string(),
            cash: 99_900.0,
            equity: 99_900.0,
            gross_exposure: 0.0,
            positions: HashMap::new(),
        };

        let report = ReconcileReport::diff(&snap_int, &snap_act);
        // -100/100000 * 10000 = -10 bps
        assert!(
            (report.equity_drift_bps - (-10.0)).abs() < 0.01,
            "drift_bps={}",
            report.equity_drift_bps
        );
    }

    // ── Phantom position (actual has position internal doesn't) ──
    #[test]
    fn phantom_position_in_actual_detected() {
        let p = prices();
        let syms = symbols();

        let internal = PaperBroker::new(100_000.0, 0.0, 0.0); // no positions
        let mut actual = PaperBroker::new(100_000.0, 0.0, 0.0);
        let buy = Order {
            date: date(),
            market: "US".to_string(),
            symbol: "MSFT".to_string(),
            side: Side::Buy,
            qty: 10,
        };
        actual.execute_orders(&[buy], &p);

        let snap_int = ReconcileSnapshot::capture(&internal, &p, date(), "internal", &syms);
        let snap_act = ReconcileSnapshot::capture(&actual, &p, date(), "ibkr_paper", &syms);
        let report = ReconcileReport::diff(&snap_int, &snap_act);

        assert!(!report.clean);
        assert_eq!(report.position_drifts.len(), 1);
        assert_eq!(report.position_drifts[0].symbol, "MSFT");
        assert_eq!(report.position_drifts[0].expected_qty, 0);
        assert_eq!(report.position_drifts[0].actual_qty, 10);
    }

    // ── JSONL output works ────────────────────────────────────
    #[test]
    fn report_writes_valid_jsonl() {
        let snap = ReconcileSnapshot {
            date: date(),
            source: "test".to_string(),
            cash: 100_000.0,
            equity: 100_000.0,
            gross_exposure: 0.0,
            positions: HashMap::new(),
        };
        let report = ReconcileReport::diff(&snap, &snap);

        let mut buf = Vec::new();
        report.write_jsonl(&mut buf).expect("write");
        let line = String::from_utf8(buf).expect("utf8");
        let parsed: serde_json::Value = serde_json::from_str(line.trim()).expect("parse json");
        assert_eq!(parsed["clean"], true);
    }
}
