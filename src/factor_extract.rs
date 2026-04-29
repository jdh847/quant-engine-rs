//! Extract raw factor values from bar history for IC analysis.
//!
//! Standalone module — does NOT touch the strategy hot path. Re-implements
//! the same factor formulas used by `MomentumTrendStrategy` so we can run
//! offline IC diagnostics over any bar dataset.
//!
//! Use `extract_factor_timeline` to build a timeline of `DailyFactorSnapshot`
//! that can be fed into `factor_ic::compute_factor_ics`.

use std::collections::{BTreeMap, HashMap, VecDeque};

use chrono::NaiveDate;

use crate::factor_ic::DailyFactorSnapshot;
use crate::model::Bar;

/// Window sizes for factor extraction. Mirrors the relevant fields of
/// `StrategyConfig` so the IC analysis matches what the strategy sees.
#[derive(Debug, Clone, Copy)]
pub struct FactorWindows {
    pub long_window: usize,
    pub vol_window: usize,
    pub mean_reversion_window: usize,
    pub volume_window: usize,
}

impl Default for FactorWindows {
    fn default() -> Self {
        Self {
            long_window: 7,
            vol_window: 5,
            mean_reversion_window: 3,
            volume_window: 5,
        }
    }
}

/// Per-symbol rolling history.
#[derive(Debug, Default)]
struct SymbolHistory {
    closes: VecDeque<f64>,
    volumes: VecDeque<f64>,
}

fn stddev(xs: &[f64]) -> f64 {
    if xs.len() < 2 {
        return 0.0;
    }
    let mean = xs.iter().sum::<f64>() / xs.len() as f64;
    let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / xs.len() as f64;
    var.sqrt()
}

/// Compute the four raw factors for a single bar given its prior history.
/// Returns None if there isn't enough history yet.
fn compute_factors(
    history: &SymbolHistory,
    current_close: f64,
    current_volume: f64,
    win: &FactorWindows,
) -> Option<(f64, f64, f64, f64)> {
    let closes_len = history.closes.len();
    let needed = win
        .long_window
        .max(win.vol_window + 1)
        .max(win.mean_reversion_window + 1);
    if closes_len < needed {
        return None;
    }
    if history.volumes.len() < win.volume_window {
        return None;
    }

    let closes: Vec<f64> = history.closes.iter().copied().collect();

    // momentum = current_close / close[len-long_window] - 1
    let long_base = closes[closes_len - win.long_window];
    let momentum = current_close / long_base - 1.0;

    // mean_reversion = -(current_close / close[len-mr_window] - 1)
    let mr_base = closes[closes_len - win.mean_reversion_window];
    let mean_reversion = -(current_close / mr_base - 1.0);

    // volatility = stddev of last vol_window log returns ending at current_close
    let vol_slice_start = closes_len - win.vol_window;
    let mut returns = Vec::with_capacity(win.vol_window);
    for i in vol_slice_start..closes_len - 1 {
        returns.push(closes[i + 1] / closes[i] - 1.0);
    }
    // Include the return into current_close
    returns.push(current_close / closes[closes_len - 1] - 1.0);
    let vol = stddev(&returns).max(1e-6);

    // volume_signal = current_volume / avg(last volume_window) - 1
    let volumes: Vec<f64> = history.volumes.iter().copied().collect();
    let vol_slice = &volumes[volumes.len() - win.volume_window..];
    let avg_vol = vol_slice.iter().sum::<f64>() / vol_slice.len() as f64;
    let volume_signal = if avg_vol > 0.0 {
        current_volume / avg_vol - 1.0
    } else {
        0.0
    };

    Some((momentum, mean_reversion, vol, volume_signal))
}

/// Build a timeline of cross-sectional factor snapshots from a chronologically
/// sorted bar stream.
///
/// For each date, the snapshot contains:
/// - factor values per symbol (momentum, mean_reversion, low_vol = -vol, volume)
/// - forward returns: next-period close-to-close return per symbol
///
/// Symbols missing on the next date are simply omitted from forward_returns.
pub fn extract_factor_timeline(
    bars: &[Bar],
    windows: &FactorWindows,
) -> Vec<DailyFactorSnapshot> {
    // Group bars by date, preserving chronological order.
    let mut by_date: BTreeMap<NaiveDate, Vec<&Bar>> = BTreeMap::new();
    for bar in bars {
        by_date.entry(bar.date).or_default().push(bar);
    }

    // Per-symbol history accumulated as we walk forward.
    let mut history: HashMap<String, SymbolHistory> = HashMap::new();
    // Date-ordered factor values: date -> symbol -> (mom, mr, low_vol, volume)
    let mut day_factors: BTreeMap<NaiveDate, HashMap<String, (f64, f64, f64, f64)>> =
        BTreeMap::new();
    // Date-ordered closes: date -> symbol -> close (for forward return calc)
    let mut day_closes: BTreeMap<NaiveDate, HashMap<String, f64>> = BTreeMap::new();

    for (&date, day_bars) in &by_date {
        let mut factors_today: HashMap<String, (f64, f64, f64, f64)> = HashMap::new();
        let mut closes_today: HashMap<String, f64> = HashMap::new();

        for bar in day_bars {
            let h = history.entry(bar.symbol.clone()).or_default();
            // Compute factors using PRIOR history (before pushing today's bar).
            if let Some(f) = compute_factors(h, bar.close, bar.volume, windows) {
                factors_today.insert(bar.symbol.clone(), f);
            }
            closes_today.insert(bar.symbol.clone(), bar.close);
            // Now push today's bar into history for tomorrow's factor calc.
            h.closes.push_back(bar.close);
            h.volumes.push_back(bar.volume);
            // Cap history at long_window so we don't grow unbounded.
            let max_len = windows
                .long_window
                .max(windows.vol_window + 1)
                .max(windows.mean_reversion_window + 1)
                .max(windows.volume_window)
                + 1;
            while h.closes.len() > max_len {
                h.closes.pop_front();
            }
            while h.volumes.len() > max_len {
                h.volumes.pop_front();
            }
        }

        if !factors_today.is_empty() {
            day_factors.insert(date, factors_today);
        }
        day_closes.insert(date, closes_today);
    }

    // Build snapshots with forward returns.
    let dates: Vec<NaiveDate> = day_closes.keys().copied().collect();
    let mut snapshots = Vec::new();
    for (i, date) in dates.iter().enumerate() {
        let Some(factors) = day_factors.get(date) else {
            continue;
        };
        let Some(closes_today) = day_closes.get(date) else {
            continue;
        };
        let Some(next_date) = dates.get(i + 1) else {
            continue; // no forward return possible
        };
        let Some(closes_tomorrow) = day_closes.get(next_date) else {
            continue;
        };

        let mut forward_returns = HashMap::new();
        for (symbol, today_close) in closes_today {
            if *today_close <= 0.0 {
                continue;
            }
            if let Some(tomorrow_close) = closes_tomorrow.get(symbol) {
                forward_returns
                    .insert(symbol.clone(), tomorrow_close / today_close - 1.0);
            }
        }
        if forward_returns.is_empty() {
            continue;
        }

        // Split each factor into its own cross-section map.
        let mut factor_map: HashMap<String, HashMap<String, f64>> = HashMap::new();
        let momentum = factor_map.entry("momentum".to_string()).or_default();
        for (sym, (m, _, _, _)) in factors {
            momentum.insert(sym.clone(), *m);
        }
        let mr = factor_map.entry("mean_reversion".to_string()).or_default();
        for (sym, (_, m, _, _)) in factors {
            mr.insert(sym.clone(), *m);
        }
        // low_vol = inverse of volatility (higher = lower vol). Use -vol so
        // larger value means "lower vol", consistent with strategy scoring.
        let lv = factor_map.entry("low_vol".to_string()).or_default();
        for (sym, (_, _, v, _)) in factors {
            lv.insert(sym.clone(), -v);
        }
        let vol_sig = factor_map.entry("volume".to_string()).or_default();
        for (sym, (_, _, _, vsig)) in factors {
            vol_sig.insert(sym.clone(), *vsig);
        }

        snapshots.push(DailyFactorSnapshot {
            date: *date,
            factors: factor_map,
            forward_returns,
        });
    }

    snapshots
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use crate::factor_ic::compute_factor_ics;
    use crate::model::Bar;

    use super::{extract_factor_timeline, FactorWindows};

    fn d(day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2025, 1, day).expect("valid")
    }

    fn bar(date: NaiveDate, sym: &str, close: f64, volume: f64) -> Bar {
        Bar {
            date,
            market: "US".to_string(),
            symbol: sym.to_string(),
            close,
            volume,
        }
    }

    #[test]
    fn empty_input_produces_empty_timeline() {
        let timeline = extract_factor_timeline(&[], &FactorWindows::default());
        assert!(timeline.is_empty());
    }

    #[test]
    fn insufficient_history_produces_no_snapshots() {
        // Only 3 days of data, need at least long_window=7
        let bars = vec![
            bar(d(1), "AAPL", 100.0, 1000.0),
            bar(d(2), "AAPL", 101.0, 1000.0),
            bar(d(3), "AAPL", 102.0, 1000.0),
        ];
        let timeline = extract_factor_timeline(&bars, &FactorWindows::default());
        assert!(
            timeline.is_empty(),
            "should not produce snapshots without enough history"
        );
    }

    #[test]
    fn momentum_factor_predicts_in_trending_data() {
        // Build a dataset where momentum truly predicts the next return.
        // 3 symbols. Symbol with highest momentum has highest forward return.
        let win = FactorWindows::default();
        let mut bars = Vec::new();
        // Generate 15 days of warm-up + signal days
        // Symbol A: weak uptrend (1% per day)
        // Symbol B: medium uptrend (2% per day)
        // Symbol C: strong uptrend (3% per day)
        let mut a = 100.0;
        let mut b = 100.0;
        let mut c = 100.0;
        for day in 1..=20u32 {
            bars.push(bar(d(day), "A", a, 1000.0));
            bars.push(bar(d(day), "B", b, 1000.0));
            bars.push(bar(d(day), "C", c, 1000.0));
            a *= 1.01;
            b *= 1.02;
            c *= 1.03;
        }
        let timeline = extract_factor_timeline(&bars, &win);
        assert!(!timeline.is_empty());

        // momentum factor should have high IC
        let reports = compute_factor_ics(&timeline);
        let momentum = reports.iter().find(|r| r.factor == "momentum").unwrap();
        assert!(
            momentum.mean_ic > 0.9,
            "momentum should have strong positive IC in trending data, got {}",
            momentum.mean_ic
        );
    }

    #[test]
    fn forward_return_is_computed_correctly() {
        let win = FactorWindows {
            long_window: 3,
            vol_window: 2,
            mean_reversion_window: 2,
            volume_window: 2,
        };
        // Two symbols, simple price progression
        let bars = vec![
            // day 1
            bar(d(1), "X", 100.0, 1000.0),
            bar(d(1), "Y", 200.0, 1000.0),
            // day 2
            bar(d(2), "X", 102.0, 1000.0),
            bar(d(2), "Y", 200.0, 1000.0),
            // day 3
            bar(d(3), "X", 104.0, 1000.0),
            bar(d(3), "Y", 201.0, 1000.0),
            // day 4
            bar(d(4), "X", 105.0, 1000.0),
            bar(d(4), "Y", 203.0, 1000.0),
            // day 5
            bar(d(5), "X", 110.0, 1000.0),
            bar(d(5), "Y", 210.0, 1000.0),
        ];
        let timeline = extract_factor_timeline(&bars, &win);
        // The last-day snapshot has no forward return (skipped). For the
        // second-to-last day (day 4), forward returns are computed from day 5:
        //   X: 110/105 - 1 ≈ 0.0476
        //   Y: 210/203 - 1 ≈ 0.0345
        let day4_snap = timeline.iter().find(|s| s.date == d(4));
        assert!(
            day4_snap.is_some(),
            "should have a snapshot for day 4 with forward returns"
        );
        let snap = day4_snap.unwrap();
        let fwd_x = snap.forward_returns["X"];
        let fwd_y = snap.forward_returns["Y"];
        assert!((fwd_x - (110.0 / 105.0 - 1.0)).abs() < 1e-9);
        assert!((fwd_y - (210.0 / 203.0 - 1.0)).abs() < 1e-9);
    }

    #[test]
    fn last_day_has_no_forward_return_snapshot() {
        let win = FactorWindows {
            long_window: 3,
            vol_window: 2,
            mean_reversion_window: 2,
            volume_window: 2,
        };
        let mut bars = Vec::new();
        for day in 1..=10u32 {
            bars.push(bar(d(day), "X", 100.0 + day as f64, 1000.0));
        }
        let timeline = extract_factor_timeline(&bars, &win);
        let last_date = timeline.iter().map(|s| s.date).max().unwrap();
        // Last snapshot's date should NOT be the last bar's date, because that
        // last bar has no forward return. Last bar is day 10.
        assert!(last_date < d(10));
    }
}
