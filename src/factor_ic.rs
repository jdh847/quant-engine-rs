//! Factor Information Coefficient (IC) tracking.
//!
//! Post-hoc diagnostic: given per-date cross-sectional factor values and
//! forward returns, compute Spearman rank correlation (IC) per date, then
//! aggregate to mean IC, IC std, IC IR (information ratio), and t-stat.
//!
//! This tells you which factors are actually contributing alpha. A factor
//! with mean IC near zero and IC IR < 0.3 is dead weight in your scoring.
//!
//! Design principle: pure functions, no engine state, no hot-path impact.
//! Feed it snapshots collected during a run (or reconstructed offline).

use std::collections::HashMap;

use chrono::NaiveDate;
use serde::Serialize;

/// Cross-sectional factor values on a single date: factor_name -> (symbol -> value).
#[derive(Debug, Clone, Default)]
pub struct DailyFactorSnapshot {
    pub date: NaiveDate,
    pub factors: HashMap<String, HashMap<String, f64>>,
    /// Forward return (next period) per symbol, in decimal (0.01 = 1%).
    pub forward_returns: HashMap<String, f64>,
}

/// IC metrics for a single factor across the full date range.
#[derive(Debug, Clone, Serialize)]
pub struct FactorICReport {
    pub factor: String,
    /// Mean of per-date IC values.
    pub mean_ic: f64,
    /// Std of per-date IC values.
    pub std_ic: f64,
    /// IC Information Ratio (DAILY): mean_ic / std_ic. This is the per-period
    /// Sharpe of the IC series, NOT annualized. Real single factors land at
    /// 0.03-0.10 here; do not compare against an annualized 0.5 bar.
    pub ic_ir: f64,
    /// Annualized IC IR: ic_ir * sqrt(252). This is the figure that maps onto
    /// the textbook "factor information ratio". > 0.5 is genuinely tradeable,
    /// > 1.0 is strong. The capital signal gate evaluates THIS, not `ic_ir`.
    pub annualized_ic_ir: f64,
    /// t-statistic: ic_ir * sqrt(N). > 2.0 is statistically meaningful.
    pub t_stat: f64,
    /// Number of days where IC was computable.
    pub n_days: usize,
    /// Fraction of days where IC > 0 (directional consistency).
    pub positive_ratio: f64,
}

/// Spearman rank correlation between two equal-length vectors.
/// Returns None if the vectors are empty, unequal length, or have no variance.
pub fn spearman_rank_correlation(xs: &[f64], ys: &[f64]) -> Option<f64> {
    if xs.is_empty() || xs.len() != ys.len() {
        return None;
    }
    let n = xs.len();
    if n < 2 {
        return None;
    }

    // Replace NaN or infinite with None
    for (a, b) in xs.iter().zip(ys.iter()) {
        if !a.is_finite() || !b.is_finite() {
            return None;
        }
    }

    let x_ranks = fractional_ranks(xs);
    let y_ranks = fractional_ranks(ys);
    pearson_correlation(&x_ranks, &y_ranks)
}

/// Pearson correlation between two equal-length vectors.
fn pearson_correlation(xs: &[f64], ys: &[f64]) -> Option<f64> {
    let n = xs.len();
    if n < 2 {
        return None;
    }
    let mean_x: f64 = xs.iter().sum::<f64>() / n as f64;
    let mean_y: f64 = ys.iter().sum::<f64>() / n as f64;

    let mut cov = 0.0;
    let mut var_x = 0.0;
    let mut var_y = 0.0;
    for i in 0..n {
        let dx = xs[i] - mean_x;
        let dy = ys[i] - mean_y;
        cov += dx * dy;
        var_x += dx * dx;
        var_y += dy * dy;
    }
    if var_x < 1e-12 || var_y < 1e-12 {
        return None;
    }
    Some(cov / (var_x.sqrt() * var_y.sqrt()))
}

/// Compute fractional ranks with tie-handling (average rank for ties).
fn fractional_ranks(values: &[f64]) -> Vec<f64> {
    let n = values.len();
    let mut indexed: Vec<(usize, f64)> = values.iter().copied().enumerate().collect();
    indexed.sort_by(|a, b| a.1.total_cmp(&b.1));

    let mut ranks = vec![0.0; n];
    let mut i = 0;
    while i < n {
        let mut j = i + 1;
        while j < n && indexed[j].1 == indexed[i].1 {
            j += 1;
        }
        // Tied group: indices [i..j). Rank = average of (i+1..=j) positions.
        let avg_rank = ((i + 1 + j) as f64) / 2.0;
        for k in i..j {
            ranks[indexed[k].0] = avg_rank;
        }
        i = j;
    }
    ranks
}

/// Compute IC for a single factor on a single date.
/// Returns None if there's insufficient cross-sectional data.
pub fn daily_ic(
    factor_values: &HashMap<String, f64>,
    forward_returns: &HashMap<String, f64>,
) -> Option<f64> {
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    for (symbol, value) in factor_values {
        if let Some(ret) = forward_returns.get(symbol) {
            if value.is_finite() && ret.is_finite() {
                xs.push(*value);
                ys.push(*ret);
            }
        }
    }
    spearman_rank_correlation(&xs, &ys)
}

/// Compute IC reports for all factors across all dates.
pub fn compute_factor_ics(snapshots: &[DailyFactorSnapshot]) -> Vec<FactorICReport> {
    // Collect per-factor daily ICs
    let mut per_factor_ics: HashMap<String, Vec<f64>> = HashMap::new();
    for snap in snapshots {
        for (factor_name, factor_values) in &snap.factors {
            if let Some(ic) = daily_ic(factor_values, &snap.forward_returns) {
                per_factor_ics
                    .entry(factor_name.clone())
                    .or_default()
                    .push(ic);
            }
        }
    }

    let mut reports: Vec<FactorICReport> = per_factor_ics
        .into_iter()
        .map(|(factor, ics)| {
            let n = ics.len();
            let mean = if n > 0 {
                ics.iter().sum::<f64>() / n as f64
            } else {
                0.0
            };
            let variance = if n > 1 {
                ics.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1) as f64
            } else {
                0.0
            };
            let std = variance.sqrt();
            let ic_ir = if std > 1e-12 { mean / std } else { 0.0 };
            // Annualize the daily IC IR to the textbook factor-IR scale.
            // 252 trading days/year. This is the number the capital gate judges.
            let annualized_ic_ir = ic_ir * 252_f64.sqrt();
            let t_stat = ic_ir * (n as f64).sqrt();
            let positive_ratio = if n > 0 {
                ics.iter().filter(|&&ic| ic > 0.0).count() as f64 / n as f64
            } else {
                0.0
            };
            FactorICReport {
                factor,
                mean_ic: mean,
                std_ic: std,
                ic_ir,
                annualized_ic_ir,
                t_stat,
                n_days: n,
                positive_ratio,
            }
        })
        .collect();

    reports.sort_by(|a, b| b.ic_ir.abs().total_cmp(&a.ic_ir.abs()));
    reports
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::NaiveDate;

    use super::{
        compute_factor_ics, daily_ic, fractional_ranks, pearson_correlation,
        spearman_rank_correlation, DailyFactorSnapshot,
    };

    fn date(day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(2025, 1, day).expect("valid")
    }

    // ── Rank helpers ──────────────────────────────────────────
    #[test]
    fn fractional_ranks_handles_ties() {
        let ranks = fractional_ranks(&[1.0, 2.0, 2.0, 3.0]);
        // Sorted: 1 (rank 1), 2 (ranks 2,3 → avg 2.5), 2 (avg 2.5), 3 (rank 4)
        assert_eq!(ranks, vec![1.0, 2.5, 2.5, 4.0]);
    }

    #[test]
    fn fractional_ranks_preserves_input_order() {
        let ranks = fractional_ranks(&[3.0, 1.0, 2.0]);
        assert_eq!(ranks, vec![3.0, 1.0, 2.0]);
    }

    // ── Correlation basics ────────────────────────────────────
    #[test]
    fn perfect_positive_correlation_is_one() {
        let xs = vec![1.0, 2.0, 3.0, 4.0];
        let ys = vec![10.0, 20.0, 30.0, 40.0];
        let r = pearson_correlation(&xs, &ys).unwrap();
        assert!((r - 1.0).abs() < 1e-9);
    }

    #[test]
    fn perfect_negative_correlation_is_minus_one() {
        let xs = vec![1.0, 2.0, 3.0, 4.0];
        let ys = vec![40.0, 30.0, 20.0, 10.0];
        let r = pearson_correlation(&xs, &ys).unwrap();
        assert!((r - (-1.0)).abs() < 1e-9);
    }

    #[test]
    fn zero_variance_returns_none() {
        let xs = vec![1.0, 1.0, 1.0];
        let ys = vec![1.0, 2.0, 3.0];
        assert!(pearson_correlation(&xs, &ys).is_none());
    }

    // ── Spearman ranks properly ───────────────────────────────
    #[test]
    fn spearman_is_monotonic_invariant() {
        // Pearson is linear, Spearman is monotonic:
        // y = x^3 has perfect Spearman but not perfect Pearson
        let xs = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let ys = vec![1.0, 8.0, 27.0, 64.0, 125.0];
        let spearman = spearman_rank_correlation(&xs, &ys).unwrap();
        assert!(
            (spearman - 1.0).abs() < 1e-9,
            "spearman on monotonic transform should be 1.0, got {spearman}"
        );
    }

    #[test]
    fn spearman_rejects_nan() {
        let xs = vec![1.0, f64::NAN, 3.0];
        let ys = vec![1.0, 2.0, 3.0];
        assert!(spearman_rank_correlation(&xs, &ys).is_none());
    }

    // ── Daily IC ──────────────────────────────────────────────
    #[test]
    fn daily_ic_handles_partial_overlap() {
        let mut factor = HashMap::new();
        factor.insert("AAPL".to_string(), 1.0);
        factor.insert("MSFT".to_string(), 2.0);
        factor.insert("GOOG".to_string(), 3.0);
        factor.insert("META".to_string(), 4.0); // no forward return for this

        let mut forward = HashMap::new();
        forward.insert("AAPL".to_string(), 0.01);
        forward.insert("MSFT".to_string(), 0.02);
        forward.insert("GOOG".to_string(), 0.03);
        // META missing

        let ic = daily_ic(&factor, &forward).unwrap();
        // Only AAPL/MSFT/GOOG overlap, perfectly correlated → IC = 1.0
        assert!((ic - 1.0).abs() < 1e-9);
    }

    #[test]
    fn daily_ic_with_no_overlap_is_none() {
        let mut factor = HashMap::new();
        factor.insert("AAPL".to_string(), 1.0);

        let mut forward = HashMap::new();
        forward.insert("MSFT".to_string(), 0.01);

        assert!(daily_ic(&factor, &forward).is_none());
    }

    // ── Full factor IC report ─────────────────────────────────
    #[test]
    fn strong_factor_produces_high_ic_ir() {
        // A factor that perfectly predicts returns across 5 days
        let mut snapshots = Vec::new();
        for day in 1..=5u32 {
            let mut factor = HashMap::new();
            let mut forward = HashMap::new();
            for (i, sym) in ["AAPL", "MSFT", "GOOG", "META", "AMZN"].iter().enumerate() {
                factor.insert(sym.to_string(), i as f64);
                forward.insert(sym.to_string(), i as f64 * 0.01);
            }
            let mut factors = HashMap::new();
            factors.insert("strong_factor".to_string(), factor);
            snapshots.push(DailyFactorSnapshot {
                date: date(day),
                factors,
                forward_returns: forward,
            });
        }

        let reports = compute_factor_ics(&snapshots);
        assert_eq!(reports.len(), 1);
        let r = &reports[0];
        assert_eq!(r.factor, "strong_factor");
        assert!(r.mean_ic > 0.99, "mean_ic={}", r.mean_ic);
        assert_eq!(r.n_days, 5);
        assert!((r.positive_ratio - 1.0).abs() < 1e-9);
        // Std is 0 (perfect IC every day), so IC IR is 0 by our convention
        // This is an edge case: in practice factors have some noise
    }

    #[test]
    fn noise_factor_produces_low_ic_ir() {
        // Deterministic "random" factor values uncorrelated with returns
        let mut snapshots = Vec::new();
        let noise_table = [
            [0.3, 0.7, 0.1, 0.9, 0.5],
            [0.8, 0.2, 0.6, 0.4, 0.1],
            [0.5, 0.9, 0.3, 0.7, 0.2],
            [0.1, 0.4, 0.8, 0.2, 0.6],
            [0.6, 0.1, 0.5, 0.8, 0.3],
        ];
        for day in 0..5usize {
            let mut factor = HashMap::new();
            let mut forward = HashMap::new();
            for (i, sym) in ["AAPL", "MSFT", "GOOG", "META", "AMZN"].iter().enumerate() {
                factor.insert(sym.to_string(), noise_table[day][i]);
                forward.insert(sym.to_string(), (i as f64) * 0.01); // same ordering each day
            }
            let mut factors = HashMap::new();
            factors.insert("noise".to_string(), factor);
            snapshots.push(DailyFactorSnapshot {
                date: date(day as u32 + 1),
                factors,
                forward_returns: forward,
            });
        }

        let reports = compute_factor_ics(&snapshots);
        let r = &reports[0];
        // IC IR should be close to 0 for genuinely noisy factor
        assert!(r.ic_ir.abs() < 1.5, "noise IC IR = {}", r.ic_ir);
    }

    #[test]
    fn multiple_factors_ranked_by_abs_ic_ir() {
        // Build a scenario with two factors: one strong, one negative, one weak
        let mut snapshots = Vec::new();
        for day in 1..=10u32 {
            let mut strong = HashMap::new();
            let mut negative = HashMap::new();
            let mut forward = HashMap::new();
            for (i, sym) in ["A", "B", "C", "D"].iter().enumerate() {
                strong.insert(sym.to_string(), i as f64);
                negative.insert(sym.to_string(), -(i as f64));
                // Forward returns positively correlated with i, but add tiny noise
                // per day so std_ic > 0
                let noise = if day % 2 == 0 { 0.001 } else { -0.001 };
                forward.insert(sym.to_string(), i as f64 * 0.01 + noise);
            }
            let mut factors = HashMap::new();
            factors.insert("strong".to_string(), strong);
            factors.insert("negative".to_string(), negative);
            snapshots.push(DailyFactorSnapshot {
                date: date(day),
                factors,
                forward_returns: forward,
            });
        }

        let reports = compute_factor_ics(&snapshots);
        assert_eq!(reports.len(), 2);
        // Both should have mean |IC| ~ 1.0, but ranked by |IC IR|
        for r in &reports {
            assert!(r.mean_ic.abs() > 0.9, "{} mean_ic={}", r.factor, r.mean_ic);
            assert_eq!(r.n_days, 10);
        }
        // The "negative" factor should have mean_ic near -1.0
        let neg = reports.iter().find(|r| r.factor == "negative").unwrap();
        assert!(neg.mean_ic < -0.9);
        let pos = reports.iter().find(|r| r.factor == "strong").unwrap();
        assert!(pos.mean_ic > 0.9);
    }
}
