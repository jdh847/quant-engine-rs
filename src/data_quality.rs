use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
};

use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use serde::Deserialize;

use crate::calendar::ExchangeCalendar;
use crate::config::BotConfig;

#[derive(Debug, Clone)]
pub struct DataQualityRequest {
    pub return_outlier_threshold: f64,
    pub gap_days_threshold: i64,
}

#[derive(Debug, Clone)]
pub struct DataQualityRow {
    pub market: String,
    pub rows: usize,
    pub unique_symbols: usize,
    pub duplicate_rows: usize,
    pub invalid_close_rows: usize,
    pub invalid_volume_rows: usize,
    pub date_order_violations: usize,
    pub return_outliers: usize,
    pub large_gaps: usize,
    pub non_trading_day_rows: usize,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct DataQualityReport {
    pub rows: Vec<DataQualityRow>,
}

#[derive(Debug, Deserialize)]
struct CsvBar {
    date: String,
    symbol: String,
    close: f64,
    #[serde(default)]
    adj_close: Option<f64>,
    volume: f64,
}

pub fn run_data_quality_check(
    cfg: &BotConfig,
    request: &DataQualityRequest,
    output_dir: impl AsRef<Path>,
) -> Result<DataQualityReport> {
    if request.return_outlier_threshold <= 0.0 {
        return Err(anyhow!("return_outlier_threshold must be > 0"));
    }
    if request.gap_days_threshold <= 0 {
        return Err(anyhow!("gap_days_threshold must be > 0"));
    }

    let mut rows = Vec::new();
    let mut cal = ExchangeCalendar::new();
    for market in cfg.markets.values() {
        cal.add_holidays(&market.name, &market.holiday_dates);
    }
    for market in cfg.markets.values() {
        let row = inspect_market_csv(
            &market.name,
            &market.data_file,
            request.return_outlier_threshold,
            request.gap_days_threshold,
            &cal,
        )?;
        rows.push(row);
    }

    let report = DataQualityReport { rows };
    write_report(output_dir, &report)?;
    Ok(report)
}

fn inspect_market_csv(
    market: &str,
    path: &Path,
    outlier_threshold: f64,
    gap_days_threshold: i64,
    cal: &ExchangeCalendar,
) -> Result<DataQualityRow> {
    let mut rdr = csv::Reader::from_path(path)
        .with_context(|| format!("open market csv failed: {}", path.display()))?;

    let mut seen = HashSet::new();
    let mut symbols = HashSet::new();
    let mut last_date: HashMap<String, NaiveDate> = HashMap::new();
    let mut last_close: HashMap<String, f64> = HashMap::new();

    let mut rows = 0usize;
    let mut duplicate_rows = 0usize;
    let mut invalid_close_rows = 0usize;
    let mut invalid_volume_rows = 0usize;
    let mut date_order_violations = 0usize;
    let mut return_outliers = 0usize;
    let mut large_gaps = 0usize;
    let mut non_trading_day_rows = 0usize;

    for record in rdr.deserialize::<CsvBar>() {
        let record = record.with_context(|| format!("parse csv row failed: {}", path.display()))?;
        rows += 1;
        symbols.insert(record.symbol.clone());

        let date = NaiveDate::parse_from_str(&record.date, "%Y-%m-%d")
            .with_context(|| format!("invalid date {} in {}", record.date, path.display()))?;
        if !seen.insert((date, record.symbol.clone())) {
            duplicate_rows += 1;
        }

        if !cal.is_trading_day(market, date) {
            non_trading_day_rows += 1;
        }

        let close = record.adj_close.unwrap_or(record.close);
        if !close.is_finite() || close <= 0.0 {
            invalid_close_rows += 1;
        }
        if !record.volume.is_finite() || record.volume < 0.0 {
            invalid_volume_rows += 1;
        }

        if let Some(prev_date) = last_date.get(&record.symbol) {
            if date < *prev_date {
                date_order_violations += 1;
            }
            let gap = (date - *prev_date).num_days();
            if gap > gap_days_threshold {
                large_gaps += 1;
            }
        }
        if let Some(prev_close) = last_close.get(&record.symbol) {
            if prev_close.abs() > 1e-9 {
                let ret = close / *prev_close - 1.0;
                if ret.abs() > outlier_threshold {
                    return_outliers += 1;
                }
            }
        }
        last_date.insert(record.symbol.clone(), date);
        last_close.insert(record.symbol, close);
    }

    let status = if duplicate_rows > 0
        || invalid_close_rows > 0
        || invalid_volume_rows > 0
        || date_order_violations > 0
    {
        "FAIL"
    } else if return_outliers > 0 || large_gaps > 0 || non_trading_day_rows > 0 {
        "WARN"
    } else {
        "PASS"
    }
    .to_string();

    Ok(DataQualityRow {
        market: market.to_string(),
        rows,
        unique_symbols: symbols.len(),
        duplicate_rows,
        invalid_close_rows,
        invalid_volume_rows,
        date_order_violations,
        return_outliers,
        large_gaps,
        non_trading_day_rows,
        status,
    })
}

fn write_report(output_dir: impl AsRef<Path>, report: &DataQualityReport) -> Result<()> {
    let dir = output_dir.as_ref();
    fs::create_dir_all(dir)?;

    let csv_path = dir.join("data_quality_report.csv");
    let mut wtr = csv::Writer::from_path(csv_path)?;
    wtr.write_record([
        "market",
        "rows",
        "unique_symbols",
        "duplicate_rows",
        "invalid_close_rows",
        "invalid_volume_rows",
        "date_order_violations",
        "return_outliers",
        "large_gaps",
        "non_trading_day_rows",
        "status",
    ])?;
    for row in &report.rows {
        wtr.write_record([
            row.market.clone(),
            row.rows.to_string(),
            row.unique_symbols.to_string(),
            row.duplicate_rows.to_string(),
            row.invalid_close_rows.to_string(),
            row.invalid_volume_rows.to_string(),
            row.date_order_violations.to_string(),
            row.return_outliers.to_string(),
            row.large_gaps.to_string(),
            row.non_trading_day_rows.to_string(),
            row.status.clone(),
        ])?;
    }
    wtr.flush()?;

    let total_rows = report.rows.iter().map(|r| r.rows).sum::<usize>();
    let fails = report.rows.iter().filter(|r| r.status == "FAIL").count();
    let warns = report.rows.iter().filter(|r| r.status == "WARN").count();
    let summary = format!(
        "markets={}\ntotal_rows={}\nfail_markets={}\nwarn_markets={}\n",
        report.rows.len(),
        total_rows,
        fails,
        warns
    );
    fs::write(dir.join("data_quality_summary.txt"), summary)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::config::load_config;

    use super::{inspect_market_csv, run_data_quality_check, DataQualityRequest};

    #[test]
    fn data_quality_runs_on_sample_data() {
        let cfg = load_config("config/bot.toml").expect("load config");
        let report = run_data_quality_check(
            &cfg,
            &DataQualityRequest {
                return_outlier_threshold: 0.35,
                gap_days_threshold: 10,
            },
            "outputs_rust/test_data_quality",
        )
        .expect("quality check");
        assert!(!report.rows.is_empty());
    }

    #[test]
    fn non_trading_days_are_flagged_as_warn() {
        let dir = std::env::temp_dir().join("pqbot_data_quality_non_trading_day");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("mkdir");

        // 2025-01-04 is a Saturday.
        let csv_path = dir.join("us.csv");
        fs::write(
            &csv_path,
            "date,symbol,close,volume\n2025-01-04,AAA,10.0,1000\n",
        )
        .expect("write csv");

        let cal = crate::calendar::ExchangeCalendar::new();
        let row = inspect_market_csv("US", &csv_path, 0.35, 10, &cal).expect("inspect");
        assert_eq!(row.non_trading_day_rows, 1);
        assert_eq!(row.status, "WARN");
    }
}
