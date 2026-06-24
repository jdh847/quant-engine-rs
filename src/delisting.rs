//! Delisting / corporate-action terminal events.
//!
//! Feeds the engine's forced-liquidation hook so that a name leaving the
//! tradeable universe (bankruptcy, M&A, demotion) realizes its terminal P&L
//! instead of being marked at a stale price forever. Without this, even a
//! survivorship-free price dataset would still leak survivorship bias on the
//! P&L side: a delisted holding would keep its last close as equity forever.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::NaiveDate;

/// A single delisting event: a holding is removed from the market at `date` and
/// settled at `terminal_price` per share.
#[derive(Debug, Clone)]
pub struct DelistingEvent {
    pub date: NaiveDate,
    pub market: String,
    pub symbol: String,
    /// Terminal value per share (bankruptcy recovery ~0, M&A cash-out, or last
    /// trade for a demotion). Negative values are clamped to 0 by the broker.
    pub terminal_price: f64,
    pub reason: String,
}

/// Load delisting events from a CSV with header
/// `market,symbol,delist_date,terminal_price,reason`.
/// Rows with a blank market/symbol are skipped; `reason` is optional.
pub fn load_delistings(path: &Path) -> Result<Vec<DelistingEvent>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_path(path)
        .with_context(|| format!("opening delistings file {}", path.display()))?;

    let mut out = Vec::new();
    for (i, record) in rdr.records().enumerate() {
        let record =
            record.with_context(|| format!("reading delistings row {}", i + 1))?;
        if record.len() < 4 {
            continue;
        }
        let market = record.get(0).unwrap_or("").trim().to_string();
        let symbol = record.get(1).unwrap_or("").trim().to_string();
        if market.is_empty() || symbol.is_empty() {
            continue;
        }
        let date_str = record.get(2).unwrap_or("").trim();
        let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").with_context(|| {
            format!("invalid delist_date '{}' on row {}", date_str, i + 1)
        })?;
        let terminal_price: f64 = record.get(3).unwrap_or("").trim().parse().unwrap_or(0.0);
        let reason = record.get(4).unwrap_or("").trim().to_string();

        out.push(DelistingEvent {
            date,
            market,
            symbol,
            terminal_price,
            reason,
        });
    }
    Ok(out)
}

/// Group events by date for O(1) per-day lookup inside the engine loop.
pub fn group_by_date(events: Vec<DelistingEvent>) -> HashMap<NaiveDate, Vec<DelistingEvent>> {
    let mut map: HashMap<NaiveDate, Vec<DelistingEvent>> = HashMap::new();
    for event in events {
        map.entry(event.date).or_default().push(event);
    }
    map
}

/// Convenience: load a delistings CSV next to a dataset if it exists, returning
/// an empty map when the file is absent (delistings are optional).
pub fn load_optional_grouped(path: &Path) -> Result<HashMap<NaiveDate, Vec<DelistingEvent>>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    Ok(group_by_date(load_delistings(path)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_tmp(contents: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "delistings_test_{}.csv",
            contents.len() as u64 * 2654435761
        ));
        let mut f = std::fs::File::create(&path).expect("create tmp");
        f.write_all(contents.as_bytes()).expect("write tmp");
        path
    }

    #[test]
    fn loads_and_parses_events() {
        let path = write_tmp(
            "market,symbol,delist_date,terminal_price,reason\n\
             US,DEAD,2020-03-16,0.0,bankruptcy\n\
             US,ACQ,2021-06-01,52.5,acquired\n",
        );
        let events = load_delistings(&path).expect("load");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].symbol, "DEAD");
        assert_eq!(events[0].date, NaiveDate::from_ymd_opt(2020, 3, 16).unwrap());
        assert!((events[0].terminal_price - 0.0).abs() < 1e-9);
        assert_eq!(events[0].reason, "bankruptcy");
        assert!((events[1].terminal_price - 52.5).abs() < 1e-9);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn skips_blank_rows_and_tolerates_missing_reason() {
        let path = write_tmp(
            "market,symbol,delist_date,terminal_price,reason\n\
             US,NOREASON,2022-01-10,1.25\n\
             ,,,,\n",
        );
        let events = load_delistings(&path).expect("load");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].symbol, "NOREASON");
        assert_eq!(events[0].reason, "");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn groups_by_date() {
        let events = vec![
            DelistingEvent {
                date: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                market: "US".into(),
                symbol: "A".into(),
                terminal_price: 0.0,
                reason: String::new(),
            },
            DelistingEvent {
                date: NaiveDate::from_ymd_opt(2020, 1, 1).unwrap(),
                market: "US".into(),
                symbol: "B".into(),
                terminal_price: 0.0,
                reason: String::new(),
            },
        ];
        let grouped = group_by_date(events);
        assert_eq!(grouped.len(), 1);
        assert_eq!(grouped[&NaiveDate::from_ymd_opt(2020, 1, 1).unwrap()].len(), 2);
    }
}
