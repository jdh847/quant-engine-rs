use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::NaiveDate;
use serde::Deserialize;

use crate::model::Bar;

#[derive(Debug, Deserialize)]
struct CsvBar {
    date: String,
    symbol: String,
    close: f64,
    volume: f64,
}

#[derive(Debug, Default, Clone)]
pub struct CsvDataPortal {
    bars_by_day: BTreeMap<NaiveDate, HashMap<String, Vec<Bar>>>,
}

#[derive(Debug, Clone)]
pub struct ReplayEvent {
    pub seq: u64,
    pub date: NaiveDate,
    pub market: String,
    pub bars: Vec<Bar>,
}

impl CsvDataPortal {
    pub fn new(market_files: Vec<(String, PathBuf)>) -> Result<Self> {
        let mut portal = Self::default();

        for (market, file) in market_files {
            let mut reader = csv::Reader::from_path(&file)
                .with_context(|| format!("open csv failed: {}", file.display()))?;

            for row in reader.deserialize::<CsvBar>() {
                let row = row.with_context(|| format!("parse row failed in {}", file.display()))?;
                let bar_date = NaiveDate::parse_from_str(&row.date, "%Y-%m-%d")
                    .with_context(|| format!("invalid date {} in {}", row.date, file.display()))?;

                let bar = Bar {
                    date: bar_date,
                    market: market.clone(),
                    symbol: row.symbol,
                    close: row.close,
                    volume: row.volume,
                };

                portal
                    .bars_by_day
                    .entry(bar_date)
                    .or_default()
                    .entry(market.clone())
                    .or_default()
                    .push(bar);
            }
        }

        for markets in portal.bars_by_day.values_mut() {
            for bars in markets.values_mut() {
                bars.sort_by(|a, b| a.symbol.cmp(&b.symbol));
            }
        }

        Ok(portal)
    }

    pub fn trading_dates(&self) -> Vec<NaiveDate> {
        self.bars_by_day.keys().copied().collect()
    }

    pub fn bars_for(&self, date: NaiveDate, market: &str) -> Vec<Bar> {
        self.bars_by_day
            .get(&date)
            .and_then(|m| m.get(market))
            .cloned()
            .unwrap_or_default()
    }

    pub fn slice_by_dates(&self, selected_dates: &[NaiveDate]) -> Self {
        let mut sliced = Self::default();

        for date in selected_dates {
            if let Some(day_map) = self.bars_by_day.get(date) {
                sliced.bars_by_day.insert(*date, day_map.clone());
            }
        }

        sliced
    }

    pub fn replay_events(&self, market_order: &[String]) -> Vec<ReplayEvent> {
        let order_map: HashMap<&str, usize> = market_order
            .iter()
            .enumerate()
            .map(|(idx, m)| (m.as_str(), idx))
            .collect();

        let mut seq = 1u64;
        let mut events = Vec::new();
        for (date, day_map) in &self.bars_by_day {
            let mut market_bars: Vec<(&String, &Vec<Bar>)> = day_map.iter().collect();
            market_bars.sort_by(|(a, _), (b, _)| {
                let ia = order_map.get(a.as_str()).copied().unwrap_or(usize::MAX);
                let ib = order_map.get(b.as_str()).copied().unwrap_or(usize::MAX);
                ia.cmp(&ib).then_with(|| a.cmp(b))
            });

            for (market, bars) in market_bars {
                events.push(ReplayEvent {
                    seq,
                    date: *date,
                    market: market.clone(),
                    bars: bars.clone(),
                });
                seq += 1;
            }
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::NaiveDate;

    use super::{CsvDataPortal, ReplayEvent};
    use crate::model::Bar;

    #[test]
    fn replay_events_respect_market_order() {
        let date = NaiveDate::from_ymd_opt(2025, 1, 2).expect("date");
        let mut day = HashMap::new();
        day.insert(
            "JP".to_string(),
            vec![Bar {
                date,
                market: "JP".to_string(),
                symbol: "7203".to_string(),
                close: 100.0,
                volume: 1000.0,
            }],
        );
        day.insert(
            "US".to_string(),
            vec![Bar {
                date,
                market: "US".to_string(),
                symbol: "AAPL".to_string(),
                close: 100.0,
                volume: 1000.0,
            }],
        );

        let mut bars_by_day = std::collections::BTreeMap::new();
        bars_by_day.insert(date, day);
        let portal = CsvDataPortal { bars_by_day };

        let events: Vec<ReplayEvent> = portal.replay_events(&["US".to_string(), "JP".to_string()]);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].market, "US");
        assert_eq!(events[1].market, "JP");
    }
}
