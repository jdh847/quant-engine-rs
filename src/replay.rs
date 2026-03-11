use std::{collections::BTreeSet, fs, path::Path};

use anyhow::Result;

use crate::{config::BotConfig, data::CsvDataPortal};

#[derive(Debug, Clone, Default)]
pub struct ReplaySummary {
    pub events: usize,
    pub dates: usize,
    pub markets: usize,
}

pub fn run_event_replay(
    cfg: &BotConfig,
    data: &CsvDataPortal,
    output_dir: impl AsRef<Path>,
) -> Result<ReplaySummary> {
    let dir = output_dir.as_ref();
    fs::create_dir_all(dir)?;

    let market_order: Vec<String> = cfg.markets.keys().cloned().collect();
    let events = data.replay_events(&market_order);

    let mut wtr = csv::Writer::from_path(dir.join("event_replay.csv"))?;
    wtr.write_record([
        "seq",
        "date",
        "market",
        "bar_count",
        "symbols",
        "avg_close",
        "notional_proxy",
    ])?;

    let mut dates = BTreeSet::new();
    let mut markets = BTreeSet::new();

    for event in &events {
        dates.insert(event.date);
        markets.insert(event.market.clone());

        let bar_count = event.bars.len().max(1);
        let avg_close = event.bars.iter().map(|b| b.close).sum::<f64>() / bar_count as f64;
        let notional_proxy = event.bars.iter().map(|b| b.close * b.volume).sum::<f64>();
        let symbols = event
            .bars
            .iter()
            .map(|b| b.symbol.clone())
            .collect::<Vec<_>>()
            .join("|");

        wtr.write_record([
            event.seq.to_string(),
            event.date.to_string(),
            event.market.clone(),
            event.bars.len().to_string(),
            symbols,
            format!("{avg_close:.6}"),
            format!("{notional_proxy:.6}"),
        ])?;
    }
    wtr.flush()?;

    let summary = ReplaySummary {
        events: events.len(),
        dates: dates.len(),
        markets: markets.len(),
    };
    fs::write(
        dir.join("event_replay_summary.txt"),
        format!(
            "events={}\ndates={}\nmarkets={}\n",
            summary.events, summary.dates, summary.markets
        ),
    )?;

    Ok(summary)
}

#[cfg(test)]
mod tests {
    use crate::{config::load_config, data::CsvDataPortal};

    use super::run_event_replay;

    #[test]
    fn replay_output_is_generated() {
        let cfg = load_config("config/bot.toml").expect("load config");
        let data = CsvDataPortal::new(
            cfg.markets
                .values()
                .map(|m| (m.name.clone(), m.data_file.clone()))
                .collect(),
        )
        .expect("load data");

        let report = run_event_replay(&cfg, &data, "outputs_rust/test_replay").expect("replay");
        assert!(report.events > 0);
    }
}
