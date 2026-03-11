use std::{
    collections::{HashMap, VecDeque},
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;
use chrono::NaiveDate;

use crate::{config::BotConfig, data::CsvDataPortal, model::Bar};

#[derive(Debug, Clone, Default)]
struct SymbolHistory {
    closes: VecDeque<f64>,
    volumes: VecDeque<f64>,
}

#[derive(Debug, Clone)]
struct FactorPoint {
    symbol: String,
    momentum: f64,
    mean_reversion: f64,
    volatility: f64,
    volume_signal: f64,
    trend_ok: bool,
}

#[derive(Debug, Clone)]
struct AttributionRow {
    date: NaiveDate,
    market: String,
    factor_momentum: f64,
    factor_mean_reversion: f64,
    factor_low_vol: f64,
    factor_volume: f64,
    composite_alpha: f64,
    selected_symbols: usize,
}

pub struct AttributionArtifacts {
    pub csv_path: PathBuf,
    pub summary_path: PathBuf,
}

pub fn write_factor_attribution_report(
    cfg: &BotConfig,
    data: &CsvDataPortal,
    output_dir: impl AsRef<Path>,
) -> Result<AttributionArtifacts> {
    let out_dir = output_dir.as_ref();
    fs::create_dir_all(out_dir)?;

    let mut history: HashMap<(String, String), SymbolHistory> = HashMap::new();
    let max_len = cfg
        .strategy
        .long_window
        .max(cfg.strategy.vol_window + 1)
        .max(cfg.strategy.mean_reversion_window + 1)
        .max(cfg.strategy.volume_window);

    let mut rows = Vec::new();
    for date in data.trading_dates() {
        for market in cfg.markets.keys() {
            let bars = data.bars_for(date, market);
            if bars.is_empty() {
                continue;
            }
            let points = collect_factor_points(&bars, &mut history, max_len, cfg);
            if points.is_empty() {
                continue;
            }
            if let Some(row) = build_row(date, market, points, cfg) {
                rows.push(row);
            }
        }
    }

    let csv_path = out_dir.join("factor_attribution.csv");
    let summary_path = out_dir.join("factor_attribution_summary.txt");
    write_csv(&csv_path, &rows)?;
    write_summary(&summary_path, &rows)?;

    Ok(AttributionArtifacts {
        csv_path,
        summary_path,
    })
}

fn collect_factor_points(
    bars: &[Bar],
    history: &mut HashMap<(String, String), SymbolHistory>,
    max_len: usize,
    cfg: &BotConfig,
) -> Vec<FactorPoint> {
    let mut points = Vec::new();

    for bar in bars {
        let key = (bar.market.clone(), bar.symbol.clone());
        let h = history.entry(key).or_default();
        h.closes.push_back(bar.close);
        h.volumes.push_back(bar.volume);
        while h.closes.len() > max_len {
            h.closes.pop_front();
        }
        while h.volumes.len() > max_len {
            h.volumes.pop_front();
        }

        if h.closes.len()
            < cfg
                .strategy
                .long_window
                .max(cfg.strategy.vol_window + 1)
                .max(cfg.strategy.mean_reversion_window + 1)
            || h.volumes.len() < cfg.strategy.volume_window
        {
            continue;
        }

        let len = h.closes.len();
        let closes = h.closes.make_contiguous();
        let short_slice = &closes[len - cfg.strategy.short_window..len];
        let long_slice = &closes[len - cfg.strategy.long_window..len];
        let short_ma = short_slice.iter().sum::<f64>() / short_slice.len() as f64;
        let long_ma = long_slice.iter().sum::<f64>() / long_slice.len() as f64;
        let trend_ok = short_ma > long_ma;

        let momentum = bar.close / long_slice[0] - 1.0;
        let mr_base_idx = len - 1 - cfg.strategy.mean_reversion_window;
        let mean_reversion = -(bar.close / closes[mr_base_idx] - 1.0);
        let vol_slice = &closes[len - (cfg.strategy.vol_window + 1)..];
        let returns: Vec<f64> = vol_slice.windows(2).map(|w| w[1] / w[0] - 1.0).collect();
        let volatility = stddev(&returns).max(1e-6);

        let volumes = h.volumes.make_contiguous();
        let volume_slice = &volumes[volumes.len() - cfg.strategy.volume_window..];
        let avg_volume = volume_slice.iter().sum::<f64>() / volume_slice.len() as f64;
        let volume_signal = if avg_volume > 0.0 {
            bar.volume / avg_volume - 1.0
        } else {
            0.0
        };

        points.push(FactorPoint {
            symbol: bar.symbol.clone(),
            momentum,
            mean_reversion,
            volatility,
            volume_signal,
            trend_ok,
        });
    }

    points
}

fn build_row(
    date: NaiveDate,
    market: &str,
    points: Vec<FactorPoint>,
    cfg: &BotConfig,
) -> Option<AttributionRow> {
    let momentum_z = winsorized_zscores(
        &points
            .iter()
            .map(|p| (p.symbol.clone(), p.momentum))
            .collect::<Vec<_>>(),
        cfg.strategy.winsorize_pct,
    );
    let mean_reversion_z = winsorized_zscores(
        &points
            .iter()
            .map(|p| (p.symbol.clone(), p.mean_reversion))
            .collect::<Vec<_>>(),
        cfg.strategy.winsorize_pct,
    );
    let volatility_z = winsorized_zscores(
        &points
            .iter()
            .map(|p| (p.symbol.clone(), p.volatility))
            .collect::<Vec<_>>(),
        cfg.strategy.winsorize_pct,
    );
    let volume_z = winsorized_zscores(
        &points
            .iter()
            .map(|p| (p.symbol.clone(), p.volume_signal))
            .collect::<Vec<_>>(),
        cfg.strategy.winsorize_pct,
    );

    let mut rows = Vec::new();
    for p in points {
        if p.momentum < cfg.strategy.min_momentum || !p.trend_ok {
            continue;
        }

        let z_m = *momentum_z.get(&p.symbol).unwrap_or(&0.0);
        let z_r = *mean_reversion_z.get(&p.symbol).unwrap_or(&0.0);
        let z_lv = -*volatility_z.get(&p.symbol).unwrap_or(&0.0);
        let z_v = *volume_z.get(&p.symbol).unwrap_or(&0.0);

        let (c_m, c_r, c_lv, c_v) = if cfg.strategy.strategy_plugin == "momentum_guard" {
            // Keep attribution aligned with simplified plugin behavior.
            (z_m, 0.0, z_lv, 0.0)
        } else {
            (
                cfg.strategy.factor_momentum_weight * z_m,
                cfg.strategy.factor_mean_reversion_weight * z_r,
                cfg.strategy.factor_low_vol_weight * z_lv,
                cfg.strategy.factor_volume_weight * z_v,
            )
        };
        rows.push((c_m, c_r, c_lv, c_v, c_m + c_r + c_lv + c_v));
    }

    if rows.is_empty() {
        return None;
    }

    let n = rows.len() as f64;
    let avg_m = rows.iter().map(|r| r.0.abs()).sum::<f64>() / n;
    let avg_r = rows.iter().map(|r| r.1.abs()).sum::<f64>() / n;
    let avg_lv = rows.iter().map(|r| r.2.abs()).sum::<f64>() / n;
    let avg_v = rows.iter().map(|r| r.3.abs()).sum::<f64>() / n;
    let avg_c = rows.iter().map(|r| r.4).sum::<f64>() / n;

    Some(AttributionRow {
        date,
        market: market.to_string(),
        factor_momentum: avg_m,
        factor_mean_reversion: avg_r,
        factor_low_vol: avg_lv,
        factor_volume: avg_v,
        composite_alpha: avg_c,
        selected_symbols: rows.len(),
    })
}

fn write_csv(path: &Path, rows: &[AttributionRow]) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;
    wtr.write_record([
        "date",
        "market",
        "factor_momentum",
        "factor_mean_reversion",
        "factor_low_vol",
        "factor_volume",
        "composite_alpha",
        "selected_symbols",
    ])?;

    for row in rows {
        wtr.write_record([
            row.date.to_string(),
            row.market.clone(),
            format!("{:.6}", row.factor_momentum),
            format!("{:.6}", row.factor_mean_reversion),
            format!("{:.6}", row.factor_low_vol),
            format!("{:.6}", row.factor_volume),
            format!("{:.6}", row.composite_alpha),
            row.selected_symbols.to_string(),
        ])?;
    }
    wtr.flush()?;
    Ok(())
}

fn write_summary(path: &Path, rows: &[AttributionRow]) -> Result<()> {
    if rows.is_empty() {
        fs::write(path, "rows=0\n")?;
        return Ok(());
    }
    let n = rows.len() as f64;
    let avg_m = rows.iter().map(|r| r.factor_momentum).sum::<f64>() / n;
    let avg_r = rows.iter().map(|r| r.factor_mean_reversion).sum::<f64>() / n;
    let avg_lv = rows.iter().map(|r| r.factor_low_vol).sum::<f64>() / n;
    let avg_v = rows.iter().map(|r| r.factor_volume).sum::<f64>() / n;
    let avg_c = rows.iter().map(|r| r.composite_alpha).sum::<f64>() / n;

    let abs_m = rows.iter().map(|r| r.factor_momentum.abs()).sum::<f64>() / n;
    let abs_r = rows
        .iter()
        .map(|r| r.factor_mean_reversion.abs())
        .sum::<f64>()
        / n;
    let abs_lv = rows.iter().map(|r| r.factor_low_vol.abs()).sum::<f64>() / n;
    let abs_v = rows.iter().map(|r| r.factor_volume.abs()).sum::<f64>() / n;
    let abs_total = (abs_m + abs_r + abs_lv + abs_v).max(1e-9);

    let selected = rows.iter().map(|r| r.selected_symbols as f64).sum::<f64>() / n;
    let text = format!(
        "rows={}\navg_factor_momentum={:.6}\navg_factor_mean_reversion={:.6}\navg_factor_low_vol={:.6}\navg_factor_volume={:.6}\navg_composite_alpha={:.6}\nshare_factor_momentum={:.2}%\nshare_factor_mean_reversion={:.2}%\nshare_factor_low_vol={:.2}%\nshare_factor_volume={:.2}%\navg_selected_symbols={:.2}\n",
        rows.len(),
        avg_m,
        avg_r,
        avg_lv,
        avg_v,
        avg_c,
        abs_m / abs_total * 100.0,
        abs_r / abs_total * 100.0,
        abs_lv / abs_total * 100.0,
        abs_v / abs_total * 100.0,
        selected
    );
    fs::write(path, text)?;
    Ok(())
}

fn winsorized_zscores(values: &[(String, f64)], pct: f64) -> HashMap<String, f64> {
    let winsorized = winsorize(values, pct);
    zscores(&winsorized)
}

fn winsorize(values: &[(String, f64)], pct: f64) -> Vec<(String, f64)> {
    if values.len() < 4 || pct <= 0.0 {
        return values.to_vec();
    }

    let mut sorted: Vec<f64> = values.iter().map(|(_, v)| *v).collect();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let low_idx = ((sorted.len() as f64 - 1.0) * pct).floor() as usize;
    let high_idx = ((sorted.len() as f64 - 1.0) * (1.0 - pct)).ceil() as usize;
    let low = sorted[low_idx.min(sorted.len() - 1)];
    let high = sorted[high_idx.min(sorted.len() - 1)];

    values
        .iter()
        .map(|(k, v)| (k.clone(), v.clamp(low, high)))
        .collect()
}

fn zscores(values: &[(String, f64)]) -> HashMap<String, f64> {
    if values.is_empty() {
        return HashMap::new();
    }
    let mean = values.iter().map(|(_, v)| *v).sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|(_, v)| {
            let d = *v - mean;
            d * d
        })
        .sum::<f64>()
        / values.len() as f64;
    let stdev = variance.sqrt();
    if stdev < 1e-9 {
        return values
            .iter()
            .map(|(symbol, _)| (symbol.clone(), 0.0))
            .collect();
    }

    values
        .iter()
        .map(|(symbol, value)| (symbol.clone(), (*value - mean) / stdev))
        .collect()
}

fn stddev(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|v| {
            let d = *v - mean;
            d * d
        })
        .sum::<f64>()
        / values.len() as f64;
    variance.sqrt()
}

#[cfg(test)]
mod tests {
    use crate::{config::load_config, data::CsvDataPortal};

    use super::write_factor_attribution_report;

    #[test]
    fn factor_attribution_outputs_files() {
        let cfg = load_config("config/bot.toml").expect("load config");
        let data = CsvDataPortal::new(
            cfg.markets
                .values()
                .map(|m| (m.name.clone(), m.data_file.clone()))
                .collect(),
        )
        .expect("load data");

        let out = write_factor_attribution_report(&cfg, &data, "outputs_rust/test_attribution")
            .expect("write attribution");
        assert!(out.csv_path.exists());
        assert!(out.summary_path.exists());
    }
}
