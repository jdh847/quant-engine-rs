use std::{
    collections::{HashMap, VecDeque},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Result};
use chrono::NaiveDate;
use serde::Serialize;

use crate::{
    config::BotConfig,
    data::CsvDataPortal,
    engine::BacktestStats,
    model::Bar,
    optimize::{run_walk_forward, WalkForwardReport, WalkForwardRequest},
};

#[derive(Debug, Clone)]
pub struct ResearchReportRequest {
    pub walk_forward: WalkForwardRequest,
    pub factor_decay_horizons: Vec<usize>,
    pub regime_vol_window: usize,
    pub regime_fast_window: usize,
    pub regime_slow_window: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalkForwardDeepDiveRow {
    pub fold: usize,
    pub strategy_plugin: String,
    pub portfolio_method: String,
    pub short_window: usize,
    pub long_window: usize,
    pub vol_window: usize,
    pub top_n: usize,
    pub min_momentum: f64,
    pub train_score: f64,
    pub test_pnl_ratio: f64,
    pub test_sharpe: f64,
    pub test_calmar: f64,
    pub test_drawdown: f64,
    pub train_test_gap: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalkForwardDeepDiveSummary {
    pub folds: usize,
    pub avg_test_pnl_ratio: f64,
    pub avg_test_sharpe: f64,
    pub avg_train_test_gap: f64,
    pub strategy_turnover_ratio: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RegimeSplitRow {
    pub market: String,
    pub regime_bucket: String,
    pub observations: usize,
    pub avg_factor_momentum: f64,
    pub avg_factor_mean_reversion: f64,
    pub avg_factor_low_vol: f64,
    pub avg_factor_volume: f64,
    pub avg_composite_alpha: f64,
    pub avg_selected_symbols: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct FactorDecayRow {
    pub scope: String,
    pub factor: String,
    pub horizon_days: usize,
    pub observations: usize,
    pub ic: f64,
    pub top_quintile_avg_return: f64,
    pub bottom_quintile_avg_return: f64,
    pub long_short_spread: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResearchReport {
    pub walk_forward_summary: WalkForwardDeepDiveSummary,
    pub walk_forward_rows: Vec<WalkForwardDeepDiveRow>,
    pub regime_rows: Vec<RegimeSplitRow>,
    pub factor_decay_rows: Vec<FactorDecayRow>,
}

#[derive(Debug, Clone)]
pub struct ResearchReportArtifacts {
    pub json_path: PathBuf,
    pub markdown_path: PathBuf,
    pub html_path: PathBuf,
}

#[derive(Debug, Clone, Default)]
struct SymbolHistory {
    closes: VecDeque<f64>,
    volumes: VecDeque<f64>,
}

#[derive(Debug, Clone)]
struct SignalPoint {
    symbol: String,
    momentum: f64,
    mean_reversion: f64,
    volatility: f64,
    volume_signal: f64,
    trend_ok: bool,
}

#[derive(Debug, Clone)]
struct SignalSnapshot {
    date: NaiveDate,
    market: String,
    symbol: String,
    factor_momentum: f64,
    factor_mean_reversion: f64,
    factor_low_vol: f64,
    factor_volume: f64,
    composite_alpha: f64,
}

#[derive(Debug, Clone, Default)]
struct SignalAggregate {
    factor_momentum: f64,
    factor_mean_reversion: f64,
    factor_low_vol: f64,
    factor_volume: f64,
    composite_alpha: f64,
    selected_symbols: usize,
}

#[derive(Debug, Clone)]
struct MarketRegimePoint {
    date: NaiveDate,
    market: String,
    vol_bucket: String,
    trend_bucket: String,
}

pub fn write_research_report(
    cfg: &BotConfig,
    data: &CsvDataPortal,
    req: &ResearchReportRequest,
    output_dir: impl AsRef<Path>,
) -> Result<(ResearchReport, ResearchReportArtifacts)> {
    validate_request(req)?;
    let dir = output_dir.as_ref();
    fs::create_dir_all(dir)?;

    let walk_dir = dir.join("walk_forward");
    let walk_report = run_walk_forward(cfg, data, &req.walk_forward, &walk_dir)?;
    let walk_forward_rows = build_walk_forward_rows(&walk_report);
    let walk_forward_summary = summarize_walk_forward(&walk_forward_rows);

    let snapshots = build_signal_snapshots(cfg, data)?;
    let regime_rows = build_regime_split_report(cfg, data, &snapshots, req);
    let factor_decay_rows = build_factor_decay_report(data, &snapshots, &req.factor_decay_horizons);

    write_walk_forward_deep_dive_csv(dir.join("walk_forward_deep_dive.csv"), &walk_forward_rows)?;
    write_regime_split_csv(dir.join("regime_split.csv"), &regime_rows)?;
    write_factor_decay_csv(dir.join("factor_decay.csv"), &factor_decay_rows)?;

    let report = ResearchReport {
        walk_forward_summary,
        walk_forward_rows,
        regime_rows,
        factor_decay_rows,
    };
    let artifacts = write_summary_artifacts(dir, &report)?;
    Ok((report, artifacts))
}

fn validate_request(req: &ResearchReportRequest) -> Result<()> {
    if req.factor_decay_horizons.is_empty() {
        return Err(anyhow!("factor_decay_horizons cannot be empty"));
    }
    if req.regime_vol_window < 2 {
        return Err(anyhow!("regime_vol_window must be >= 2"));
    }
    if req.regime_fast_window < 2 || req.regime_slow_window <= req.regime_fast_window {
        return Err(anyhow!(
            "regime windows invalid: need fast >= 2 and slow > fast"
        ));
    }
    Ok(())
}

fn build_walk_forward_rows(report: &WalkForwardReport) -> Vec<WalkForwardDeepDiveRow> {
    report
        .folds
        .iter()
        .map(|fold| WalkForwardDeepDiveRow {
            fold: fold.fold,
            strategy_plugin: fold.best_strategy.strategy_plugin.clone(),
            portfolio_method: fold.best_strategy.portfolio_method.clone(),
            short_window: fold.best_strategy.short_window,
            long_window: fold.best_strategy.long_window,
            vol_window: fold.best_strategy.vol_window,
            top_n: fold.best_strategy.top_n,
            min_momentum: fold.best_strategy.min_momentum,
            train_score: fold.train_score,
            test_pnl_ratio: fold.test_stats.pnl_ratio,
            test_sharpe: fold.test_stats.sharpe,
            test_calmar: fold.test_stats.calmar,
            test_drawdown: fold.test_stats.max_drawdown,
            train_test_gap: fold.train_score - score_test_stats(&fold.test_stats),
        })
        .collect()
}

fn summarize_walk_forward(rows: &[WalkForwardDeepDiveRow]) -> WalkForwardDeepDiveSummary {
    if rows.is_empty() {
        return WalkForwardDeepDiveSummary {
            folds: 0,
            avg_test_pnl_ratio: 0.0,
            avg_test_sharpe: 0.0,
            avg_train_test_gap: 0.0,
            strategy_turnover_ratio: 0.0,
        };
    }

    let folds = rows.len();
    let mut strategy_switches = 0usize;
    for pair in rows.windows(2) {
        let prev = &pair[0];
        let curr = &pair[1];
        if prev.strategy_plugin != curr.strategy_plugin
            || prev.portfolio_method != curr.portfolio_method
            || prev.short_window != curr.short_window
            || prev.long_window != curr.long_window
            || prev.vol_window != curr.vol_window
            || prev.top_n != curr.top_n
        {
            strategy_switches += 1;
        }
    }

    WalkForwardDeepDiveSummary {
        folds,
        avg_test_pnl_ratio: rows.iter().map(|r| r.test_pnl_ratio).sum::<f64>() / folds as f64,
        avg_test_sharpe: rows.iter().map(|r| r.test_sharpe).sum::<f64>() / folds as f64,
        avg_train_test_gap: rows.iter().map(|r| r.train_test_gap).sum::<f64>() / folds as f64,
        strategy_turnover_ratio: if folds <= 1 {
            0.0
        } else {
            strategy_switches as f64 / (folds - 1) as f64
        },
    }
}

fn build_signal_snapshots(cfg: &BotConfig, data: &CsvDataPortal) -> Result<Vec<SignalSnapshot>> {
    let max_len = cfg
        .strategy
        .long_window
        .max(cfg.strategy.vol_window + 1)
        .max(cfg.strategy.mean_reversion_window + 1)
        .max(cfg.strategy.volume_window);
    let mut history: HashMap<(String, String), SymbolHistory> = HashMap::new();
    let mut snapshots = Vec::new();

    for date in data.trading_dates() {
        for market in cfg.markets.keys() {
            let bars = data.bars_for(date, market);
            if bars.is_empty() {
                continue;
            }
            let points = collect_signal_points(&bars, &mut history, max_len, cfg);
            if points.is_empty() {
                continue;
            }
            snapshots.extend(build_signal_snapshots_for_day(date, market, &points, cfg));
        }
    }

    Ok(snapshots)
}

fn collect_signal_points(
    bars: &[Bar],
    history: &mut HashMap<(String, String), SymbolHistory>,
    max_len: usize,
    cfg: &BotConfig,
) -> Vec<SignalPoint> {
    let mut out = Vec::new();

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

        out.push(SignalPoint {
            symbol: bar.symbol.clone(),
            momentum,
            mean_reversion,
            volatility,
            volume_signal,
            trend_ok,
        });
    }

    out
}

fn build_signal_snapshots_for_day(
    date: NaiveDate,
    market: &str,
    points: &[SignalPoint],
    cfg: &BotConfig,
) -> Vec<SignalSnapshot> {
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

    let mut out = Vec::new();
    for p in points {
        if p.momentum < cfg.strategy.min_momentum || !p.trend_ok {
            continue;
        }

        let z_m = *momentum_z.get(&p.symbol).unwrap_or(&0.0);
        let z_r = *mean_reversion_z.get(&p.symbol).unwrap_or(&0.0);
        let z_lv = -*volatility_z.get(&p.symbol).unwrap_or(&0.0);
        let z_v = *volume_z.get(&p.symbol).unwrap_or(&0.0);

        let (factor_momentum, factor_mean_reversion, factor_low_vol, factor_volume) =
            if cfg.strategy.strategy_plugin == "momentum_guard" {
                (z_m, 0.0, z_lv, 0.0)
            } else {
                (
                    cfg.strategy.factor_momentum_weight * z_m,
                    cfg.strategy.factor_mean_reversion_weight * z_r,
                    cfg.strategy.factor_low_vol_weight * z_lv,
                    cfg.strategy.factor_volume_weight * z_v,
                )
            };

        out.push(SignalSnapshot {
            date,
            market: market.to_string(),
            symbol: p.symbol.clone(),
            factor_momentum,
            factor_mean_reversion,
            factor_low_vol,
            factor_volume,
            composite_alpha: factor_momentum
                + factor_mean_reversion
                + factor_low_vol
                + factor_volume,
        });
    }
    out
}

fn build_regime_split_report(
    cfg: &BotConfig,
    data: &CsvDataPortal,
    snapshots: &[SignalSnapshot],
    req: &ResearchReportRequest,
) -> Vec<RegimeSplitRow> {
    let regime_points = compute_market_regimes(cfg, data, req);
    let mut signal_by_bucket: HashMap<(String, NaiveDate), SignalAggregate> = HashMap::new();
    for snap in snapshots {
        let entry = signal_by_bucket
            .entry((snap.market.clone(), snap.date))
            .or_default();
        entry.factor_momentum += snap.factor_momentum;
        entry.factor_mean_reversion += snap.factor_mean_reversion;
        entry.factor_low_vol += snap.factor_low_vol;
        entry.factor_volume += snap.factor_volume;
        entry.composite_alpha += snap.composite_alpha;
        entry.selected_symbols += 1;
    }

    let mut grouped: HashMap<(String, String), Vec<SignalAggregate>> = HashMap::new();
    for regime in regime_points {
        if let Some(agg) = signal_by_bucket.get(&(regime.market.clone(), regime.date)) {
            let mut normalized = agg.clone();
            let n = normalized.selected_symbols.max(1) as f64;
            normalized.factor_momentum /= n;
            normalized.factor_mean_reversion /= n;
            normalized.factor_low_vol /= n;
            normalized.factor_volume /= n;
            normalized.composite_alpha /= n;
            grouped
                .entry((
                    regime.market.clone(),
                    format!("{}_{}", regime.trend_bucket, regime.vol_bucket),
                ))
                .or_default()
                .push(normalized.clone());
            grouped
                .entry((
                    "ALL".to_string(),
                    format!("{}_{}", regime.trend_bucket, regime.vol_bucket),
                ))
                .or_default()
                .push(normalized);
        }
    }

    let mut rows = grouped
        .into_iter()
        .map(|((market, regime_bucket), values)| {
            let n = values.len() as f64;
            RegimeSplitRow {
                market,
                regime_bucket,
                observations: values.len(),
                avg_factor_momentum: values.iter().map(|v| v.factor_momentum).sum::<f64>() / n,
                avg_factor_mean_reversion: values
                    .iter()
                    .map(|v| v.factor_mean_reversion)
                    .sum::<f64>()
                    / n,
                avg_factor_low_vol: values.iter().map(|v| v.factor_low_vol).sum::<f64>() / n,
                avg_factor_volume: values.iter().map(|v| v.factor_volume).sum::<f64>() / n,
                avg_composite_alpha: values.iter().map(|v| v.composite_alpha).sum::<f64>() / n,
                avg_selected_symbols: values
                    .iter()
                    .map(|v| v.selected_symbols as f64)
                    .sum::<f64>()
                    / n,
            }
        })
        .collect::<Vec<_>>();

    rows.sort_by(|a, b| {
        a.market
            .cmp(&b.market)
            .then_with(|| a.regime_bucket.cmp(&b.regime_bucket))
    });
    rows
}

fn compute_market_regimes(
    cfg: &BotConfig,
    data: &CsvDataPortal,
    req: &ResearchReportRequest,
) -> Vec<MarketRegimePoint> {
    let mut market_series: HashMap<String, Vec<(NaiveDate, f64)>> = HashMap::new();
    for date in data.trading_dates() {
        for market in cfg.markets.keys() {
            let bars = data.bars_for(date, market);
            if bars.is_empty() {
                continue;
            }
            let avg_close = bars.iter().map(|b| b.close).sum::<f64>() / bars.len() as f64;
            market_series
                .entry(market.clone())
                .or_default()
                .push((date, avg_close));
        }
    }

    let mut thresholds = HashMap::new();
    let mut raw_points: HashMap<String, Vec<(NaiveDate, f64, bool)>> = HashMap::new();
    for (market, series) in &market_series {
        let mut vols = Vec::new();
        let mut points = Vec::new();
        for idx in 0..series.len() {
            if idx + 1 < req.regime_slow_window
                || idx + 1 < req.regime_vol_window + 1
                || idx + 1 < req.regime_fast_window
            {
                continue;
            }
            let fast = mean(
                &series[idx + 1 - req.regime_fast_window..=idx]
                    .iter()
                    .map(|(_, close)| *close)
                    .collect::<Vec<_>>(),
            );
            let slow = mean(
                &series[idx + 1 - req.regime_slow_window..=idx]
                    .iter()
                    .map(|(_, close)| *close)
                    .collect::<Vec<_>>(),
            );
            let vol_returns = series[idx + 1 - (req.regime_vol_window + 1)..=idx]
                .windows(2)
                .map(|w| w[1].1 / w[0].1 - 1.0)
                .collect::<Vec<_>>();
            let vol = stddev(&vol_returns);
            vols.push(vol);
            points.push((series[idx].0, vol, fast > slow));
        }
        if !vols.is_empty() {
            let (low, high) = terciles(&vols);
            thresholds.insert(market.clone(), (low, high));
            raw_points.insert(market.clone(), points);
        }
    }

    let mut out = Vec::new();
    for (market, points) in raw_points {
        let (low, high) = thresholds.get(&market).copied().unwrap_or((0.0, 0.0));
        for (date, vol, is_bull) in points {
            let vol_bucket = if vol <= low {
                "LOW_VOL"
            } else if vol >= high {
                "HIGH_VOL"
            } else {
                "MID_VOL"
            };
            let trend_bucket = if is_bull { "BULL" } else { "BEAR" };
            out.push(MarketRegimePoint {
                date,
                market: market.clone(),
                vol_bucket: vol_bucket.to_string(),
                trend_bucket: trend_bucket.to_string(),
            });
        }
    }
    out
}

fn build_factor_decay_report(
    data: &CsvDataPortal,
    snapshots: &[SignalSnapshot],
    horizons: &[usize],
) -> Vec<FactorDecayRow> {
    let index = build_forward_return_index(data, horizons);
    let mut rows = Vec::new();

    for scope in scopes_from_snapshots(snapshots) {
        for factor in [
            "momentum",
            "mean_reversion",
            "low_vol",
            "volume",
            "composite",
        ] {
            for &horizon in horizons {
                let mut pairs = Vec::new();
                let mut top_pairs = Vec::new();
                for snap in snapshots
                    .iter()
                    .filter(|s| scope == "ALL" || s.market == scope)
                {
                    let Some(fwd) = index
                        .get(&(snap.market.clone(), snap.symbol.clone(), snap.date))
                        .and_then(|m| m.get(&horizon))
                        .copied()
                    else {
                        continue;
                    };
                    let signal = match factor {
                        "momentum" => snap.factor_momentum,
                        "mean_reversion" => snap.factor_mean_reversion,
                        "low_vol" => snap.factor_low_vol,
                        "volume" => snap.factor_volume,
                        _ => snap.composite_alpha,
                    };
                    pairs.push((signal, fwd));
                    top_pairs.push((signal, fwd));
                }

                if pairs.is_empty() {
                    continue;
                }

                top_pairs.sort_by(|a, b| a.0.total_cmp(&b.0));
                let quintile = (top_pairs.len() / 5).max(1);
                let bottom_avg = top_pairs
                    .iter()
                    .take(quintile)
                    .map(|(_, ret)| *ret)
                    .sum::<f64>()
                    / quintile as f64;
                let top_avg = top_pairs
                    .iter()
                    .rev()
                    .take(quintile)
                    .map(|(_, ret)| *ret)
                    .sum::<f64>()
                    / quintile as f64;

                rows.push(FactorDecayRow {
                    scope: scope.clone(),
                    factor: factor.to_string(),
                    horizon_days: horizon,
                    observations: pairs.len(),
                    ic: pearson_corr(&pairs),
                    top_quintile_avg_return: top_avg,
                    bottom_quintile_avg_return: bottom_avg,
                    long_short_spread: top_avg - bottom_avg,
                });
            }
        }
    }

    rows.sort_by(|a, b| {
        a.scope
            .cmp(&b.scope)
            .then_with(|| a.factor.cmp(&b.factor))
            .then_with(|| a.horizon_days.cmp(&b.horizon_days))
    });
    rows
}

fn build_forward_return_index(
    data: &CsvDataPortal,
    horizons: &[usize],
) -> HashMap<(String, String, NaiveDate), HashMap<usize, f64>> {
    let mut series: HashMap<(String, String), Vec<(NaiveDate, f64)>> = HashMap::new();
    for date in data.trading_dates() {
        let markets = ["US", "A", "JP"];
        for market in markets {
            for bar in data.bars_for(date, market) {
                series
                    .entry((bar.market.clone(), bar.symbol.clone()))
                    .or_default()
                    .push((bar.date, bar.close));
            }
        }
    }

    let mut index = HashMap::new();
    for ((market, symbol), rows) in series {
        for idx in 0..rows.len() {
            let mut fwd = HashMap::new();
            for &horizon in horizons {
                if idx + horizon < rows.len() {
                    fwd.insert(horizon, rows[idx + horizon].1 / rows[idx].1 - 1.0);
                }
            }
            index.insert((market.clone(), symbol.clone(), rows[idx].0), fwd);
        }
    }
    index
}

fn scopes_from_snapshots(snapshots: &[SignalSnapshot]) -> Vec<String> {
    let mut scopes = vec!["ALL".to_string()];
    let mut markets = snapshots
        .iter()
        .map(|s| s.market.clone())
        .collect::<Vec<_>>();
    markets.sort();
    markets.dedup();
    scopes.extend(markets);
    scopes
}

fn write_walk_forward_deep_dive_csv(path: PathBuf, rows: &[WalkForwardDeepDiveRow]) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;
    wtr.write_record([
        "fold",
        "strategy_plugin",
        "portfolio_method",
        "short_window",
        "long_window",
        "vol_window",
        "top_n",
        "min_momentum",
        "train_score",
        "test_pnl_ratio",
        "test_sharpe",
        "test_calmar",
        "test_drawdown",
        "train_test_gap",
    ])?;
    for row in rows {
        wtr.write_record([
            row.fold.to_string(),
            row.strategy_plugin.clone(),
            row.portfolio_method.clone(),
            row.short_window.to_string(),
            row.long_window.to_string(),
            row.vol_window.to_string(),
            row.top_n.to_string(),
            format!("{:.6}", row.min_momentum),
            format!("{:.6}", row.train_score),
            format!("{:.6}", row.test_pnl_ratio),
            format!("{:.6}", row.test_sharpe),
            format!("{:.6}", row.test_calmar),
            format!("{:.6}", row.test_drawdown),
            format!("{:.6}", row.train_test_gap),
        ])?;
    }
    wtr.flush()?;
    Ok(())
}

fn write_regime_split_csv(path: PathBuf, rows: &[RegimeSplitRow]) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;
    wtr.write_record([
        "market",
        "regime_bucket",
        "observations",
        "avg_factor_momentum",
        "avg_factor_mean_reversion",
        "avg_factor_low_vol",
        "avg_factor_volume",
        "avg_composite_alpha",
        "avg_selected_symbols",
    ])?;
    for row in rows {
        wtr.write_record([
            row.market.clone(),
            row.regime_bucket.clone(),
            row.observations.to_string(),
            format!("{:.6}", row.avg_factor_momentum),
            format!("{:.6}", row.avg_factor_mean_reversion),
            format!("{:.6}", row.avg_factor_low_vol),
            format!("{:.6}", row.avg_factor_volume),
            format!("{:.6}", row.avg_composite_alpha),
            format!("{:.6}", row.avg_selected_symbols),
        ])?;
    }
    wtr.flush()?;
    Ok(())
}

fn write_factor_decay_csv(path: PathBuf, rows: &[FactorDecayRow]) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;
    wtr.write_record([
        "scope",
        "factor",
        "horizon_days",
        "observations",
        "ic",
        "top_quintile_avg_return",
        "bottom_quintile_avg_return",
        "long_short_spread",
    ])?;
    for row in rows {
        wtr.write_record([
            row.scope.clone(),
            row.factor.clone(),
            row.horizon_days.to_string(),
            row.observations.to_string(),
            format!("{:.6}", row.ic),
            format!("{:.6}", row.top_quintile_avg_return),
            format!("{:.6}", row.bottom_quintile_avg_return),
            format!("{:.6}", row.long_short_spread),
        ])?;
    }
    wtr.flush()?;
    Ok(())
}

fn write_summary_artifacts(dir: &Path, report: &ResearchReport) -> Result<ResearchReportArtifacts> {
    let json_path = dir.join("research_report.json");
    let markdown_path = dir.join("research_report.md");
    let html_path = dir.join("research_report.html");

    fs::write(&json_path, serde_json::to_string_pretty(report)?)?;
    fs::write(&markdown_path, render_markdown(report))?;
    fs::write(&html_path, render_html(report))?;

    Ok(ResearchReportArtifacts {
        json_path,
        markdown_path,
        html_path,
    })
}

fn render_markdown(report: &ResearchReport) -> String {
    let mut out = String::new();
    out.push_str("# Research Report\n\n");
    out.push_str("## Walk-Forward\n\n");
    out.push_str(&format!(
        "- folds: {}\n- avg_test_pnl_ratio: {:.4}%\n- avg_test_sharpe: {:.4}\n- avg_train_test_gap: {:.4}\n- strategy_turnover_ratio: {:.2}%\n\n",
        report.walk_forward_summary.folds,
        report.walk_forward_summary.avg_test_pnl_ratio * 100.0,
        report.walk_forward_summary.avg_test_sharpe,
        report.walk_forward_summary.avg_train_test_gap,
        report.walk_forward_summary.strategy_turnover_ratio * 100.0
    ));
    out.push_str(
        "| Fold | Strategy | Portfolio | Train Score | Test PnL % | Test Sharpe | Gap |\n",
    );
    out.push_str("| --- | --- | --- | ---: | ---: | ---: | ---: |\n");
    for row in &report.walk_forward_rows {
        out.push_str(&format!(
            "| {} | {} | {} | {:.4} | {:.4}% | {:.4} | {:.4} |\n",
            row.fold,
            row.strategy_plugin,
            row.portfolio_method,
            row.train_score,
            row.test_pnl_ratio * 100.0,
            row.test_sharpe,
            row.train_test_gap
        ));
    }

    out.push_str("\n## Regime Split\n\n");
    out.push_str("| Market | Regime | Obs | Avg Composite | Avg Momentum | Avg Low Vol |\n");
    out.push_str("| --- | --- | ---: | ---: | ---: | ---: |\n");
    for row in &report.regime_rows {
        out.push_str(&format!(
            "| {} | {} | {} | {:.4} | {:.4} | {:.4} |\n",
            row.market,
            row.regime_bucket,
            row.observations,
            row.avg_composite_alpha,
            row.avg_factor_momentum,
            row.avg_factor_low_vol
        ));
    }

    out.push_str("\n## Factor Decay\n\n");
    out.push_str("| Scope | Factor | Horizon | Obs | IC | Top Q Avg Ret | Long/Short |\n");
    out.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: |\n");
    for row in &report.factor_decay_rows {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {:.4} | {:.4}% | {:.4}% |\n",
            row.scope,
            row.factor,
            row.horizon_days,
            row.observations,
            row.ic,
            row.top_quintile_avg_return * 100.0,
            row.long_short_spread * 100.0
        ));
    }
    out
}

fn render_html(report: &ResearchReport) -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Research Report</title>
  <style>
    :root {{ color-scheme: light; --bg:#f6f1e8; --card:#fffdf8; --ink:#1c1a17; --muted:#6f685e; --line:#d9cfc1; --accent:#1f6f78; --accent2:#d9822b; }}
    body {{ margin:0; font-family: Georgia, "Iowan Old Style", serif; background: radial-gradient(circle at top left, #fff7e8, var(--bg)); color:var(--ink); }}
    main {{ max-width: 1180px; margin: 0 auto; padding: 32px 20px 48px; }}
    h1,h2 {{ margin: 0 0 12px; }}
    .sub {{ color:var(--muted); margin-bottom:24px; }}
    .grid {{ display:grid; gap:16px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); margin-bottom:24px; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:18px; padding:18px; box-shadow:0 10px 28px rgba(28,26,23,.05); }}
    .k {{ color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.08em; }}
    .v {{ font-size:28px; font-weight:700; margin-top:6px; }}
    table {{ width:100%; border-collapse:collapse; background:var(--card); border:1px solid var(--line); border-radius:16px; overflow:hidden; }}
    th,td {{ padding:10px 12px; border-bottom:1px solid var(--line); text-align:left; font-size:14px; }}
    th {{ background:#f2eadc; }}
    section {{ margin-top:28px; }}
  </style>
</head>
<body>
  <main>
    <h1>Research Report</h1>
    <div class="sub">Walk-forward, regime split, and factor decay diagnostics</div>
    <div class="grid">
      <div class="card"><div class="k">Walk-Forward Folds</div><div class="v">{folds}</div></div>
      <div class="card"><div class="k">Avg Test PnL</div><div class="v">{avg_pnl:.2}%</div></div>
      <div class="card"><div class="k">Avg Test Sharpe</div><div class="v">{avg_sharpe:.2}</div></div>
      <div class="card"><div class="k">Strategy Turnover</div><div class="v">{turnover:.1}%</div></div>
    </div>
    <section>
      <h2>Walk-Forward</h2>
      {walk_table}
    </section>
    <section>
      <h2>Regime Split</h2>
      {regime_table}
    </section>
    <section>
      <h2>Factor Decay</h2>
      {decay_table}
    </section>
  </main>
</body>
</html>"#,
        folds = report.walk_forward_summary.folds,
        avg_pnl = report.walk_forward_summary.avg_test_pnl_ratio * 100.0,
        avg_sharpe = report.walk_forward_summary.avg_test_sharpe,
        turnover = report.walk_forward_summary.strategy_turnover_ratio * 100.0,
        walk_table = html_walk_table(&report.walk_forward_rows),
        regime_table = html_regime_table(&report.regime_rows),
        decay_table = html_decay_table(&report.factor_decay_rows),
    )
}

fn html_walk_table(rows: &[WalkForwardDeepDiveRow]) -> String {
    let mut out = String::from(
        "<table><thead><tr><th>Fold</th><th>Strategy</th><th>Portfolio</th><th>Train Score</th><th>Test PnL %</th><th>Test Sharpe</th><th>Gap</th></tr></thead><tbody>",
    );
    for row in rows {
        out.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{:.4}</td><td>{:.4}%</td><td>{:.4}</td><td>{:.4}</td></tr>",
            row.fold,
            escape_html(&row.strategy_plugin),
            escape_html(&row.portfolio_method),
            row.train_score,
            row.test_pnl_ratio * 100.0,
            row.test_sharpe,
            row.train_test_gap
        ));
    }
    out.push_str("</tbody></table>");
    out
}

fn html_regime_table(rows: &[RegimeSplitRow]) -> String {
    let mut out = String::from(
        "<table><thead><tr><th>Market</th><th>Regime</th><th>Obs</th><th>Avg Composite</th><th>Avg Momentum</th><th>Avg Low Vol</th></tr></thead><tbody>",
    );
    for row in rows {
        out.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{:.4}</td><td>{:.4}</td><td>{:.4}</td></tr>",
            escape_html(&row.market),
            escape_html(&row.regime_bucket),
            row.observations,
            row.avg_composite_alpha,
            row.avg_factor_momentum,
            row.avg_factor_low_vol
        ));
    }
    out.push_str("</tbody></table>");
    out
}

fn html_decay_table(rows: &[FactorDecayRow]) -> String {
    let mut out = String::from(
        "<table><thead><tr><th>Scope</th><th>Factor</th><th>Horizon</th><th>Obs</th><th>IC</th><th>Top Quintile Avg Ret</th><th>Long/Short</th></tr></thead><tbody>",
    );
    for row in rows {
        out.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{:.4}</td><td>{:.4}%</td><td>{:.4}%</td></tr>",
            escape_html(&row.scope),
            escape_html(&row.factor),
            row.horizon_days,
            row.observations,
            row.ic,
            row.top_quintile_avg_return * 100.0,
            row.long_short_spread * 100.0
        ));
    }
    out.push_str("</tbody></table>");
    out
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn terciles(values: &[f64]) -> (f64, f64) {
    if values.is_empty() {
        return (0.0, 0.0);
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let low_idx = ((sorted.len() as f64 - 1.0) * 0.33).round() as usize;
    let high_idx = ((sorted.len() as f64 - 1.0) * 0.67).round() as usize;
    (
        sorted[low_idx.min(sorted.len() - 1)],
        sorted[high_idx.min(sorted.len() - 1)],
    )
}

fn pearson_corr(values: &[(f64, f64)]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let mean_x = values.iter().map(|(x, _)| *x).sum::<f64>() / values.len() as f64;
    let mean_y = values.iter().map(|(_, y)| *y).sum::<f64>() / values.len() as f64;
    let mut cov = 0.0;
    let mut var_x = 0.0;
    let mut var_y = 0.0;
    for (x, y) in values {
        let dx = *x - mean_x;
        let dy = *y - mean_y;
        cov += dx * dy;
        var_x += dx * dx;
        var_y += dy * dy;
    }
    if var_x <= 1e-12 || var_y <= 1e-12 {
        0.0
    } else {
        cov / (var_x.sqrt() * var_y.sqrt())
    }
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

fn score_test_stats(stats: &BacktestStats) -> f64 {
    stats.pnl_ratio + stats.sharpe * 0.10 + stats.calmar * 0.05 - stats.max_drawdown * 0.8
}

fn escape_html(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use crate::{config::load_config, data::CsvDataPortal};

    use super::{write_research_report, ResearchReportRequest, WalkForwardRequest};

    #[test]
    fn research_report_writes_core_outputs() {
        let cfg = load_config("config/bot.toml").expect("load config");
        let data = CsvDataPortal::new(
            cfg.markets
                .values()
                .map(|m| (m.name.clone(), m.data_file.clone()))
                .collect(),
        )
        .expect("load data");

        let req = ResearchReportRequest {
            walk_forward: WalkForwardRequest {
                train_days: 10,
                test_days: 4,
                strategy_plugins: vec!["layered_multi_factor".to_string()],
                short_windows: vec![3],
                long_windows: vec![7],
                vol_windows: vec![5],
                top_ns: vec![1],
                min_momentums: vec![0.001],
                portfolio_methods: vec!["risk_parity".to_string()],
            },
            factor_decay_horizons: vec![1, 3, 5],
            regime_vol_window: 5,
            regime_fast_window: 3,
            regime_slow_window: 7,
        };

        let (_report, artifacts) =
            write_research_report(&cfg, &data, &req, "outputs_rust/test_research_report")
                .expect("research report");
        assert!(artifacts.json_path.exists());
        assert!(artifacts.markdown_path.exists());
        assert!(artifacts.html_path.exists());
    }
}
