use std::{
    collections::{BTreeMap, HashMap, VecDeque},
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
pub struct FactorQuintileRow {
    pub scope: String,
    pub factor: String,
    pub horizon_days: usize,
    pub observations: usize,
    pub q1_avg_return: f64,
    pub q2_avg_return: f64,
    pub q3_avg_return: f64,
    pub q4_avg_return: f64,
    pub q5_avg_return: f64,
    pub monotonicity_score: f64,
    pub q5_q1_spread: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RegimeDecayRow {
    pub market: String,
    pub regime_bucket: String,
    pub factor: String,
    pub horizon_days: usize,
    pub observations: usize,
    pub ic: f64,
    pub long_short_spread: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RollingIcRow {
    pub date: String,
    pub factor: String,
    pub horizon_days: usize,
    pub observations: usize,
    pub ic: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResearchReport {
    pub walk_forward_summary: WalkForwardDeepDiveSummary,
    pub walk_forward_rows: Vec<WalkForwardDeepDiveRow>,
    pub regime_rows: Vec<RegimeSplitRow>,
    pub factor_decay_rows: Vec<FactorDecayRow>,
    pub factor_quintile_rows: Vec<FactorQuintileRow>,
    pub regime_decay_rows: Vec<RegimeDecayRow>,
    pub rolling_ic_rows: Vec<RollingIcRow>,
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
    let regime_points = compute_market_regimes(cfg, data, req);
    let regime_rows = build_regime_split_report(&regime_points, &snapshots);
    let factor_decay_rows = build_factor_decay_report(data, &snapshots, &req.factor_decay_horizons);
    let factor_quintile_rows =
        build_factor_quintile_report(data, &snapshots, &req.factor_decay_horizons);
    let regime_decay_rows =
        build_regime_decay_report(data, &snapshots, &regime_points, &req.factor_decay_horizons);
    let rolling_ic_rows = build_rolling_ic_report(data, &snapshots, &req.factor_decay_horizons);

    write_walk_forward_deep_dive_csv(dir.join("walk_forward_deep_dive.csv"), &walk_forward_rows)?;
    write_regime_split_csv(dir.join("regime_split.csv"), &regime_rows)?;
    write_factor_decay_csv(dir.join("factor_decay.csv"), &factor_decay_rows)?;
    write_factor_quintile_csv(dir.join("factor_quintiles.csv"), &factor_quintile_rows)?;
    write_regime_decay_csv(dir.join("regime_decay.csv"), &regime_decay_rows)?;
    write_rolling_ic_csv(dir.join("rolling_ic.csv"), &rolling_ic_rows)?;

    let report = ResearchReport {
        walk_forward_summary,
        walk_forward_rows,
        regime_rows,
        factor_decay_rows,
        factor_quintile_rows,
        regime_decay_rows,
        rolling_ic_rows,
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
    regime_points: &[MarketRegimePoint],
    snapshots: &[SignalSnapshot],
) -> Vec<RegimeSplitRow> {
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

fn build_factor_quintile_report(
    data: &CsvDataPortal,
    snapshots: &[SignalSnapshot],
    horizons: &[usize],
) -> Vec<FactorQuintileRow> {
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
                    let signal = factor_signal(snap, factor);
                    pairs.push((signal, fwd));
                }
                if pairs.len() < 5 {
                    continue;
                }
                pairs.sort_by(|a, b| a.0.total_cmp(&b.0));
                let quintiles = average_quintile_returns(&pairs);
                let monotonicity_score = pearson_corr(
                    &quintiles
                        .iter()
                        .enumerate()
                        .map(|(idx, val)| (idx as f64 + 1.0, *val))
                        .collect::<Vec<_>>(),
                );
                rows.push(FactorQuintileRow {
                    scope: scope.clone(),
                    factor: factor.to_string(),
                    horizon_days: horizon,
                    observations: pairs.len(),
                    q1_avg_return: quintiles[0],
                    q2_avg_return: quintiles[1],
                    q3_avg_return: quintiles[2],
                    q4_avg_return: quintiles[3],
                    q5_avg_return: quintiles[4],
                    monotonicity_score,
                    q5_q1_spread: quintiles[4] - quintiles[0],
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

fn build_regime_decay_report(
    data: &CsvDataPortal,
    snapshots: &[SignalSnapshot],
    regime_points: &[MarketRegimePoint],
    horizons: &[usize],
) -> Vec<RegimeDecayRow> {
    let index = build_forward_return_index(data, horizons);
    let mut regime_lookup = HashMap::new();
    for regime in regime_points {
        regime_lookup.insert(
            (regime.market.clone(), regime.date),
            format!("{}_{}", regime.trend_bucket, regime.vol_bucket),
        );
    }

    let mut grouped: HashMap<(String, String, String, usize), Vec<(f64, f64)>> = HashMap::new();
    for snap in snapshots {
        let Some(regime_bucket) = regime_lookup
            .get(&(snap.market.clone(), snap.date))
            .cloned()
        else {
            continue;
        };
        let Some(horizon_map) = index.get(&(snap.market.clone(), snap.symbol.clone(), snap.date))
        else {
            continue;
        };
        for &horizon in horizons {
            let Some(forward_ret) = horizon_map.get(&horizon).copied() else {
                continue;
            };
            for factor in [
                "momentum",
                "mean_reversion",
                "low_vol",
                "volume",
                "composite",
            ] {
                let signal = factor_signal(snap, factor);
                grouped
                    .entry((
                        snap.market.clone(),
                        regime_bucket.clone(),
                        factor.to_string(),
                        horizon,
                    ))
                    .or_default()
                    .push((signal, forward_ret));
                grouped
                    .entry((
                        "ALL".to_string(),
                        regime_bucket.clone(),
                        factor.to_string(),
                        horizon,
                    ))
                    .or_default()
                    .push((signal, forward_ret));
            }
        }
    }

    let mut rows = grouped
        .into_iter()
        .filter_map(|((market, regime_bucket, factor, horizon_days), pairs)| {
            if pairs.len() < 5 {
                return None;
            }
            let mut sorted = pairs.clone();
            sorted.sort_by(|a, b| a.0.total_cmp(&b.0));
            let quintiles = average_quintile_returns(&sorted);
            Some(RegimeDecayRow {
                market,
                regime_bucket,
                factor,
                horizon_days,
                observations: pairs.len(),
                ic: pearson_corr(&pairs),
                long_short_spread: quintiles[4] - quintiles[0],
            })
        })
        .collect::<Vec<_>>();

    rows.sort_by(|a, b| {
        a.market
            .cmp(&b.market)
            .then_with(|| a.regime_bucket.cmp(&b.regime_bucket))
            .then_with(|| a.factor.cmp(&b.factor))
            .then_with(|| a.horizon_days.cmp(&b.horizon_days))
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

fn build_rolling_ic_report(
    data: &CsvDataPortal,
    snapshots: &[SignalSnapshot],
    horizons: &[usize],
) -> Vec<RollingIcRow> {
    let index = build_forward_return_index(data, horizons);
    let mut grouped: BTreeMap<(NaiveDate, String, usize), Vec<(f64, f64)>> = BTreeMap::new();

    for snap in snapshots {
        let Some(horizon_map) = index.get(&(snap.market.clone(), snap.symbol.clone(), snap.date))
        else {
            continue;
        };
        for &horizon in horizons {
            let Some(forward_ret) = horizon_map.get(&horizon).copied() else {
                continue;
            };
            for (factor, signal) in [
                ("momentum", snap.factor_momentum),
                ("mean_reversion", snap.factor_mean_reversion),
                ("low_vol", snap.factor_low_vol),
                ("volume", snap.factor_volume),
                ("composite", snap.composite_alpha),
            ] {
                grouped
                    .entry((snap.date, factor.to_string(), horizon))
                    .or_default()
                    .push((signal, forward_ret));
            }
        }
    }

    grouped
        .into_iter()
        .filter_map(|((date, factor, horizon_days), values)| {
            if values.len() < 3 {
                return None;
            }
            Some(RollingIcRow {
                date: date.to_string(),
                factor,
                horizon_days,
                observations: values.len(),
                ic: pearson_corr(&values),
            })
        })
        .collect()
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

fn write_factor_quintile_csv(path: PathBuf, rows: &[FactorQuintileRow]) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;
    wtr.write_record([
        "scope",
        "factor",
        "horizon_days",
        "observations",
        "q1_avg_return",
        "q2_avg_return",
        "q3_avg_return",
        "q4_avg_return",
        "q5_avg_return",
        "monotonicity_score",
        "q5_q1_spread",
    ])?;
    for row in rows {
        wtr.write_record([
            row.scope.clone(),
            row.factor.clone(),
            row.horizon_days.to_string(),
            row.observations.to_string(),
            format!("{:.6}", row.q1_avg_return),
            format!("{:.6}", row.q2_avg_return),
            format!("{:.6}", row.q3_avg_return),
            format!("{:.6}", row.q4_avg_return),
            format!("{:.6}", row.q5_avg_return),
            format!("{:.6}", row.monotonicity_score),
            format!("{:.6}", row.q5_q1_spread),
        ])?;
    }
    wtr.flush()?;
    Ok(())
}

fn write_regime_decay_csv(path: PathBuf, rows: &[RegimeDecayRow]) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;
    wtr.write_record([
        "market",
        "regime_bucket",
        "factor",
        "horizon_days",
        "observations",
        "ic",
        "long_short_spread",
    ])?;
    for row in rows {
        wtr.write_record([
            row.market.clone(),
            row.regime_bucket.clone(),
            row.factor.clone(),
            row.horizon_days.to_string(),
            row.observations.to_string(),
            format!("{:.6}", row.ic),
            format!("{:.6}", row.long_short_spread),
        ])?;
    }
    wtr.flush()?;
    Ok(())
}

fn write_rolling_ic_csv(path: PathBuf, rows: &[RollingIcRow]) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;
    wtr.write_record(["date", "factor", "horizon_days", "observations", "ic"])?;
    for row in rows {
        wtr.write_record([
            row.date.clone(),
            row.factor.clone(),
            row.horizon_days.to_string(),
            row.observations.to_string(),
            format!("{:.6}", row.ic),
        ])?;
    }
    wtr.flush()?;
    Ok(())
}

fn write_summary_artifacts(dir: &Path, report: &ResearchReport) -> Result<ResearchReportArtifacts> {
    let json_path = dir.join("research_report.json");
    let markdown_path = dir.join("research_report.md");
    let html_path = dir.join("research_report.html");
    let summary_path = dir.join("research_report_summary.txt");

    fs::write(&json_path, serde_json::to_string_pretty(report)?)?;
    fs::write(&markdown_path, render_markdown(report))?;
    fs::write(&html_path, render_html(report))?;
    fs::write(&summary_path, render_summary(report))?;

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
    out.push_str("\n## Rolling IC\n\n");
    out.push_str("| Date | Factor | Horizon | Obs | IC |\n");
    out.push_str("| --- | --- | ---: | ---: | ---: |\n");
    for row in &report.rolling_ic_rows {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {:.4} |\n",
            row.date, row.factor, row.horizon_days, row.observations, row.ic
        ));
    }
    out
}

fn render_summary(report: &ResearchReport) -> String {
    let mut winner_counts: BTreeMap<(String, String), usize> = BTreeMap::new();
    for row in &report.walk_forward_rows {
        *winner_counts
            .entry((row.strategy_plugin.clone(), row.portfolio_method.clone()))
            .or_default() += 1;
    }
    let dominant_winner = winner_counts
        .iter()
        .max_by(|a, b| a.1.cmp(b.1).then_with(|| a.0.cmp(b.0)));
    let dominant_winner_count = dominant_winner.map(|(_, count)| *count).unwrap_or(0);
    let dominant_winner_concentration = if report.walk_forward_rows.is_empty() {
        0.0
    } else {
        dominant_winner_count as f64 / report.walk_forward_rows.len() as f64
    };
    let unstable_folds = report
        .walk_forward_rows
        .iter()
        .filter(|row| row.test_sharpe <= 0.0 || row.train_test_gap > 0.5)
        .count();
    let best_decay = report
        .factor_decay_rows
        .iter()
        .max_by(|a, b| a.ic.total_cmp(&b.ic));
    let best_monotonic = report
        .factor_quintile_rows
        .iter()
        .max_by(|a, b| a.monotonicity_score.total_cmp(&b.monotonicity_score));
    let best_regime_decay = report
        .regime_decay_rows
        .iter()
        .max_by(|a, b| a.ic.total_cmp(&b.ic));
    let latest_rolling = report.rolling_ic_rows.last();
    format!(
        "folds={}\navg_test_pnl_ratio={:.4}%\navg_test_sharpe={:.4}\navg_train_test_gap={:.4}\nstrategy_turnover_ratio={:.2}%\ndominant_winner_strategy_plugin={}\ndominant_winner_portfolio_method={}\ndominant_winner_count={}\ndominant_winner_concentration={:.2}%\nunstable_folds={}\nregime_rows={}\nfactor_decay_rows={}\nfactor_quintile_rows={}\nregime_decay_rows={}\nrolling_ic_rows={}\nbest_decay_factor={}\nbest_decay_horizon_days={}\nbest_decay_ic={:.4}\nbest_monotonic_factor={}\nbest_monotonic_horizon_days={}\nbest_monotonicity_score={:.4}\nbest_regime_decay_market={}\nbest_regime_decay_bucket={}\nbest_regime_decay_factor={}\nbest_regime_decay_horizon_days={}\nbest_regime_decay_ic={:.4}\nlatest_rolling_factor={}\nlatest_rolling_horizon_days={}\nlatest_rolling_ic={:.4}\n",
        report.walk_forward_summary.folds,
        report.walk_forward_summary.avg_test_pnl_ratio * 100.0,
        report.walk_forward_summary.avg_test_sharpe,
        report.walk_forward_summary.avg_train_test_gap,
        report.walk_forward_summary.strategy_turnover_ratio * 100.0,
        dominant_winner
            .map(|((plugin, _), _)| plugin.as_str())
            .unwrap_or("-"),
        dominant_winner
            .map(|((_, method), _)| method.as_str())
            .unwrap_or("-"),
        dominant_winner_count,
        dominant_winner_concentration * 100.0,
        unstable_folds,
        report.regime_rows.len(),
        report.factor_decay_rows.len(),
        report.factor_quintile_rows.len(),
        report.regime_decay_rows.len(),
        report.rolling_ic_rows.len(),
        best_decay.map(|r| r.factor.as_str()).unwrap_or("-"),
        best_decay.map(|r| r.horizon_days).unwrap_or(0),
        best_decay.map(|r| r.ic).unwrap_or(0.0),
        best_monotonic.map(|r| r.factor.as_str()).unwrap_or("-"),
        best_monotonic.map(|r| r.horizon_days).unwrap_or(0),
        best_monotonic
            .map(|r| r.monotonicity_score)
            .unwrap_or(0.0),
        best_regime_decay.map(|r| r.market.as_str()).unwrap_or("-"),
        best_regime_decay
            .map(|r| r.regime_bucket.as_str())
            .unwrap_or("-"),
        best_regime_decay.map(|r| r.factor.as_str()).unwrap_or("-"),
        best_regime_decay.map(|r| r.horizon_days).unwrap_or(0),
        best_regime_decay.map(|r| r.ic).unwrap_or(0.0),
        latest_rolling.map(|r| r.factor.as_str()).unwrap_or("-"),
        latest_rolling.map(|r| r.horizon_days).unwrap_or(0),
        latest_rolling.map(|r| r.ic).unwrap_or(0.0),
    )
}

fn render_html(report: &ResearchReport) -> String {
    let report_json = json_for_html(report);
    let template = r##"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Research Report</title>
  <style>
    :root {{ color-scheme: light; --bg:#f6f1e8; --card:#fffdf8; --ink:#1c1a17; --muted:#6f685e; --line:#d9cfc1; --accent:#1f6f78; --accent2:#d9822b; --accent3:#a63d40; }}
    body {{ margin:0; font-family: Georgia, "Iowan Old Style", serif; background: radial-gradient(circle at top left, #fff7e8, var(--bg)); color:var(--ink); }}
    main {{ max-width: 1240px; margin: 0 auto; padding: 32px 20px 48px; }}
    h1,h2 {{ margin: 0 0 12px; }}
    .sub {{ color:var(--muted); margin-bottom:24px; }}
    .grid {{ display:grid; gap:16px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); margin-bottom:24px; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:18px; padding:18px; box-shadow:0 10px 28px rgba(28,26,23,.05); }}
    .k {{ color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.08em; }}
    .v {{ font-size:28px; font-weight:700; margin-top:6px; }}
    .grid-2 {{ display:grid; grid-template-columns: 1.1fr .9fr; gap:16px; }}
    .grid-3 {{ display:grid; grid-template-columns: repeat(3, 1fr); gap:16px; }}
    .toolbar {{ display:flex; flex-wrap:wrap; gap:12px; align-items:center; margin:0 0 16px; }}
    .pill {{ display:inline-flex; align-items:center; gap:6px; padding:8px 12px; border-radius:999px; background:#f2eadc; border:1px solid var(--line); color:var(--muted); font-size:13px; }}
    select {{ border:1px solid var(--line); border-radius:10px; background:#fff; padding:8px 10px; color:var(--ink); }}
    table {{ width:100%; border-collapse:collapse; background:var(--card); border:1px solid var(--line); border-radius:16px; overflow:hidden; }}
    th,td {{ padding:10px 12px; border-bottom:1px solid var(--line); text-align:left; font-size:14px; }}
    th {{ background:#f2eadc; }}
    section {{ margin-top:28px; }}
    .chart-card {{ min-height: 280px; }}
    .bars {{ display:flex; align-items:flex-end; gap:10px; min-height:220px; padding:16px 0 8px; }}
    .bar-col {{ flex:1; min-width:0; display:flex; flex-direction:column; align-items:center; gap:8px; }}
    .bar-wrap {{ width:100%; max-width:58px; height:180px; display:flex; align-items:flex-end; }}
    .bar {{ width:100%; border-radius:12px 12px 6px 6px; background:linear-gradient(180deg, var(--accent), #18474d); position:relative; }}
    .bar.neg {{ background:linear-gradient(180deg, var(--accent3), #6a2325); }}
    .bar-note {{ font-size:12px; color:var(--muted); text-align:center; }}
    .bar-label {{ font-size:12px; color:var(--ink); }}
    .heatmap {{ display:grid; gap:10px; }}
    .heat-row {{ display:grid; gap:10px; grid-template-columns: 150px repeat(8, minmax(70px, 1fr)); align-items:center; }}
    .heat-head {{ color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.08em; }}
    .heat-factor {{ font-size:13px; font-weight:700; }}
    .heat-cell {{ border-radius:12px; padding:12px 8px; text-align:center; border:1px solid rgba(28,26,23,.06); }}
    .heat-value {{ font-size:16px; font-weight:700; }}
    .heat-meta {{ font-size:11px; color:rgba(28,26,23,.68); margin-top:4px; }}
    .regime-grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap:12px; }}
    .regime-card {{ padding:14px; border-radius:16px; background:#fcfaf5; border:1px solid var(--line); }}
    .regime-title {{ font-size:12px; color:var(--muted); text-transform:uppercase; letter-spacing:.08em; margin-bottom:8px; }}
    .regime-main {{ font-size:24px; font-weight:700; margin-bottom:8px; }}
    .regime-sub {{ font-size:13px; color:var(--muted); line-height:1.45; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }}
    @media (max-width: 920px) {{
      .grid-2, .grid-3 {{ grid-template-columns: 1fr; }}
      .heat-row {{ grid-template-columns: 120px repeat(8, minmax(58px, 1fr)); }}
    }}
  </style>
</head>
<body>
  <main>
    <h1>Research Report</h1>
    <div class="sub">Walk-forward, regime split, and factor decay diagnostics</div>
    <div class="grid">
      <div class="card"><div class="k">Walk-Forward Folds</div><div class="v">__FOLDS__</div></div>
      <div class="card"><div class="k">Avg Test PnL</div><div class="v">__AVG_PNL__%</div></div>
      <div class="card"><div class="k">Avg Test Sharpe</div><div class="v">__AVG_SHARPE__</div></div>
      <div class="card"><div class="k">Strategy Turnover</div><div class="v">__TURNOVER__%</div></div>
    </div>
    <section class="grid-2">
      <div class="card chart-card">
        <h2>Walk-Forward Edge</h2>
        <div class="sub">Each fold shows out-of-sample PnL, with color encoding for positive vs negative.</div>
        <div id="walk-bars" class="bars"></div>
      </div>
      <div class="card chart-card">
        <h2>Research Posture</h2>
        <div class="sub">Compact view of selection stability and train-vs-test gap.</div>
        <div id="walk-posture" class="grid-3"></div>
      </div>
    </section>
    <section>
      <div class="toolbar">
        <h2 style="margin-right:auto;">Factor Decay</h2>
        <label class="pill">Scope
          <select id="decay-scope"></select>
        </label>
        <label class="pill">Metric
          <select id="decay-metric">
            <option value="ic">IC</option>
            <option value="long_short_spread">Long/Short</option>
          </select>
        </label>
      </div>
      <div class="grid-2">
        <div class="card chart-card">
        <div class="sub">Heatmap of factor IC by horizon. Warm cells are stronger, cool cells are weaker.</div>
        <div id="decay-heatmap" class="heatmap"></div>
        </div>
        <div class="card chart-card">
          <div class="sub">Curve view across horizons for each factor.</div>
          <div id="decay-curve"></div>
        </div>
      </div>
      <div class="card" style="margin-top:16px;">
        <div class="sub">Full decay table</div>
        <div id="decay-table"></div>
      </div>
    </section>
    <section>
      <div class="toolbar">
        <h2 style="margin-right:auto;">Quantile Ladder</h2>
        <label class="pill">Scope
          <select id="quintile-scope"></select>
        </label>
        <label class="pill">Horizon
          <select id="quintile-horizon"></select>
        </label>
      </div>
      <div class="grid-2">
        <div class="card chart-card">
          <div class="sub">Q1 to Q5 forward returns and monotonicity per factor.</div>
          <div id="quintile-chart"></div>
        </div>
        <div class="card">
          <div class="sub">Full quintile table</div>
          <div id="quintile-table"></div>
        </div>
      </div>
    </section>
    <section>
      <div class="toolbar">
        <h2 style="margin-right:auto;">Rolling IC</h2>
        <label class="pill">Horizon
          <select id="rolling-horizon"></select>
        </label>
      </div>
      <div class="card chart-card">
        <div class="sub">Cross-sectional IC over time, grouped by factor.</div>
        <div id="rolling-ic-chart"></div>
      </div>
      <div class="card" style="margin-top:16px;">
        <div class="sub">Latest rolling IC rows</div>
        <div id="rolling-ic-table"></div>
      </div>
    </section>
    <section>
      <div class="toolbar">
        <h2 style="margin-right:auto;">Regime Split</h2>
        <label class="pill">Market
          <select id="regime-market"></select>
        </label>
      </div>
      <div class="grid-2">
        <div class="card chart-card">
          <div class="sub">Composite alpha by volatility/trend bucket.</div>
          <div id="regime-cards" class="regime-grid"></div>
        </div>
        <div class="card">
          <div class="sub">Detailed rows</div>
          <div id="regime-table"></div>
        </div>
      </div>
    </section>
    <section>
      <div class="toolbar">
        <h2 style="margin-right:auto;">Regime-Conditional Decay</h2>
        <label class="pill">Market
          <select id="regime-decay-market"></select>
        </label>
        <label class="pill">Regime
          <select id="regime-decay-bucket"></select>
        </label>
      </div>
      <div class="card">
        <div class="sub">IC and long/short spread for each factor conditioned on market regime.</div>
        <div id="regime-decay-table"></div>
      </div>
    </section>
    <section>
      <h2>Walk-Forward Table</h2>
      <div id="walk-table"></div>
    </section>
  </main>
  <script id="report-data" type="application/json">__REPORT_JSON__</script>
  <script>
    const report = JSON.parse(document.getElementById('report-data').textContent);

    function fmtPct(v) {{
      return `${{(v * 100).toFixed(2)}}%`;
    }}

    function fmtNum(v) {{
      return Number(v).toFixed(3);
    }}

    function esc(text) {{
      return String(text)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;');
    }}

    function renderWalkBars() {{
      const root = document.getElementById('walk-bars');
      const rows = report.walk_forward_rows;
      const maxAbs = Math.max(...rows.map(r => Math.abs(r.test_pnl_ratio)), 0.0001);
      root.innerHTML = rows.map(row => {{
        const pct = Math.abs(row.test_pnl_ratio) / maxAbs;
        const height = Math.max(18, pct * 180);
        const neg = row.test_pnl_ratio < 0 ? 'neg' : '';
        return `<div class="bar-col">
          <div class="bar-note mono">PnL ${{fmtPct(row.test_pnl_ratio)}}</div>
          <div class="bar-wrap"><div class="bar ${{neg}}" style="height:${{height}}px"></div></div>
          <div class="bar-label">Fold ${{row.fold}}</div>
          <div class="bar-note">${{esc(row.strategy_plugin)}} / ${{esc(row.portfolio_method)}}</div>
        </div>`;
      }}).join('');
    }}

    function renderWalkPosture() {{
      const root = document.getElementById('walk-posture');
      const s = report.walk_forward_summary;
      const cards = [
        ['Avg Test Sharpe', fmtNum(s.avg_test_sharpe)],
        ['Avg Train/Test Gap', fmtNum(s.avg_train_test_gap)],
        ['Strategy Turnover', `${{(s.strategy_turnover_ratio * 100).toFixed(1)}}%`],
      ];
      root.innerHTML = cards.map(([k, v]) => `<div class="card"><div class="k">${{esc(k)}}</div><div class="v">${{esc(v)}}</div></div>`).join('');
    }}

    function buildHeatColor(value) {{
      const clamped = Math.max(-0.25, Math.min(0.25, value));
      if (clamped >= 0) {{
        const alpha = 0.18 + (clamped / 0.25) * 0.6;
        return `rgba(31, 111, 120, ${{alpha.toFixed(3)}})`;
      }}
      const alpha = 0.18 + (Math.abs(clamped) / 0.25) * 0.6;
      return `rgba(166, 61, 64, ${{alpha.toFixed(3)}})`;
    }}

    function renderDecay(scope) {{
      const rows = report.factor_decay_rows.filter(r => r.scope === scope);
      const horizons = [...new Set(rows.map(r => r.horizon_days))].sort((a, b) => a - b);
      const factors = ['momentum', 'mean_reversion', 'low_vol', 'volume', 'composite'];
      const root = document.getElementById('decay-heatmap');
      root.innerHTML = '';
      root.insertAdjacentHTML('beforeend', `<div class="heat-row heat-head"><div>Factor</div>${{horizons.map(h => `<div>${{h}}d</div>`).join('')}}</div>`);
      factors.forEach(factor => {{
        const cells = horizons.map(h => rows.find(r => r.factor === factor && r.horizon_days === h));
        root.insertAdjacentHTML('beforeend', `<div class="heat-row">
          <div class="heat-factor">${{esc(factor)}}</div>
          ${{
            cells.map(cell => {{
              if (!cell) return '<div class="heat-cell">-</div>';
              return `<div class="heat-cell" style="background:${{buildHeatColor(cell.ic)}};">
                <div class="heat-value">${{fmtNum(cell.ic)}}</div>
                <div class="heat-meta">${{fmtPct(cell.long_short_spread)}}</div>
              </div>`;
            }}).join('')
          }}
        </div>`);
      }});

      const table = document.getElementById('decay-table');
      table.innerHTML = `<table><thead><tr><th>Factor</th><th>Horizon</th><th>Obs</th><th>IC</th><th>Top Q Avg Ret</th><th>Bottom Q Avg Ret</th><th>Long/Short</th></tr></thead><tbody>${
        rows.map(row => `<tr>
          <td>${{esc(row.factor)}}</td>
          <td>${{row.horizon_days}}d</td>
          <td>${{row.observations}}</td>
          <td>${{fmtNum(row.ic)}}</td>
          <td>${{fmtPct(row.top_quintile_avg_return)}}</td>
          <td>${{fmtPct(row.bottom_quintile_avg_return)}}</td>
          <td>${{fmtPct(row.long_short_spread)}}</td>
        </tr>`).join('')
      }</tbody></table>`;

      renderDecayCurve(scope, document.getElementById('decay-metric').value);
    }}

    function lineChartSvg(series, width, height) {{
      const padding = 24;
      const allValues = series.flatMap(s => s.values.map(v => v.y));
      const min = Math.min(...allValues, 0);
      const max = Math.max(...allValues, 0);
      const span = Math.max(max - min, 1e-6);
      const colors = ['#1f6f78', '#d9822b', '#a63d40', '#4d6c3b', '#5b4b8a'];
      const lines = series.map((s, idx) => {{
        const pts = s.values.map((v, i) => {{
          const x = padding + (i * (width - padding * 2)) / Math.max(s.values.length - 1, 1);
          const y = height - padding - ((v.y - min) / span) * (height - padding * 2);
          return `${{x.toFixed(1)}},${{y.toFixed(1)}}`;
        }}).join(' ');
        const color = colors[idx % colors.length];
        return `<polyline fill="none" stroke="${{color}}" stroke-width="3" points="${{pts}}" />
          <text x="${{width - padding}}" y="${{padding + idx * 16}}" fill="${{color}}" font-size="12" text-anchor="end">${{esc(s.name)}}</text>`;
      }}).join('');
      const zeroY = height - padding - ((0 - min) / span) * (height - padding * 2);
      return `<svg viewBox="0 0 ${{width}} ${{height}}" width="100%" height="${{height}}">
        <line x1="${{padding}}" y1="${{zeroY}}" x2="${{width - padding}}" y2="${{zeroY}}" stroke="#d9cfc1" stroke-dasharray="4 4" />
        <line x1="${{padding}}" y1="${{padding}}" x2="${{padding}}" y2="${{height - padding}}" stroke="#d9cfc1" />
        <line x1="${{padding}}" y1="${{height - padding}}" x2="${{width - padding}}" y2="${{height - padding}}" stroke="#d9cfc1" />
        ${{lines}}
      </svg>`;
    }}

    function renderDecayCurve(scope, metric) {{
      const rows = report.factor_decay_rows.filter(r => r.scope === scope);
      const factors = ['momentum', 'mean_reversion', 'low_vol', 'volume', 'composite'];
      const series = factors.map(factor => {{
        const values = rows
          .filter(r => r.factor === factor)
          .sort((a, b) => a.horizon_days - b.horizon_days)
          .map(r => ({{
            x: r.horizon_days,
            y: metric === 'ic' ? r.ic : r.long_short_spread,
          }}));
        return {{ name: factor, values }};
      }}).filter(s => s.values.length > 0);
      const root = document.getElementById('decay-curve');
      root.innerHTML = series.length
        ? lineChartSvg(series, 560, 250) +
          `<div class="sub" style="margin-top:10px;">Metric: ${{esc(metric)}} | Scope: ${{esc(scope)}}</div>`
        : '<div class="sub">No decay curve data</div>';
    }}

    function renderQuintiles(scope, horizon) {{
      const rows = report.factor_quintile_rows
        .filter(r => r.scope === scope && Number(r.horizon_days) === Number(horizon));
      const chart = document.getElementById('quintile-chart');
      const series = rows.map(row => ({{
        name: row.factor,
        values: [row.q1_avg_return, row.q2_avg_return, row.q3_avg_return, row.q4_avg_return, row.q5_avg_return]
          .map((y, idx) => ({{ x: idx + 1, y }})),
      }}));
      chart.innerHTML = series.length
        ? lineChartSvg(series, 560, 250) + `<div class="sub" style="margin-top:10px;">Scope: ${{esc(scope)}} | Horizon: ${{horizon}}d</div>`
        : '<div class="sub">No quantile data</div>';

      document.getElementById('quintile-table').innerHTML =
        `<table><thead><tr><th>Factor</th><th>Obs</th><th>Q1</th><th>Q2</th><th>Q3</th><th>Q4</th><th>Q5</th><th>Monotonicity</th><th>Q5-Q1</th></tr></thead><tbody>${
          rows.map(row => `<tr>
            <td>${{esc(row.factor)}}</td>
            <td>${{row.observations}}</td>
            <td>${{fmtPct(row.q1_avg_return)}}</td>
            <td>${{fmtPct(row.q2_avg_return)}}</td>
            <td>${{fmtPct(row.q3_avg_return)}}</td>
            <td>${{fmtPct(row.q4_avg_return)}}</td>
            <td>${{fmtPct(row.q5_avg_return)}}</td>
            <td>${{fmtNum(row.monotonicity_score)}}</td>
            <td>${{fmtPct(row.q5_q1_spread)}}</td>
          </tr>`).join('')
        }</tbody></table>`;
    }}

    function renderRollingIc(horizon) {{
      const rows = report.rolling_ic_rows
        .filter(r => Number(r.horizon_days) === Number(horizon))
        .sort((a, b) => a.date.localeCompare(b.date));
      const factors = ['momentum', 'mean_reversion', 'low_vol', 'volume', 'composite'];
      const series = factors.map(factor => {{
        const values = rows
          .filter(r => r.factor === factor)
          .map((r, idx) => ({{
            x: idx,
            y: r.ic,
            date: r.date,
          }}));
        return {{ name: factor, values }};
      }}).filter(s => s.values.length > 0);
      const chart = document.getElementById('rolling-ic-chart');
      chart.innerHTML = series.length
        ? lineChartSvg(series, 920, 260)
        : '<div class="sub">No rolling IC data</div>';

      const recent = rows.slice(-20).reverse();
      document.getElementById('rolling-ic-table').innerHTML =
        `<table><thead><tr><th>Date</th><th>Factor</th><th>Obs</th><th>IC</th></tr></thead><tbody>${
          recent.map(row => `<tr>
            <td>${{esc(row.date)}}</td>
            <td>${{esc(row.factor)}}</td>
            <td>${{row.observations}}</td>
            <td>${{fmtNum(row.ic)}}</td>
          </tr>`).join('')
        }</tbody></table>`;
    }}

    function renderRegime(market) {{
      const rows = report.regime_rows.filter(r => r.market === market);
      const cards = document.getElementById('regime-cards');
      cards.innerHTML = rows.map(row => `<div class="regime-card">
        <div class="regime-title">${{esc(row.regime_bucket)}}</div>
        <div class="regime-main">${{fmtNum(row.avg_composite_alpha)}}</div>
        <div class="regime-sub">obs=${{row.observations}} | momentum=${{fmtNum(row.avg_factor_momentum)}} | low-vol=${{fmtNum(row.avg_factor_low_vol)}}</div>
      </div>`).join('');

      const table = document.getElementById('regime-table');
      table.innerHTML = `<table><thead><tr><th>Regime</th><th>Obs</th><th>Composite</th><th>Momentum</th><th>Mean Rev</th><th>Low Vol</th><th>Volume</th><th>Selected</th></tr></thead><tbody>${
        rows.map(row => `<tr>
          <td>${{esc(row.regime_bucket)}}</td>
          <td>${{row.observations}}</td>
          <td>${{fmtNum(row.avg_composite_alpha)}}</td>
          <td>${{fmtNum(row.avg_factor_momentum)}}</td>
          <td>${{fmtNum(row.avg_factor_mean_reversion)}}</td>
          <td>${{fmtNum(row.avg_factor_low_vol)}}</td>
          <td>${{fmtNum(row.avg_factor_volume)}}</td>
          <td>${{fmtNum(row.avg_selected_symbols)}}</td>
        </tr>`).join('')
      }</tbody></table>`;
    }}

    function renderRegimeDecay(market, bucket) {{
      const rows = report.regime_decay_rows
        .filter(r => r.market === market && r.regime_bucket === bucket);
      document.getElementById('regime-decay-table').innerHTML =
        `<table><thead><tr><th>Factor</th><th>Horizon</th><th>Obs</th><th>IC</th><th>Long/Short</th></tr></thead><tbody>${
          rows.map(row => `<tr>
            <td>${{esc(row.factor)}}</td>
            <td>${{row.horizon_days}}d</td>
            <td>${{row.observations}}</td>
            <td>${{fmtNum(row.ic)}}</td>
            <td>${{fmtPct(row.long_short_spread)}}</td>
          </tr>`).join('')
        }</tbody></table>`;
    }}

    function renderWalkTable() {{
      const root = document.getElementById('walk-table');
      root.innerHTML = `<table><thead><tr><th>Fold</th><th>Strategy</th><th>Portfolio</th><th>Train Score</th><th>Test PnL</th><th>Sharpe</th><th>Calmar</th><th>Drawdown</th><th>Gap</th></tr></thead><tbody>${
        report.walk_forward_rows.map(row => `<tr>
          <td>${{row.fold}}</td>
          <td>${{esc(row.strategy_plugin)}}</td>
          <td>${{esc(row.portfolio_method)}}</td>
          <td>${{fmtNum(row.train_score)}}</td>
          <td>${{fmtPct(row.test_pnl_ratio)}}</td>
          <td>${{fmtNum(row.test_sharpe)}}</td>
          <td>${{fmtNum(row.test_calmar)}}</td>
          <td>${{fmtPct(row.test_drawdown)}}</td>
          <td>${{fmtNum(row.train_test_gap)}}</td>
        </tr>`).join('')
      }</tbody></table>`;
    }}

    function init() {{
      renderWalkBars();
      renderWalkPosture();
      renderWalkTable();

      const scopeSel = document.getElementById('decay-scope');
      const scopes = [...new Set(report.factor_decay_rows.map(r => r.scope))];
      scopeSel.innerHTML = scopes.map(scope => `<option value="${{esc(scope)}}">${{esc(scope)}}</option>`).join('');
      scopeSel.addEventListener('change', () => renderDecay(scopeSel.value));
      document.getElementById('decay-metric').addEventListener('change', () => renderDecay(scopeSel.value));
      renderDecay(scopes[0] || 'ALL');

      const horizonSel = document.getElementById('rolling-horizon');
      const horizons = [...new Set(report.rolling_ic_rows.map(r => r.horizon_days))].sort((a, b) => a - b);
      horizonSel.innerHTML = horizons.map(h => `<option value="${{h}}">${{h}}d</option>`).join('');
      horizonSel.addEventListener('change', () => renderRollingIc(horizonSel.value));
      renderRollingIc(horizons[0] || 1);

      const quintileScopeSel = document.getElementById('quintile-scope');
      const quintileScopes = [...new Set(report.factor_quintile_rows.map(r => r.scope))];
      quintileScopeSel.innerHTML = quintileScopes.map(scope => `<option value="${{esc(scope)}}">${{esc(scope)}}</option>`).join('');
      const quintileHorizonSel = document.getElementById('quintile-horizon');
      const quintileHorizons = [...new Set(report.factor_quintile_rows.map(r => r.horizon_days))].sort((a, b) => a - b);
      quintileHorizonSel.innerHTML = quintileHorizons.map(h => `<option value="${{h}}">${{h}}d</option>`).join('');
      quintileScopeSel.addEventListener('change', () => renderQuintiles(quintileScopeSel.value, quintileHorizonSel.value));
      quintileHorizonSel.addEventListener('change', () => renderQuintiles(quintileScopeSel.value, quintileHorizonSel.value));
      renderQuintiles(quintileScopes[0] || 'ALL', quintileHorizons[0] || 1);

      const marketSel = document.getElementById('regime-market');
      const markets = [...new Set(report.regime_rows.map(r => r.market))];
      marketSel.innerHTML = markets.map(m => `<option value="${{esc(m)}}">${{esc(m)}}</option>`).join('');
      marketSel.addEventListener('change', () => renderRegime(marketSel.value));
      renderRegime(markets[0] || 'ALL');

      const regimeDecayMarketSel = document.getElementById('regime-decay-market');
      const regimeDecayMarkets = [...new Set(report.regime_decay_rows.map(r => r.market))];
      regimeDecayMarketSel.innerHTML = regimeDecayMarkets.map(m => `<option value="${{esc(m)}}">${{esc(m)}}</option>`).join('');
      const regimeDecayBucketSel = document.getElementById('regime-decay-bucket');
      const firstMarket = regimeDecayMarkets[0] || 'ALL';
      function syncRegimeBuckets() {{
        const buckets = [...new Set(report.regime_decay_rows.filter(r => r.market === regimeDecayMarketSel.value).map(r => r.regime_bucket))];
        regimeDecayBucketSel.innerHTML = buckets.map(b => `<option value="${{esc(b)}}">${{esc(b)}}</option>`).join('');
        renderRegimeDecay(regimeDecayMarketSel.value, buckets[0] || '');
      }}
      regimeDecayMarketSel.addEventListener('change', syncRegimeBuckets);
      regimeDecayBucketSel.addEventListener('change', () => renderRegimeDecay(regimeDecayMarketSel.value, regimeDecayBucketSel.value));
      regimeDecayMarketSel.value = firstMarket;
      syncRegimeBuckets();
    }}

    init();
  </script>
</body>
</html>"##;
    template
        .replace("__FOLDS__", &report.walk_forward_summary.folds.to_string())
        .replace(
            "__AVG_PNL__",
            &format!(
                "{:.2}",
                report.walk_forward_summary.avg_test_pnl_ratio * 100.0
            ),
        )
        .replace(
            "__AVG_SHARPE__",
            &format!("{:.2}", report.walk_forward_summary.avg_test_sharpe),
        )
        .replace(
            "__TURNOVER__",
            &format!(
                "{:.1}",
                report.walk_forward_summary.strategy_turnover_ratio * 100.0
            ),
        )
        .replace("__REPORT_JSON__", &report_json)
}

fn mean(values: &[f64]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<f64>() / values.len() as f64
    }
}

fn factor_signal(snap: &SignalSnapshot, factor: &str) -> f64 {
    match factor {
        "momentum" => snap.factor_momentum,
        "mean_reversion" => snap.factor_mean_reversion,
        "low_vol" => snap.factor_low_vol,
        "volume" => snap.factor_volume,
        _ => snap.composite_alpha,
    }
}

fn average_quintile_returns(sorted_pairs: &[(f64, f64)]) -> [f64; 5] {
    let mut out = [0.0; 5];
    let len = sorted_pairs.len();
    for (bucket, slot) in out.iter_mut().enumerate() {
        let start = bucket * len / 5;
        let end = ((bucket + 1) * len / 5).max(start + 1).min(len);
        let slice = &sorted_pairs[start..end];
        *slot = slice.iter().map(|(_, ret)| *ret).sum::<f64>() / slice.len() as f64;
    }
    out
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

fn json_for_html<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| "{}".to_string())
        .replace("</", "<\\/")
}

#[cfg(test)]
mod tests {
    use std::fs;

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
        let summary_path =
            std::path::Path::new("outputs_rust/test_research_report/research_report_summary.txt");
        let quintile_path =
            std::path::Path::new("outputs_rust/test_research_report/factor_quintiles.csv");
        let regime_decay_path =
            std::path::Path::new("outputs_rust/test_research_report/regime_decay.csv");
        let summary_text = fs::read_to_string(summary_path).expect("read summary");
        assert!(artifacts.json_path.exists());
        assert!(artifacts.markdown_path.exists());
        assert!(artifacts.html_path.exists());
        assert!(summary_path.exists());
        assert!(quintile_path.exists());
        assert!(regime_decay_path.exists());
        assert!(summary_text.contains("dominant_winner_strategy_plugin="));
        assert!(summary_text.contains("dominant_winner_concentration="));
        assert!(summary_text.contains("unstable_folds="));
    }
}
