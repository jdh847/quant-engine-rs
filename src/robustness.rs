use std::{collections::HashSet, fs, path::Path};

use anyhow::{anyhow, Result};
use chrono::NaiveDate;

use crate::{
    config::{BotConfig, StrategyConfig},
    data::CsvDataPortal,
    engine::{summarize_result, BacktestStats, QuantBotEngine, RunResult},
};

#[derive(Debug, Clone)]
pub struct RobustnessRequest {
    pub train_days: usize,
    pub test_days: usize,
    pub strategy_plugins: Vec<String>,
    pub short_windows: Vec<usize>,
    pub long_windows: Vec<usize>,
    pub vol_windows: Vec<usize>,
    pub top_ns: Vec<usize>,
    pub min_momentums: Vec<f64>,
    pub portfolio_methods: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct RobustnessFoldRow {
    pub fold: usize,
    pub train_start: NaiveDate,
    pub train_end: NaiveDate,
    pub test_start: NaiveDate,
    pub test_end: NaiveDate,
    pub candidates: usize,
    pub selected_strategy: StrategyConfig,
    pub selected_train_score: f64,
    pub selected_test_score: f64,
    pub selected_test_stats: BacktestStats,
    pub pbo_flag: bool,
    pub deflated_sharpe_proxy: f64,
}

#[derive(Debug, Clone)]
pub struct RobustnessReport {
    pub folds: Vec<RobustnessFoldRow>,
}

pub fn run_robustness_assessment(
    base_cfg: &BotConfig,
    full_data: &CsvDataPortal,
    req: &RobustnessRequest,
    output_dir: impl AsRef<Path>,
) -> Result<RobustnessReport> {
    validate_request(req)?;
    let dates = full_data.trading_dates();
    if dates.len() < req.train_days + req.test_days {
        return Err(anyhow!(
            "not enough dates: need at least {}, got {}",
            req.train_days + req.test_days,
            dates.len()
        ));
    }

    let mut folds = Vec::new();
    let mut fold = 1usize;
    let mut start = 0usize;

    while start + req.train_days + req.test_days <= dates.len() {
        let train_slice = &dates[start..start + req.train_days];
        let test_slice = &dates[start + req.train_days..start + req.train_days + req.test_days];
        let mut candidates = Vec::new();

        for strategy_plugin in &req.strategy_plugins {
            validate_strategy_plugin(strategy_plugin)?;
            for short_window in &req.short_windows {
                for long_window in &req.long_windows {
                    if short_window >= long_window {
                        continue;
                    }
                    for vol_window in &req.vol_windows {
                        for top_n in &req.top_ns {
                            for min_momentum in &req.min_momentums {
                                for portfolio_method in &req.portfolio_methods {
                                    validate_portfolio_method(portfolio_method)?;

                                    let mut cfg = base_cfg.clone();
                                    cfg.strategy = StrategyConfig {
                                        strategy_plugin: strategy_plugin.clone(),
                                        short_window: *short_window,
                                        long_window: *long_window,
                                        vol_window: *vol_window,
                                        top_n: *top_n,
                                        min_momentum: *min_momentum,
                                        mean_reversion_window: base_cfg
                                            .strategy
                                            .mean_reversion_window,
                                        volume_window: base_cfg.strategy.volume_window,
                                        factor_momentum_weight: base_cfg
                                            .strategy
                                            .factor_momentum_weight,
                                        factor_mean_reversion_weight: base_cfg
                                            .strategy
                                            .factor_mean_reversion_weight,
                                        factor_low_vol_weight: base_cfg
                                            .strategy
                                            .factor_low_vol_weight,
                                        factor_volume_weight: base_cfg
                                            .strategy
                                            .factor_volume_weight,
                                        risk_parity_blend: base_cfg.strategy.risk_parity_blend,
                                        max_turnover_ratio: base_cfg.strategy.max_turnover_ratio,
                                        portfolio_method: portfolio_method.clone(),
                                        hrp_lookback: base_cfg.strategy.hrp_lookback,
                                        winsorize_pct: base_cfg.strategy.winsorize_pct,
                                        layer1_select_ratio: base_cfg.strategy.layer1_select_ratio,
                                        industry_neutral_strength: base_cfg
                                            .strategy
                                            .industry_neutral_strength,
                                        regime_vol_window: base_cfg.strategy.regime_vol_window,
                                        regime_target_vol: base_cfg.strategy.regime_target_vol,
                                        regime_floor_scale: base_cfg.strategy.regime_floor_scale,
                                        regime_ceiling_scale: base_cfg
                                            .strategy
                                            .regime_ceiling_scale,
                                    };

                                    let train_data = full_data.slice_by_dates(train_slice);
                                    let train_result = QuantBotEngine::from_config_force_sim(
                                        cfg.clone(),
                                        train_data,
                                    )
                                    .run();
                                    let train_stats = summarize_result(&train_result);
                                    let train_score = score_stats(&train_stats);

                                    let warmup =
                                        cfg.strategy.long_window.max(cfg.strategy.vol_window + 1);
                                    let warmup_start = train_slice.len().saturating_sub(warmup);
                                    let mut combo = Vec::new();
                                    combo.extend_from_slice(&train_slice[warmup_start..]);
                                    combo.extend_from_slice(test_slice);

                                    let combo_data = full_data.slice_by_dates(&combo);
                                    let combo_result = QuantBotEngine::from_config_force_sim(
                                        cfg.clone(),
                                        combo_data,
                                    )
                                    .run();
                                    let test_set: HashSet<NaiveDate> =
                                        test_slice.iter().copied().collect();
                                    let test_result = RunResult {
                                        equity_curve: combo_result
                                            .equity_curve
                                            .into_iter()
                                            .filter(|p| test_set.contains(&p.date))
                                            .collect(),
                                        trades: combo_result
                                            .trades
                                            .into_iter()
                                            .filter(|t| test_set.contains(&t.date))
                                            .collect(),
                                        rejections: combo_result
                                            .rejections
                                            .into_iter()
                                            .filter(|r| test_set.contains(&r.date))
                                            .collect(),
                                    };
                                    let test_stats = summarize_result(&test_result);
                                    let test_score = score_stats(&test_stats);

                                    candidates.push(CandidateEval {
                                        strategy: cfg.strategy,
                                        train_score,
                                        test_score,
                                        test_stats,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }

        if candidates.is_empty() {
            return Err(anyhow!("no valid robustness candidates generated"));
        }

        candidates.sort_by(|a, b| b.train_score.total_cmp(&a.train_score));
        let selected = candidates[0].clone();
        let mut test_scores = candidates.iter().map(|c| c.test_score).collect::<Vec<_>>();
        test_scores.sort_by(|a, b| a.total_cmp(b));
        let median_test = median(&test_scores);
        let pbo_flag = selected.test_score < median_test;
        let deflated_sharpe_proxy =
            selected.test_stats.sharpe - 0.20 * (candidates.len() as f64).ln();

        folds.push(RobustnessFoldRow {
            fold,
            train_start: train_slice[0],
            train_end: train_slice[train_slice.len() - 1],
            test_start: test_slice[0],
            test_end: test_slice[test_slice.len() - 1],
            candidates: candidates.len(),
            selected_strategy: selected.strategy,
            selected_train_score: selected.train_score,
            selected_test_score: selected.test_score,
            selected_test_stats: selected.test_stats,
            pbo_flag,
            deflated_sharpe_proxy,
        });

        fold += 1;
        start += req.test_days;
    }

    let report = RobustnessReport { folds };
    write_report(output_dir, &report)?;
    Ok(report)
}

#[derive(Debug, Clone)]
struct CandidateEval {
    strategy: StrategyConfig,
    train_score: f64,
    test_score: f64,
    test_stats: BacktestStats,
}

fn score_stats(stats: &BacktestStats) -> f64 {
    stats.pnl_ratio + stats.sharpe * 0.10 + stats.calmar * 0.05 - stats.max_drawdown * 0.8
}

fn median(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let mid = values.len() / 2;
    if values.len().is_multiple_of(2) {
        (values[mid - 1] + values[mid]) / 2.0
    } else {
        values[mid]
    }
}

fn validate_request(req: &RobustnessRequest) -> Result<()> {
    if req.train_days == 0 || req.test_days == 0 {
        return Err(anyhow!("train_days and test_days must be > 0"));
    }
    if req.strategy_plugins.is_empty()
        || req.short_windows.is_empty()
        || req.long_windows.is_empty()
        || req.vol_windows.is_empty()
        || req.top_ns.is_empty()
        || req.min_momentums.is_empty()
        || req.portfolio_methods.is_empty()
    {
        return Err(anyhow!("robustness parameter lists cannot be empty"));
    }
    Ok(())
}

fn validate_portfolio_method(method: &str) -> Result<()> {
    if method == "risk_parity" || method == "hrp" {
        Ok(())
    } else {
        Err(anyhow!(
            "unsupported portfolio method: {method}; expected risk_parity or hrp"
        ))
    }
}

fn validate_strategy_plugin(plugin: &str) -> Result<()> {
    if plugin == "layered_multi_factor" || plugin == "momentum_guard" {
        Ok(())
    } else {
        Err(anyhow!(
            "unsupported strategy plugin: {plugin}; expected layered_multi_factor or momentum_guard"
        ))
    }
}

fn write_report(output_dir: impl AsRef<Path>, report: &RobustnessReport) -> Result<()> {
    let dir = output_dir.as_ref();
    fs::create_dir_all(dir)?;

    let csv_path = dir.join("robustness_folds.csv");
    let mut wtr = csv::Writer::from_path(csv_path)?;
    wtr.write_record([
        "fold",
        "train_start",
        "train_end",
        "test_start",
        "test_end",
        "candidates",
        "strategy_plugin",
        "portfolio_method",
        "short_window",
        "long_window",
        "vol_window",
        "top_n",
        "min_momentum",
        "selected_train_score",
        "selected_test_score",
        "selected_test_pnl_ratio",
        "selected_test_sharpe",
        "selected_test_calmar",
        "selected_test_drawdown",
        "pbo_flag",
        "deflated_sharpe_proxy",
    ])?;

    for row in &report.folds {
        wtr.write_record([
            row.fold.to_string(),
            row.train_start.to_string(),
            row.train_end.to_string(),
            row.test_start.to_string(),
            row.test_end.to_string(),
            row.candidates.to_string(),
            row.selected_strategy.strategy_plugin.clone(),
            row.selected_strategy.portfolio_method.clone(),
            row.selected_strategy.short_window.to_string(),
            row.selected_strategy.long_window.to_string(),
            row.selected_strategy.vol_window.to_string(),
            row.selected_strategy.top_n.to_string(),
            format!("{:.6}", row.selected_strategy.min_momentum),
            format!("{:.6}", row.selected_train_score),
            format!("{:.6}", row.selected_test_score),
            format!("{:.6}", row.selected_test_stats.pnl_ratio),
            format!("{:.6}", row.selected_test_stats.sharpe),
            format!("{:.6}", row.selected_test_stats.calmar),
            format!("{:.6}", row.selected_test_stats.max_drawdown),
            row.pbo_flag.to_string(),
            format!("{:.6}", row.deflated_sharpe_proxy),
        ])?;
    }
    wtr.flush()?;

    let folds = report.folds.len() as f64;
    let pbo_proxy = if folds > 0.0 {
        report.folds.iter().filter(|r| r.pbo_flag).count() as f64 / folds
    } else {
        0.0
    };
    let oos_win_rate = if folds > 0.0 {
        report
            .folds
            .iter()
            .filter(|r| r.selected_test_stats.pnl_ratio > 0.0)
            .count() as f64
            / folds
    } else {
        0.0
    };
    let avg_test_sharpe = if folds > 0.0 {
        report
            .folds
            .iter()
            .map(|r| r.selected_test_stats.sharpe)
            .sum::<f64>()
            / folds
    } else {
        0.0
    };
    let avg_deflated_sharpe_proxy = if folds > 0.0 {
        report
            .folds
            .iter()
            .map(|r| r.deflated_sharpe_proxy)
            .sum::<f64>()
            / folds
    } else {
        0.0
    };

    let summary = format!(
        "folds={}\npbo_proxy={:.4}%\noos_win_rate={:.4}%\navg_test_sharpe={:.6}\navg_deflated_sharpe_proxy={:.6}\n",
        report.folds.len(),
        pbo_proxy * 100.0,
        oos_win_rate * 100.0,
        avg_test_sharpe,
        avg_deflated_sharpe_proxy
    );
    fs::write(dir.join("robustness_summary.txt"), summary)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{config::load_config, data::CsvDataPortal};

    use super::{run_robustness_assessment, RobustnessRequest};

    #[test]
    fn robustness_generates_rows() {
        let cfg = load_config("config/bot.toml").expect("load config");
        let data = CsvDataPortal::new(
            cfg.markets
                .values()
                .map(|m| (m.name.clone(), m.data_file.clone()))
                .collect(),
        )
        .expect("load data");

        let req = RobustnessRequest {
            train_days: 10,
            test_days: 4,
            strategy_plugins: vec!["layered_multi_factor".to_string()],
            short_windows: vec![3],
            long_windows: vec![7],
            vol_windows: vec![5],
            top_ns: vec![1],
            min_momentums: vec![0.0],
            portfolio_methods: vec!["risk_parity".to_string(), "hrp".to_string()],
        };

        let report = run_robustness_assessment(&cfg, &data, &req, "outputs_rust/test_robustness")
            .expect("robustness");
        assert!(!report.folds.is_empty());
    }
}
