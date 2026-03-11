use std::{collections::HashSet, fs, path::Path};

use anyhow::{anyhow, Result};
use chrono::NaiveDate;

use crate::{
    config::{BotConfig, StrategyConfig},
    data::CsvDataPortal,
    engine::{summarize_result, BacktestStats, QuantBotEngine},
};

#[derive(Debug, Clone)]
pub struct WalkForwardRequest {
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
pub struct FoldResult {
    pub fold: usize,
    pub train_start: NaiveDate,
    pub train_end: NaiveDate,
    pub test_start: NaiveDate,
    pub test_end: NaiveDate,
    pub best_strategy: StrategyConfig,
    pub train_score: f64,
    pub test_stats: BacktestStats,
}

#[derive(Debug, Clone)]
pub struct WalkForwardReport {
    pub folds: Vec<FoldResult>,
}

pub fn run_walk_forward(
    base_cfg: &BotConfig,
    full_data: &CsvDataPortal,
    req: &WalkForwardRequest,
    output_dir: impl AsRef<Path>,
) -> Result<WalkForwardReport> {
    if req.train_days == 0 || req.test_days == 0 {
        return Err(anyhow!("train_days and test_days must be > 0"));
    }
    if req.strategy_plugins.is_empty() {
        return Err(anyhow!("strategy_plugins cannot be empty"));
    }
    if req.portfolio_methods.is_empty() {
        return Err(anyhow!("portfolio_methods cannot be empty"));
    }

    let dates = full_data.trading_dates();
    if dates.len() < req.train_days + req.test_days {
        return Err(anyhow!(
            "not enough dates: need at least {}, got {}",
            req.train_days + req.test_days,
            dates.len()
        ));
    }

    let mut folds = Vec::new();
    let mut fold_index = 1usize;
    let mut start = 0usize;

    while start + req.train_days + req.test_days <= dates.len() {
        let train_slice = &dates[start..start + req.train_days];
        let test_slice = &dates[start + req.train_days..start + req.train_days + req.test_days];

        let mut best: Option<(StrategyConfig, f64)> = None;

        for short_window in &req.short_windows {
            for long_window in &req.long_windows {
                if short_window >= long_window {
                    continue;
                }
                for vol_window in &req.vol_windows {
                    for top_n in &req.top_ns {
                        for min_momentum in &req.min_momentums {
                            for strategy_plugin in &req.strategy_plugins {
                                validate_strategy_plugin(strategy_plugin)?;
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
                                    let score = objective_score(&train_stats);

                                    if best.as_ref().map(|(_, s)| score > *s).unwrap_or(true) {
                                        best = Some((cfg.strategy.clone(), score));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let Some((best_strategy, train_score)) = best else {
            return Err(anyhow!("no valid strategy candidates generated"));
        };

        let mut test_cfg = base_cfg.clone();
        test_cfg.strategy = best_strategy.clone();

        let warmup_days = best_strategy.long_window.max(best_strategy.vol_window + 1);
        let train_warmup_start = train_slice.len().saturating_sub(warmup_days);
        let mut combined_dates = Vec::new();
        combined_dates.extend_from_slice(&train_slice[train_warmup_start..]);
        combined_dates.extend_from_slice(test_slice);

        let combined_data = full_data.slice_by_dates(&combined_dates);
        let combined_result = QuantBotEngine::from_config_force_sim(test_cfg, combined_data).run();
        let test_date_set: HashSet<NaiveDate> = test_slice.iter().copied().collect();
        let test_result = crate::engine::RunResult {
            equity_curve: combined_result
                .equity_curve
                .into_iter()
                .filter(|p| test_date_set.contains(&p.date))
                .collect(),
            trades: combined_result
                .trades
                .into_iter()
                .filter(|t| test_date_set.contains(&t.date))
                .collect(),
            rejections: combined_result
                .rejections
                .into_iter()
                .filter(|r| test_date_set.contains(&r.date))
                .collect(),
        };
        let test_stats = summarize_result(&test_result);

        folds.push(FoldResult {
            fold: fold_index,
            train_start: train_slice[0],
            train_end: train_slice[train_slice.len() - 1],
            test_start: test_slice[0],
            test_end: test_slice[test_slice.len() - 1],
            best_strategy,
            train_score,
            test_stats,
        });

        fold_index += 1;
        start += req.test_days;
    }

    let report = WalkForwardReport { folds };
    write_walk_forward_report(output_dir, &report)?;
    Ok(report)
}

fn objective_score(stats: &BacktestStats) -> f64 {
    let stability_penalty = stats.max_drawdown * 0.8;
    let activity_bonus = if stats.trades > 0 { 0.01 } else { -1.0 };
    let quality_bonus = stats.sharpe * 0.08 + stats.calmar * 0.04;
    stats.pnl_ratio - stability_penalty + activity_bonus + quality_bonus
}

fn write_walk_forward_report(
    output_dir: impl AsRef<Path>,
    report: &WalkForwardReport,
) -> Result<()> {
    let dir = output_dir.as_ref();
    fs::create_dir_all(dir)?;

    let csv_path = dir.join("walk_forward_folds.csv");
    let mut wtr = csv::Writer::from_path(csv_path)?;
    wtr.write_record([
        "fold",
        "train_start",
        "train_end",
        "test_start",
        "test_end",
        "short_window",
        "long_window",
        "vol_window",
        "top_n",
        "min_momentum",
        "strategy_plugin",
        "portfolio_method",
        "train_score",
        "test_pnl_ratio",
        "test_max_drawdown",
        "test_sharpe",
        "test_calmar",
        "test_trades",
    ])?;

    for row in &report.folds {
        wtr.write_record([
            row.fold.to_string(),
            row.train_start.to_string(),
            row.train_end.to_string(),
            row.test_start.to_string(),
            row.test_end.to_string(),
            row.best_strategy.short_window.to_string(),
            row.best_strategy.long_window.to_string(),
            row.best_strategy.vol_window.to_string(),
            row.best_strategy.top_n.to_string(),
            format!("{:.6}", row.best_strategy.min_momentum),
            row.best_strategy.strategy_plugin.clone(),
            row.best_strategy.portfolio_method.clone(),
            format!("{:.6}", row.train_score),
            format!("{:.6}", row.test_stats.pnl_ratio),
            format!("{:.6}", row.test_stats.max_drawdown),
            format!("{:.6}", row.test_stats.sharpe),
            format!("{:.6}", row.test_stats.calmar),
            row.test_stats.trades.to_string(),
        ])?;
    }
    wtr.flush()?;

    let summary_path = dir.join("walk_forward_summary.txt");
    let avg_test_pnl = if report.folds.is_empty() {
        0.0
    } else {
        report
            .folds
            .iter()
            .map(|f| f.test_stats.pnl_ratio)
            .sum::<f64>()
            / report.folds.len() as f64
    };
    let avg_test_drawdown = if report.folds.is_empty() {
        0.0
    } else {
        report
            .folds
            .iter()
            .map(|f| f.test_stats.max_drawdown)
            .sum::<f64>()
            / report.folds.len() as f64
    };
    let avg_test_sharpe = if report.folds.is_empty() {
        0.0
    } else {
        report
            .folds
            .iter()
            .map(|f| f.test_stats.sharpe)
            .sum::<f64>()
            / report.folds.len() as f64
    };

    let summary = format!(
        "folds={}\navg_test_pnl_ratio={:.4}%\navg_test_max_drawdown={:.4}%\navg_test_sharpe={:.4}\n",
        report.folds.len(),
        avg_test_pnl * 100.0,
        avg_test_drawdown * 100.0,
        avg_test_sharpe
    );
    fs::write(summary_path, summary)?;

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

#[cfg(test)]
mod tests {
    use crate::{config::load_config, data::CsvDataPortal};

    use super::{run_walk_forward, WalkForwardRequest};

    #[test]
    fn walk_forward_runs_at_least_one_fold() {
        let cfg = load_config("config/bot.toml").expect("load config");
        let data = CsvDataPortal::new(
            cfg.markets
                .values()
                .map(|m| (m.name.clone(), m.data_file.clone()))
                .collect(),
        )
        .expect("load data");

        let req = WalkForwardRequest {
            train_days: 10,
            test_days: 4,
            strategy_plugins: vec!["layered_multi_factor".to_string()],
            short_windows: vec![3],
            long_windows: vec![7],
            vol_windows: vec![5],
            top_ns: vec![1, 2],
            min_momentums: vec![0.001, 0.002],
            portfolio_methods: vec!["risk_parity".to_string(), "hrp".to_string()],
        };

        let report = run_walk_forward(&cfg, &data, &req, "outputs_rust/test_opt").expect("wf run");
        assert!(!report.folds.is_empty());
    }
}
