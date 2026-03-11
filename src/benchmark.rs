use std::{fs, path::Path};

use anyhow::Result;
use sha2::{Digest, Sha256};

use crate::{
    config::{BotConfig, StrategyConfig},
    data::CsvDataPortal,
    engine::{summarize_result, BacktestStats, QuantBotEngine},
};

#[derive(Debug, Clone)]
pub struct BenchmarkRequest {
    pub strategy_plugins: Vec<String>,
    pub portfolio_methods: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct BenchmarkRow {
    pub scenario: String,
    pub strategy_plugin: String,
    pub portfolio_method: String,
    pub score: f64,
    pub stats: BacktestStats,
    pub strategy: StrategyConfig,
}

#[derive(Debug, Clone)]
pub struct BenchmarkReport {
    pub rows: Vec<BenchmarkRow>,
}

pub fn run_benchmark_suite(
    base_cfg: &BotConfig,
    full_data: &CsvDataPortal,
    output_dir: impl AsRef<Path>,
    req: &BenchmarkRequest,
) -> Result<BenchmarkReport> {
    if req.strategy_plugins.is_empty() || req.portfolio_methods.is_empty() {
        return Err(anyhow::anyhow!(
            "benchmark strategy_plugins and portfolio_methods cannot be empty"
        ));
    }

    let scenarios = vec![
        (
            "global_baseline",
            None,
            scenario_strategy(&base_cfg.strategy, 3, 11, 5, 2, 0.001),
        ),
        (
            "us_precision",
            Some("US"),
            scenario_strategy(&base_cfg.strategy, 4, 11, 7, 2, 0.002),
        ),
        (
            "jp_swing",
            Some("JP"),
            scenario_strategy(&base_cfg.strategy, 5, 11, 7, 1, 0.001),
        ),
        (
            "a_trend",
            Some("A"),
            scenario_strategy(&base_cfg.strategy, 3, 9, 5, 1, 0.002),
        ),
    ];

    let mut rows = Vec::new();
    for strategy_plugin in &req.strategy_plugins {
        validate_strategy_plugin(strategy_plugin)?;
        for portfolio_method in &req.portfolio_methods {
            validate_portfolio_method(portfolio_method)?;

            for (name, market, strategy) in &scenarios {
                let mut cfg = base_cfg.clone();
                cfg.strategy = StrategyConfig {
                    strategy_plugin: strategy_plugin.clone(),
                    portfolio_method: portfolio_method.clone(),
                    ..strategy.clone()
                };

                if let Some(mkt) = *market {
                    for item in cfg.markets.values_mut() {
                        item.allocation = 0.0;
                    }
                    if let Some(target) = cfg.markets.get_mut(mkt) {
                        target.allocation = 1.0;
                    }
                }

                let result = QuantBotEngine::from_config_force_sim(cfg, full_data.clone()).run();
                let stats = summarize_result(&result);
                let score = benchmark_score(&stats);

                rows.push(BenchmarkRow {
                    scenario: (*name).to_string(),
                    strategy_plugin: strategy_plugin.clone(),
                    portfolio_method: portfolio_method.clone(),
                    score,
                    stats,
                    strategy: strategy.clone(),
                });
            }
        }
    }

    rows.sort_by(|a, b| b.score.total_cmp(&a.score));

    let report = BenchmarkReport { rows };
    write_benchmark_report(output_dir, base_cfg, &report)?;
    Ok(report)
}

fn scenario_strategy(
    base: &StrategyConfig,
    short_window: usize,
    long_window: usize,
    vol_window: usize,
    top_n: usize,
    min_momentum: f64,
) -> StrategyConfig {
    StrategyConfig {
        strategy_plugin: base.strategy_plugin.clone(),
        short_window,
        long_window,
        vol_window,
        top_n,
        min_momentum,
        mean_reversion_window: base.mean_reversion_window,
        volume_window: base.volume_window,
        factor_momentum_weight: base.factor_momentum_weight,
        factor_mean_reversion_weight: base.factor_mean_reversion_weight,
        factor_low_vol_weight: base.factor_low_vol_weight,
        factor_volume_weight: base.factor_volume_weight,
        risk_parity_blend: base.risk_parity_blend,
        max_turnover_ratio: base.max_turnover_ratio,
        portfolio_method: base.portfolio_method.clone(),
        hrp_lookback: base.hrp_lookback,
        winsorize_pct: base.winsorize_pct,
        layer1_select_ratio: base.layer1_select_ratio,
        industry_neutral_strength: base.industry_neutral_strength,
        regime_vol_window: base.regime_vol_window,
        regime_target_vol: base.regime_target_vol,
        regime_floor_scale: base.regime_floor_scale,
        regime_ceiling_scale: base.regime_ceiling_scale,
    }
}

fn benchmark_score(stats: &BacktestStats) -> f64 {
    let risk_penalty = stats.max_drawdown * 0.8;
    let quality_bonus = stats.sharpe * 0.08 + stats.calmar * 0.02;
    stats.pnl_ratio + quality_bonus - risk_penalty
}

fn write_benchmark_report(
    output_dir: impl AsRef<Path>,
    cfg: &BotConfig,
    report: &BenchmarkReport,
) -> Result<()> {
    let dir = output_dir.as_ref();
    fs::create_dir_all(dir)?;

    let mut wtr = csv::Writer::from_path(dir.join("baseline_results.csv"))?;
    wtr.write_record([
        "rank",
        "scenario",
        "score",
        "strategy_plugin",
        "portfolio_method",
        "short_window",
        "long_window",
        "vol_window",
        "top_n",
        "min_momentum",
        "pnl_ratio",
        "max_drawdown",
        "cagr",
        "sharpe",
        "sortino",
        "calmar",
        "daily_win_rate",
        "profit_factor",
        "trades",
        "rejections",
    ])?;

    for (idx, row) in report.rows.iter().enumerate() {
        wtr.write_record([
            (idx + 1).to_string(),
            row.scenario.clone(),
            format!("{:.6}", row.score),
            row.strategy_plugin.clone(),
            row.portfolio_method.clone(),
            row.strategy.short_window.to_string(),
            row.strategy.long_window.to_string(),
            row.strategy.vol_window.to_string(),
            row.strategy.top_n.to_string(),
            format!("{:.6}", row.strategy.min_momentum),
            format!("{:.6}", row.stats.pnl_ratio),
            format!("{:.6}", row.stats.max_drawdown),
            format!("{:.6}", row.stats.cagr),
            format!("{:.6}", row.stats.sharpe),
            format!("{:.6}", row.stats.sortino),
            format!("{:.6}", row.stats.calmar),
            format!("{:.6}", row.stats.daily_win_rate),
            format!("{:.6}", row.stats.profit_factor),
            row.stats.trades.to_string(),
            row.stats.rejections.to_string(),
        ])?;
    }
    wtr.flush()?;

    write_dataset_manifest(dir.join("dataset_manifest.csv"), cfg)?;

    let mut lines = vec![
        "# Benchmark Baselines".to_string(),
        String::new(),
        "| Rank | Scenario | Plugin | Method | Score | PnL | MaxDD | Sharpe | Calmar | Trades |"
            .to_string(),
        "|---:|---|---|---|---:|---:|---:|---:|---:|---:|".to_string(),
    ];
    for (idx, row) in report.rows.iter().enumerate() {
        lines.push(format!(
            "| {} | {} | {} | {} | {:.4} | {:.2}% | {:.2}% | {:.3} | {:.3} | {} |",
            idx + 1,
            row.scenario,
            row.strategy_plugin,
            row.portfolio_method,
            row.score,
            row.stats.pnl_ratio * 100.0,
            row.stats.max_drawdown * 100.0,
            row.stats.sharpe,
            row.stats.calmar,
            row.stats.trades
        ));
    }

    fs::write(dir.join("baseline_report.md"), lines.join("\n") + "\n")?;

    Ok(())
}

fn write_dataset_manifest(path: impl AsRef<Path>, cfg: &BotConfig) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;
    wtr.write_record(["market", "file", "sha256", "rows"])?;

    for market in cfg.markets.values() {
        let bytes = fs::read(&market.data_file)?;
        let hash = format!("{:x}", Sha256::digest(&bytes));

        let mut rdr = csv::Reader::from_reader(bytes.as_slice());
        let rows = rdr.records().count();

        wtr.write_record([
            market.name.clone(),
            market.data_file.display().to_string(),
            hash,
            rows.to_string(),
        ])?;
    }

    wtr.flush()?;
    Ok(())
}

fn validate_portfolio_method(method: &str) -> Result<()> {
    if method == "risk_parity" || method == "hrp" {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "unsupported portfolio method: {method}; expected risk_parity or hrp"
        ))
    }
}

fn validate_strategy_plugin(plugin: &str) -> Result<()> {
    if plugin == "layered_multi_factor" || plugin == "momentum_guard" {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "unsupported strategy plugin: {plugin}; expected layered_multi_factor or momentum_guard"
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::{config::load_config, data::CsvDataPortal};

    use super::{run_benchmark_suite, BenchmarkRequest};

    #[test]
    fn benchmark_produces_rows() {
        let cfg = load_config("config/bot.toml").expect("load config");
        let data = CsvDataPortal::new(
            cfg.markets
                .values()
                .map(|m| (m.name.clone(), m.data_file.clone()))
                .collect(),
        )
        .expect("load data");

        let req = BenchmarkRequest {
            strategy_plugins: vec![
                "layered_multi_factor".to_string(),
                "momentum_guard".to_string(),
            ],
            portfolio_methods: vec!["risk_parity".to_string(), "hrp".to_string()],
        };
        let report = run_benchmark_suite(&cfg, &data, "outputs_rust/test_benchmark", &req)
            .expect("run benchmark");
        assert!(!report.rows.is_empty());
    }
}
