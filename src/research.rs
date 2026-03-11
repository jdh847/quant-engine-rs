use std::{collections::BTreeSet, fs, path::Path};

use anyhow::{anyhow, Result};

use crate::{
    config::{BotConfig, StrategyConfig},
    data::CsvDataPortal,
    engine::{summarize_result, BacktestStats, QuantBotEngine},
};

#[derive(Debug, Clone)]
pub struct ResearchRequest {
    pub target_markets: Vec<String>,
    pub strategy_plugins: Vec<String>,
    pub short_windows: Vec<usize>,
    pub long_windows: Vec<usize>,
    pub vol_windows: Vec<usize>,
    pub top_ns: Vec<usize>,
    pub min_momentums: Vec<f64>,
    pub portfolio_methods: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ResearchRow {
    pub rank: usize,
    pub scenario: String,
    pub strategy: StrategyConfig,
    pub score: f64,
    pub stats: BacktestStats,
}

#[derive(Debug, Clone)]
pub struct ResearchReport {
    pub rows: Vec<ResearchRow>,
}

pub fn run_cross_market_research(
    base_cfg: &BotConfig,
    full_data: &CsvDataPortal,
    req: &ResearchRequest,
    output_dir: impl AsRef<Path>,
) -> Result<ResearchReport> {
    if req.short_windows.is_empty()
        || req.long_windows.is_empty()
        || req.vol_windows.is_empty()
        || req.top_ns.is_empty()
        || req.min_momentums.is_empty()
        || req.strategy_plugins.is_empty()
        || req.portfolio_methods.is_empty()
    {
        return Err(anyhow!("research parameter lists cannot be empty"));
    }

    let mut scenarios = BTreeSet::new();
    scenarios.insert("GLOBAL".to_string());
    for market in &req.target_markets {
        scenarios.insert(market.to_uppercase());
    }

    let mut rows = Vec::new();

    for scenario in scenarios {
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

                                    if scenario != "GLOBAL" {
                                        apply_single_market_allocation(&mut cfg, &scenario);
                                    }

                                    let result = QuantBotEngine::from_config_force_sim(
                                        cfg.clone(),
                                        full_data.clone(),
                                    )
                                    .run();
                                    let stats = summarize_result(&result);
                                    let score = research_score(&stats);

                                    rows.push(ResearchRow {
                                        rank: 0,
                                        scenario: scenario.clone(),
                                        strategy: cfg.strategy,
                                        score,
                                        stats,
                                    });
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    rows.sort_by(|a, b| b.score.total_cmp(&a.score));
    for (idx, row) in rows.iter_mut().enumerate() {
        row.rank = idx + 1;
    }

    let report = ResearchReport { rows };
    write_research_report(output_dir, &report)?;
    Ok(report)
}

fn apply_single_market_allocation(cfg: &mut BotConfig, target_market: &str) {
    let key = target_market.to_uppercase();

    if !cfg.markets.contains_key(&key) {
        return;
    }

    for market in cfg.markets.values_mut() {
        market.allocation = 0.0;
    }

    if let Some(market) = cfg.markets.get_mut(&key) {
        market.allocation = 1.0;
    }
}

fn research_score(stats: &BacktestStats) -> f64 {
    let risk_penalty = stats.max_drawdown * 0.9;
    let quality_bonus = stats.sharpe * 0.12 + stats.sortino * 0.06 + stats.calmar * 0.04;
    let reliability_bonus = stats.daily_win_rate * 0.2;
    stats.pnl_ratio + quality_bonus + reliability_bonus - risk_penalty
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

fn write_research_report(output_dir: impl AsRef<Path>, report: &ResearchReport) -> Result<()> {
    let dir = output_dir.as_ref();
    fs::create_dir_all(dir)?;

    let csv_path = dir.join("research_leaderboard.csv");
    let mut wtr = csv::Writer::from_path(csv_path)?;
    wtr.write_record([
        "rank",
        "scenario",
        "short_window",
        "long_window",
        "vol_window",
        "top_n",
        "min_momentum",
        "strategy_plugin",
        "portfolio_method",
        "score",
        "pnl_ratio",
        "max_drawdown",
        "sharpe",
        "sortino",
        "calmar",
        "daily_win_rate",
        "profit_factor",
        "trades",
        "rejections",
    ])?;

    for row in &report.rows {
        wtr.write_record([
            row.rank.to_string(),
            row.scenario.clone(),
            row.strategy.short_window.to_string(),
            row.strategy.long_window.to_string(),
            row.strategy.vol_window.to_string(),
            row.strategy.top_n.to_string(),
            format!("{:.6}", row.strategy.min_momentum),
            row.strategy.strategy_plugin.clone(),
            row.strategy.portfolio_method.clone(),
            format!("{:.6}", row.score),
            format!("{:.6}", row.stats.pnl_ratio),
            format!("{:.6}", row.stats.max_drawdown),
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

    let top_lines: Vec<String> = report
        .rows
        .iter()
        .take(15)
        .map(|r| {
            format!(
                "| {} | {} | ({},{},{},{},{:.4},{}+{}) | {:.4} | {:.2}% | {:.2}% | {:.3} | {:.3} |",
                r.rank,
                r.scenario,
                r.strategy.short_window,
                r.strategy.long_window,
                r.strategy.vol_window,
                r.strategy.top_n,
                r.strategy.min_momentum,
                r.strategy.strategy_plugin,
                r.strategy.portfolio_method,
                r.score,
                r.stats.pnl_ratio * 100.0,
                r.stats.max_drawdown * 100.0,
                r.stats.sharpe,
                r.stats.calmar
            )
        })
        .collect();

    let md = format!(
        "# Research Leaderboard\n\n| Rank | Scenario | Strategy (short,long,vol,top_n,min_mom,plugin+method) | Score | PnL | MaxDD | Sharpe | Calmar |\n|---:|---|---|---:|---:|---:|---:|---:|\n{}\n",
        top_lines.join("\n")
    );
    fs::write(dir.join("research_leaderboard.md"), md)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{config::load_config, data::CsvDataPortal};

    use super::{run_cross_market_research, ResearchRequest};

    #[test]
    fn research_generates_rows() {
        let cfg = load_config("config/bot.toml").expect("load config");
        let data = CsvDataPortal::new(
            cfg.markets
                .values()
                .map(|m| (m.name.clone(), m.data_file.clone()))
                .collect(),
        )
        .expect("load data");

        let req = ResearchRequest {
            target_markets: vec!["US".to_string(), "A".to_string(), "JP".to_string()],
            strategy_plugins: vec!["layered_multi_factor".to_string()],
            short_windows: vec![3],
            long_windows: vec![7],
            vol_windows: vec![5],
            top_ns: vec![1, 2],
            min_momentums: vec![0.001, 0.002],
            portfolio_methods: vec!["risk_parity".to_string(), "hrp".to_string()],
        };

        let report = run_cross_market_research(&cfg, &data, &req, "outputs_rust/test_research")
            .expect("research run");
        assert!(!report.rows.is_empty());
    }
}
