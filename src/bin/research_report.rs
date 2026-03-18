use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use private_quant_bot::{
    config::{load_config, BotConfig},
    data::CsvDataPortal,
    research_report::{write_research_report, ResearchReportRequest},
    strategy::is_supported_strategy_plugin,
    WalkForwardRequest,
};

#[derive(Parser, Debug)]
#[command(
    name = "research-report",
    about = "Generate walk-forward, regime split, and factor decay reports"
)]
struct Cli {
    #[arg(long, default_value = "config/bot.toml")]
    config: PathBuf,
    #[arg(long, default_value = "outputs_rust/research_report")]
    output_dir: PathBuf,
    #[arg(long, default_value_t = 12)]
    train_days: usize,
    #[arg(long, default_value_t = 5)]
    test_days: usize,
    #[arg(long, default_value = "3,4,5")]
    short_windows: String,
    #[arg(long, default_value = "7,9,11")]
    long_windows: String,
    #[arg(long, default_value = "5,7")]
    vol_windows: String,
    #[arg(long, default_value = "1,2")]
    top_ns: String,
    #[arg(long, default_value = "0.001,0.002,0.003", allow_hyphen_values = true)]
    min_momentums: String,
    #[arg(long, default_value = "")]
    strategy_plugins: String,
    #[arg(long, default_value = "risk_parity,hrp")]
    portfolio_methods: String,
    #[arg(long, default_value = "1,3,5,10")]
    factor_decay_horizons: String,
    #[arg(long, default_value_t = 10)]
    regime_vol_window: usize,
    #[arg(long, default_value_t = 5)]
    regime_fast_window: usize,
    #[arg(long, default_value_t = 20)]
    regime_slow_window: usize,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let cfg = load_config(&cli.config)?;
    let data = load_data_for_config(&cfg)?;
    let request = ResearchReportRequest {
        walk_forward: WalkForwardRequest {
            train_days: cli.train_days,
            test_days: cli.test_days,
            strategy_plugins: resolve_strategy_plugins(&cli.strategy_plugins, &cfg)?,
            short_windows: parse_usize_list(&cli.short_windows)?,
            long_windows: parse_usize_list(&cli.long_windows)?,
            vol_windows: parse_usize_list(&cli.vol_windows)?,
            top_ns: parse_usize_list(&cli.top_ns)?,
            min_momentums: parse_f64_list(&cli.min_momentums)?,
            portfolio_methods: parse_portfolio_methods(&cli.portfolio_methods)?,
        },
        factor_decay_horizons: parse_usize_list(&cli.factor_decay_horizons)?,
        regime_vol_window: cli.regime_vol_window,
        regime_fast_window: cli.regime_fast_window,
        regime_slow_window: cli.regime_slow_window,
    };

    let (report, artifacts) = write_research_report(&cfg, &data, &request, &cli.output_dir)?;
    println!(
        "research report complete: folds={} regime_rows={} factor_decay_rows={}",
        report.walk_forward_rows.len(),
        report.regime_rows.len(),
        report.factor_decay_rows.len()
    );
    println!(
        "artifacts: {}, {}, {}",
        artifacts.markdown_path.display(),
        artifacts.html_path.display(),
        artifacts.json_path.display()
    );
    Ok(())
}

fn load_data_for_config(cfg: &BotConfig) -> Result<CsvDataPortal> {
    CsvDataPortal::new(
        cfg.markets
            .values()
            .map(|m| (m.name.clone(), m.data_file.clone()))
            .collect(),
    )
}

fn parse_usize_list(text: &str) -> Result<Vec<usize>> {
    let out: Vec<usize> = text
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| {
            v.parse::<usize>()
                .with_context(|| format!("invalid usize: {v}"))
        })
        .collect::<Result<Vec<_>>>()?;
    if out.is_empty() {
        return Err(anyhow!("empty usize list"));
    }
    Ok(out)
}

fn parse_f64_list(text: &str) -> Result<Vec<f64>> {
    let out: Vec<f64> = text
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| {
            v.parse::<f64>()
                .with_context(|| format!("invalid f64: {v}"))
        })
        .collect::<Result<Vec<_>>>()?;
    if out.is_empty() {
        return Err(anyhow!("empty f64 list"));
    }
    Ok(out)
}

fn parse_portfolio_methods(text: &str) -> Result<Vec<String>> {
    let methods: Vec<String> = text
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_lowercase())
        .collect();
    if methods.is_empty() {
        return Err(anyhow!("empty portfolio method list"));
    }
    for method in &methods {
        if method != "risk_parity" && method != "hrp" {
            return Err(anyhow!(
                "unsupported portfolio method: {method}; expected risk_parity or hrp"
            ));
        }
    }
    Ok(methods)
}

fn resolve_strategy_plugins(text: &str, cfg: &BotConfig) -> Result<Vec<String>> {
    if text.trim().is_empty() {
        return Ok(vec![cfg.strategy.strategy_plugin.clone()]);
    }
    let values: Vec<String> = text
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
        .collect();
    if values.is_empty() {
        return Err(anyhow!("empty strategy plugin list"));
    }
    for plugin in &values {
        if !is_supported_strategy_plugin(plugin) {
            return Err(anyhow!("unsupported strategy plugin: {plugin}"));
        }
    }
    Ok(values)
}
