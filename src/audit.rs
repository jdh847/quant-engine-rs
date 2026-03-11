use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::{config::BotConfig, engine::BacktestStats};

#[derive(Debug, Clone, Serialize)]
pub struct AuditSnapshot {
    pub generated_at_utc: String,
    pub command: String,
    pub note: String,
    pub config_path: String,
    pub config_sha256: String,
    pub broker_mode: String,
    pub paper_only: bool,
    pub base_currency: String,
    pub strategy_plugin: String,
    pub portfolio_method: String,
    pub markets: Vec<AuditMarketFile>,
    pub stats: AuditStats,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuditMarketFile {
    pub market: String,
    pub currency: String,
    pub fx_to_base: f64,
    pub data_file: AuditFileHash,
    pub industry_file: Option<AuditFileHash>,
    pub holiday_file: Option<AuditFileHash>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuditFileHash {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuditStats {
    pub start_equity: f64,
    pub end_equity: f64,
    pub pnl: f64,
    pub pnl_ratio: f64,
    pub max_drawdown: f64,
    pub trades: usize,
    pub rejections: usize,
    pub cagr: f64,
    pub sharpe: f64,
    pub sortino: f64,
    pub calmar: f64,
    pub daily_win_rate: f64,
    pub profit_factor: f64,
}

pub fn write_audit_snapshot(
    output_dir: impl AsRef<Path>,
    command: &str,
    note: &str,
    config_path: &Path,
    cfg: &BotConfig,
    stats: &BacktestStats,
) -> Result<PathBuf> {
    let output_dir = output_dir.as_ref();
    fs::create_dir_all(output_dir)
        .with_context(|| format!("create audit output dir {}", output_dir.display()))?;

    let config_bytes =
        fs::read(config_path).with_context(|| format!("read config {}", config_path.display()))?;
    let config_sha256 = hex_lower(&Sha256::digest(&config_bytes));

    let mut markets = Vec::new();
    for m in cfg.markets.values() {
        let data_file = file_hash(&m.data_file)?;
        let industry_file = match &m.industry_file {
            Some(p) => Some(file_hash(p)?),
            None => None,
        };
        let holiday_file = match &m.holiday_file {
            Some(p) => Some(file_hash(p)?),
            None => None,
        };
        markets.push(AuditMarketFile {
            market: m.name.clone(),
            currency: m.currency.clone(),
            fx_to_base: m.fx_to_base,
            data_file,
            industry_file,
            holiday_file,
        });
    }
    markets.sort_by(|a, b| a.market.cmp(&b.market));

    let snapshot = AuditSnapshot {
        generated_at_utc: chrono::Utc::now().to_rfc3339(),
        command: command.to_string(),
        note: note.to_string(),
        config_path: config_path.display().to_string(),
        config_sha256,
        broker_mode: cfg.broker.mode.clone(),
        paper_only: cfg.broker.paper_only,
        base_currency: cfg.start.base_currency.clone(),
        strategy_plugin: cfg.strategy.strategy_plugin.clone(),
        portfolio_method: cfg.strategy.portfolio_method.clone(),
        markets,
        stats: AuditStats {
            start_equity: stats.start_equity,
            end_equity: stats.end_equity,
            pnl: stats.pnl,
            pnl_ratio: stats.pnl_ratio,
            max_drawdown: stats.max_drawdown,
            trades: stats.trades,
            rejections: stats.rejections,
            cagr: stats.cagr,
            sharpe: stats.sharpe,
            sortino: stats.sortino,
            calmar: stats.calmar,
            daily_win_rate: stats.daily_win_rate,
            profit_factor: stats.profit_factor,
        },
    };

    let json_path = output_dir.join("audit_snapshot.json");
    fs::write(
        &json_path,
        serde_json::to_string_pretty(&snapshot).context("serialize audit json failed")?,
    )
    .with_context(|| format!("write {}", json_path.display()))?;

    let summary = format!(
        "generated_at_utc={}\ncommand={}\nnote={}\nend_equity={:.2}\npnl_ratio={:.4}%\ntrades={}\nrejections={}\nconfig_sha256={}\n",
        snapshot.generated_at_utc,
        snapshot.command,
        snapshot.note,
        snapshot.stats.end_equity,
        snapshot.stats.pnl_ratio * 100.0,
        snapshot.stats.trades,
        snapshot.stats.rejections,
        snapshot.config_sha256
    );
    fs::write(output_dir.join("audit_snapshot_summary.txt"), summary)
        .context("write audit summary")?;

    Ok(json_path)
}

fn file_hash(path: &Path) -> Result<AuditFileHash> {
    let meta = fs::metadata(path).with_context(|| format!("stat file {}", path.display()))?;
    let buf = fs::read(path).with_context(|| format!("read file {}", path.display()))?;
    Ok(AuditFileHash {
        path: path.display().to_string(),
        bytes: meta.len(),
        sha256: hex_lower(&Sha256::digest(&buf)),
    })
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}
