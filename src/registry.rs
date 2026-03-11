use std::{
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
};

use anyhow::Result;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::engine::BacktestStats;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRegistryEntry {
    pub run_id: String,
    pub timestamp_utc: String,
    pub command: String,
    pub output_dir: String,
    pub strategy_plugin: String,
    pub portfolio_method: String,
    pub markets: String,
    pub primary_metric_name: String,
    pub primary_metric_value: f64,
    pub composite_score: f64,
    pub pnl_ratio: f64,
    pub max_drawdown: f64,
    pub sharpe: f64,
    pub sortino: f64,
    pub calmar: f64,
    pub trades: usize,
    pub rejections: usize,
    pub notes: String,
}

#[derive(Debug, Clone)]
pub struct RegistryWriteReport {
    pub csv_path: PathBuf,
    pub json_path: PathBuf,
    pub markdown_path: PathBuf,
    pub total_runs: usize,
}

#[derive(Debug, Clone)]
pub struct RunRegistryBacktestInput {
    pub command: String,
    pub output_dir: PathBuf,
    pub strategy_plugin: String,
    pub portfolio_method: String,
    pub markets: String,
    pub primary_metric_name: String,
    pub primary_metric_value: f64,
    pub stats: BacktestStats,
    pub notes: String,
}

#[derive(Debug, Clone)]
pub struct RunRegistryOperationInput {
    pub command: String,
    pub output_dir: PathBuf,
    pub markets: String,
    pub primary_metric_name: String,
    pub primary_metric_value: f64,
    pub notes: String,
}

impl RunRegistryEntry {
    pub fn from_backtest_input(input: RunRegistryBacktestInput) -> Self {
        let timestamp_utc = Utc::now().to_rfc3339();
        let run_id = format!(
            "{}-{}",
            input.command.to_lowercase(),
            Utc::now().timestamp_millis()
        );
        let composite_score =
            input.stats.pnl_ratio + input.stats.sharpe * 0.10 + input.stats.calmar * 0.05
                - input.stats.max_drawdown * 0.8;

        Self {
            run_id,
            timestamp_utc,
            command: input.command,
            output_dir: input.output_dir.display().to_string(),
            strategy_plugin: input.strategy_plugin,
            portfolio_method: input.portfolio_method,
            markets: input.markets,
            primary_metric_name: input.primary_metric_name,
            primary_metric_value: input.primary_metric_value,
            composite_score,
            pnl_ratio: input.stats.pnl_ratio,
            max_drawdown: input.stats.max_drawdown,
            sharpe: input.stats.sharpe,
            sortino: input.stats.sortino,
            calmar: input.stats.calmar,
            trades: input.stats.trades,
            rejections: input.stats.rejections,
            notes: input.notes,
        }
    }

    pub fn from_operation_input(input: RunRegistryOperationInput) -> Self {
        let timestamp_utc = Utc::now().to_rfc3339();
        let run_id = format!(
            "{}-{}",
            input.command.to_lowercase(),
            Utc::now().timestamp_millis()
        );

        Self {
            run_id,
            timestamp_utc,
            command: input.command,
            output_dir: input.output_dir.display().to_string(),
            strategy_plugin: String::new(),
            portfolio_method: String::new(),
            markets: input.markets,
            primary_metric_name: input.primary_metric_name,
            primary_metric_value: input.primary_metric_value,
            composite_score: input.primary_metric_value,
            pnl_ratio: 0.0,
            max_drawdown: 0.0,
            sharpe: 0.0,
            sortino: 0.0,
            calmar: 0.0,
            trades: 0,
            rejections: 0,
            notes: input.notes,
        }
    }
}

pub fn append_run_registry(
    registry_dir: impl AsRef<Path>,
    entry: &RunRegistryEntry,
) -> Result<RegistryWriteReport> {
    let dir = registry_dir.as_ref();
    fs::create_dir_all(dir)?;

    let csv_path = dir.join("run_registry.csv");
    let needs_header = !csv_path.exists() || fs::metadata(&csv_path)?.len() == 0;

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&csv_path)?;
    let mut wtr = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(file);

    if needs_header {
        wtr.write_record([
            "run_id",
            "timestamp_utc",
            "command",
            "output_dir",
            "strategy_plugin",
            "portfolio_method",
            "markets",
            "primary_metric_name",
            "primary_metric_value",
            "composite_score",
            "pnl_ratio",
            "max_drawdown",
            "sharpe",
            "sortino",
            "calmar",
            "trades",
            "rejections",
            "notes",
        ])?;
    }

    wtr.write_record([
        entry.run_id.clone(),
        entry.timestamp_utc.clone(),
        entry.command.clone(),
        entry.output_dir.clone(),
        entry.strategy_plugin.clone(),
        entry.portfolio_method.clone(),
        entry.markets.clone(),
        entry.primary_metric_name.clone(),
        format!("{:.8}", entry.primary_metric_value),
        format!("{:.8}", entry.composite_score),
        format!("{:.8}", entry.pnl_ratio),
        format!("{:.8}", entry.max_drawdown),
        format!("{:.8}", entry.sharpe),
        format!("{:.8}", entry.sortino),
        format!("{:.8}", entry.calmar),
        entry.trades.to_string(),
        entry.rejections.to_string(),
        entry.notes.clone(),
    ])?;
    wtr.flush()?;

    let entries = read_run_registry(&csv_path)?;
    let (json_path, markdown_path) = write_registry_views(dir, &entries)?;

    Ok(RegistryWriteReport {
        csv_path,
        json_path,
        markdown_path,
        total_runs: entries.len(),
    })
}

pub fn read_run_registry(path: impl AsRef<Path>) -> Result<Vec<RunRegistryEntry>> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(Vec::new());
    }

    let mut rows = Vec::new();
    let mut rdr = csv::Reader::from_path(path)?;
    for row in rdr.deserialize() {
        rows.push(row?);
    }
    Ok(rows)
}

pub fn top_registry_entries(entries: &[RunRegistryEntry], limit: usize) -> Vec<RunRegistryEntry> {
    let mut sorted = entries.to_vec();
    sorted.sort_by(|a, b| {
        b.composite_score
            .total_cmp(&a.composite_score)
            .then_with(|| b.primary_metric_value.total_cmp(&a.primary_metric_value))
            .then_with(|| b.timestamp_utc.cmp(&a.timestamp_utc))
    });
    sorted.into_iter().take(limit).collect()
}

pub fn write_registry_views(
    registry_dir: impl AsRef<Path>,
    entries: &[RunRegistryEntry],
) -> Result<(PathBuf, PathBuf)> {
    let dir = registry_dir.as_ref();
    fs::create_dir_all(dir)?;

    let json_path = dir.join("run_registry.json");
    fs::write(&json_path, serde_json::to_string_pretty(entries)?)?;

    let markdown_path = dir.join("run_registry_top.md");
    let top = top_registry_entries(entries, 20);
    let mut lines = vec![
        "# Run Registry (Top 20 by composite_score)".to_string(),
        String::new(),
        format!("total_runs={}", entries.len()),
        String::new(),
        "| Rank | Time (UTC) | Command | Plugin | Method | Metric | Composite | PnL | MaxDD | Sharpe |"
            .to_string(),
        "|---:|---|---|---|---|---:|---:|---:|---:|---:|".to_string(),
    ];

    for (idx, row) in top.iter().enumerate() {
        let plugin = if row.strategy_plugin.is_empty() {
            "-"
        } else {
            &row.strategy_plugin
        };
        let method = if row.portfolio_method.is_empty() {
            "-"
        } else {
            &row.portfolio_method
        };
        lines.push(format!(
            "| {} | {} | {} | {} | {} | {}:{:.4} | {:.4} | {:.2}% | {:.2}% | {:.3} |",
            idx + 1,
            safe_markdown(&row.timestamp_utc),
            safe_markdown(&row.command),
            safe_markdown(plugin),
            safe_markdown(method),
            safe_markdown(&row.primary_metric_name),
            row.primary_metric_value,
            row.composite_score,
            row.pnl_ratio * 100.0,
            row.max_drawdown * 100.0,
            row.sharpe
        ));
    }

    fs::write(&markdown_path, lines.join("\n") + "\n")?;
    Ok((json_path, markdown_path))
}

pub fn infer_registry_root(output_dir: impl AsRef<Path>) -> PathBuf {
    let path = output_dir.as_ref();
    let leaf = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or_default();
    let known_subdirs = [
        "optimize",
        "research",
        "benchmark",
        "replay",
        "robustness",
        "data_quality",
        "daemon",
    ];

    if known_subdirs.contains(&leaf) {
        return path.parent().unwrap_or(path).to_path_buf();
    }

    path.to_path_buf()
}

fn safe_markdown(value: &str) -> String {
    value.replace('|', "/")
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::{
        append_run_registry, read_run_registry, RunRegistryBacktestInput, RunRegistryEntry,
        RunRegistryOperationInput,
    };
    use crate::engine::BacktestStats;

    #[test]
    fn registry_appends_and_reads_rows() {
        let out_dir = "outputs_rust/test_registry";
        let _ = fs::remove_dir_all(out_dir);
        fs::create_dir_all(out_dir).expect("mkdir");

        let stats = BacktestStats {
            pnl_ratio: 0.12,
            max_drawdown: 0.05,
            sharpe: 1.1,
            sortino: 1.4,
            calmar: 2.4,
            trades: 12,
            rejections: 1,
            ..BacktestStats::default()
        };
        let entry = RunRegistryEntry::from_backtest_input(RunRegistryBacktestInput {
            command: "run".to_string(),
            output_dir: PathBuf::from("outputs_rust"),
            strategy_plugin: "layered_multi_factor".to_string(),
            portfolio_method: "risk_parity".to_string(),
            markets: "A|JP|US".to_string(),
            primary_metric_name: "pnl_ratio".to_string(),
            primary_metric_value: 0.12,
            stats,
            notes: "unit-test".to_string(),
        });
        let report = append_run_registry(out_dir, &entry).expect("append");
        assert!(report.csv_path.exists());
        assert!(report.json_path.exists());
        assert!(report.markdown_path.exists());

        let rows = read_run_registry(report.csv_path).expect("read");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].command, "run");
    }

    #[test]
    fn registry_accepts_operational_entries() {
        let out_dir = "outputs_rust/test_registry_ops";
        let _ = fs::remove_dir_all(out_dir);
        fs::create_dir_all(out_dir).expect("mkdir");

        let entry = RunRegistryEntry::from_operation_input(RunRegistryOperationInput {
            command: "validate-data".to_string(),
            output_dir: PathBuf::from("outputs_rust/data_quality"),
            markets: "A|JP|US".to_string(),
            primary_metric_name: "pass_rate".to_string(),
            primary_metric_value: 1.0,
            notes: "all pass".to_string(),
        });
        let report = append_run_registry(out_dir, &entry).expect("append");
        let rows = read_run_registry(report.csv_path).expect("read");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].primary_metric_name, "pass_rate");
    }
}
