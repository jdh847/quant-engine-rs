use std::{
    collections::HashMap,
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use anyhow::{anyhow, Result};
use serde::Deserialize;

use crate::{
    paper_hints::{build_paper_hints, PaperHintsCompareInput, PaperHintsDaemonInput},
    registry::{read_run_registry, top_registry_entries, RunRegistryEntry},
};

#[derive(Debug, Clone)]
pub struct ControlCenterRequest {
    pub output_dir: PathBuf,
    pub refresh_secs: u64,
    pub cycles: usize,
}

#[derive(Debug, Clone, Default)]
pub struct ControlCenterReport {
    pub ticks: usize,
    pub registry_runs: usize,
    pub last_end_equity: f64,
}

#[derive(Debug, Clone, Default)]
struct ControlCenterSnapshot {
    summary: HashMap<String, String>,
    registry_runs: usize,
    registry_top: Vec<RunRegistryEntry>,
    daemon: Option<DaemonState>,
    data_quality: Option<DataQualityStatus>,
    robustness: HashMap<String, String>,
    research_report: HashMap<String, String>,
    recent_compare: Option<CompareStatus>,
}

#[derive(Debug, Clone, Deserialize)]
struct DaemonState {
    last_cycle: usize,
    last_end_equity: f64,
    max_drawdown_observed: f64,
    alerts: usize,
}

#[derive(Debug, Clone, Default)]
struct DataQualityStatus {
    pass: usize,
    warn: usize,
    fail: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct CompareFieldStatus {
    key: String,
    changed: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct CompareWinnerStatus {
    winner: String,
}

#[derive(Debug, Clone, Deserialize)]
struct CompareReportStatus {
    winner_summary: CompareWinnerStatus,
    metric_rows: Vec<CompareFieldStatus>,
    audit_rows: Vec<CompareFieldStatus>,
    data_quality_rows: Vec<CompareFieldStatus>,
    research_rows: Vec<CompareFieldStatus>,
}

#[derive(Debug, Clone)]
struct CompareStatus {
    winner: String,
    metric_changes: usize,
    audit_changes: usize,
    data_quality_changes: usize,
    research_changes: usize,
    top_research_keys: Vec<String>,
}

pub fn run_control_center(req: &ControlCenterRequest) -> Result<ControlCenterReport> {
    if req.cycles == 0 {
        return Err(anyhow!("cycles must be > 0"));
    }

    let mut report = ControlCenterReport::default();
    for tick in 1..=req.cycles {
        let snapshot = collect_snapshot(&req.output_dir)?;
        render_snapshot(&req.output_dir, tick, req.cycles, &snapshot)?;

        report.ticks = tick;
        report.registry_runs = snapshot.registry_runs;
        report.last_end_equity = parse_decimal(snapshot.summary.get("end_equity")).unwrap_or(0.0);

        if tick < req.cycles && req.refresh_secs > 0 {
            thread::sleep(Duration::from_secs(req.refresh_secs));
        }
    }

    Ok(report)
}

fn collect_snapshot(root: &Path) -> Result<ControlCenterSnapshot> {
    let summary_path = first_existing_path(&[root.join("summary.txt")]);
    let summary = match summary_path {
        Some(path) => parse_key_value_file(&path)?,
        None => HashMap::new(),
    };

    let registry_path = root.join("run_registry.csv");
    let registry_entries = if registry_path.exists() {
        read_run_registry(&registry_path)?
    } else {
        Vec::new()
    };
    let registry_top = top_registry_entries(&registry_entries, 5);

    let daemon_path = first_existing_path(&[
        root.join("paper_daemon_state.json"),
        root.join("daemon").join("paper_daemon_state.json"),
    ]);
    let daemon = daemon_path
        .as_ref()
        .map(fs::read_to_string)
        .transpose()?
        .map(|text| serde_json::from_str::<DaemonState>(&text))
        .transpose()?;

    let quality_path = first_existing_path(&[
        root.join("data_quality_report.csv"),
        root.join("data_quality").join("data_quality_report.csv"),
    ]);
    let data_quality = quality_path
        .as_ref()
        .map(|path| load_data_quality_status(path))
        .transpose()?;

    let robustness_path = first_existing_path(&[
        root.join("robustness_summary.txt"),
        root.join("robustness").join("robustness_summary.txt"),
    ]);
    let robustness = match robustness_path {
        Some(path) => parse_key_value_file(&path)?,
        None => HashMap::new(),
    };

    let research_report_path = first_existing_path(&[
        root.join("research_report_summary.txt"),
        root.join("research_report")
            .join("research_report_summary.txt"),
    ]);
    let research_report = match research_report_path {
        Some(path) => parse_key_value_file(&path)?,
        None => HashMap::new(),
    };
    let recent_compare = load_recent_compare_status(root);

    Ok(ControlCenterSnapshot {
        summary,
        registry_runs: registry_entries.len(),
        registry_top,
        daemon,
        data_quality,
        robustness,
        research_report,
        recent_compare,
    })
}

fn render_snapshot(
    root: &Path,
    tick: usize,
    total_ticks: usize,
    snapshot: &ControlCenterSnapshot,
) -> Result<()> {
    let mut out = io::stdout();
    write!(out, "\x1B[2J\x1B[H")?;
    writeln!(out, "Private Quant Bot Control Center")?;
    writeln!(
        out,
        "root={} | tick={}/{}",
        root.display(),
        tick,
        total_ticks
    )?;
    writeln!(out)?;

    writeln!(
        out,
        "Run Summary | end_equity={} pnl_ratio={} max_drawdown={} sharpe={} trades={} rejections={}",
        snapshot
            .summary
            .get("end_equity")
            .map_or("-", String::as_str),
        snapshot
            .summary
            .get("pnl_ratio")
            .map_or("-", String::as_str),
        snapshot
            .summary
            .get("max_drawdown")
            .map_or("-", String::as_str),
        snapshot.summary.get("sharpe").map_or("-", String::as_str),
        snapshot.summary.get("trades").map_or("-", String::as_str),
        snapshot
            .summary
            .get("rejections")
            .map_or("-", String::as_str),
    )?;

    writeln!(out, "Registry | total_runs={}", snapshot.registry_runs)?;
    if snapshot.registry_top.is_empty() {
        writeln!(out, "  (no runs yet)")?;
    } else {
        for (idx, row) in snapshot.registry_top.iter().enumerate() {
            let plugin = if row.strategy_plugin.is_empty() {
                "-"
            } else {
                row.strategy_plugin.as_str()
            };
            let method = if row.portfolio_method.is_empty() {
                "-"
            } else {
                row.portfolio_method.as_str()
            };
            writeln!(
                out,
                "  {}. {} {} {}:{:.4} score={:.4} plugin={} method={}",
                idx + 1,
                row.timestamp_utc,
                row.command,
                row.primary_metric_name,
                row.primary_metric_value,
                row.composite_score,
                plugin,
                method
            )?;
        }
    }

    if let Some(daemon) = &snapshot.daemon {
        writeln!(
            out,
            "Daemon | cycle={} alerts={} last_end_equity={:.2} max_drawdown_observed={:.4}%",
            daemon.last_cycle,
            daemon.alerts,
            daemon.last_end_equity,
            daemon.max_drawdown_observed * 100.0
        )?;
    }

    if let Some(quality) = &snapshot.data_quality {
        writeln!(
            out,
            "Data Quality | pass={} warn={} fail={}",
            quality.pass, quality.warn, quality.fail
        )?;
    }

    if !snapshot.robustness.is_empty() {
        writeln!(
            out,
            "Robustness | folds={} avg_test_pnl_ratio={} avg_deflated_sharpe_proxy={}",
            snapshot.robustness.get("folds").map_or("-", String::as_str),
            snapshot
                .robustness
                .get("avg_selected_test_pnl_ratio")
                .or_else(|| snapshot.robustness.get("avg_test_pnl_ratio"))
                .map_or("-", String::as_str),
            snapshot
                .robustness
                .get("avg_deflated_sharpe_proxy")
                .map_or("-", String::as_str),
        )?;
    }

    if !snapshot.research_report.is_empty() {
        let dq_pass = snapshot.data_quality.as_ref().map(|q| q.pass).unwrap_or(0);
        let dq_warn = snapshot.data_quality.as_ref().map(|q| q.warn).unwrap_or(0);
        let dq_fail = snapshot.data_quality.as_ref().map(|q| q.fail).unwrap_or(0);
        writeln!(
            out,
            "Ops Center | run_sharpe={} dq={}/{}/{} audit={} compare={} research_sharpe={} rotation={} switches={}",
            snapshot.summary.get("sharpe").map_or("-", String::as_str),
            dq_pass,
            dq_warn,
            dq_fail,
            if snapshot
                .research_report
                .contains_key("top_regime_leader_market")
            {
                "ready"
            } else {
                "partial"
            },
            if snapshot.recent_compare.is_some() {
                "ready"
            } else {
                "missing"
            },
            snapshot
                .research_report
                .get("avg_test_sharpe")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("current_rotation_leader_factor")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("rotation_switches")
                .map_or("-", String::as_str),
        )?;
        writeln!(
            out,
            "Research Report | folds={} avg_test_sharpe={} best_decay={} {}d ic={} latest_rolling={} {}d ic={}",
            snapshot
                .research_report
                .get("folds")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("avg_test_sharpe")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("best_decay_factor")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("best_decay_horizon_days")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("best_decay_ic")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("latest_rolling_factor")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("latest_rolling_horizon_days")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("latest_rolling_ic")
                .map_or("-", String::as_str),
        )?;
        writeln!(
            out,
            "Research Signals | best_monotonic={} {}d score={} best_regime_decay={} {} {} {}d ic={}",
            snapshot
                .research_report
                .get("best_monotonic_factor")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("best_monotonic_horizon_days")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("best_monotonicity_score")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("best_regime_decay_market")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("best_regime_decay_bucket")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("best_regime_decay_factor")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("best_regime_decay_horizon_days")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("best_regime_decay_ic")
                .map_or("-", String::as_str),
        )?;
        writeln!(
            out,
            "Walk-Forward Winners | dominant={} / {} count={} concentration={} unstable_folds={}",
            snapshot
                .research_report
                .get("dominant_winner_strategy_plugin")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("dominant_winner_portfolio_method")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("dominant_winner_count")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("dominant_winner_concentration")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("unstable_folds")
                .map_or("-", String::as_str),
        )?;
        writeln!(
            out,
            "Regime Leaders | top={} {} {} {}d ic={} dominant_factor={} count={} positive_regimes={} avg_ic={}",
            snapshot
                .research_report
                .get("top_regime_leader_market")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("top_regime_leader_bucket")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("top_regime_leader_factor")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("top_regime_leader_horizon_days")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("top_regime_leader_ic")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("dominant_regime_factor")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("dominant_regime_factor_count")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("positive_regime_leader_count")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("avg_regime_leader_ic")
                .map_or("-", String::as_str),
        )?;
        writeln!(
            out,
            "Factor Rotation | horizon={}d current={} {} ic={} dominant={} count={} switches={} streak={} {}",
            snapshot
                .research_report
                .get("rotation_default_horizon_days")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("current_rotation_date")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("current_rotation_leader_factor")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("current_rotation_leader_ic")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("dominant_rotation_factor")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("dominant_rotation_factor_count")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("rotation_switches")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("latest_rotation_streak_factor")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("latest_rotation_streak_count")
                .map_or("-", String::as_str),
        )?;
        writeln!(
            out,
            "Regime x Rotation | focus={} {} {} {}d aligned={} mismatched={} ratio={}",
            snapshot
                .research_report
                .get("regime_rotation_focus_market")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("regime_rotation_focus_bucket")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("regime_rotation_focus_factor")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("regime_rotation_focus_horizon_days")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("aligned_regime_count")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("mismatched_regime_count")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("regime_rotation_alignment_ratio")
                .map_or("-", String::as_str),
        )?;
        writeln!(
            out,
            "Regime Transitions | top={} {} -> {} count={} latest={} {} {} -> {} avg_gap_days={}",
            snapshot
                .research_report
                .get("top_regime_transition_market")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("top_regime_transition_from_bucket")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("top_regime_transition_to_bucket")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("top_regime_transition_count")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("latest_regime_transition_date")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("latest_regime_transition_market")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("latest_regime_transition_from_bucket")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("latest_regime_transition_to_bucket")
                .map_or("-", String::as_str),
            snapshot
                .research_report
                .get("avg_regime_transition_gap_days")
                .map_or("-", String::as_str),
        )?;
        if let Some(compare) = &snapshot.recent_compare {
            writeln!(
                out,
                "Research Compare | winner={} changes={} metric/audit/dq={}/{}/{} top={}",
                compare.winner,
                compare.research_changes,
                compare.metric_changes,
                compare.audit_changes,
                compare.data_quality_changes,
                if compare.top_research_keys.is_empty() {
                    "-".to_string()
                } else {
                    compare.top_research_keys.join(",")
                }
            )?;
        }
        let daemon_input = snapshot
            .daemon
            .as_ref()
            .map(|daemon| PaperHintsDaemonInput {
                last_cycle: daemon.last_cycle,
                last_end_equity: daemon.last_end_equity,
                max_drawdown_observed: daemon.max_drawdown_observed,
                alerts: daemon.alerts,
            });
        let compare_input =
            snapshot
                .recent_compare
                .as_ref()
                .map(|compare| PaperHintsCompareInput {
                    winner: compare.winner.clone(),
                    research_changes: compare.research_changes,
                    top_research_keys: compare.top_research_keys.clone(),
                });
        let hints = build_paper_hints(
            &snapshot.research_report,
            daemon_input.as_ref(),
            compare_input.as_ref(),
        );
        writeln!(
            out,
            "Paper Hints | stance={} markets={} headline={}",
            hints.stance,
            if hints.watch_markets.is_empty() {
                "-".to_string()
            } else {
                hints.watch_markets.join("|")
            },
            hints.headline
        )?;
        if !hints.market_hints.is_empty() {
            writeln!(
                out,
                "Paper Hint Feed | {}",
                hints
                    .market_hints
                    .iter()
                    .map(|hint| format!("{}:{}:{}", hint.market, hint.stance, hint.headline))
                    .collect::<Vec<_>>()
                    .join(" | ")
            )?;
        }
    }

    out.flush()?;
    Ok(())
}

fn first_existing_path(paths: &[PathBuf]) -> Option<PathBuf> {
    paths.iter().find(|p| p.exists()).cloned()
}

fn load_recent_compare_status(root: &Path) -> Option<CompareStatus> {
    let mut candidate_dirs = Vec::<PathBuf>::new();
    if root.join("compare_report.json").exists() {
        candidate_dirs.push(root.to_path_buf());
    }
    if let Ok(read_dir) = fs::read_dir(root) {
        for entry in read_dir.flatten() {
            let path = entry.path();
            if path.is_dir() && path.join("compare_report.json").exists() {
                candidate_dirs.push(path);
            }
        }
    }
    if let Some(parent) = root.parent() {
        if let Ok(read_dir) = fs::read_dir(parent) {
            for entry in read_dir.flatten() {
                let path = entry.path();
                if path.is_dir()
                    && path.join("compare_report.json").exists()
                    && !candidate_dirs.iter().any(|p| p == &path)
                {
                    candidate_dirs.push(path);
                }
            }
        }
    }

    let latest_dir = candidate_dirs.into_iter().max_by_key(|dir| {
        fs::metadata(dir.join("compare_report.json"))
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
    })?;
    let text = fs::read_to_string(latest_dir.join("compare_report.json")).ok()?;
    let report: CompareReportStatus = serde_json::from_str(&text).ok()?;
    Some(CompareStatus {
        winner: report.winner_summary.winner,
        metric_changes: report.metric_rows.iter().filter(|row| row.changed).count(),
        audit_changes: report.audit_rows.iter().filter(|row| row.changed).count(),
        data_quality_changes: report
            .data_quality_rows
            .iter()
            .filter(|row| row.changed)
            .count(),
        research_changes: report
            .research_rows
            .iter()
            .filter(|row| row.changed)
            .count(),
        top_research_keys: report
            .research_rows
            .iter()
            .filter(|row| row.changed)
            .map(|row| row.key.clone())
            .take(3)
            .collect(),
    })
}

fn parse_key_value_file(path: &Path) -> Result<HashMap<String, String>> {
    let text = fs::read_to_string(path)?;
    Ok(text
        .lines()
        .filter_map(|line| {
            let (k, v) = line.split_once('=')?;
            Some((k.trim().to_string(), v.trim().to_string()))
        })
        .collect())
}

fn load_data_quality_status(path: &Path) -> Result<DataQualityStatus> {
    let mut rdr = csv::Reader::from_path(path)?;
    let mut status = DataQualityStatus::default();
    for rec in rdr.records() {
        let rec = rec?;
        match rec.get(9).unwrap_or_default() {
            "PASS" => status.pass += 1,
            "WARN" => status.warn += 1,
            "FAIL" => status.fail += 1,
            _ => {}
        }
    }
    Ok(status)
}

fn parse_decimal(value: Option<&String>) -> Option<f64> {
    let raw = value?;
    let trimmed = raw.trim().trim_end_matches('%');
    trimmed.parse::<f64>().ok()
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use crate::{
        engine::BacktestStats,
        registry::{append_run_registry, RunRegistryBacktestInput, RunRegistryEntry},
    };

    use super::{run_control_center, ControlCenterRequest};

    #[test]
    fn control_center_runs_single_tick() {
        let dir = std::env::temp_dir().join("private_quant_bot_control_center_test");
        if dir.exists() {
            fs::remove_dir_all(&dir).ok();
        }
        fs::create_dir_all(&dir).expect("mkdir");
        fs::write(
            dir.join("summary.txt"),
            "end_equity=1001000.0\npnl_ratio=1.2000%\nmax_drawdown=0.4000%\n",
        )
        .expect("write summary");

        let entry = RunRegistryEntry::from_backtest_input(RunRegistryBacktestInput {
            command: "run".to_string(),
            output_dir: PathBuf::from("outputs_rust"),
            strategy_plugin: "layered_multi_factor".to_string(),
            portfolio_method: "risk_parity".to_string(),
            markets: "A|JP|US".to_string(),
            primary_metric_name: "pnl_ratio".to_string(),
            primary_metric_value: 0.012,
            stats: BacktestStats {
                pnl_ratio: 0.012,
                max_drawdown: 0.004,
                sharpe: 1.2,
                ..BacktestStats::default()
            },
            notes: "test".to_string(),
        });
        append_run_registry(&dir, &entry).expect("append");

        let report = run_control_center(&ControlCenterRequest {
            output_dir: dir,
            refresh_secs: 0,
            cycles: 1,
        })
        .expect("run control center");

        assert_eq!(report.ticks, 1);
        assert_eq!(report.registry_runs, 1);
        assert!(report.last_end_equity > 0.0);
    }
}
