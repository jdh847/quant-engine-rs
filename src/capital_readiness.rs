use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    time::SystemTime,
};

use serde::{Deserialize, Serialize};

use crate::validation_snapshot::ValidationSnapshot;

#[derive(Debug, Clone, Serialize, Default)]
pub struct CapitalReadinessReport {
    pub decision: String,
    pub summary: String,
    pub blockers: Vec<String>,
    pub signal_gate: CapitalGateReport,
    pub portfolio_gate: CapitalGateReport,
    pub execution_gate: CapitalGateReport,
    pub behavior_gate: CapitalGateReport,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct CapitalGateReport {
    pub status: String,
    pub headline: String,
    pub detail: String,
    pub artifact: String,
}

#[derive(Debug, Clone, Deserialize)]
struct FactorIcCompat {
    factor: String,
    mean_ic: f64,
    std_ic: f64,
    ic_ir: f64,
    /// Annualized IC IR (ic_ir * sqrt(252)). `serde(default)` keeps old
    /// factor_ic.jsonl files (written before this field existed) parseable;
    /// for those we backfill it from the daily ic_ir below.
    #[serde(default)]
    annualized_ic_ir: f64,
    t_stat: f64,
    n_days: usize,
    positive_ratio: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct FactorDecompCompat {
    profile: String,
    scenario: String,
    score: f64,
    pnl_ratio: f64,
    sharpe: f64,
    trades: usize,
    rejections: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct FactorDecayCompat {
    profile: String,
    decay_delta: f64,
    latest_score: f64,
    latest_pnl_ratio: f64,
    latest_sharpe: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct FactorIcRollingCompat {
    factor: String,
    window_index: usize,
    start_date: String,
    end_date: String,
    ic_ir: f64,
    t_stat: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct FactorIcRegimeCompat {
    regime: String,
    factor: String,
    ic_ir: f64,
    t_stat: f64,
}

#[derive(Debug, Clone, Deserialize)]
struct ReconcileCompat {
    date: String,
    expected_source: String,
    actual_source: String,
    equity_drift_bps: f64,
    clean: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct BehaviorGateCompat {
    approved_for_tiny_pilot: bool,
    parameter_freeze_days: usize,
    observation_days: usize,
    notes: Vec<String>,
}

pub fn evaluate_capital_readiness(
    root: &Path,
    validation: &ValidationSnapshot,
) -> CapitalReadinessReport {
    let signal_gate = evaluate_signal_gate(root);
    let portfolio_gate = evaluate_portfolio_gate(validation);
    let execution_gate = evaluate_execution_gate(root);
    let behavior_gate = evaluate_behavior_gate(root, validation);

    let gates = [
        ("signal", &signal_gate),
        ("portfolio", &portfolio_gate),
        ("execution", &execution_gate),
        ("behavior", &behavior_gate),
    ];
    let blockers = gates
        .iter()
        .filter(|(_, gate)| gate.status != "PASS")
        .map(|(name, gate)| format!("{name}:{status}", status = gate.status))
        .collect::<Vec<_>>();
    let decision = if blockers.is_empty() {
        "GO_TINY_PILOT_ONLY".to_string()
    } else {
        "NO_GO".to_string()
    };
    let summary = format!(
        "decision={} | signal={} | portfolio={} | execution={} | behavior={}",
        decision,
        signal_gate.status,
        portfolio_gate.status,
        execution_gate.status,
        behavior_gate.status
    );

    CapitalReadinessReport {
        decision,
        summary,
        blockers,
        signal_gate,
        portfolio_gate,
        execution_gate,
        behavior_gate,
    }
}

fn evaluate_signal_gate(root: &Path) -> CapitalGateReport {
    if let Some(path) = find_latest_factor_ic_path(root) {
        let reports = load_factor_ic_reports(&path);
        if !reports.is_empty() {
            let best = reports
                .iter()
                .max_by(|a, b| a.annualized_ic_ir.abs().total_cmp(&b.annualized_ic_ir.abs()))
                .expect("non-empty factor ic");
            // A factor is "stable" (tradeable signal evidence) when it clears
            // ALL of: annualized IC IR >= 0.5 (the textbook factor-IR bar),
            // |t| >= 2.0 (statistically distinguishable from zero), and at
            // least one full year of cross-sections (n_days >= 252). The 0.5
            // bar is now applied to the ANNUALIZED ic_ir; the previous code
            // applied 0.5 to the daily ic_ir, which is an unreachable bar
            // (~annualized 7.9) that no real single factor meets.
            let stable_count = reports
                .iter()
                .filter(|row| {
                    row.annualized_ic_ir.abs() >= 0.5
                        && row.t_stat.abs() >= 2.0
                        && row.n_days >= 252
                })
                .count();
            let rolling_best = load_best_factor_ic_rolling(root);
            let regime_best = load_best_factor_ic_regime(root);
            let headline = format!(
                "best={} ann_ic_ir={:.2} (daily={:.3}) t={:.2} days={}",
                best.factor, best.annualized_ic_ir, best.ic_ir, best.t_stat, best.n_days
            );
            let mut detail = format!(
                "stable_factors={} mean_ic={:.4} std_ic={:.4} pos_rt={:.2}",
                stable_count, best.mean_ic, best.std_ic, best.positive_ratio
            );
            if let Some(rolling) = rolling_best {
                detail.push_str(&format!(
                    " | best_rolling={} w{} ic_ir={:.3} t={:.2} {}->{}",
                    rolling.factor,
                    rolling.window_index,
                    rolling.ic_ir,
                    rolling.t_stat,
                    rolling.start_date,
                    rolling.end_date
                ));
            }
            if let Some(regime) = regime_best {
                detail.push_str(&format!(
                    " | best_regime={} {} ic_ir={:.3} t={:.2}",
                    regime.regime, regime.factor, regime.ic_ir, regime.t_stat
                ));
            }
            let status = if stable_count >= 1 { "PASS" } else { "FAIL" };
            return CapitalGateReport {
                status: status.to_string(),
                headline,
                detail,
                artifact: path.display().to_string(),
            };
        }
    }

    let (decomp_rows, decomp_path) = load_latest_factor_decomposition(root);
    let (decay_rows, decay_path) = load_latest_factor_decay(root);
    let artifact = [decomp_path, decay_path]
        .into_iter()
        .flatten()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>()
        .join(" | ");
    if !decomp_rows.is_empty() || !decay_rows.is_empty() {
        let best_profile = decomp_rows
            .iter()
            .max_by(|a, b| a.score.total_cmp(&b.score))
            .map(|row| {
                format!(
                    "{} score={:.3} sharpe={:.2} pnl={:.2}% scenario={} trades={} rej={}",
                    row.profile,
                    row.score,
                    row.sharpe,
                    row.pnl_ratio * 100.0,
                    row.scenario,
                    row.trades,
                    row.rejections
                )
            })
            .unwrap_or_else(|| "-".to_string());
        let best_decay = decay_rows
            .iter()
            .max_by(|a, b| a.latest_sharpe.total_cmp(&b.latest_sharpe))
            .map(|row| {
                format!(
                    "{} latest_sharpe={:.2} latest_pnl={:.2}% decay_delta={:.3} latest_score={:.3}",
                    row.profile,
                    row.latest_sharpe,
                    row.latest_pnl_ratio * 100.0,
                    row.decay_delta,
                    row.latest_score
                )
            })
            .unwrap_or_else(|| "-".to_string());
        return CapitalGateReport {
            status: "UNKNOWN".to_string(),
            headline: "factor_ic not run; only proxy research artifacts available".to_string(),
            detail: format!("best_profile={} | best_decay={}", best_profile, best_decay),
            artifact,
        };
    }

    CapitalGateReport {
        status: "UNKNOWN".to_string(),
        headline: "no signal-quality artifact found".to_string(),
        detail: "run factor-ic on real-history data before considering capital".to_string(),
        artifact: "-".to_string(),
    }
}

fn evaluate_portfolio_gate(validation: &ValidationSnapshot) -> CapitalGateReport {
    let Some(full) = validation.full_real_window.as_ref() else {
        return CapitalGateReport {
            status: "FAIL".to_string(),
            headline: "missing full-window real-history readiness".to_string(),
            detail: "need full real OOS evidence before capital".to_string(),
            artifact: "-".to_string(),
        };
    };
    let recent = validation.recent_real_window.as_ref();
    let us_long = validation.us_long_sample.as_ref();
    let route = validation.route_decision.as_ref();

    let full_ok = full.sharpe.unwrap_or(f64::NEG_INFINITY) >= 1.0
        && full.max_drawdown.unwrap_or(f64::INFINITY) <= 0.15
        && full.pnl_ratio.unwrap_or(f64::NEG_INFINITY) > 0.0;
    let recent_ok = recent
        .map(|row| {
            row.sharpe.unwrap_or(f64::NEG_INFINITY) >= 1.0
                && row.max_drawdown.unwrap_or(f64::INFINITY) <= 0.15
                && row.pnl_ratio.unwrap_or(f64::NEG_INFINITY) > 0.0
        })
        .unwrap_or(false);
    let long_ok = us_long
        .map(|row| row.sharpe.unwrap_or(f64::NEG_INFINITY) >= 1.0)
        .unwrap_or(false);
    let status = if full_ok && recent_ok && long_ok {
        "PASS"
    } else if full.sharpe.unwrap_or(0.0) > 0.0 {
        "WATCH"
    } else {
        "FAIL"
    };
    let headline = format!(
        "full sharpe={:.2} dd={:.2}% | recent sharpe={} | route={}",
        full.sharpe.unwrap_or(0.0),
        full.max_drawdown.unwrap_or(0.0) * 100.0,
        recent
            .and_then(|row| row.sharpe)
            .map(|v| format!("{v:.2}"))
            .unwrap_or_else(|| "-".to_string()),
        route
            .map(|row| if row.decision.is_empty() { "-" } else { row.decision.as_str() })
            .unwrap_or("-"),
    );
    let detail = format!(
        "recent pnl={} | us_long sharpe={} pnl={} | baseline_vs_candidate={} vs {}",
        recent
            .and_then(|row| row.pnl_ratio)
            .map(format_pct)
            .unwrap_or_else(|| "-".to_string()),
        us_long
            .and_then(|row| row.sharpe)
            .map(|v| format!("{v:.2}"))
            .unwrap_or_else(|| "-".to_string()),
        us_long
            .and_then(|row| row.pnl_ratio)
            .map(format_pct)
            .unwrap_or_else(|| "-".to_string()),
        route
            .and_then(|row| row.baseline_pnl_ratio)
            .map(format_pct)
            .unwrap_or_else(|| "-".to_string()),
        route
            .and_then(|row| row.candidate_pnl_ratio)
            .map(format_pct)
            .unwrap_or_else(|| "-".to_string()),
    );
    CapitalGateReport {
        status: status.to_string(),
        headline,
        detail,
        artifact: full.output_dir.clone(),
    }
}

fn evaluate_execution_gate(root: &Path) -> CapitalGateReport {
    let Some(path) = find_latest_reconcile_path(root) else {
        return CapitalGateReport {
            status: "UNKNOWN".to_string(),
            headline: "no reconcile artifact found".to_string(),
            detail: "need real IBKR paper reconcile evidence before capital".to_string(),
            artifact: "-".to_string(),
        };
    };
    let rows = load_reconcile_rows(&path);
    if rows.is_empty() {
        return CapitalGateReport {
            status: "UNKNOWN".to_string(),
            headline: "reconcile artifact empty".to_string(),
            detail: "need real IBKR paper reconcile evidence before capital".to_string(),
            artifact: path.display().to_string(),
        };
    }
    let unique_days = rows
        .iter()
        .map(|row| row.date.clone())
        .collect::<BTreeSet<_>>()
        .len();
    let uses_real_actual = rows
        .iter()
        .any(|row| row.actual_source != "internal" || row.expected_source != "internal");
    let avg_abs_drift_bps = rows
        .iter()
        .map(|row| row.equity_drift_bps.abs())
        .sum::<f64>()
        / rows.len() as f64;
    let clean_ratio = rows.iter().filter(|row| row.clean).count() as f64 / rows.len() as f64;

    let status = if uses_real_actual && unique_days >= 30 && avg_abs_drift_bps < 5.0 && clean_ratio >= 0.95 {
        "PASS"
    } else if uses_real_actual {
        "WATCH"
    } else {
        "FAIL"
    };
    let headline = if uses_real_actual {
        format!(
            "real broker evidence: days={} avg_drift_bps={:.2} clean_ratio={:.0}%",
            unique_days,
            avg_abs_drift_bps,
            clean_ratio * 100.0
        )
    } else {
        format!(
            "dry_run/internal only: days={} avg_drift_bps={:.2}",
            unique_days, avg_abs_drift_bps
        )
    };
    let detail = format!(
        "expected_sources={} | actual_sources={}",
        distinct_values(rows.iter().map(|row| row.expected_source.clone())),
        distinct_values(rows.iter().map(|row| row.actual_source.clone())),
    );
    CapitalGateReport {
        status: status.to_string(),
        headline,
        detail,
        artifact: path.display().to_string(),
    }
}

fn evaluate_behavior_gate(root: &Path, validation: &ValidationSnapshot) -> CapitalGateReport {
    let path = root.join("capital_behavior_gate.json");
    if path.exists() {
        if let Ok(text) = fs::read_to_string(&path) {
            if let Ok(report) = serde_json::from_str::<BehaviorGateCompat>(&text) {
                let status = if report.approved_for_tiny_pilot
                    && report.parameter_freeze_days >= 28
                    && report.observation_days >= 30
                {
                    "PASS"
                } else {
                    "WATCH"
                };
                return CapitalGateReport {
                    status: status.to_string(),
                    headline: format!(
                        "freeze_days={} observation_days={}",
                        report.parameter_freeze_days, report.observation_days
                    ),
                    detail: if report.notes.is_empty() {
                        "manual behavior log present".to_string()
                    } else {
                        report.notes.join(" | ")
                    },
                    artifact: path.display().to_string(),
                };
            }
        }
    }

    let route_decision = validation
        .route_decision
        .as_ref()
        .map(|row| row.decision.clone())
        .unwrap_or_default();
    let headline = if route_decision == "keep_current_defaults" {
        "defaults were not promoted on weak long-sample evidence".to_string()
    } else {
        "no manual freeze/observation log found".to_string()
    };
    let detail = if route_decision == "keep_current_defaults" {
        "good sign, but still no explicit parameter-freeze or pilot observation artifact".to_string()
    } else {
        "add outputs_rust/capital_behavior_gate.json once a frozen observation window exists".to_string()
    };
    CapitalGateReport {
        status: "UNKNOWN".to_string(),
        headline,
        detail,
        artifact: if route_decision.is_empty() {
            "-".to_string()
        } else {
            "compare_us_long_route/route_decision_us.txt".to_string()
        },
    }
}

fn find_latest_factor_ic_path(root: &Path) -> Option<PathBuf> {
    let mut candidates = walk_paths(root, 2)
        .into_iter()
        .filter(|path| {
            path.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| name.contains("factor_ic") && name.ends_with(".jsonl"))
                    .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    sort_paths_by_modified_desc(&mut candidates);
    candidates.into_iter().next()
}

fn load_factor_ic_reports(path: &Path) -> Vec<FactorIcCompat> {
    fs::read_to_string(path)
        .ok()
        .map(|text| {
            text.lines()
                .filter_map(|line| serde_json::from_str::<FactorIcCompat>(line).ok())
                .map(|mut row| {
                    // Backfill for legacy files that predate the annualized field.
                    if row.annualized_ic_ir == 0.0 && row.ic_ir != 0.0 {
                        row.annualized_ic_ir = row.ic_ir * 252_f64.sqrt();
                    }
                    row
                })
                .collect()
        })
        .unwrap_or_default()
}

fn load_latest_factor_decomposition(root: &Path) -> (Vec<FactorDecompCompat>, Option<PathBuf>) {
    let mut candidates = walk_paths(root, 2)
        .into_iter()
        .filter(|path| {
            path.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| name == "factor_decomposition_us.csv")
                    .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    sort_paths_by_modified_desc(&mut candidates);
    let Some(path) = candidates.into_iter().next() else {
        return (Vec::new(), None);
    };
    let mut rdr = match csv::Reader::from_path(&path) {
        Ok(rdr) => rdr,
        Err(_) => return (Vec::new(), Some(path)),
    };
    let rows = rdr.deserialize().filter_map(Result::ok).collect::<Vec<_>>();
    (rows, Some(path))
}

fn load_latest_factor_decay(root: &Path) -> (Vec<FactorDecayCompat>, Option<PathBuf>) {
    let mut candidates = walk_paths(root, 2)
        .into_iter()
        .filter(|path| {
            path.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| name == "factor_decay_us.csv")
                    .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    sort_paths_by_modified_desc(&mut candidates);
    let Some(path) = candidates.into_iter().next() else {
        return (Vec::new(), None);
    };
    let mut rdr = match csv::Reader::from_path(&path) {
        Ok(rdr) => rdr,
        Err(_) => return (Vec::new(), Some(path)),
    };
    let rows = rdr.deserialize().filter_map(Result::ok).collect::<Vec<_>>();
    (rows, Some(path))
}

fn load_best_factor_ic_rolling(root: &Path) -> Option<FactorIcRollingCompat> {
    let path = root
        .join("research_us_long_factor_ic")
        .join("factor_ic_rolling_us.csv");
    let mut rdr = csv::Reader::from_path(path).ok()?;
    rdr.deserialize()
        .filter_map(Result::ok)
        .max_by(|a: &FactorIcRollingCompat, b: &FactorIcRollingCompat| a.ic_ir.total_cmp(&b.ic_ir))
}

fn load_best_factor_ic_regime(root: &Path) -> Option<FactorIcRegimeCompat> {
    let path = root
        .join("research_us_long_factor_ic")
        .join("factor_ic_regime_us.csv");
    let mut rdr = csv::Reader::from_path(path).ok()?;
    rdr.deserialize()
        .filter_map(Result::ok)
        .max_by(|a: &FactorIcRegimeCompat, b: &FactorIcRegimeCompat| a.ic_ir.total_cmp(&b.ic_ir))
}

fn find_latest_reconcile_path(root: &Path) -> Option<PathBuf> {
    let mut candidates = walk_paths(root, 2)
        .into_iter()
        .filter(|path| {
            path.is_file()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| name.contains("reconcile") && name.ends_with(".jsonl"))
                    .unwrap_or(false)
        })
        .collect::<Vec<_>>();
    sort_paths_by_modified_desc(&mut candidates);
    candidates.into_iter().next()
}

fn load_reconcile_rows(path: &Path) -> Vec<ReconcileCompat> {
    fs::read_to_string(path)
        .ok()
        .map(|text| {
            text.lines()
                .filter_map(|line| serde_json::from_str::<ReconcileCompat>(line).ok())
                .collect()
        })
        .unwrap_or_default()
}

fn walk_paths(root: &Path, max_depth: usize) -> Vec<PathBuf> {
    let mut stack = vec![(root.to_path_buf(), 0usize)];
    let mut out = Vec::new();
    while let Some((path, depth)) = stack.pop() {
        out.push(path.clone());
        if depth >= max_depth || !path.is_dir() {
            continue;
        }
        if let Ok(read_dir) = fs::read_dir(&path) {
            for entry in read_dir.flatten() {
                stack.push((entry.path(), depth + 1));
            }
        }
    }
    out
}

fn sort_paths_by_modified_desc(paths: &mut [PathBuf]) {
    paths.sort_by_key(|path| {
        std::cmp::Reverse(
            fs::metadata(path)
                .and_then(|meta| meta.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH),
        )
    });
}

fn distinct_values<I>(iter: I) -> String
where
    I: Iterator<Item = String>,
{
    let mut set = BTreeSet::new();
    for value in iter {
        if !value.trim().is_empty() {
            set.insert(value);
        }
    }
    if set.is_empty() {
        "-".to_string()
    } else {
        set.into_iter().collect::<Vec<_>>().join("|")
    }
}

fn format_pct(value: f64) -> String {
    format!("{:.2}%", value * 100.0)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::validation_snapshot::load_validation_snapshot;

    use super::evaluate_capital_readiness;

    #[test]
    fn capital_readiness_is_no_go_without_factor_ic_or_real_ibkr() {
        let root = std::env::temp_dir().join(format!(
            "pqbot_capital_readiness_{}",
            std::process::id()
        ));
        if root.exists() {
            fs::remove_dir_all(&root).ok();
        }
        fs::create_dir_all(root.join("readiness_real_new")).expect("mkdir readiness");
        fs::create_dir_all(root.join("readiness_real_recent")).expect("mkdir readiness recent");
        fs::create_dir_all(root.join("run_us_long_tuned_20260416")).expect("mkdir run");
        fs::create_dir_all(root.join("compare_us_long_route")).expect("mkdir route");
        fs::create_dir_all(root.join("research_us_long_factor_decomp")).expect("mkdir decomp");
        fs::create_dir_all(root.join("research_us_long_factor_decay")).expect("mkdir decay");

        fs::write(
            root.join("readiness_real_new").join("readiness_report.json"),
            r#"{"history_days":585,"readiness_tier":"WATCH_ONLY","notes":[],"data_quality":{"rows":[{"status":"PASS"},{"status":"PASS"},{"status":"PASS"}]},"oos":{"test_start":"2025-07-30","test_end":"2026-04-01","pnl_ratio":0.0555,"max_drawdown":0.0958,"sharpe":0.7782,"trades":645,"rejections":102}}"#,
        )
        .expect("write full readiness");
        fs::write(
            root.join("readiness_real_recent")
                .join("readiness_report.json"),
            r#"{"history_days":261,"readiness_tier":"WATCH_ONLY","notes":["data_quality_warn"],"data_quality":{"rows":[{"status":"WARN"},{"status":"PASS"},{"status":"PASS"}]},"oos":{"test_start":"2025-10-01","test_end":"2026-04-01","pnl_ratio":-0.0374,"max_drawdown":0.0932,"sharpe":-1.1690,"trades":120,"rejections":12}}"#,
        )
        .expect("write recent readiness");
        fs::write(
            root.join("run_us_long_tuned_20260416").join("summary.txt"),
            "start_equity=1000000.00\nend_equity=1003261.45\npnl=3261.45\npnl_ratio=0.3261%\nmax_drawdown=26.1649%\nsharpe=0.0591\ntrades=4016\nrejections=1891\n",
        )
        .expect("write run");
        fs::write(
            root.join("compare_us_long_route")
                .join("route_decision_us.txt"),
            "baseline_pnl_ratio=0.075109\ncandidate_pnl_ratio=0.003261\ndecision=keep_current_defaults\n",
        )
        .expect("write route");
        fs::write(
            root.join("ibkr_reconcile.jsonl"),
            r#"{"date":"2025-01-13","expected_source":"internal","actual_source":"internal","equity_drift_bps":0.0,"clean":true}"#,
        )
        .expect("write reconcile");
        fs::write(
            root.join("research_us_long_factor_decomp")
                .join("factor_decomposition_us.csv"),
            "profile,scenario,score,pnl_ratio,max_drawdown,sharpe,trades,rejections,delta_score_vs_all,delta_pnl_vs_all,delta_sharpe_vs_all\nall_factors,US,-0.050037,0.019591,0.111963,0.075873,581,1753,0.0,0.0,0.0\nmean_reversion_only,US,0.140221,0.133981,0.082819,0.358569,594,1685,0.190258,0.11439,0.282696\n",
        )
        .expect("write decomp");
        fs::write(
            root.join("research_us_long_factor_decay")
                .join("factor_decay_us.csv"),
            "profile,windows,avg_score_early,avg_score_late,decay_delta,decay_ratio,trend_slope,latest_score,latest_pnl_ratio,latest_sharpe\nall_factors,16,-0.151,0.128,0.279,-0.84,0.038,1.109,0.059,2.6554\n",
        )
        .expect("write decay");

        let validation = load_validation_snapshot(&root);
        let report = evaluate_capital_readiness(&root, &validation);
        assert_eq!(report.decision, "NO_GO");
        assert_eq!(report.signal_gate.status, "UNKNOWN");
        assert_eq!(report.execution_gate.status, "FAIL");
        assert!(report.portfolio_gate.status == "WATCH" || report.portfolio_gate.status == "FAIL");
    }
}
