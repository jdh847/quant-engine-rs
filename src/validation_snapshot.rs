use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    time::SystemTime,
};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Default)]
pub struct ValidationSnapshot {
    pub full_real_window: Option<ValidationReadinessUi>,
    pub recent_real_window: Option<ValidationReadinessUi>,
    pub us_long_sample: Option<ValidationRunUi>,
    pub route_decision: Option<RouteDecisionUi>,
}

impl ValidationSnapshot {
    pub fn has_any(&self) -> bool {
        self.full_real_window.is_some()
            || self.recent_real_window.is_some()
            || self.us_long_sample.is_some()
            || self.route_decision.is_some()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ValidationReadinessUi {
    pub output_dir: String,
    pub history_days: usize,
    pub readiness_tier: String,
    pub test_start: String,
    pub test_end: String,
    pub pnl_ratio: Option<f64>,
    pub max_drawdown: Option<f64>,
    pub sharpe: Option<f64>,
    pub trades: Option<usize>,
    pub rejections: Option<usize>,
    pub data_quality_status: String,
    pub pass_markets: usize,
    pub warn_markets: usize,
    pub fail_markets: usize,
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ValidationRunUi {
    pub output_dir: String,
    pub start_equity: Option<f64>,
    pub end_equity: Option<f64>,
    pub pnl: Option<f64>,
    pub pnl_ratio: Option<f64>,
    pub max_drawdown: Option<f64>,
    pub sharpe: Option<f64>,
    pub sortino: Option<f64>,
    pub calmar: Option<f64>,
    pub trades: Option<usize>,
    pub rejections: Option<usize>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RouteDecisionUi {
    pub output_dir: String,
    pub baseline_score: Option<f64>,
    pub candidate_score: Option<f64>,
    pub baseline_pnl_ratio: Option<f64>,
    pub candidate_pnl_ratio: Option<f64>,
    pub baseline_max_drawdown: Option<f64>,
    pub candidate_max_drawdown: Option<f64>,
    pub decision: String,
    pub targets_updated: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ReadinessReportCompat {
    #[serde(default)]
    history_days: usize,
    #[serde(default)]
    readiness_tier: String,
    #[serde(default)]
    notes: Vec<String>,
    #[serde(default)]
    data_quality: ReadinessDataQualityCompat,
    #[serde(default)]
    oos: Option<ReadinessOosCompat>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ReadinessDataQualityCompat {
    #[serde(default)]
    rows: Vec<ReadinessDataQualityRowCompat>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ReadinessDataQualityRowCompat {
    #[serde(default)]
    status: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ReadinessOosCompat {
    #[serde(default)]
    test_start: String,
    #[serde(default)]
    test_end: String,
    #[serde(default)]
    pnl_ratio: f64,
    #[serde(default)]
    max_drawdown: f64,
    #[serde(default)]
    sharpe: f64,
    #[serde(default)]
    trades: usize,
    #[serde(default)]
    rejections: usize,
}

pub fn load_validation_snapshot(root: &Path) -> ValidationSnapshot {
    ValidationSnapshot {
        full_real_window: load_readiness_snapshot(&root.join("readiness_real_new")),
        recent_real_window: load_readiness_snapshot(&root.join("readiness_real_recent")),
        us_long_sample: find_latest_prefixed_dir(root, "run_us_long_tuned_")
            .and_then(|dir| load_run_summary_snapshot(&dir)),
        route_decision: load_route_decision_snapshot(&root.join("compare_us_long_route")),
    }
}

fn load_readiness_snapshot(dir: &Path) -> Option<ValidationReadinessUi> {
    let report_path = dir.join("readiness_report.json");
    let text = fs::read_to_string(&report_path).ok()?;
    let report: ReadinessReportCompat = serde_json::from_str(&text).ok()?;
    let pass_markets = report
        .data_quality
        .rows
        .iter()
        .filter(|row| row.status == "PASS")
        .count();
    let warn_markets = report
        .data_quality
        .rows
        .iter()
        .filter(|row| row.status == "WARN")
        .count();
    let fail_markets = report
        .data_quality
        .rows
        .iter()
        .filter(|row| row.status == "FAIL")
        .count();
    let data_quality_status = if fail_markets > 0 {
        "FAIL"
    } else if warn_markets > 0 {
        "WARN"
    } else if pass_markets > 0 {
        "PASS"
    } else {
        "MISSING"
    };
    Some(ValidationReadinessUi {
        output_dir: dir.display().to_string(),
        history_days: report.history_days,
        readiness_tier: report.readiness_tier,
        test_start: report
            .oos
            .as_ref()
            .map(|oos| oos.test_start.clone())
            .unwrap_or_default(),
        test_end: report
            .oos
            .as_ref()
            .map(|oos| oos.test_end.clone())
            .unwrap_or_default(),
        pnl_ratio: report.oos.as_ref().map(|oos| oos.pnl_ratio),
        max_drawdown: report.oos.as_ref().map(|oos| oos.max_drawdown),
        sharpe: report.oos.as_ref().map(|oos| oos.sharpe),
        trades: report.oos.as_ref().map(|oos| oos.trades),
        rejections: report.oos.as_ref().map(|oos| oos.rejections),
        data_quality_status: data_quality_status.to_string(),
        pass_markets,
        warn_markets,
        fail_markets,
        notes: report.notes,
    })
}

fn load_run_summary_snapshot(dir: &Path) -> Option<ValidationRunUi> {
    let map = parse_key_value_file(&dir.join("summary.txt")).ok()?;
    Some(ValidationRunUi {
        output_dir: dir.display().to_string(),
        start_equity: parse_decimal(map.get("start_equity")),
        end_equity: parse_decimal(map.get("end_equity")),
        pnl: parse_decimal(map.get("pnl")),
        pnl_ratio: parse_ratio(map.get("pnl_ratio")),
        max_drawdown: parse_ratio(map.get("max_drawdown")),
        sharpe: parse_decimal(map.get("sharpe")),
        sortino: parse_decimal(map.get("sortino")),
        calmar: parse_decimal(map.get("calmar")),
        trades: parse_usize(map.get("trades")),
        rejections: parse_usize(map.get("rejections")),
    })
}

fn load_route_decision_snapshot(dir: &Path) -> Option<RouteDecisionUi> {
    let map = parse_key_value_file(&dir.join("route_decision_us.txt")).ok()?;
    Some(RouteDecisionUi {
        output_dir: dir.display().to_string(),
        baseline_score: parse_decimal(map.get("baseline_score")),
        candidate_score: parse_decimal(map.get("candidate_score")),
        baseline_pnl_ratio: parse_ratio(map.get("baseline_pnl_ratio")),
        candidate_pnl_ratio: parse_ratio(map.get("candidate_pnl_ratio")),
        baseline_max_drawdown: parse_ratio(map.get("baseline_max_drawdown")),
        candidate_max_drawdown: parse_ratio(map.get("candidate_max_drawdown")),
        decision: map.get("decision").cloned().unwrap_or_default(),
        targets_updated: map.get("targets_updated").cloned().unwrap_or_default(),
    })
}

fn parse_key_value_file(path: &Path) -> std::io::Result<HashMap<String, String>> {
    let text = fs::read_to_string(path)?;
    Ok(text
        .lines()
        .filter_map(|line| {
            let (key, value) = line.split_once('=')?;
            Some((key.trim().to_string(), value.trim().to_string()))
        })
        .collect())
}

fn parse_decimal(value: Option<&String>) -> Option<f64> {
    value.and_then(|raw| raw.trim().parse::<f64>().ok())
}

fn parse_ratio(value: Option<&String>) -> Option<f64> {
    let raw = value?.trim();
    if let Some(percent) = raw.strip_suffix('%') {
        percent.trim().parse::<f64>().ok().map(|v| v / 100.0)
    } else {
        raw.parse::<f64>().ok()
    }
}

fn parse_usize(value: Option<&String>) -> Option<usize> {
    value.and_then(|raw| raw.trim().parse::<usize>().ok())
}

fn find_latest_prefixed_dir(root: &Path, prefix: &str) -> Option<PathBuf> {
    let mut candidates = fs::read_dir(root)
        .ok()?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_dir()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(|name| name.starts_with(prefix))
                    .unwrap_or(false)
                && path.join("summary.txt").exists()
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|path| {
        std::cmp::Reverse(
            fs::metadata(path)
                .and_then(|meta| meta.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH),
        )
    });
    candidates.into_iter().next()
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::load_validation_snapshot;

    #[test]
    fn loads_readiness_run_and_route_snapshots() {
        let root = std::env::temp_dir().join(format!(
            "pqbot_validation_snapshot_{}",
            std::process::id()
        ));
        if root.exists() {
            fs::remove_dir_all(&root).ok();
        }
        fs::create_dir_all(root.join("readiness_real_new")).expect("mkdir readiness");
        fs::create_dir_all(root.join("readiness_real_recent")).expect("mkdir readiness recent");
        fs::create_dir_all(root.join("run_us_long_tuned_20260416")).expect("mkdir run");
        fs::create_dir_all(root.join("compare_us_long_route")).expect("mkdir route");

        fs::write(
            root.join("readiness_real_new").join("readiness_report.json"),
            r#"{
  "history_days": 585,
  "readiness_tier": "WATCH_ONLY",
  "notes": ["network_disabled"],
  "data_quality": { "rows": [{"status":"PASS"},{"status":"PASS"},{"status":"PASS"}] },
  "oos": {
    "test_start": "2025-07-30",
    "test_end": "2026-04-01",
    "pnl_ratio": 0.0555,
    "max_drawdown": 0.0958,
    "sharpe": 0.7782,
    "trades": 645,
    "rejections": 102
  }
}"#,
        )
        .expect("write readiness");
        fs::write(
            root.join("readiness_real_recent")
                .join("readiness_report.json"),
            r#"{
  "history_days": 261,
  "readiness_tier": "WATCH_ONLY",
  "notes": ["data_quality_warn"],
  "data_quality": { "rows": [{"status":"WARN"},{"status":"PASS"},{"status":"PASS"}] },
  "oos": {
    "test_start": "2025-10-01",
    "test_end": "2026-04-01",
    "pnl_ratio": -0.0374,
    "max_drawdown": 0.0932,
    "sharpe": -1.1690,
    "trades": 120,
    "rejections": 12
  }
}"#,
        )
        .expect("write readiness recent");
        fs::write(
            root.join("run_us_long_tuned_20260416").join("summary.txt"),
            "start_equity=1000000.00\nend_equity=1003261.45\npnl=3261.45\npnl_ratio=0.3261%\nmax_drawdown=26.1649%\nsharpe=0.0591\ntrades=4016\nrejections=1891\n",
        )
        .expect("write run summary");
        fs::write(
            root.join("compare_us_long_route")
                .join("route_decision_us.txt"),
            "baseline_score=0.046679\ncandidate_score=-0.220583\nbaseline_pnl_ratio=0.075109\ncandidate_pnl_ratio=0.003261\nbaseline_max_drawdown=0.081453\ncandidate_max_drawdown=0.261649\ndecision=keep_current_defaults\ntargets_updated=\n",
        )
        .expect("write route");

        let snapshot = load_validation_snapshot(&root);
        assert_eq!(
            snapshot
                .full_real_window
                .as_ref()
                .expect("full readiness")
                .data_quality_status,
            "PASS"
        );
        assert_eq!(
            snapshot
                .recent_real_window
                .as_ref()
                .expect("recent readiness")
                .data_quality_status,
            "WARN"
        );
        assert_eq!(
            snapshot
                .route_decision
                .as_ref()
                .expect("route")
                .decision,
            "keep_current_defaults"
        );
        assert!(
            snapshot
                .us_long_sample
                .as_ref()
                .expect("long sample")
                .pnl_ratio
                .unwrap_or_default()
                > 0.003
        );
    }
}
