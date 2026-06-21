use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use serde::Serialize;

use crate::{
    config::load_config,
    data::CsvDataPortal,
    data_quality::{run_data_quality_check, DataQualityReport, DataQualityRequest},
    doctor::{run_doctor, DoctorReport},
    engine::{summarize_result, QuantBotEngine},
    safety::{is_ibkr_paper_allowed, is_network_allowed, is_trading_kill_switch_armed},
};

#[derive(Debug, Clone)]
pub struct ReadinessRequest {
    pub config_path: PathBuf,
    pub output_dir: PathBuf,
    pub train_ratio: f64,
    pub min_history_days: usize,
    pub min_oos_days: usize,
    pub return_outlier_threshold: f64,
    pub gap_days_threshold: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReadinessOosSummary {
    pub train_start: String,
    pub train_end: String,
    pub test_start: String,
    pub test_end: String,
    pub train_days: usize,
    pub test_days: usize,
    pub pnl_ratio: f64,
    pub max_drawdown: f64,
    pub sharpe: f64,
    pub sortino: f64,
    pub calmar: f64,
    pub daily_win_rate: f64,
    pub profit_factor: f64,
    pub trades: usize,
    pub rejections: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReadinessReport {
    pub config_path: PathBuf,
    pub doctor: DoctorReport,
    pub data_quality: DataQualityReport,
    pub history_days: usize,
    pub kill_switch_armed: bool,
    pub network_allowed: bool,
    pub ibkr_paper_allowed: bool,
    pub train_ratio: f64,
    pub min_history_days: usize,
    pub min_oos_days: usize,
    pub oos: Option<ReadinessOosSummary>,
    pub readiness_tier: String,
    pub pilot_capital_ratio: f64,
    pub notes: Vec<String>,
}

pub fn run_readiness(req: &ReadinessRequest) -> Result<ReadinessReport> {
    if !(0.0..1.0).contains(&req.train_ratio) {
        return Err(anyhow!("train_ratio must be in (0, 1)"));
    }
    if req.min_history_days == 0 {
        return Err(anyhow!("min_history_days must be > 0"));
    }
    if req.min_oos_days == 0 {
        return Err(anyhow!("min_oos_days must be > 0"));
    }
    if req.return_outlier_threshold <= 0.0 {
        return Err(anyhow!("return_outlier_threshold must be > 0"));
    }
    if req.gap_days_threshold <= 0 {
        return Err(anyhow!("gap_days_threshold must be > 0"));
    }

    let cfg = load_config(&req.config_path)?;
    let market_files = cfg
        .markets
        .values()
        .map(|m| (m.name.clone(), m.data_file.clone()))
        .collect::<Vec<_>>();
    let data = CsvDataPortal::new(market_files).context("load market csv data failed")?;
    let debug_readiness = std::env::var("PQBOT_DEBUG_READINESS").is_ok();
    if debug_readiness {
        eprintln!("[readiness] loaded data portal");
    }
    if debug_readiness {
        eprintln!("[readiness] running doctor");
    }
    let doctor = run_doctor(&crate::doctor::DoctorRequest {
        config_path: req.config_path.clone(),
    })?;
    if debug_readiness {
        eprintln!("[readiness] running data quality");
    }
    let data_quality = run_data_quality_check(
        &cfg,
        &DataQualityRequest {
            return_outlier_threshold: req.return_outlier_threshold,
            gap_days_threshold: req.gap_days_threshold,
        },
        &req.output_dir,
    )?;
    if debug_readiness {
        eprintln!("[readiness] evaluating oos gate");
    }

    let history_days = data.trading_dates().len();
    let kill_switch_armed = is_trading_kill_switch_armed();
    let network_allowed = is_network_allowed();
    let ibkr_paper_allowed = is_ibkr_paper_allowed();

    let mut notes = Vec::new();
    if kill_switch_armed {
        notes.push("kill_switch_armed".to_string());
    }
    if history_days < req.min_history_days {
        notes.push(format!(
            "history_too_short:{}<{}",
            history_days, req.min_history_days
        ));
    }
    if data_quality.rows.iter().any(|row| row.status == "FAIL") {
        notes.push("data_quality_fail".to_string());
    }
    if data_quality.rows.iter().any(|row| row.status == "WARN") {
        notes.push("data_quality_warn".to_string());
    }
    if history_days < req.min_history_days || data_quality.rows.is_empty() {
        notes.push("oos_proxy_only".to_string());
    }
    if !network_allowed {
        notes.push("network_disabled".to_string());
    }
    if !ibkr_paper_allowed && cfg.broker.mode == "ibkr_paper" {
        notes.push("ibkr_paper_not_opted_in".to_string());
    }

    let oos = if !kill_switch_armed
        && history_days >= req.min_history_days
        && history_days >= req.min_oos_days + 2
    {
        if debug_readiness {
            eprintln!("[readiness] running oos split");
        }
        Some(run_oos_check(&cfg, &data, req.train_ratio)?)
    } else {
        None
    };
    if debug_readiness {
        eprintln!("[readiness] computing tier");
    }

    let (readiness_tier, pilot_capital_ratio) =
        readiness_tier_and_pilot_ratio(&cfg, history_days, &data_quality, oos.as_ref());

    let report = ReadinessReport {
        config_path: req.config_path.clone(),
        doctor,
        data_quality,
        history_days,
        kill_switch_armed,
        network_allowed,
        ibkr_paper_allowed,
        train_ratio: req.train_ratio,
        min_history_days: req.min_history_days,
        min_oos_days: req.min_oos_days,
        oos,
        readiness_tier,
        pilot_capital_ratio,
        notes,
    };

    write_report(&req.output_dir, &report)?;
    Ok(report)
}

fn run_oos_check(
    cfg: &crate::config::BotConfig,
    data: &CsvDataPortal,
    train_ratio: f64,
) -> Result<ReadinessOosSummary> {
    let dates = data.trading_dates();
    if dates.len() < 2 {
        return Err(anyhow!("not enough dates for readiness OOS check"));
    }

    let mut train_days = ((dates.len() as f64) * train_ratio).round() as usize;
    train_days = train_days.clamp(1, dates.len() - 1);
    let test_days = dates.len() - train_days;
    if test_days == 0 {
        return Err(anyhow!("readiness OOS split produced zero test days"));
    }

    let train_slice = &dates[..train_days];
    let test_slice = &dates[train_days..];
    let warmup = cfg.strategy.long_window.max(cfg.strategy.vol_window + 1);
    let warmup_start = train_slice.len().saturating_sub(warmup);

    let mut combined_dates = Vec::new();
    combined_dates.extend_from_slice(&train_slice[warmup_start..]);
    combined_dates.extend_from_slice(test_slice);

    let combined_data = data.slice_by_dates(&combined_dates);
    let result = QuantBotEngine::from_config_force_sim(cfg.clone(), combined_data).run();
    let test_set: HashSet<NaiveDate> = test_slice.iter().copied().collect();
    let test_result = crate::engine::RunResult {
        equity_curve: result
            .equity_curve
            .into_iter()
            .filter(|p| test_set.contains(&p.date))
            .collect(),
        trades: result
            .trades
            .into_iter()
            .filter(|t| test_set.contains(&t.date))
            .collect(),
        rejections: result
            .rejections
            .into_iter()
            .filter(|r| test_set.contains(&r.date))
            .collect(),
    };
    let stats = summarize_result(&test_result);

    Ok(ReadinessOosSummary {
        train_start: train_slice[0].to_string(),
        train_end: train_slice[train_slice.len() - 1].to_string(),
        test_start: test_slice[0].to_string(),
        test_end: test_slice[test_slice.len() - 1].to_string(),
        train_days,
        test_days,
        pnl_ratio: stats.pnl_ratio,
        max_drawdown: stats.max_drawdown,
        sharpe: stats.sharpe,
        sortino: stats.sortino,
        calmar: stats.calmar,
        daily_win_rate: stats.daily_win_rate,
        profit_factor: stats.profit_factor,
        trades: stats.trades,
        rejections: stats.rejections,
    })
}

fn readiness_tier_and_pilot_ratio(
    cfg: &crate::config::BotConfig,
    history_days: usize,
    data_quality: &DataQualityReport,
    oos: Option<&ReadinessOosSummary>,
) -> (String, f64) {
    if is_trading_kill_switch_armed() {
        return ("BLOCKED_BY_KILL_SWITCH".to_string(), 0.0);
    }

    if history_days < 2 {
        return ("NOT_READY".to_string(), 0.0);
    }

    let has_fail = data_quality.rows.iter().any(|row| row.status == "FAIL");
    if has_fail {
        return ("NOT_READY".to_string(), 0.0);
    }

    let Some(oos) = oos else {
        return ("WATCH_ONLY".to_string(), 0.0);
    };

    if oos.trades == 0 {
        return ("WATCH_ONLY".to_string(), 0.0);
    }

    let data_quality_clean = data_quality.rows.iter().all(|r| r.status == "PASS");
    let pilot_ready = oos.sharpe >= 2.0
        && oos.max_drawdown <= 0.04
        && oos.pnl_ratio > 0.0
        && oos.profit_factor >= 1.20
        && oos.trades >= 120
        && oos.test_days >= 90
        && data_quality_clean;
    let paper_ready = oos.sharpe >= 1.25
        && oos.max_drawdown <= 0.08
        && oos.pnl_ratio > 0.0
        && oos.profit_factor >= 1.05
        && oos.trades >= 60;

    if pilot_ready {
        let pilot_ratio = (cfg.risk.max_gross_exposure_ratio * 0.01).clamp(0.0025, 0.005);
        return ("SMALL_PILOT_READY".to_string(), pilot_ratio);
    }
    if paper_ready {
        return ("PAPER_READY".to_string(), 0.0);
    }
    ("WATCH_ONLY".to_string(), 0.0)
}

fn write_report(output_dir: impl AsRef<Path>, report: &ReadinessReport) -> Result<()> {
    let dir = output_dir.as_ref();
    fs::create_dir_all(dir)?;
    fs::write(
        dir.join("readiness_report.json"),
        serde_json::to_string_pretty(report)?,
    )?;

    let mut lines = Vec::new();
    lines.push(format!("tier={}", report.readiness_tier));
    lines.push(format!("history_days={}", report.history_days));
    lines.push(format!("kill_switch_armed={}", report.kill_switch_armed));
    lines.push(format!("network_allowed={}", report.network_allowed));
    lines.push(format!("ibkr_paper_allowed={}", report.ibkr_paper_allowed));
    lines.push(format!(
        "data_quality_markets={}",
        report.data_quality.rows.len()
    ));
    lines.push(format!(
        "data_quality_fail_markets={}",
        report
            .data_quality
            .rows
            .iter()
            .filter(|r| r.status == "FAIL")
            .count()
    ));
    lines.push(format!(
        "data_quality_warn_markets={}",
        report
            .data_quality
            .rows
            .iter()
            .filter(|r| r.status == "WARN")
            .count()
    ));
    if let Some(oos) = &report.oos {
        lines.push(format!("oos_train_days={}", oos.train_days));
        lines.push(format!("oos_test_days={}", oos.test_days));
        lines.push(format!("oos_pnl_ratio={:.4}%", oos.pnl_ratio * 100.0));
        lines.push(format!("oos_max_drawdown={:.4}%", oos.max_drawdown * 100.0));
        lines.push(format!("oos_sharpe={:.4}", oos.sharpe));
        lines.push(format!("oos_sortino={:.4}", oos.sortino));
        lines.push(format!("oos_calmar={:.4}", oos.calmar));
        lines.push(format!(
            "oos_daily_win_rate={:.4}%",
            oos.daily_win_rate * 100.0
        ));
        lines.push(format!("oos_profit_factor={:.4}", oos.profit_factor));
        lines.push(format!("oos_trades={}", oos.trades));
        lines.push(format!("oos_rejections={}", oos.rejections));
    } else {
        lines.push("oos=unavailable".to_string());
    }
    lines.push(format!(
        "pilot_capital_ratio={:.4}%",
        report.pilot_capital_ratio * 100.0
    ));
    if !report.notes.is_empty() {
        lines.push(format!("notes={}", report.notes.join(",")));
    }
    fs::write(dir.join("readiness_summary.txt"), lines.join("\n") + "\n")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{path::Path, path::PathBuf};

    use crate::data_quality::{DataQualityReport, DataQualityRow};

    use super::{
        readiness_tier_and_pilot_ratio, run_readiness, ReadinessOosSummary, ReadinessRequest,
    };

    #[test]
    fn readiness_report_writes_summary() {
        let report = run_readiness(&ReadinessRequest {
            config_path: PathBuf::from("config/bot.toml"),
            output_dir: PathBuf::from("outputs_rust/test_readiness"),
            train_ratio: 0.6,
            min_history_days: 10,
            min_oos_days: 2,
            return_outlier_threshold: 0.35,
            gap_days_threshold: 10,
        })
        .expect("readiness");
        assert!(!report.readiness_tier.is_empty());
    }

    #[test]
    fn readiness_real_history_smoke_if_present() {
        let config_path = Path::new("config/bot_real.toml");
        if !config_path.is_file() {
            return;
        }
        let report = run_readiness(&ReadinessRequest {
            config_path: config_path.to_path_buf(),
            output_dir: PathBuf::from("outputs_rust/test_readiness_real_smoke"),
            train_ratio: 0.70,
            min_history_days: 252,
            min_oos_days: 60,
            return_outlier_threshold: 0.35,
            gap_days_threshold: 10,
        })
        .expect("real-history readiness");
        assert_eq!(report.readiness_tier, "WATCH_ONLY");
        assert!(report.history_days >= 252);
        assert!(report.oos.is_some());
    }

    #[test]
    fn pilot_ready_requires_clean_data_quality() {
        let cfg = crate::config::load_config("config/bot.toml").expect("load config");
        let oos = ReadinessOosSummary {
            train_start: "2025-01-01".to_string(),
            train_end: "2025-06-30".to_string(),
            test_start: "2025-07-01".to_string(),
            test_end: "2025-09-30".to_string(),
            train_days: 120,
            test_days: 120,
            pnl_ratio: 0.25,
            max_drawdown: 0.02,
            sharpe: 3.0,
            sortino: 4.0,
            calmar: 8.0,
            daily_win_rate: 0.55,
            profit_factor: 1.4,
            trades: 200,
            rejections: 5,
        };
        let clean = DataQualityReport {
            rows: vec![DataQualityRow {
                market: "US".to_string(),
                rows: 100,
                unique_symbols: 12,
                duplicate_rows: 0,
                invalid_close_rows: 0,
                invalid_volume_rows: 0,
                date_order_violations: 0,
                return_outliers: 0,
                large_gaps: 0,
                non_trading_day_rows: 0,
                status: "PASS".to_string(),
            }],
        };
        let warn = DataQualityReport {
            rows: vec![DataQualityRow {
                status: "WARN".to_string(),
                ..clean.rows[0].clone()
            }],
        };

        let (tier_clean, pilot_ratio_clean) =
            readiness_tier_and_pilot_ratio(&cfg, 600, &clean, Some(&oos));
        assert_eq!(tier_clean, "SMALL_PILOT_READY");
        assert!(pilot_ratio_clean > 0.0);

        let (tier_warn, pilot_ratio_warn) =
            readiness_tier_and_pilot_ratio(&cfg, 600, &warn, Some(&oos));
        assert_ne!(tier_warn, "SMALL_PILOT_READY");
        assert_eq!(pilot_ratio_warn, 0.0);
    }
}
