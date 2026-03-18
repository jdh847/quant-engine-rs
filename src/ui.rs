use std::{
    fs,
    path::{Path, PathBuf},
    time::SystemTime,
};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::i18n::{dashboard_text, DashboardText, Language};
use crate::registry::{infer_registry_root, read_run_registry, RunRegistryEntry};

#[derive(Debug, Serialize)]
struct EquityRow {
    date: String,
    equity: f64,
    cash: f64,
    gross_exposure: f64,
    net_exposure: f64,
}

#[derive(Debug, Clone, Serialize)]
struct DataQualityRowUi {
    market: String,
    rows: i64,
    unique_symbols: i64,
    duplicate_rows: i64,
    invalid_close_rows: i64,
    invalid_volume_rows: i64,
    date_order_violations: i64,
    return_outliers: i64,
    large_gaps: i64,
    non_trading_day_rows: i64,
    status: String,
    issues: String,
}

#[derive(Debug, Clone, Serialize)]
struct AuditMarketUi {
    market: String,
    currency: String,
    fx_to_base: f64,
    data_file: String,
    data_sha256: String,
    industry_file: String,
    industry_sha256: String,
    holiday_file: String,
    holiday_sha256: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct AuditSnapshotCompat {
    #[serde(default)]
    config_sha256: String,
    #[serde(default)]
    markets: Vec<AuditMarketCompat>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct AuditMarketCompat {
    #[serde(default)]
    market: String,
    #[serde(default)]
    currency: String,
    #[serde(default)]
    fx_to_base: f64,
    #[serde(default)]
    data_file: AuditFileCompat,
    #[serde(default)]
    industry_file: Option<AuditFileCompat>,
    #[serde(default)]
    holiday_file: Option<AuditFileCompat>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct AuditFileCompat {
    #[serde(default)]
    path: String,
    #[serde(default)]
    sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FactorDecayRowUi {
    scope: String,
    factor: String,
    horizon_days: usize,
    observations: usize,
    ic: f64,
    top_quintile_avg_return: f64,
    bottom_quintile_avg_return: f64,
    long_short_spread: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RollingIcRowUi {
    date: String,
    factor: String,
    horizon_days: usize,
    observations: usize,
    ic: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RegimeSplitRowUi {
    market: String,
    regime_bucket: String,
    observations: usize,
    avg_factor_momentum: f64,
    avg_factor_mean_reversion: f64,
    avg_factor_low_vol: f64,
    avg_factor_volume: f64,
    avg_composite_alpha: f64,
    avg_selected_symbols: f64,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ResearchReportCompat {
    #[serde(default)]
    regime_rows: Vec<RegimeSplitRowUi>,
    #[serde(default)]
    factor_decay_rows: Vec<FactorDecayRowUi>,
    #[serde(default)]
    rolling_ic_rows: Vec<RollingIcRowUi>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct CompareReportCompat {
    #[serde(default)]
    baseline_dir: String,
    #[serde(default)]
    candidate_dir: String,
    #[serde(default)]
    metric_rows: Vec<CompareFieldCompat>,
    #[serde(default)]
    audit_rows: Vec<CompareFieldCompat>,
    #[serde(default)]
    data_quality_rows: Vec<CompareFieldCompat>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct CompareFieldCompat {
    #[serde(default)]
    changed: bool,
}

#[derive(Debug, Clone, Serialize)]
struct RecentCompareUi {
    output_dir: String,
    html_href: String,
    json_href: String,
    baseline_dir: String,
    candidate_dir: String,
    metric_changes: usize,
    audit_changes: usize,
    data_quality_changes: usize,
}

#[derive(Debug, Serialize)]
struct TradeRow {
    date: String,
    market: String,
    symbol: String,
    side: String,
    qty: i64,
    price: f64,
    fees: f64,
}

#[derive(Debug, Serialize)]
struct RejectionRow {
    date: String,
    market: String,
    symbol: String,
    side: String,
    qty: i64,
    reason: String,
}

#[derive(Debug, Clone, Serialize)]
struct StrategyCompareRowUi {
    strategy_plugin: String,
    portfolio_method: String,
    runs: usize,
    avg_score: f64,
    best_score: f64,
    avg_pnl_ratio: f64,
    avg_max_drawdown: f64,
    avg_sharpe: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaderboardRowUi {
    rank: usize,
    source: String,
    timestamp_utc: String,
    command: String,
    scenario: String,
    strategy_plugin: String,
    portfolio_method: String,
    score: f64,
    pnl_ratio: f64,
    max_drawdown: f64,
    sharpe: f64,
    notes: String,
}

#[derive(Debug, Serialize)]
struct DashboardI18n {
    en: DashboardI18nText,
    #[serde(rename = "zh-CN")]
    zh_cn: DashboardI18nText,
    #[serde(rename = "ja")]
    ja: DashboardI18nText,
}

#[derive(Debug, Serialize)]
struct DashboardI18nText {
    title: String,
    subtitle: String,
    generated_from: String,
    overview: String,
    series: String,
    equity: String,
    cash: String,
    gross_exposure: String,
    net_exposure: String,
    equity_curve: String,
    run_summary: String,
    audit: String,
    data_quality: String,
    status: String,
    rows: String,
    issues: String,
    data_file: String,
    industry_file: String,
    holiday_file: String,
    sha256: String,
    kpi_start_equity: String,
    kpi_end_equity: String,
    kpi_pnl: String,
    kpi_pnl_ratio: String,
    kpi_max_drawdown: String,
    kpi_cagr: String,
    kpi_sharpe: String,
    kpi_trades: String,
    kpi_rejections: String,
    recent_trades: String,
    filters: String,
    all: String,
    search: String,
    date: String,
    market: String,
    symbol: String,
    side: String,
    qty: String,
    price: String,
    fees: String,
    runs_label: String,
    rejections: String,
    reason: String,
    factors: String,
    strategy_comparison: String,
    public_leaderboard: String,
    plugin: String,
    method: String,
    source: String,
    time_range: String,
    all_time: String,
    recent_7d: String,
    recent_30d: String,
    scenario: String,
    score: String,
    command_label: String,
    rank: String,
    avg_score: String,
    best_score: String,
    avg_pnl_short: String,
    avg_sharpe: String,
    top_runs: String,
    composite_score: String,
    latest_vs_selected: String,
    current_label: String,
    selected_label: String,
    delta_label: String,
    run_details: String,
    selected_combo: String,
    selected_entry: String,
    time_label: String,
    notes_label: String,
    compare_runs: String,
    baseline_run: String,
    candidate_run: String,
    output_dir_label: String,
    copy_command_label: String,
    compare_hint: String,
    compare_needs_two_runs: String,
    recent_compare: String,
    open_report_html: String,
    open_report_json: String,
    metric_changes_label: String,
    audit_changes_label: String,
    data_quality_changes_label: String,
    compare_not_found: String,
    research: String,
    decay_overview: String,
    rolling_ic: String,
    regime_split: String,
    folds: String,
    avg_test_sharpe_short: String,
    best_decay: String,
    latest_rolling: String,
    horizon_days: String,
    ic_short: String,
    spread: String,
    scope: String,
    metric: String,
    observations: String,
    composite_alpha: String,
    regime_bucket: String,
    avg_selected_symbols: String,
    start: String,
    end: String,
    buy: String,
    sell: String,
    live_on: String,
    live_fallback: String,
    live_init: String,
}

fn i18n_text(t: DashboardText) -> DashboardI18nText {
    DashboardI18nText {
        title: t.title.to_string(),
        subtitle: t.subtitle.to_string(),
        generated_from: t.generated_from.to_string(),
        overview: t.overview.to_string(),
        series: t.series.to_string(),
        equity: t.equity.to_string(),
        cash: t.cash.to_string(),
        gross_exposure: t.gross_exposure.to_string(),
        net_exposure: t.net_exposure.to_string(),
        equity_curve: t.equity_curve.to_string(),
        run_summary: t.run_summary.to_string(),
        audit: t.audit.to_string(),
        data_quality: t.data_quality.to_string(),
        status: t.status.to_string(),
        rows: t.rows.to_string(),
        issues: t.issues.to_string(),
        data_file: t.data_file.to_string(),
        industry_file: t.industry_file.to_string(),
        holiday_file: t.holiday_file.to_string(),
        sha256: t.sha256.to_string(),
        kpi_start_equity: t.kpi_start_equity.to_string(),
        kpi_end_equity: t.kpi_end_equity.to_string(),
        kpi_pnl: t.kpi_pnl.to_string(),
        kpi_pnl_ratio: t.kpi_pnl_ratio.to_string(),
        kpi_max_drawdown: t.kpi_max_drawdown.to_string(),
        kpi_cagr: t.kpi_cagr.to_string(),
        kpi_sharpe: t.kpi_sharpe.to_string(),
        kpi_trades: t.kpi_trades.to_string(),
        kpi_rejections: t.kpi_rejections.to_string(),
        recent_trades: t.recent_trades.to_string(),
        filters: t.filters.to_string(),
        all: t.all.to_string(),
        search: t.search.to_string(),
        date: t.date.to_string(),
        market: t.market.to_string(),
        symbol: t.symbol.to_string(),
        side: t.side.to_string(),
        qty: t.qty.to_string(),
        price: t.price.to_string(),
        fees: t.fees.to_string(),
        runs_label: t.runs_label.to_string(),
        rejections: t.rejections.to_string(),
        reason: t.reason.to_string(),
        factors: t.factors.to_string(),
        strategy_comparison: t.strategy_comparison.to_string(),
        public_leaderboard: t.public_leaderboard.to_string(),
        plugin: t.plugin.to_string(),
        method: t.method.to_string(),
        source: t.source.to_string(),
        time_range: t.time_range.to_string(),
        all_time: t.all_time.to_string(),
        recent_7d: t.recent_7d.to_string(),
        recent_30d: t.recent_30d.to_string(),
        scenario: t.scenario.to_string(),
        score: t.score.to_string(),
        command_label: t.command_label.to_string(),
        rank: t.rank.to_string(),
        avg_score: t.avg_score.to_string(),
        best_score: t.best_score.to_string(),
        avg_pnl_short: t.avg_pnl_short.to_string(),
        avg_sharpe: t.avg_sharpe.to_string(),
        top_runs: t.top_runs.to_string(),
        composite_score: t.composite_score.to_string(),
        latest_vs_selected: t.latest_vs_selected.to_string(),
        current_label: t.current_label.to_string(),
        selected_label: t.selected_label.to_string(),
        delta_label: t.delta_label.to_string(),
        run_details: t.run_details.to_string(),
        selected_combo: t.selected_combo.to_string(),
        selected_entry: t.selected_entry.to_string(),
        time_label: t.time_label.to_string(),
        notes_label: t.notes_label.to_string(),
        compare_runs: t.compare_runs.to_string(),
        baseline_run: t.baseline_run.to_string(),
        candidate_run: t.candidate_run.to_string(),
        output_dir_label: t.output_dir_label.to_string(),
        copy_command_label: t.copy_command_label.to_string(),
        compare_hint: t.compare_hint.to_string(),
        compare_needs_two_runs: t.compare_needs_two_runs.to_string(),
        recent_compare: t.recent_compare.to_string(),
        open_report_html: t.open_report_html.to_string(),
        open_report_json: t.open_report_json.to_string(),
        metric_changes_label: t.metric_changes_label.to_string(),
        audit_changes_label: t.audit_changes_label.to_string(),
        data_quality_changes_label: t.data_quality_changes_label.to_string(),
        compare_not_found: t.compare_not_found.to_string(),
        research: t.research.to_string(),
        decay_overview: t.decay_overview.to_string(),
        rolling_ic: t.rolling_ic.to_string(),
        regime_split: t.regime_split.to_string(),
        folds: t.folds.to_string(),
        avg_test_sharpe_short: t.avg_test_sharpe_short.to_string(),
        best_decay: t.best_decay.to_string(),
        latest_rolling: t.latest_rolling.to_string(),
        horizon_days: t.horizon_days.to_string(),
        ic_short: t.ic_short.to_string(),
        spread: t.spread.to_string(),
        scope: t.scope.to_string(),
        metric: t.metric.to_string(),
        observations: t.observations.to_string(),
        composite_alpha: t.composite_alpha.to_string(),
        regime_bucket: t.regime_bucket.to_string(),
        avg_selected_symbols: t.avg_selected_symbols.to_string(),
        start: t.start.to_string(),
        end: t.end.to_string(),
        buy: t.buy.to_string(),
        sell: t.sell.to_string(),
        live_on: t.live_on.to_string(),
        live_fallback: t.live_fallback.to_string(),
        live_init: t.live_init.to_string(),
    }
}

pub fn build_dashboard(output_dir: impl AsRef<Path>) -> Result<PathBuf> {
    build_dashboard_with_language(output_dir, Language::En)
}

pub fn build_dashboard_with_language(
    output_dir: impl AsRef<Path>,
    language: Language,
) -> Result<PathBuf> {
    let output_dir = output_dir.as_ref();
    let summary_path = output_dir.join("summary.txt");
    let equity_path = output_dir.join("equity_curve.csv");
    let trades_path = output_dir.join("trades.csv");
    let rejections_path = output_dir.join("rejections.csv");
    let factor_summary_path = output_dir.join("factor_attribution_summary.txt");
    let audit_summary_path = output_dir.join("audit_snapshot_summary.txt");
    let registry_root = infer_registry_root(output_dir);
    let registry_path = registry_root.join("run_registry.csv");
    let leaderboard_path = registry_root
        .join("leaderboard")
        .join("leaderboard_public.csv");
    let registry_refresh_path = if registry_root == output_dir {
        "./run_registry.csv".to_string()
    } else {
        "../run_registry.csv".to_string()
    };
    let leaderboard_refresh_path = if registry_root == output_dir {
        "./leaderboard/leaderboard_public.csv".to_string()
    } else {
        "../leaderboard/leaderboard_public.csv".to_string()
    };
    let research_summary_path = if output_dir.join("research_report_summary.txt").exists() {
        output_dir.join("research_report_summary.txt")
    } else if output_dir
        .join("research_report")
        .join("research_report_summary.txt")
        .exists()
    {
        output_dir
            .join("research_report")
            .join("research_report_summary.txt")
    } else {
        output_dir.join("research_report_summary.txt")
    };
    let research_json_path = if output_dir.join("research_report.json").exists() {
        output_dir.join("research_report.json")
    } else if output_dir
        .join("research_report")
        .join("research_report.json")
        .exists()
    {
        output_dir
            .join("research_report")
            .join("research_report.json")
    } else {
        output_dir.join("research_report.json")
    };
    let data_quality_summary_path = if output_dir.join("data_quality_summary.txt").exists() {
        output_dir.join("data_quality_summary.txt")
    } else if output_dir
        .join("data_quality")
        .join("data_quality_summary.txt")
        .exists()
    {
        output_dir
            .join("data_quality")
            .join("data_quality_summary.txt")
    } else {
        output_dir.join("data_quality_summary.txt")
    };
    let data_quality_report_path = if output_dir.join("data_quality_report.csv").exists() {
        output_dir.join("data_quality_report.csv")
    } else if output_dir
        .join("data_quality")
        .join("data_quality_report.csv")
        .exists()
    {
        output_dir
            .join("data_quality")
            .join("data_quality_report.csv")
    } else {
        output_dir.join("data_quality_report.csv")
    };
    let audit_json_path = output_dir.join("audit_snapshot.json");

    let summary = fs::read_to_string(summary_path).unwrap_or_else(|_| "no summary".to_string());
    let summary_html = escape_html(&summary);
    let summary_kv = parse_kv_lines(&summary);
    let equity_rows = read_equity_rows(&equity_path)?;
    let trade_rows = read_trade_rows(&trades_path)?;
    let rejection_rows = read_rejection_rows(&rejections_path)?;
    let factor_summary = fs::read_to_string(&factor_summary_path).unwrap_or_else(|_| String::new());
    let factor_kv = parse_kv_lines(&factor_summary);
    let research_summary =
        fs::read_to_string(&research_summary_path).unwrap_or_else(|_| String::new());
    let research_summary_html = escape_html(&research_summary);
    let research_summary_kv = parse_kv_lines(&research_summary);
    let audit_summary =
        fs::read_to_string(&audit_summary_path).unwrap_or_else(|_| "no audit snapshot".to_string());
    let audit_html = escape_html(&audit_summary);
    let data_quality_summary = fs::read_to_string(&data_quality_summary_path)
        .unwrap_or_else(|_| "no data quality summary".to_string());
    let data_quality_html = escape_html(&data_quality_summary);
    let data_quality_rows = read_data_quality_rows(&data_quality_report_path)?;
    let (audit_config_sha, audit_markets) = read_audit_snapshot(&audit_json_path);
    let (research_regime_rows, research_decay_rows, research_rolling_rows) =
        read_research_report(&research_json_path);
    let registry_rows = read_registry_rows(&registry_path)?;
    let strategy_compare_rows = build_strategy_compare_rows(&registry_rows);
    let leaderboard_rows = read_leaderboard_rows(&leaderboard_path)?;
    let recent_compare = discover_recent_compare(output_dir);

    let trade_json = serde_json::to_string(&trade_rows)?;
    let rejection_json = serde_json::to_string(&rejection_rows)?;
    let equity_rows_json = serde_json::to_string(&equity_rows)?;
    let summary_kv_json = serde_json::to_string(&summary_kv)?;
    let factor_kv_json = serde_json::to_string(&factor_kv)?;
    let research_summary_kv_json = serde_json::to_string(&research_summary_kv)?;
    let registry_rows_json = serde_json::to_string(&registry_rows)?;
    let strategy_compare_json = serde_json::to_string(&strategy_compare_rows)?;
    let leaderboard_rows_json = serde_json::to_string(&leaderboard_rows)?;
    let research_regime_json = serde_json::to_string(&research_regime_rows)?;
    let research_decay_json = serde_json::to_string(&research_decay_rows)?;
    let research_rolling_json = serde_json::to_string(&research_rolling_rows)?;
    let data_quality_json = serde_json::to_string(&data_quality_rows)?;
    let audit_markets_json = serde_json::to_string(&audit_markets)?;
    let audit_config_sha_json = serde_json::to_string(&audit_config_sha)?;
    let recent_compare_json = serde_json::to_string(&recent_compare)?;
    let registry_refresh_path_json = serde_json::to_string(&registry_refresh_path)?;
    let leaderboard_refresh_path_json = serde_json::to_string(&leaderboard_refresh_path)?;
    let text = dashboard_text(language);
    let text_en = dashboard_text(Language::En);
    let text_zh = dashboard_text(Language::Zh);
    let text_ja = dashboard_text(Language::Ja);
    let i18n = DashboardI18n {
        en: i18n_text(text_en),
        zh_cn: i18n_text(text_zh),
        ja: i18n_text(text_ja),
    };
    let i18n_json = serde_json::to_string(&i18n)?;
    let default_lang_json = serde_json::to_string(language.html_lang())?;

    let html = format!(
        r#"<!doctype html>
<html lang="{html_lang}">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Private Quant Bot Dashboard</title>
<style>
:root {{
  --panel: rgba(255,255,255,0.92);
  --panel2: rgba(255,255,255,0.80);
  --ink: #0b1220;
  --muted: rgba(11,18,32,0.60);
  --line: rgba(11,18,32,0.12);
  --accent: #0f766e;
  --accent2: #f59e0b;
  --danger: #b91c1c;
}}
* {{ box-sizing: border-box; }}
body {{
  margin: 0;
  font-family: "Avenir Next", "Avenir", "Helvetica Neue", sans-serif;
  color: var(--ink);
  background:
    radial-gradient(1200px 600px at 12% 10%, rgba(245, 158, 11, 0.20), transparent 55%),
    radial-gradient(900px 520px at 75% 12%, rgba(15, 118, 110, 0.25), transparent 60%),
    radial-gradient(900px 600px at 70% 85%, rgba(16, 185, 129, 0.18), transparent 60%),
    linear-gradient(180deg, #f7fafc 0%, #ecfeff 60%, #f0fdf4 100%);
}}
.wrap {{ max-width: 1200px; margin: 24px auto; padding: 0 16px 24px; }}
.head {{ display: flex; justify-content: space-between; align-items: flex-end; gap: 12px; margin-bottom: 14px; }}
.title {{ font-size: 28px; font-weight: 800; letter-spacing: 0.2px; }}
.sub {{ color: var(--muted); font-size: 14px; }}
.head-right {{ display: flex; align-items: center; gap: 10px; flex-wrap: wrap; justify-content: flex-end; }}
.lang-switch {{ border: 1px solid rgba(15,23,42,0.12); background: rgba(255,255,255,0.95); border-radius: 10px; padding: 6px 10px; font-size: 13px; }}
.chip {{ border: 1px solid rgba(15,23,42,0.12); background: rgba(255,255,255,0.75); border-radius: 999px; padding: 6px 10px; font-size: 12px; }}
.grid {{ display: grid; grid-template-columns: 1.3fr 1fr; gap: 16px; }}
.panel {{
  background: var(--panel);
  border: 1px solid var(--line);
  border-radius: 18px;
  padding: 16px;
  box-shadow: 0 10px 30px rgba(15, 23, 42, 0.06);
  animation: rise 380ms ease both;
}}
.panel[data-delay="1"] {{ animation-delay: 40ms; }}
.panel[data-delay="2"] {{ animation-delay: 80ms; }}
.panel[data-delay="3"] {{ animation-delay: 120ms; }}
.panel[data-delay="4"] {{ animation-delay: 160ms; }}
.panel[data-delay="5"] {{ animation-delay: 200ms; }}
.panel h3 {{ margin: 0 0 12px 0; font-size: 16px; }}
.toolbar {{ display: flex; align-items: center; justify-content: space-between; gap: 10px; margin-bottom: 10px; }}
.select {{ border: 1px solid rgba(15,23,42,0.12); background: rgba(255,255,255,0.90); border-radius: 10px; padding: 6px 10px; font-size: 13px; }}
#chartWrap {{ position: relative; }}
#chart {{ width: 100%; height: 360px; border-radius: 12px; background: linear-gradient(180deg, rgba(236,254,255,0.9) 0%, rgba(255,255,255,0.95) 70%); border: 1px solid rgba(15,23,42,0.10); }}
.tooltip {{
  position: absolute;
  pointer-events: none;
  background: rgba(255,255,255,0.96);
  border: 1px solid rgba(15, 23, 42, 0.14);
  border-radius: 12px;
  padding: 10px 12px;
  box-shadow: 0 12px 40px rgba(15, 23, 42, 0.12);
  font-size: 12px;
  display: none;
}}
.summary {{ white-space: pre-wrap; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; background: rgba(255,255,255,0.65); padding: 10px; border-radius: 10px; border: 1px solid rgba(15, 23, 42, 0.10); }}
.kpis {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }}
.kpi {{ background: var(--panel2); border: 1px solid rgba(15, 23, 42, 0.10); border-radius: 14px; padding: 12px; }}
.kpi .k {{ color: var(--muted); font-size: 12px; }}
.kpi .v {{ font-size: 18px; font-weight: 800; margin-top: 6px; letter-spacing: 0.2px; }}
.kpi .v.negative {{ color: var(--danger); }}
.kpi .v.positive {{ color: var(--accent); }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th, td {{ text-align: left; padding: 8px; border-bottom: 1px solid rgba(15,23,42,0.06); }}
th {{ color: var(--muted); font-weight: 600; }}
.tag-buy {{ color: #065f46; background: #d1fae5; padding: 2px 6px; border-radius: 999px; font-weight: 700; }}
.tag-sell {{ color: #7f1d1d; background: #fee2e2; padding: 2px 6px; border-radius: 999px; font-weight: 700; }}
.pill {{ display: inline-flex; align-items: center; gap: 8px; border-radius: 999px; padding: 6px 10px; border: 1px solid rgba(15,23,42,0.12); background: rgba(255,255,255,0.75); font-size: 12px; }}
.pill.ok {{ background: rgba(209, 250, 229, 0.70); border-color: rgba(6, 95, 70, 0.25); color: #065f46; font-weight: 800; }}
.pill.warn {{ background: rgba(254, 243, 199, 0.85); border-color: rgba(180, 83, 9, 0.25); color: #92400e; font-weight: 800; }}
.pill.bad {{ background: rgba(254, 226, 226, 0.80); border-color: rgba(127, 29, 29, 0.25); color: #7f1d1d; font-weight: 800; }}
.dot {{ width: 8px; height: 8px; border-radius: 999px; background: var(--accent2); }}
.dot.ok {{ background: var(--accent); }}
.filters {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 10px; margin-bottom: 10px; }}
.filters input {{ border: 1px solid rgba(15,23,42,0.12); background: rgba(255,255,255,0.90); border-radius: 10px; padding: 8px 10px; font-size: 13px; }}
.factor-bars {{ display: grid; gap: 8px; }}
.bar-row {{ display: grid; grid-template-columns: 140px 1fr 64px; gap: 10px; align-items: center; }}
.bar-track {{ height: 10px; border-radius: 999px; background: rgba(15,23,42,0.08); overflow: hidden; }}
.bar-fill {{ height: 100%; border-radius: 999px; background: linear-gradient(90deg, rgba(15,118,110,0.95), rgba(245,158,11,0.92)); }}
.mini-grid {{ display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap:10px; }}
.mini-kpi {{ background: var(--panel2); border: 1px solid rgba(15, 23, 42, 0.10); border-radius: 14px; padding: 12px; }}
.mini-kpi .k {{ color: var(--muted); font-size: 12px; }}
.mini-kpi .v {{ font-size: 16px; font-weight: 800; margin-top: 6px; line-height: 1.3; }}
.stack {{ display:grid; gap: 12px; }}
.table-card {{ border: 1px solid rgba(15,23,42,0.08); border-radius: 14px; overflow: hidden; background: rgba(255,255,255,0.7); }}
.table-card table {{ font-size: 12px; }}
.table-card th, .table-card td {{ padding: 7px 8px; }}
.subtle-title {{ color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 8px; }}
.mini-toolbar {{ display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin-bottom:10px; }}
.chart-shell {{ border: 1px solid rgba(15,23,42,0.08); border-radius: 14px; background: rgba(255,255,255,0.7); padding: 10px; min-height: 240px; }}
.chart-shell svg {{ width: 100%; height: 220px; display:block; }}
.regime-grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap:12px; }}
.regime-card {{ padding:14px; border-radius:16px; background:rgba(255,255,255,0.74); border:1px solid rgba(15,23,42,0.08); }}
.regime-title {{ font-size:12px; color:var(--muted); text-transform:uppercase; letter-spacing:.08em; margin-bottom:8px; }}
.regime-main {{ font-size:22px; font-weight:800; margin-bottom:8px; }}
.regime-sub {{ font-size:13px; color:var(--muted); line-height:1.45; }}
.compare-bars {{ display:flex; align-items:flex-end; gap:10px; min-height:220px; padding:8px 0 4px; }}
.compare-col {{ flex:1; min-width:0; display:flex; flex-direction:column; align-items:center; gap:8px; }}
.compare-wrap {{ width:100%; max-width:70px; height:170px; display:flex; align-items:flex-end; }}
.compare-bar {{ width:100%; border-radius:12px 12px 6px 6px; background:linear-gradient(180deg, rgba(15,118,110,0.95), rgba(2,132,199,0.92)); }}
.compare-bar.active {{ outline: 3px solid rgba(245,158,11,0.85); outline-offset: 4px; }}
.compare-note {{ font-size:12px; color:var(--muted); text-align:center; }}
.clickable {{ cursor:pointer; }}
.selected-row {{ background: rgba(245,158,11,0.10); }}
.compare-kpis {{ display:grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap:12px; }}
.compare-kpi {{ background: rgba(255,255,255,0.74); border:1px solid rgba(15,23,42,0.08); border-radius:16px; padding:14px; }}
.compare-kpi .head {{ color: var(--muted); font-size:12px; margin-bottom:8px; }}
.compare-kpi .line {{ display:flex; justify-content:space-between; gap:10px; font-size:13px; padding:3px 0; }}
.compare-kpi .delta-pos {{ color: #0f766e; font-weight: 800; }}
.compare-kpi .delta-neg {{ color: #b91c1c; font-weight: 800; }}
.action-btn {{ border: 1px solid rgba(15,23,42,0.12); background: rgba(255,255,255,0.92); border-radius: 10px; padding: 8px 12px; font-size: 13px; font-weight: 700; cursor: pointer; }}
.action-btn.link {{ text-decoration:none; color: var(--ink); display:inline-flex; align-items:center; }}
.action-btn:disabled {{ opacity: 0.55; cursor: default; }}
.compare-setup {{ display:grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap:12px; }}
.compare-block {{ margin-top:10px; }}
.compare-links {{ display:flex; gap:8px; flex-wrap:wrap; margin-top:10px; }}
@keyframes rise {{ from {{ transform: translateY(8px); opacity: 0; }} to {{ transform: translateY(0); opacity: 1; }} }}
@media (max-width: 960px) {{
  .grid {{ grid-template-columns: 1fr; }}
  #chart {{ height: 280px; }}
  .filters {{ grid-template-columns: 1fr; }}
  .bar-row {{ grid-template-columns: 120px 1fr 56px; }}
  .mini-grid {{ grid-template-columns: 1fr; }}
  .compare-kpis {{ grid-template-columns: 1fr; }}
  .compare-setup {{ grid-template-columns: 1fr; }}
}}
</style>
</head>
<body>
  <div class="wrap">
    <div class="head">
      <div>
        <div class="title" id="title">{title}</div>
        <div class="sub" id="subtitle">{subtitle}</div>
      </div>
      <div class="head-right">
        <select id="lang-switch" class="lang-switch">
          <option value="en">English</option>
          <option value="zh-CN">中文</option>
          <option value="ja">日本語</option>
        </select>
        <div class="sub" id="generated-from">{generated_from}</div>
        <div class="pill" id="live-pill"><span class="dot" id="live-dot"></span><span id="live-status">live refresh: init</span></div>
      </div>
    </div>

    <div class="grid">
      <section class="panel" data-delay="1">
        <div class="toolbar">
          <h3 id="equity-curve" style="margin:0;">{equity_curve}</h3>
          <div style="display:flex; gap:10px; align-items:center;">
            <span class="sub" id="series-label">{series}</span>
            <select id="series-select" class="select">
              <option value="equity">Equity</option>
              <option value="cash">Cash</option>
              <option value="gross_exposure">Gross</option>
              <option value="net_exposure">Net</option>
            </select>
          </div>
        </div>
        <div id="chartWrap">
          <canvas id="chart"></canvas>
          <div class="tooltip" id="tooltip"></div>
        </div>
      </section>

      <section class="panel" data-delay="2">
        <div class="toolbar">
          <h3 id="overview" style="margin:0;">{overview}</h3>
          <span class="chip" id="meta-chip"></span>
        </div>
        <div class="kpis" id="kpis"></div>
        <div style="margin-top: 12px;">
          <h3 id="run-summary" style="margin:0 0 10px 0;">{run_summary}</h3>
          <div class="summary" id="summary-block">{summary_html}</div>
        </div>
        <div style="margin-top: 12px;">
          <h3 id="audit-title" style="margin:0 0 10px 0;">{audit}</h3>
          <div class="summary" id="audit-block">{audit_html}</div>
          <div class="sub" id="audit-hint" style="margin-top:8px;"></div>
          <table style="margin-top:10px;">
            <thead>
              <tr>
                <th id="audit-th-market">{market}</th>
                <th id="audit-th-data">{data_file}</th>
                <th id="audit-th-sha">{sha256}</th>
                <th id="audit-th-industry">{industry_file}</th>
                <th id="audit-th-holiday">{holiday_file}</th>
              </tr>
            </thead>
            <tbody id="audit-markets"></tbody>
          </table>
        </div>
        <div style="margin-top: 12px;">
          <h3 id="data-quality-title" style="margin:0 0 10px 0;">{data_quality}</h3>
          <div class="summary" id="data-quality-block">{data_quality_html}</div>
          <div class="sub" id="dq-hint" style="margin-top:8px;"></div>
          <table style="margin-top:10px;">
            <thead>
              <tr>
                <th id="dq-th-market">{market}</th>
                <th id="dq-th-status">{status}</th>
                <th id="dq-th-rows">{rows}</th>
                <th id="dq-th-issues">{issues}</th>
              </tr>
            </thead>
            <tbody id="dq-rows"></tbody>
          </table>
        </div>
      </section>
    </div>

    <section class="panel" data-delay="3" style="margin-top: 16px;">
      <div class="toolbar">
        <h3 id="recent-trades" style="margin:0;">{recent_trades}</h3>
        <span class="chip" id="trade-stats"></span>
      </div>
      <div class="filters">
        <select id="filter-market" class="select"></select>
        <select id="filter-side" class="select"></select>
        <input id="filter-symbol" type="text" placeholder="symbol..." />
      </div>
      <table>
        <thead>
          <tr>
            <th id="th-date">{date}</th><th id="th-market">{market}</th><th id="th-symbol">{symbol}</th><th id="th-side">{side}</th><th id="th-qty">{qty}</th><th id="th-price">{price}</th><th id="th-fees">{fees}</th>
          </tr>
        </thead>
        <tbody id="trades"></tbody>
      </table>
    </section>

    <div class="grid" style="margin-top: 16px;">
      <section class="panel" data-delay="4">
        <div class="toolbar">
          <h3 id="rejections-title" style="margin:0;">{rejections}</h3>
          <span class="chip" id="rejection-stats"></span>
        </div>
        <table>
          <thead>
            <tr>
              <th id="rej-th-date">{date}</th><th id="rej-th-market">{market}</th><th id="rej-th-symbol">{symbol}</th><th id="rej-th-side">{side}</th><th id="rej-th-qty">{qty}</th><th id="rej-th-reason">{reason}</th>
            </tr>
          </thead>
          <tbody id="rejections"></tbody>
        </table>
      </section>

      <section class="panel" data-delay="5">
        <div class="toolbar">
          <h3 id="factors-title" style="margin:0;">{factors}</h3>
          <span class="chip" id="factor-stats"></span>
        </div>
        <div class="factor-bars" id="factor-bars"></div>
      </section>
    </div>

    <section class="panel" data-delay="5" style="margin-top: 16px;">
      <div class="toolbar">
        <h3 id="research-title" style="margin:0;">{research}</h3>
        <span class="chip" id="research-stats"></span>
      </div>
      <div class="mini-grid" id="research-kpis"></div>
      <div style="margin-top: 12px;">
        <div class="summary" id="research-summary-block">{research_summary_html}</div>
      </div>
      <div class="grid" style="margin-top: 12px;">
        <div class="stack">
          <div>
            <div class="mini-toolbar">
              <span class="subtle-title" id="decay-chart-title" style="margin:0;">{decay_overview}</span>
              <label class="pill"><span id="research-scope-label">{scope}</span>
                <select id="research-decay-scope" class="select"></select>
              </label>
              <label class="pill"><span id="research-metric-label">{metric}</span>
                <select id="research-decay-metric" class="select">
                  <option value="ic">{ic_short}</option>
                  <option value="long_short_spread">{spread}</option>
                </select>
              </label>
            </div>
            <div class="chart-shell" id="research-decay-chart"></div>
          </div>
          <div>
            <div class="subtle-title" id="decay-title">{decay_overview}</div>
            <div class="table-card">
              <table>
                <thead>
                  <tr>
                    <th id="research-decay-factor">{factors}</th>
                    <th id="research-decay-scope-th">{scope}</th>
                    <th id="research-decay-horizon">{horizon_days}</th>
                    <th id="research-decay-ic">{ic_short}</th>
                    <th id="research-decay-spread">{spread}</th>
                  </tr>
                </thead>
                <tbody id="research-decay-rows"></tbody>
              </table>
            </div>
          </div>
        </div>
        <div class="stack">
          <div>
            <div class="mini-toolbar">
              <span class="subtle-title" id="rolling-chart-title" style="margin:0;">{rolling_ic}</span>
              <label class="pill"><span id="research-rolling-horizon-label">{horizon_days}</span>
                <select id="research-rolling-horizon-select" class="select"></select>
              </label>
            </div>
            <div class="chart-shell" id="research-rolling-chart"></div>
          </div>
          <div>
            <div class="subtle-title" id="rolling-title">{rolling_ic}</div>
            <div class="table-card">
              <table>
                <thead>
                  <tr>
                    <th id="research-rolling-date">{date}</th>
                    <th id="research-rolling-factor">{factors}</th>
                    <th id="research-rolling-horizon">{horizon_days}</th>
                    <th id="research-rolling-ic">{ic_short}</th>
                  </tr>
                </thead>
                <tbody id="research-rolling-rows"></tbody>
              </table>
            </div>
          </div>
        </div>
      </div>
      <div style="margin-top: 12px;">
        <div class="mini-toolbar">
          <span class="subtle-title" id="regime-title">{regime_split}</span>
          <label class="pill"><span id="regime-market-label">{market}</span>
            <select id="research-regime-market" class="select"></select>
          </label>
        </div>
        <div class="grid">
          <div class="chart-shell">
            <div id="research-regime-cards" class="regime-grid"></div>
          </div>
          <div class="table-card">
            <table>
              <thead>
                <tr>
                  <th id="research-regime-bucket">{regime_bucket}</th>
                  <th id="research-regime-obs">{observations}</th>
                  <th id="research-regime-composite">{composite_alpha}</th>
                  <th id="research-regime-momentum">{factors}</th>
                  <th id="research-regime-low-vol">low-vol</th>
                </tr>
              </thead>
              <tbody id="research-regime-rows"></tbody>
            </table>
          </div>
        </div>
      </div>
    </section>

    <section class="panel" data-delay="5" style="margin-top: 16px;">
      <div class="toolbar">
        <h3 id="strategy-comparison-title" style="margin:0;">{strategy_comparison}</h3>
        <span class="chip" id="strategy-comparison-stats"></span>
      </div>
      <div class="toolbar" style="margin-bottom:10px;">
        <label class="pill"><span id="strategy-time-label">{time_range}</span>
          <select id="strategy-time-select" class="select">
            <option value="ALL">{all_time}</option>
            <option value="7D">{recent_7d}</option>
            <option value="30D">{recent_30d}</option>
          </select>
        </label>
      </div>
      <div class="grid">
        <div class="chart-shell">
          <div class="subtle-title" id="top-runs-title">{top_runs}</div>
          <div id="strategy-compare-chart" class="compare-bars"></div>
        </div>
        <div class="table-card">
          <table>
            <thead>
              <tr>
                <th id="strategy-th-plugin">{plugin}</th>
                <th id="strategy-th-method">{method}</th>
                <th id="strategy-th-runs">{runs_label}</th>
                <th id="strategy-th-avg-score">{avg_score}</th>
                <th id="strategy-th-best-score">{best_score}</th>
                <th id="strategy-th-avg-pnl">{avg_pnl_short}</th>
                <th id="strategy-th-avg-sharpe">{avg_sharpe}</th>
              </tr>
            </thead>
            <tbody id="strategy-compare-rows"></tbody>
          </table>
        </div>
      </div>
      <div style="margin-top:12px;">
        <div class="toolbar">
          <div class="subtle-title" id="strategy-compare-kpi-title" style="margin:0;">{latest_vs_selected}</div>
        </div>
        <div id="strategy-compare-kpis" class="compare-kpis"></div>
      </div>
      <div style="margin-top:12px;">
        <div class="toolbar">
          <div class="subtle-title" id="strategy-details-title" style="margin:0;">{run_details}</div>
          <span class="chip" id="strategy-selected-combo"></span>
        </div>
        <div class="table-card">
          <table>
            <thead>
              <tr>
                <th id="strategy-detail-time">{time_label}</th>
                <th id="strategy-detail-command">{command_label}</th>
                <th id="strategy-detail-score">{composite_score}</th>
                <th id="strategy-detail-pnl">{avg_pnl_short}</th>
                <th id="strategy-detail-sharpe">{avg_sharpe}</th>
                <th id="strategy-detail-notes">{notes_label}</th>
              </tr>
            </thead>
            <tbody id="strategy-detail-rows"></tbody>
          </table>
        </div>
      </div>
      <div style="margin-top:12px;">
        <div class="toolbar">
          <div class="subtle-title" id="compare-runs-title" style="margin:0;">{compare_runs}</div>
          <button id="compare-copy-btn" class="action-btn" type="button">{copy_command_label}</button>
        </div>
        <div class="compare-setup">
          <label class="stack">
            <span class="subtle-title" id="compare-baseline-label">{baseline_run}</span>
            <select id="compare-baseline-select" class="select"></select>
          </label>
          <label class="stack">
            <span class="subtle-title" id="compare-candidate-label">{candidate_run}</span>
            <select id="compare-candidate-select" class="select"></select>
          </label>
        </div>
        <div class="compare-block">
          <div class="subtle-title" id="compare-output-label">{output_dir_label}</div>
          <div class="summary" id="compare-output-dir">-</div>
        </div>
        <div class="compare-block">
          <div class="subtle-title" id="compare-command-label">{command_label}</div>
          <div class="summary" id="compare-command-block">-</div>
        </div>
        <div class="sub" id="compare-hint" style="margin-top:8px;">{compare_hint}</div>
        <div class="compare-block">
          <div class="subtle-title" id="recent-compare-title">{recent_compare}</div>
          <div class="table-card" id="recent-compare-block"></div>
        </div>
      </div>
    </section>

    <section class="panel" data-delay="5" style="margin-top: 16px;">
      <div class="toolbar">
        <h3 id="public-leaderboard-title" style="margin:0;">{public_leaderboard}</h3>
        <span class="chip" id="public-leaderboard-stats"></span>
      </div>
      <div class="toolbar" style="margin-bottom:10px;">
        <label class="pill"><span id="leaderboard-source-label">{source}</span>
          <select id="leaderboard-source-select" class="select"></select>
        </label>
        <label class="pill"><span id="leaderboard-time-label">{time_range}</span>
          <select id="leaderboard-time-select" class="select">
            <option value="ALL">{all_time}</option>
            <option value="7D">{recent_7d}</option>
            <option value="30D">{recent_30d}</option>
          </select>
        </label>
      </div>
      <div class="table-card">
        <table>
          <thead>
            <tr>
              <th id="leaderboard-th-rank">{rank}</th>
              <th id="leaderboard-th-source">{source}</th>
              <th id="leaderboard-th-command">{command_label}</th>
              <th id="leaderboard-th-scenario">{scenario}</th>
              <th id="leaderboard-th-plugin">{plugin}</th>
              <th id="leaderboard-th-method">{method}</th>
              <th id="leaderboard-th-score">{score}</th>
              <th id="leaderboard-th-pnl">{avg_pnl_short}</th>
              <th id="leaderboard-th-sharpe">{avg_sharpe}</th>
            </tr>
          </thead>
          <tbody id="leaderboard-rows"></tbody>
        </table>
      </div>
      <div style="margin-top:12px;">
        <div class="toolbar">
          <div class="subtle-title" id="leaderboard-details-title" style="margin:0;">{run_details}</div>
          <span class="chip" id="leaderboard-selected-entry"></span>
        </div>
        <div class="table-card">
          <table>
            <thead>
              <tr>
                <th id="leaderboard-detail-time">{time_label}</th>
                <th id="leaderboard-detail-source">{source}</th>
                <th id="leaderboard-detail-command">{command_label}</th>
                <th id="leaderboard-detail-scenario">{scenario}</th>
                <th id="leaderboard-detail-plugin">{plugin}</th>
                <th id="leaderboard-detail-method">{method}</th>
                <th id="leaderboard-detail-score">{score}</th>
                <th id="leaderboard-detail-notes">{notes_label}</th>
              </tr>
            </thead>
            <tbody id="leaderboard-detail-rows"></tbody>
          </table>
        </div>
      </div>
    </section>
  </div>

<script>
let equityRows = {equity_rows_json};
let labels = equityRows.map(r => r.date || '');
let points = equityRows.map(r => Number(r.equity || 0));
let trades = {trade_json};
let rejections = {rejection_json};
let summaryKv = {summary_kv_json};
let factorKv = {factor_kv_json};
let registryRows = {registry_rows_json};
let strategyCompareRows = {strategy_compare_json};
let leaderboardRows = {leaderboard_rows_json};
let researchSummaryKv = {research_summary_kv_json};
let researchRegimeRows = {research_regime_json};
let researchDecayRows = {research_decay_json};
let researchRollingRows = {research_rolling_json};
let dataQualityRows = {data_quality_json};
let auditMarkets = {audit_markets_json};
let auditConfigSha = {audit_config_sha_json};
let recentCompare = {recent_compare_json};
const registryRefreshPath = {registry_refresh_path_json};
const leaderboardRefreshPath = {leaderboard_refresh_path_json};
const i18n = {i18n_json};
const defaultLang = {default_lang_json};
let strategySelectionKey = '';
let leaderboardSelectionKey = '';
let compareBaselineRunId = '';
let compareCandidateRunId = '';

const c = document.getElementById('chart');
const ctx = c.getContext('2d');
const langSwitch = document.getElementById('lang-switch');
const summaryBlock = document.getElementById('summary-block');
const liveStatus = document.getElementById('live-status');
const liveDot = document.getElementById('live-dot');
const tooltip = document.getElementById('tooltip');
const kpisEl = document.getElementById('kpis');
const seriesSelect = document.getElementById('series-select');
const metaChip = document.getElementById('meta-chip');
const tradeStats = document.getElementById('trade-stats');
const rejectionStats = document.getElementById('rejection-stats');
const factorStats = document.getElementById('factor-stats');
const marketSel = document.getElementById('filter-market');
const sideSel = document.getElementById('filter-side');
const symbolInput = document.getElementById('filter-symbol');

function getText(lang) {{
  return i18n[lang] || i18n['en'];
}}

function parseKv(text) {{
  const out = {{}};
  (text || '').split(/\\r?\\n/).forEach(line => {{
    const m = line.match(/^\\s*([^=]+)=(.*)\\s*$/);
    if (!m) return;
    out[m[1].trim()] = m[2].trim();
  }});
  return out;
}}

function parseNum(s) {{
  if (s == null) return null;
  const raw = String(s).trim();
  if (!raw) return null;
  const pct = raw.endsWith('%');
  const n = Number(raw.replace(/%/g, ''));
  if (Number.isNaN(n)) return null;
  return pct ? n / 100.0 : n;
}}

function fmtMoney(n) {{
  if (n == null || !Number.isFinite(n)) return '-';
  const abs = Math.abs(n);
  const sign = n < 0 ? '-' : '';
  if (abs >= 1e9) return sign + (abs / 1e9).toFixed(2) + 'B';
  if (abs >= 1e6) return sign + (abs / 1e6).toFixed(2) + 'M';
  if (abs >= 1e3) return sign + (abs / 1e3).toFixed(2) + 'K';
  return sign + abs.toFixed(2);
}}

function fmtPct(ratio) {{
  if (ratio == null || !Number.isFinite(ratio)) return '-';
  return (ratio * 100).toFixed(2) + '%';
}}

function seriesLabel(key, text) {{
  if (key === 'cash') return text.cash;
  if (key === 'gross_exposure') return text.gross_exposure;
  if (key === 'net_exposure') return text.net_exposure;
  return text.equity;
}}

function extractSeries(key) {{
  return equityRows.map(r => Number(r[key] || 0));
}}

function renderChart(text) {{
  const dpr = window.devicePixelRatio || 1;
  const w = c.clientWidth;
  const h = c.clientHeight;
  c.width = Math.floor(w * dpr);
  c.height = Math.floor(h * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);

  if (points.length <= 1) {{
    return;
  }}

  const min = Math.min(...points);
  const max = Math.max(...points);
  const span = Math.max(max - min, 1);
  const pad = 24;
  const stepX = (w - pad * 2) / (points.length - 1);
  const yFor = (v) => h - pad - ((v - min) / span) * (h - pad * 2);

  // Grid
  ctx.strokeStyle = 'rgba(15, 23, 42, 0.10)';
  ctx.lineWidth = 1;
  ctx.beginPath();
  for (let i = 0; i <= 4; i++) {{
    const y = pad + (i * (h - pad * 2) / 4);
    ctx.moveTo(pad, y);
    ctx.lineTo(w - pad, y);
  }}
  ctx.stroke();

  // Line
  ctx.strokeStyle = '#0f766e';
  ctx.lineWidth = 2;
  ctx.beginPath();
  points.forEach((v, i) => {{
    const x = pad + i * stepX;
    const y = yFor(v);
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  }});
  ctx.stroke();

  // Fill
  ctx.fillStyle = 'rgba(15,118,110,0.12)';
  ctx.lineTo(w - pad, h - pad);
  ctx.lineTo(pad, h - pad);
  ctx.closePath();
  ctx.fill();

  // Labels
  ctx.fillStyle = '#334155';
  ctx.font = '12px Avenir Next, sans-serif';
  const first = points[0];
  const last = points[points.length - 1];
  ctx.fillText(text.start + ': ' + fmtMoney(first), pad, 16);
  ctx.fillText(text.end + ': ' + fmtMoney(last), w - 180, 16);

  // Hover crosshair
  if (window.__hoverIndex != null) {{
    const i = window.__hoverIndex;
    const x = pad + i * stepX;
    const y = yFor(points[i]);
    ctx.strokeStyle = 'rgba(245, 158, 11, 0.9)';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(x, pad);
    ctx.lineTo(x, h - pad);
    ctx.stroke();

    ctx.fillStyle = 'rgba(245, 158, 11, 1.0)';
    ctx.beginPath();
    ctx.arc(x, y, 3.5, 0, Math.PI * 2);
    ctx.fill();
  }}
}}

function renderTrades(text) {{
  const tbody = document.getElementById('trades');
  tbody.innerHTML = '';
  const fMarket = marketSel.value;
  const fSide = sideSel.value;
  const q = (symbolInput.value || '').trim().toUpperCase();
  const filtered = trades.filter(t => {{
    if (fMarket && fMarket !== '__all__' && t.market !== fMarket) return false;
    if (fSide && fSide !== '__all__' && t.side !== fSide) return false;
    if (q && !String(t.symbol || '').toUpperCase().includes(q)) return false;
    return true;
  }});

  filtered.slice(-60).reverse().forEach(t => {{
    const tr = document.createElement('tr');
    const sideTag = t.side === 'BUY'
      ? '<span class="tag-buy">' + text.buy + '</span>'
      : '<span class="tag-sell">' + text.sell + '</span>';
    const fees = Number(t.fees || 0);
    tr.innerHTML = `<td>${{t.date}}</td><td>${{t.market}}</td><td>${{t.symbol}}</td><td>${{sideTag}}</td><td>${{t.qty}}</td><td>${{Number(t.price||0).toFixed(4)}}</td><td>${{fees.toFixed(2)}}</td>`;
    tbody.appendChild(tr);
  }});
}}

function renderRejections() {{
  const tbody = document.getElementById('rejections');
  tbody.innerHTML = '';
  if (!rejections || rejections.length === 0) {{
    rejectionStats.textContent = '0';
    return;
  }}
  rejectionStats.textContent = String(rejections.length);
  rejections.slice(-60).reverse().forEach(r => {{
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${{r.date}}</td><td>${{r.market}}</td><td>${{r.symbol}}</td><td>${{r.side}}</td><td>${{r.qty}}</td><td>${{r.reason}}</td>`;
    tbody.appendChild(tr);
  }});
}}

function factorBarValue(key) {{
  const raw = (factorKv || {{}})[key];
  if (!raw) return null;
  const n = parseNum(raw);
  if (n == null) return null;
  return n;
}}

function renderFactors(text) {{
  const root = document.getElementById('factor-bars');
  root.innerHTML = '';
  const rows = [
    {{ key: 'share_factor_momentum', label: 'momentum' }},
    {{ key: 'share_factor_mean_reversion', label: 'mean reversion' }},
    {{ key: 'share_factor_low_vol', label: 'low-vol' }},
    {{ key: 'share_factor_volume', label: 'volume' }},
  ];
  const any = rows.some(r => factorBarValue(r.key) != null);
  if (!any) {{
    factorStats.textContent = '-';
    root.innerHTML = `<div class="sub">factor_attribution_summary.txt</div>`;
    return;
  }}
  const avgSel = (factorKv || {{}})['avg_selected_symbols'];
  factorStats.textContent = avgSel ? `${{text.avg_selected_symbols}}: ${{Number(avgSel).toFixed(2)}}` : '';
  rows.forEach(r => {{
    const v = factorBarValue(r.key);
    if (v == null) return;
    const pct = (v * 100).toFixed(1) + '%';
    const el = document.createElement('div');
    el.className = 'bar-row';
    el.innerHTML = `<div class="sub">${{r.label}}</div><div class="bar-track"><div class="bar-fill" style="width:${{Math.max(0, Math.min(100, v*100))}}%"></div></div><div style="text-align:right; font-variant-numeric: tabular-nums;">${{pct}}</div>`;
    root.appendChild(el);
  }});
}}

function fmtSignedPct(n) {{
  if (n == null || !Number.isFinite(n)) return '-';
  const sign = n > 0 ? '+' : '';
  return sign + (n * 100).toFixed(2) + '%';
}}

function parseRegistryRows(rows) {{
  return (rows || []).map((r) => ({{
    run_id: r.run_id || '',
    timestamp_utc: r.timestamp_utc || '',
    command: r.command || '',
    output_dir: r.output_dir || '',
    strategy_plugin: r.strategy_plugin || '',
    portfolio_method: r.portfolio_method || '',
    markets: r.markets || '',
    primary_metric_name: r.primary_metric_name || '',
    primary_metric_value: Number(r.primary_metric_value || 0),
    composite_score: Number(r.composite_score || 0),
    pnl_ratio: Number(r.pnl_ratio || 0),
    max_drawdown: Number(r.max_drawdown || 0),
    sharpe: Number(r.sharpe || 0),
    sortino: Number(r.sortino || 0),
    calmar: Number(r.calmar || 0),
    trades: Number(r.trades || 0),
    rejections: Number(r.rejections || 0),
    notes: r.notes || '',
  }}));
}}

function esc(text) {{
  return String(text == null ? '' : text)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}}

function lineChartSvg(series, width, height) {{
  const padding = 24;
  const allValues = series.flatMap((s) => s.values.map((v) => Number(v.y || 0)));
  if (allValues.length === 0) {{
    return '';
  }}
  const min = Math.min(...allValues, 0);
  const max = Math.max(...allValues, 0);
  const span = Math.max(max - min, 1e-6);
  const colors = ['#0f766e', '#f59e0b', '#b91c1c', '#2563eb', '#7c3aed'];
  const zeroY = height - padding - ((0 - min) / span) * (height - padding * 2);
  const lines = series.map((s, idx) => {{
    const pts = s.values.map((v, i) => {{
      const x = padding + (i * (width - padding * 2)) / Math.max(s.values.length - 1, 1);
      const y = height - padding - ((Number(v.y || 0) - min) / span) * (height - padding * 2);
      return `${{x.toFixed(1)}},${{y.toFixed(1)}}`;
    }}).join(' ');
    const color = colors[idx % colors.length];
    return `<polyline fill="none" stroke="${{color}}" stroke-width="3" points="${{pts}}" />
      <text x="${{width - padding}}" y="${{padding + idx * 16}}" fill="${{color}}" font-size="12" text-anchor="end">${{esc(s.name)}}</text>`;
  }}).join('');
  return `<svg viewBox="0 0 ${{width}} ${{height}}" aria-label="line chart">
    <line x1="${{padding}}" y1="${{zeroY}}" x2="${{width - padding}}" y2="${{zeroY}}" stroke="rgba(15,23,42,0.16)" stroke-dasharray="4 4" />
    <line x1="${{padding}}" y1="${{padding}}" x2="${{padding}}" y2="${{height - padding}}" stroke="rgba(15,23,42,0.16)" />
    <line x1="${{padding}}" y1="${{height - padding}}" x2="${{width - padding}}" y2="${{height - padding}}" stroke="rgba(15,23,42,0.16)" />
    ${{lines}}
  </svg>`;
}}

function buildStrategyCompare(rows) {{
  const grouped = new Map();
  (rows || [])
    .filter((r) => r.strategy_plugin || r.portfolio_method)
    .forEach((r) => {{
      const plugin = r.strategy_plugin || '-';
      const method = r.portfolio_method || '-';
      const key = plugin + '|' + method;
      if (!grouped.has(key)) {{
        grouped.set(key, {{
          strategy_plugin: plugin,
          portfolio_method: method,
          runs: 0,
          score_sum: 0,
          best_score: Number.NEGATIVE_INFINITY,
          pnl_sum: 0,
          dd_sum: 0,
          sharpe_sum: 0,
        }});
      }}
      const cur = grouped.get(key);
      cur.runs += 1;
      cur.score_sum += Number(r.composite_score || 0);
      cur.best_score = Math.max(cur.best_score, Number(r.composite_score || 0));
      cur.pnl_sum += Number(r.pnl_ratio || 0);
      cur.dd_sum += Number(r.max_drawdown || 0);
      cur.sharpe_sum += Number(r.sharpe || 0);
    }});
  return [...grouped.values()]
    .map((r) => ({{
      strategy_plugin: r.strategy_plugin,
      portfolio_method: r.portfolio_method,
      runs: r.runs,
      avg_score: r.runs > 0 ? r.score_sum / r.runs : 0,
      best_score: Number.isFinite(r.best_score) ? r.best_score : 0,
      avg_pnl_ratio: r.runs > 0 ? r.pnl_sum / r.runs : 0,
      avg_max_drawdown: r.runs > 0 ? r.dd_sum / r.runs : 0,
      avg_sharpe: r.runs > 0 ? r.sharpe_sum / r.runs : 0,
    }}))
    .sort((a, b) => Number(b.best_score || 0) - Number(a.best_score || 0));
}}

function comboKey(plugin, method) {{
  return (plugin || '-') + '|' + (method || '-');
}}

function leaderboardKey(row) {{
  return [
    row.rank || 0,
    row.source || '',
    row.command || '',
    row.scenario || '',
    row.strategy_plugin || '',
    row.portfolio_method || '',
  ].join('|');
}}

function parseLeaderboardRows(rows) {{
  return (rows || []).map((r) => ({{
    rank: Number(r.rank || 0),
    source: r.source || '',
    timestamp_utc: r.timestamp_utc || '',
    command: r.command || '',
    scenario: r.scenario || '',
    strategy_plugin: r.strategy_plugin || '',
    portfolio_method: r.portfolio_method || '',
    score: Number(r.score || 0),
    pnl_ratio: Number(r.pnl_ratio || 0),
    max_drawdown: Number(r.max_drawdown || 0),
    sharpe: Number(r.sharpe || 0),
    notes: r.notes || '',
  }}));
}}

function cutoffForRange(range) {{
  const now = Date.now();
  if (range === '7D') return now - 7 * 24 * 60 * 60 * 1000;
  if (range === '30D') return now - 30 * 24 * 60 * 60 * 1000;
  return null;
}}

function inTimeRange(timestamp, range) {{
  if (range === 'ALL') return true;
  if (!timestamp) return false;
  const cutoff = cutoffForRange(range);
  if (cutoff == null) return true;
  const ts = Date.parse(timestamp);
  if (Number.isNaN(ts)) return false;
  return ts >= cutoff;
}}

function fmtDelta(value, isPct) {{
  if (value == null || !Number.isFinite(value)) return '-';
  const cls = value >= 0 ? 'delta-pos' : 'delta-neg';
  const text = isPct ? fmtSignedPct(value) : ((value >= 0 ? '+' : '') + Number(value).toFixed(3));
  return `<span class="${{cls}}">${{text}}</span>`;
}}

function shellQuote(value) {{
  const s = String(value == null ? '' : value);
  return `'${{s.replace(/'/g, `'\"'\"'`)}}'`;
}}

function runOptionLabel(row) {{
  const stamp = row && row.timestamp_utc ? row.timestamp_utc : '-';
  const command = row && row.command ? row.command : '-';
  const combo = [row && row.strategy_plugin ? row.strategy_plugin : '-', row && row.portfolio_method ? row.portfolio_method : '-'].join(' / ');
  const runId = row && row.run_id ? row.run_id : '-';
  return `${{stamp}} | ${{command}} | ${{combo}} | ${{runId}}`;
}}

function parentDir(path) {{
  const normalized = String(path || '').replace(/\\\\/g, '/').replace(/\/+$/, '');
  const idx = normalized.lastIndexOf('/');
  return idx > 0 ? normalized.slice(0, idx) : '';
}}

function slugify(value) {{
  return String(value || 'run')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 40) || 'run';
}}

function suggestCompareOutputDir(baselineRow, candidateRow) {{
  const baseParent = parentDir(candidateRow && candidateRow.output_dir)
    || parentDir(baselineRow && baselineRow.output_dir)
    || 'outputs_rust';
  return `${{baseParent}}/compare_${{slugify(baselineRow && baselineRow.run_id)}}_vs_${{slugify(candidateRow && candidateRow.run_id)}}`;
}}

function renderCompareShortcut(text, availableRows, preferredRows) {{
  const baselineSelect = document.getElementById('compare-baseline-select');
  const candidateSelect = document.getElementById('compare-candidate-select');
  const outputEl = document.getElementById('compare-output-dir');
  const commandEl = document.getElementById('compare-command-block');
  const hintEl = document.getElementById('compare-hint');
  const copyBtn = document.getElementById('compare-copy-btn');
  const rows = Array.isArray(availableRows) ? availableRows.slice() : [];
  const preferred = Array.isArray(preferredRows) ? preferredRows.filter(Boolean) : [];

  if (rows.length < 2) {{
    baselineSelect.innerHTML = '';
    candidateSelect.innerHTML = '';
    outputEl.textContent = '-';
    commandEl.textContent = text.compare_needs_two_runs;
    hintEl.textContent = text.compare_needs_two_runs;
    copyBtn.disabled = true;
    copyBtn.onclick = null;
    return;
  }}

  const validIds = new Set(rows.map((row) => String(row.run_id || '')));
  const firstPreferred = preferred[0] || rows[0];
  const secondPreferred = preferred.find((row) => row && row.run_id !== (firstPreferred && firstPreferred.run_id))
    || rows.find((row) => row && row.run_id !== (firstPreferred && firstPreferred.run_id))
    || rows[1];

  if (!compareCandidateRunId || !validIds.has(compareCandidateRunId)) {{
    compareCandidateRunId = firstPreferred && firstPreferred.run_id ? firstPreferred.run_id : String(rows[0].run_id || '');
  }}
  if (!compareBaselineRunId || !validIds.has(compareBaselineRunId) || compareBaselineRunId === compareCandidateRunId) {{
    compareBaselineRunId = secondPreferred && secondPreferred.run_id ? secondPreferred.run_id : String(rows[1].run_id || '');
  }}
  if (compareBaselineRunId === compareCandidateRunId) {{
    const alt = rows.find((row) => row.run_id !== compareCandidateRunId);
    compareBaselineRunId = alt ? String(alt.run_id || '') : compareBaselineRunId;
  }}

  const optionsHtml = rows.map((row) => `<option value="${{esc(String(row.run_id || ''))}}">${{esc(runOptionLabel(row))}}</option>`).join('');
  baselineSelect.innerHTML = optionsHtml;
  candidateSelect.innerHTML = optionsHtml;
  baselineSelect.value = compareBaselineRunId;
  candidateSelect.value = compareCandidateRunId;

  baselineSelect.onchange = () => {{
    compareBaselineRunId = baselineSelect.value || '';
    renderCompareShortcut(text, rows, preferred);
  }};
  candidateSelect.onchange = () => {{
    compareCandidateRunId = candidateSelect.value || '';
    renderCompareShortcut(text, rows, preferred);
  }};

  const baselineRow = rows.find((row) => String(row.run_id || '') === compareBaselineRunId) || rows[0];
  const candidateRow = rows.find((row) => String(row.run_id || '') === compareCandidateRunId) || rows[1];
  const outputDir = suggestCompareOutputDir(baselineRow, candidateRow);
  const command = `cargo run --bin compare -- --baseline-dir ${{shellQuote(baselineRow.output_dir || '')}} --candidate-dir ${{shellQuote(candidateRow.output_dir || '')}} --output-dir ${{shellQuote(outputDir)}}`;

  outputEl.textContent = outputDir;
  commandEl.textContent = command;
  hintEl.textContent = `${{text.compare_hint}} ${{text.baseline_run}}=${{runOptionLabel(baselineRow)}} | ${{text.candidate_run}}=${{runOptionLabel(candidateRow)}}`;
  copyBtn.disabled = false;
  copyBtn.textContent = text.copy_command_label;
  copyBtn.onclick = async () => {{
    const fallbackCopy = () => {{
      const range = document.createRange();
      range.selectNodeContents(commandEl);
      const selection = window.getSelection();
      if (selection) {{
        selection.removeAllRanges();
        selection.addRange(range);
      }}
      try {{
        document.execCommand('copy');
        copyBtn.textContent = `${{text.copy_command_label}} OK`;
      }} catch (_) {{
        copyBtn.textContent = text.copy_command_label;
      }}
      window.setTimeout(() => {{
        copyBtn.textContent = text.copy_command_label;
        if (selection) selection.removeAllRanges();
      }}, 1200);
    }};
    try {{
      if (navigator.clipboard && navigator.clipboard.writeText) {{
        await navigator.clipboard.writeText(command);
        copyBtn.textContent = `${{text.copy_command_label}} OK`;
        window.setTimeout(() => {{
          copyBtn.textContent = text.copy_command_label;
        }}, 1200);
      }} else {{
        fallbackCopy();
      }}
    }} catch (_) {{
      fallbackCopy();
    }}
  }};
}}

function renderRecentCompare(text) {{
  const root = document.getElementById('recent-compare-block');
  if (!root) return;
  if (!recentCompare) {{
    root.innerHTML = `<div class="summary">${{esc(text.compare_not_found)}}</div>`;
    return;
  }}
  root.innerHTML = `
    <div style="padding:12px 14px;">
      <div class="sub"><strong>${{esc(text.baseline_run)}}:</strong> ${{esc(recentCompare.baseline_dir || '-')}}</div>
      <div class="sub"><strong>${{esc(text.candidate_run)}}:</strong> ${{esc(recentCompare.candidate_dir || '-')}}</div>
      <div class="sub"><strong>${{esc(text.output_dir_label)}}:</strong> ${{esc(recentCompare.output_dir || '-')}}</div>
      <div class="sub" style="margin-top:8px;">
        ${{esc(text.metric_changes_label)}}=${{Number(recentCompare.metric_changes || 0)}} |
        ${{esc(text.audit_changes_label)}}=${{Number(recentCompare.audit_changes || 0)}} |
        ${{esc(text.data_quality_changes_label)}}=${{Number(recentCompare.data_quality_changes || 0)}}
      </div>
      <div class="compare-links">
        <a class="action-btn link" href="${{esc(recentCompare.html_href || '#')}}">${{esc(text.open_report_html)}}</a>
        <a class="action-btn link" href="${{esc(recentCompare.json_href || '#')}}">${{esc(text.open_report_json)}}</a>
      </div>
    </div>
  `;
}}

function renderStrategyComparison(text) {{
  const selectedRange = document.getElementById('strategy-time-select').value || 'ALL';
  const filteredRegistryRows = (registryRows || []).filter((r) => inTimeRange(r.timestamp_utc, selectedRange));
  strategyCompareRows = buildStrategyCompare(filteredRegistryRows);
  const chart = document.getElementById('strategy-compare-chart');
  const body = document.getElementById('strategy-compare-rows');
  const stats = document.getElementById('strategy-comparison-stats');
  const detailBody = document.getElementById('strategy-detail-rows');
  const selectedChip = document.getElementById('strategy-selected-combo');
  if (!strategyCompareRows || strategyCompareRows.length === 0) {{
    chart.innerHTML = `<div class="sub">run_registry.csv</div>`;
    body.innerHTML = `<tr><td colspan="7" class="sub">run_registry.csv not found</td></tr>`;
    detailBody.innerHTML = `<tr><td colspan="6" class="sub">run_registry.csv not found</td></tr>`;
    selectedChip.textContent = '';
    stats.textContent = '0';
    renderCompareShortcut(text, filteredRegistryRows, []);
    return;
  }}

  const validKeys = new Set(strategyCompareRows.map((row) => comboKey(row.strategy_plugin, row.portfolio_method)));
  if (!strategySelectionKey || !validKeys.has(strategySelectionKey)) {{
    const first = strategyCompareRows[0];
    strategySelectionKey = comboKey(first.strategy_plugin, first.portfolio_method);
  }}

  const top = strategyCompareRows.slice(0, 6);
  const maxScore = Math.max(...top.map((r) => Math.abs(Number(r.best_score || 0))), 0.0001);
  chart.innerHTML = top.map((row) => {{
    const key = comboKey(row.strategy_plugin, row.portfolio_method);
    const height = Math.max(16, (Math.abs(Number(row.best_score || 0)) / maxScore) * 170);
    const active = key === strategySelectionKey ? ' active' : '';
    return `<div class="compare-col">
      <div class="compare-note">${{Number(row.best_score || 0).toFixed(3)}}</div>
      <div class="compare-wrap"><div class="compare-bar clickable${{active}}" data-combo-key="${{esc(key)}}" style="height:${{height}}px"></div></div>
      <div class="compare-note">${{esc(row.strategy_plugin)}}</div>
      <div class="compare-note">${{esc(row.portfolio_method)}}</div>
    </div>`;
  }}).join('');

  body.innerHTML = strategyCompareRows.slice(0, 10).map((row) => {{
    const key = comboKey(row.strategy_plugin, row.portfolio_method);
    const selectedCls = key === strategySelectionKey ? 'selected-row' : '';
    return `<tr class="clickable ${{selectedCls}}" data-combo-key="${{esc(key)}}">
    <td>${{esc(row.strategy_plugin)}}</td>
    <td>${{esc(row.portfolio_method)}}</td>
    <td>${{row.runs}}</td>
    <td>${{Number(row.avg_score || 0).toFixed(3)}}</td>
    <td>${{Number(row.best_score || 0).toFixed(3)}}</td>
    <td>${{fmtSignedPct(Number(row.avg_pnl_ratio || 0))}}</td>
    <td>${{Number(row.avg_sharpe || 0).toFixed(3)}}</td>
  </tr>`;
  }}).join('');

  const combos = strategyCompareRows.length;
  const runs = filteredRegistryRows.filter((r) => r.strategy_plugin || r.portfolio_method).length;
  stats.textContent = `${{runs}} runs | ${{combos}} combos | range=${{selectedRange}}`;

  chart.querySelectorAll('[data-combo-key]').forEach((el) => {{
    el.addEventListener('click', () => {{
      strategySelectionKey = el.getAttribute('data-combo-key') || '';
      renderStrategyComparison(text);
    }});
  }});
  body.querySelectorAll('[data-combo-key]').forEach((el) => {{
    el.addEventListener('click', () => {{
      strategySelectionKey = el.getAttribute('data-combo-key') || '';
      renderStrategyComparison(text);
    }});
  }});

  const detailRows = filteredRegistryRows
    .filter((r) => comboKey(r.strategy_plugin || '-', r.portfolio_method || '-') === strategySelectionKey)
    .sort((a, b) => String(b.timestamp_utc || '').localeCompare(String(a.timestamp_utc || '')));
  const selectedAgg = strategyCompareRows.find((r) => comboKey(r.strategy_plugin, r.portfolio_method) === strategySelectionKey) || null;
  selectedChip.textContent = `${{text.selected_combo}}: ${{strategySelectionKey.replace('|', ' / ')}}`;
  detailBody.innerHTML = detailRows.length === 0
    ? `<tr><td colspan="6" class="sub">No runs for selected combo</td></tr>`
    : detailRows.slice(0, 12).map((r) => `<tr>
      <td>${{esc(r.timestamp_utc || '-')}}</td>
      <td>${{esc(r.command || '-')}}</td>
      <td>${{Number(r.composite_score || 0).toFixed(3)}}</td>
      <td>${{fmtSignedPct(Number(r.pnl_ratio || 0))}}</td>
      <td>${{Number(r.sharpe || 0).toFixed(3)}}</td>
      <td>${{esc(r.notes || '-')}}</td>
    </tr>`).join('');

  const compareRoot = document.getElementById('strategy-compare-kpis');
  const currentPnl = parseNum((summaryKv || {{}}).pnl_ratio);
  const currentSharpe = parseNum((summaryKv || {{}}).sharpe);
  const currentDd = parseNum((summaryKv || {{}}).max_drawdown);
  const compareItems = [
    {{
      title: text.kpi_pnl_ratio,
      current: currentPnl,
      selected: selectedAgg ? Number(selectedAgg.avg_pnl_ratio || 0) : null,
      isPct: true,
    }},
    {{
      title: text.kpi_sharpe,
      current: currentSharpe,
      selected: selectedAgg ? Number(selectedAgg.avg_sharpe || 0) : null,
      isPct: false,
    }},
    {{
      title: text.kpi_max_drawdown,
      current: currentDd,
      selected: selectedAgg ? Number(selectedAgg.avg_max_drawdown || 0) : null,
      isPct: true,
    }},
  ];
  compareRoot.innerHTML = compareItems.map((item) => {{
    const delta = (item.current != null && item.selected != null) ? (item.current - item.selected) : null;
    const currentText = item.isPct ? fmtPct(item.current) : (item.current == null ? '-' : Number(item.current).toFixed(3));
    const selectedText = item.isPct ? fmtPct(item.selected) : (item.selected == null ? '-' : Number(item.selected).toFixed(3));
    return `<div class="compare-kpi">
      <div class="head">${{esc(item.title)}}</div>
      <div class="line"><span>${{esc(text.current_label)}}</span><strong>${{currentText}}</strong></div>
      <div class="line"><span>${{esc(text.selected_label)}}</span><strong>${{selectedText}}</strong></div>
      <div class="line"><span>${{esc(text.delta_label)}}</span>${{fmtDelta(delta, item.isPct)}}</div>
    </div>`;
  }}).join('');

  renderCompareShortcut(text, filteredRegistryRows, detailRows);
}}

function renderPublicLeaderboard(text) {{
  const sourceSel = document.getElementById('leaderboard-source-select');
  const prev = sourceSel.value || 'ALL';
  const sources = [...new Set((leaderboardRows || []).map((r) => r.source).filter(Boolean))].sort();
  const opts = ['ALL', ...sources];
  sourceSel.innerHTML = opts.map((s) => `<option value="${{esc(s)}}">${{esc(s)}}</option>`).join('');
  sourceSel.value = opts.includes(prev) ? prev : 'ALL';

  const selected = sourceSel.value;
  const selectedRange = document.getElementById('leaderboard-time-select').value || 'ALL';
  const filtered = (leaderboardRows || [])
    .filter((r) => selected === 'ALL' || r.source === selected)
    .filter((r) => selectedRange === 'ALL' ? true : inTimeRange(r.timestamp_utc, selectedRange));
  const body = document.getElementById('leaderboard-rows');
  const detailBody = document.getElementById('leaderboard-detail-rows');
  const selectedChip = document.getElementById('leaderboard-selected-entry');
  if (filtered.length > 0) {{
    const validKeys = new Set(filtered.map((r) => leaderboardKey(r)));
    if (!leaderboardSelectionKey || !validKeys.has(leaderboardSelectionKey)) {{
      leaderboardSelectionKey = leaderboardKey(filtered[0]);
    }}
  }} else {{
    leaderboardSelectionKey = '';
  }}
  body.innerHTML = filtered.length === 0
    ? `<tr><td colspan="9" class="sub">leaderboard_public.csv not found</td></tr>`
    : filtered.slice(0, 12).map((r) => {{
      const key = leaderboardKey(r);
      const selectedCls = key === leaderboardSelectionKey ? 'selected-row' : '';
      return `<tr class="clickable ${{selectedCls}}" data-leaderboard-key="${{esc(key)}}">
      <td>${{r.rank}}</td>
      <td>${{esc(r.source)}}</td>
      <td>${{esc(r.command)}}</td>
      <td>${{esc(r.scenario || '-')}}</td>
      <td>${{esc(r.strategy_plugin || '-')}}</td>
      <td>${{esc(r.portfolio_method || '-')}}</td>
      <td>${{Number(r.score || 0).toFixed(3)}}</td>
      <td>${{fmtSignedPct(Number(r.pnl_ratio || 0))}}</td>
      <td>${{Number(r.sharpe || 0).toFixed(3)}}</td>
    </tr>`;
    }}).join('');

  document.getElementById('public-leaderboard-stats').textContent =
    `${{filtered.length}} rows | source=${{selected}} | range=${{selectedRange}}`;
  body.querySelectorAll('[data-leaderboard-key]').forEach((el) => {{
    el.addEventListener('click', () => {{
      leaderboardSelectionKey = el.getAttribute('data-leaderboard-key') || '';
      renderPublicLeaderboard(text);
    }});
  }});

  const detailRows = filtered
    .filter((r) => leaderboardKey(r) === leaderboardSelectionKey)
    .slice(0, 1);
  selectedChip.textContent = detailRows.length === 0
    ? ''
    : `${{text.selected_entry}}: #${{detailRows[0].rank}} / ${{detailRows[0].source}}`;
  detailBody.innerHTML = detailRows.length === 0
    ? `<tr><td colspan="8" class="sub">No leaderboard row selected</td></tr>`
    : detailRows.map((r) => `<tr>
      <td>${{esc(r.timestamp_utc || '-')}}</td>
      <td>${{esc(r.source || '-')}}</td>
      <td>${{esc(r.command || '-')}}</td>
      <td>${{esc(r.scenario || '-')}}</td>
      <td>${{esc(r.strategy_plugin || '-')}}</td>
      <td>${{esc(r.portfolio_method || '-')}}</td>
      <td>${{Number(r.score || 0).toFixed(3)}}</td>
      <td>${{esc(r.notes || '-')}}</td>
    </tr>`).join('');
}}

function researchCardValue(key) {{
  return (researchSummaryKv || {{}})[key] || '';
}}

function syncResearchControls(text) {{
  const scopeSel = document.getElementById('research-decay-scope');
  const prevScope = scopeSel.value || 'ALL';
  const scopes = [...new Set((researchDecayRows || []).map((r) => r.scope || 'ALL'))];
  const orderedScopes = scopes.includes('ALL')
    ? ['ALL', ...scopes.filter((s) => s !== 'ALL').sort()]
    : scopes.sort();
  scopeSel.innerHTML = orderedScopes.map((scope) => `<option value="${{esc(scope)}}">${{esc(scope)}}</option>`).join('');
  scopeSel.value = orderedScopes.includes(prevScope) ? prevScope : (orderedScopes[0] || 'ALL');

  const horizonSel = document.getElementById('research-rolling-horizon-select');
  const prevHorizon = horizonSel.value || '';
  const horizons = [...new Set((researchRollingRows || []).map((r) => Number(r.horizon_days || 0)).filter(Boolean))].sort((a, b) => a - b);
  horizonSel.innerHTML = horizons.map((h) => `<option value="${{h}}">${{h}}d</option>`).join('');
  if (horizons.length > 0) {{
    horizonSel.value = horizons.map(String).includes(prevHorizon) ? prevHorizon : String(horizons[horizons.length - 1]);
  }}

  document.getElementById('research-scope-label').textContent = text.scope;
  document.getElementById('research-metric-label').textContent = text.metric;
  document.getElementById('research-rolling-horizon-label').textContent = text.horizon_days;
}}

function renderResearchCharts(text) {{
  const scope = document.getElementById('research-decay-scope').value || 'ALL';
  const metric = document.getElementById('research-decay-metric').value || 'ic';
  const decayRoot = document.getElementById('research-decay-chart');
  const factors = ['momentum', 'mean_reversion', 'low_vol', 'volume', 'composite'];
  const decaySeries = factors.map((factor) => {{
    const values = (researchDecayRows || [])
      .filter((r) => (r.scope || 'ALL') === scope && r.factor === factor)
      .sort((a, b) => Number(a.horizon_days || 0) - Number(b.horizon_days || 0))
      .map((r) => ({{
        x: Number(r.horizon_days || 0),
        y: metric === 'ic' ? Number(r.ic || 0) : Number(r.long_short_spread || 0),
      }}));
    return {{ name: factor, values }};
  }}).filter((s) => s.values.length > 0);
  decayRoot.innerHTML = decaySeries.length
    ? lineChartSvg(decaySeries, 560, 220) + `<div class="sub" style="margin-top:8px;">${{esc(text.scope)}}=${{esc(scope)}} | ${{esc(text.metric)}}=${{esc(metric === 'ic' ? text.ic_short : text.spread)}}</div>`
    : `<div class="sub">research_report.json</div>`;

  const horizon = Number(document.getElementById('research-rolling-horizon-select').value || 0);
  const rollingRoot = document.getElementById('research-rolling-chart');
  const rollingSeries = factors.map((factor) => {{
    const values = (researchRollingRows || [])
      .filter((r) => r.factor === factor && Number(r.horizon_days || 0) === horizon)
      .sort((a, b) => String(a.date || '').localeCompare(String(b.date || '')))
      .map((r, idx) => ({{
        x: idx,
        y: Number(r.ic || 0),
      }}));
    return {{ name: factor, values }};
  }}).filter((s) => s.values.length > 0);
  rollingRoot.innerHTML = rollingSeries.length
    ? lineChartSvg(rollingSeries, 560, 220) + `<div class="sub" style="margin-top:8px;">${{esc(text.horizon_days)}}=${{horizon || '-'}}d</div>`
    : `<div class="sub">rolling_ic.csv / research_report.json</div>`;
}}

function renderRegime(text) {{
  const marketSel = document.getElementById('research-regime-market');
  const prevMarket = marketSel.value || '';
  const markets = [...new Set((researchRegimeRows || []).map((r) => r.market).filter(Boolean))].sort();
  marketSel.innerHTML = markets.map((m) => `<option value="${{esc(m)}}">${{esc(m)}}</option>`).join('');
  if (markets.length > 0) {{
    marketSel.value = markets.includes(prevMarket) ? prevMarket : markets[0];
  }}

  const activeMarket = marketSel.value || markets[0] || '';
  const rows = (researchRegimeRows || []).filter((r) => r.market === activeMarket);
  const cards = document.getElementById('research-regime-cards');
  const body = document.getElementById('research-regime-rows');
  if (rows.length === 0) {{
    cards.innerHTML = `<div class="sub">research_report.json</div>`;
    body.innerHTML = `<tr><td colspan="5" class="sub">regime_split.csv / research_report.json</td></tr>`;
    return;
  }}

  cards.innerHTML = rows.map((row) => `<div class="regime-card">
    <div class="regime-title">${{esc(row.regime_bucket)}}</div>
    <div class="regime-main">${{Number(row.avg_composite_alpha || 0).toFixed(3)}}</div>
    <div class="regime-sub">${{esc(text.observations)}}=${{row.observations}} | momentum=${{Number(row.avg_factor_momentum || 0).toFixed(3)}} | low-vol=${{Number(row.avg_factor_low_vol || 0).toFixed(3)}}</div>
  </div>`).join('');

  body.innerHTML = rows.map((row) => `<tr>
    <td>${{esc(row.regime_bucket)}}</td>
    <td>${{row.observations}}</td>
    <td>${{Number(row.avg_composite_alpha || 0).toFixed(3)}}</td>
    <td>${{Number(row.avg_factor_momentum || 0).toFixed(3)}}</td>
    <td>${{Number(row.avg_factor_low_vol || 0).toFixed(3)}}</td>
  </tr>`).join('');
}}

function renderResearch(text) {{
  syncResearchControls(text);
  const kpis = document.getElementById('research-kpis');
  const decayBody = document.getElementById('research-decay-rows');
  const rollingBody = document.getElementById('research-rolling-rows');
  const folds = researchCardValue('folds');
  const avgSharpe = researchCardValue('avg_test_sharpe');
  const bestDecay = {{
    factor: researchCardValue('best_decay_factor') || '-',
    horizon: researchCardValue('best_decay_horizon_days') || '-',
    ic: researchCardValue('best_decay_ic') || '-',
  }};
  const latestRolling = {{
    factor: researchCardValue('latest_rolling_factor') || '-',
    horizon: researchCardValue('latest_rolling_horizon_days') || '-',
    ic: researchCardValue('latest_rolling_ic') || '-',
  }};

  const cards = [
    {{ k: text.folds, v: folds || '-' }},
    {{ k: text.avg_test_sharpe_short, v: avgSharpe || '-' }},
    {{ k: text.best_decay, v: `${{bestDecay.factor}} | ${{bestDecay.horizon}}d | IC=${{bestDecay.ic}}` }},
    {{ k: text.latest_rolling, v: `${{latestRolling.factor}} | ${{latestRolling.horizon}}d | IC=${{latestRolling.ic}}` }},
  ];
  kpis.innerHTML = cards.map((it) => (
    `<div class="mini-kpi"><div class="k">${{it.k}}</div><div class="v">${{it.v}}</div></div>`
  )).join('');

  const bestDecayRows = [...(researchDecayRows || [])]
    .filter((r) => !r.scope || r.scope === 'ALL')
    .sort((a, b) => Number(b.ic || 0) - Number(a.ic || 0))
    .slice(0, 6);
  decayBody.innerHTML = bestDecayRows.length === 0
    ? `<tr><td colspan="5" class="sub">research_report.json</td></tr>`
    : bestDecayRows.map((r) => (
      `<tr><td>${{r.factor}}</td><td>${{r.scope || 'ALL'}}</td><td>${{r.horizon_days}}</td><td>${{Number(r.ic || 0).toFixed(4)}}</td><td>${{fmtSignedPct(Number(r.long_short_spread || 0))}}</td></tr>`
    )).join('');

  const rollingRows = [...(researchRollingRows || [])]
    .sort((a, b) => String(b.date || '').localeCompare(String(a.date || '')) || String(a.factor || '').localeCompare(String(b.factor || '')))
    .slice(0, 8);
  rollingBody.innerHTML = rollingRows.length === 0
    ? `<tr><td colspan="4" class="sub">rolling_ic.csv / research_report.json</td></tr>`
    : rollingRows.map((r) => (
      `<tr><td>${{r.date}}</td><td>${{r.factor}}</td><td>${{r.horizon_days}}</td><td>${{Number(r.ic || 0).toFixed(4)}}</td></tr>`
    )).join('');

  const stats = [
    (folds ? (`${{text.folds}}=${{folds}}`) : ''),
    (`decay=${{(researchDecayRows || []).length}}`),
    (`rolling=${{(researchRollingRows || []).length}}`),
  ].filter(Boolean);
  document.getElementById('research-stats').textContent = stats.join(' | ');
  renderResearchCharts(text);
  renderRegime(text);
}}

function renderKpis(text) {{
  const kv = summaryKv || {{}};
  const startEq = parseNum(kv.start_equity);
  const endEq = parseNum(kv.end_equity);
  const pnl = parseNum(kv.pnl);
  const pnlRatio = parseNum(kv.pnl_ratio);
  const dd = parseNum(kv.max_drawdown);
  const cagr = parseNum(kv.cagr);
  const sharpe = parseNum(kv.sharpe);
  const tradesN = parseNum(kv.trades);
  const rejN = parseNum(kv.rejections);

  const items = [
    {{ k: text.kpi_start_equity, v: fmtMoney(startEq), cls: '' }},
    {{ k: text.kpi_end_equity, v: fmtMoney(endEq), cls: '' }},
    {{ k: text.kpi_pnl, v: fmtMoney(pnl), cls: pnl < 0 ? 'negative' : 'positive' }},
    {{ k: text.kpi_pnl_ratio, v: fmtPct(pnlRatio), cls: pnlRatio < 0 ? 'negative' : 'positive' }},
    {{ k: text.kpi_max_drawdown, v: fmtPct(dd), cls: 'negative' }},
    {{ k: text.kpi_cagr, v: fmtPct(cagr), cls: cagr < 0 ? 'negative' : 'positive' }},
    {{ k: text.kpi_sharpe, v: sharpe == null ? '-' : sharpe.toFixed(2), cls: '' }},
    {{ k: text.kpi_trades, v: tradesN == null ? '-' : String(Math.round(tradesN)), cls: '' }},
    {{ k: text.kpi_rejections, v: rejN == null ? '-' : String(Math.round(rejN)), cls: '' }},
  ];
  kpisEl.innerHTML = '';
  items.forEach(it => {{
    const el = document.createElement('div');
    el.className = 'kpi';
    el.innerHTML = `<div class="k">${{it.k}}</div><div class="v ${{it.cls}}">${{it.v}}</div>`;
    kpisEl.appendChild(el);
  }});

  const markets = [...new Set(trades.map(t => t.market).filter(Boolean))].sort();
  const fees = trades.reduce((a, t) => a + Number(t.fees || 0), 0);
  tradeStats.textContent = `${{trades.length}} trades | fees=${{fees.toFixed(2)}} | mkts=${{markets.join(',')}}`;
  metaChip.textContent = `${{labels.length}} days`;
}}

function setupFilters(text) {{
  const prevMarket = marketSel.value || '__all__';
  const prevSide = sideSel.value || '__all__';
  const mkts = [...new Set(trades.map(t => t.market).filter(Boolean))].sort();
  marketSel.innerHTML = '';
  const optAll = document.createElement('option');
  optAll.value = '__all__';
  optAll.textContent = text.all;
  marketSel.appendChild(optAll);
  mkts.forEach(m => {{
    const opt = document.createElement('option');
    opt.value = m;
    opt.textContent = m;
    marketSel.appendChild(opt);
  }});
  marketSel.value = mkts.includes(prevMarket) ? prevMarket : '__all__';

  sideSel.innerHTML = '';
  const sAll = document.createElement('option');
  sAll.value = '__all__';
  sAll.textContent = text.all;
  sideSel.appendChild(sAll);
  ['BUY','SELL'].forEach(s => {{
    const opt = document.createElement('option');
    opt.value = s;
    opt.textContent = s;
    sideSel.appendChild(opt);
  }});
  sideSel.value = (prevSide === 'BUY' || prevSide === 'SELL') ? prevSide : '__all__';
  symbolInput.placeholder = text.search + ' ' + text.symbol;

  const optEquity = seriesSelect.querySelector('option[value="equity"]');
  const optCash = seriesSelect.querySelector('option[value="cash"]');
  const optGross = seriesSelect.querySelector('option[value="gross_exposure"]');
  const optNet = seriesSelect.querySelector('option[value="net_exposure"]');
  if (optEquity) optEquity.textContent = text.equity;
  if (optCash) optCash.textContent = text.cash;
  if (optGross) optGross.textContent = text.gross_exposure;
  if (optNet) optNet.textContent = text.net_exposure;
}}

function updateTooltip(i, x, y, text) {{
  if (i == null) {{
    tooltip.style.display = 'none';
    return;
  }}
  const date = labels[i] || '';
  const v = points[i];
  tooltip.style.display = 'block';
  tooltip.innerHTML = `<div style="font-weight:800; margin-bottom:6px;">${{date}}</div><div class="sub">${{seriesLabel(seriesSelect.value, text)}}: <span style="font-weight:800; color: var(--ink);">${{fmtMoney(v)}}</span></div>`;
  const pad = 12;
  tooltip.style.left = Math.min(c.clientWidth - 10, Math.max(10, x + pad)) + 'px';
  tooltip.style.top = Math.max(10, y + pad) + 'px';
}}

function attachChartHover() {{
  const pad = 24;
  const onMove = (ev) => {{
    const rect = c.getBoundingClientRect();
    const x = ev.clientX - rect.left;
    const y = ev.clientY - rect.top;
    const w = c.clientWidth;
    if (points.length <= 1) return;
    const stepX = (w - pad * 2) / (points.length - 1);
    const i = Math.max(0, Math.min(points.length - 1, Math.round((x - pad) / stepX)));
    window.__hoverIndex = i;
    updateTooltip(i, x, y, getText(langSwitch.value));
    renderChart(getText(langSwitch.value));
  }};
  const onLeave = () => {{
    window.__hoverIndex = null;
    updateTooltip(null, 0, 0, getText(langSwitch.value));
    renderChart(getText(langSwitch.value));
  }};
  c.addEventListener('mousemove', onMove);
  c.addEventListener('mouseleave', onLeave);
  c.addEventListener('touchstart', (e) => {{
    if (!e.touches || e.touches.length === 0) return;
    onMove(e.touches[0]);
  }}, {{ passive: true }});
  c.addEventListener('touchmove', (e) => {{
    if (!e.touches || e.touches.length === 0) return;
    onMove(e.touches[0]);
  }}, {{ passive: true }});
  c.addEventListener('touchend', onLeave, {{ passive: true }});
}}

function applyLanguage(lang) {{
  const text = getText(lang);
  document.documentElement.lang = lang;
  document.title = text.title + ' | ' + text.subtitle;
  document.getElementById('title').textContent = text.title;
  document.getElementById('subtitle').textContent = text.subtitle;
  document.getElementById('generated-from').textContent = text.generated_from;
  document.getElementById('overview').textContent = text.overview;
  document.getElementById('series-label').textContent = text.series;
  document.getElementById('equity-curve').textContent = text.equity_curve;
  document.getElementById('run-summary').textContent = text.run_summary;
  document.getElementById('audit-title').textContent = text.audit;
  document.getElementById('data-quality-title').textContent = text.data_quality;
  document.getElementById('recent-trades').textContent = text.recent_trades;
  document.getElementById('th-date').textContent = text.date;
  document.getElementById('th-market').textContent = text.market;
  document.getElementById('th-symbol').textContent = text.symbol;
  document.getElementById('th-side').textContent = text.side;
  document.getElementById('th-qty').textContent = text.qty;
  document.getElementById('th-price').textContent = text.price;
  document.getElementById('th-fees').textContent = text.fees;

  document.getElementById('rejections-title').textContent = text.rejections;
  document.getElementById('rej-th-date').textContent = text.date;
  document.getElementById('rej-th-market').textContent = text.market;
  document.getElementById('rej-th-symbol').textContent = text.symbol;
  document.getElementById('rej-th-side').textContent = text.side;
  document.getElementById('rej-th-qty').textContent = text.qty;
  document.getElementById('rej-th-reason').textContent = text.reason;

  document.getElementById('factors-title').textContent = text.factors;
  document.getElementById('strategy-comparison-title').textContent = text.strategy_comparison;
  document.getElementById('top-runs-title').textContent = text.top_runs;
  document.getElementById('strategy-time-label').textContent = text.time_range;
  document.getElementById('strategy-th-plugin').textContent = text.plugin;
  document.getElementById('strategy-th-method').textContent = text.method;
  document.getElementById('strategy-th-runs').textContent = text.runs_label;
  document.getElementById('strategy-th-avg-score').textContent = text.avg_score;
  document.getElementById('strategy-th-best-score').textContent = text.best_score;
  document.getElementById('strategy-th-avg-pnl').textContent = text.avg_pnl_short;
  document.getElementById('strategy-th-avg-sharpe').textContent = text.avg_sharpe;
  document.getElementById('strategy-compare-kpi-title').textContent = text.latest_vs_selected;
  document.getElementById('strategy-details-title').textContent = text.run_details;
  document.getElementById('strategy-detail-time').textContent = text.time_label;
  document.getElementById('strategy-detail-command').textContent = text.command_label;
  document.getElementById('strategy-detail-score').textContent = text.composite_score;
  document.getElementById('strategy-detail-pnl').textContent = text.avg_pnl_short;
  document.getElementById('strategy-detail-sharpe').textContent = text.avg_sharpe;
  document.getElementById('strategy-detail-notes').textContent = text.notes_label;
  document.getElementById('compare-runs-title').textContent = text.compare_runs;
  document.getElementById('compare-baseline-label').textContent = text.baseline_run;
  document.getElementById('compare-candidate-label').textContent = text.candidate_run;
  document.getElementById('compare-output-label').textContent = text.output_dir_label;
  document.getElementById('compare-command-label').textContent = text.command_label;
  document.getElementById('compare-copy-btn').textContent = text.copy_command_label;
  document.getElementById('compare-hint').textContent = text.compare_hint;
  document.getElementById('recent-compare-title').textContent = text.recent_compare;
  document.getElementById('public-leaderboard-title').textContent = text.public_leaderboard;
  document.getElementById('leaderboard-source-label').textContent = text.source;
  document.getElementById('leaderboard-time-label').textContent = text.time_range;
  document.getElementById('leaderboard-details-title').textContent = text.run_details;
  document.getElementById('leaderboard-th-rank').textContent = text.rank;
  document.getElementById('leaderboard-th-source').textContent = text.source;
  document.getElementById('leaderboard-th-command').textContent = text.command_label;
  document.getElementById('leaderboard-th-scenario').textContent = text.scenario;
  document.getElementById('leaderboard-th-plugin').textContent = text.plugin;
  document.getElementById('leaderboard-th-method').textContent = text.method;
  document.getElementById('leaderboard-th-score').textContent = text.score;
  document.getElementById('leaderboard-th-pnl').textContent = text.avg_pnl_short;
  document.getElementById('leaderboard-th-sharpe').textContent = text.avg_sharpe;
  document.getElementById('leaderboard-detail-time').textContent = text.time_label;
  document.getElementById('leaderboard-detail-source').textContent = text.source;
  document.getElementById('leaderboard-detail-command').textContent = text.command_label;
  document.getElementById('leaderboard-detail-scenario').textContent = text.scenario;
  document.getElementById('leaderboard-detail-plugin').textContent = text.plugin;
  document.getElementById('leaderboard-detail-method').textContent = text.method;
  document.getElementById('leaderboard-detail-score').textContent = text.score;
  document.getElementById('leaderboard-detail-notes').textContent = text.notes_label;
  const strategyTimeSelect = document.getElementById('strategy-time-select');
  const leaderboardTimeSelect = document.getElementById('leaderboard-time-select');
  const setRangeOptions = (sel) => {{
    if (!sel) return;
    const current = sel.value;
    sel.innerHTML = `
      <option value="ALL">${{text.all_time}}</option>
      <option value="7D">${{text.recent_7d}}</option>
      <option value="30D">${{text.recent_30d}}</option>
    `;
    sel.value = current || 'ALL';
  }};
  setRangeOptions(strategyTimeSelect);
  setRangeOptions(leaderboardTimeSelect);
  document.getElementById('research-title').textContent = text.research;
  document.getElementById('decay-chart-title').textContent = text.decay_overview;
  document.getElementById('decay-title').textContent = text.decay_overview;
  document.getElementById('rolling-chart-title').textContent = text.rolling_ic;
  document.getElementById('rolling-title').textContent = text.rolling_ic;
  document.getElementById('regime-title').textContent = text.regime_split;
  document.getElementById('regime-market-label').textContent = text.market;
  document.getElementById('research-decay-factor').textContent = text.factors;
  document.getElementById('research-decay-scope-th').textContent = text.scope;
  document.getElementById('research-decay-horizon').textContent = text.horizon_days;
  document.getElementById('research-decay-ic').textContent = text.ic_short;
  document.getElementById('research-decay-spread').textContent = text.spread;
  document.getElementById('research-rolling-date').textContent = text.date;
  document.getElementById('research-rolling-factor').textContent = text.factors;
  document.getElementById('research-rolling-horizon').textContent = text.horizon_days;
  document.getElementById('research-rolling-ic').textContent = text.ic_short;
  document.getElementById('research-regime-bucket').textContent = text.regime_bucket;
  document.getElementById('research-regime-obs').textContent = text.observations;
  document.getElementById('research-regime-composite').textContent = text.composite_alpha;
  document.getElementById('research-regime-momentum').textContent = text.factors;
  const metricIcOpt = document.querySelector('#research-decay-metric option[value="ic"]');
  const metricSpreadOpt = document.querySelector('#research-decay-metric option[value="long_short_spread"]');
  if (metricIcOpt) metricIcOpt.textContent = text.ic_short;
  if (metricSpreadOpt) metricSpreadOpt.textContent = text.spread;
  document.getElementById('dq-th-status').textContent = text.status;
  document.getElementById('dq-th-rows').textContent = text.rows;
  document.getElementById('dq-th-issues').textContent = text.issues;
  document.getElementById('audit-th-data').textContent = text.data_file;
  document.getElementById('audit-th-sha').textContent = text.sha256;
  document.getElementById('audit-th-industry').textContent = text.industry_file;
  document.getElementById('audit-th-holiday').textContent = text.holiday_file;

  setupFilters(text);
  renderKpis(text);
  renderChart(text);
  renderTrades(text);
  renderRejections();
  renderFactors(text);
  renderStrategyComparison(text);
  renderRecentCompare(text);
  renderPublicLeaderboard(text);
  renderResearch(text);
  renderDataQuality();
  renderAudit();
}}

function parseCsv(text) {{
  const lines = text.trim().split(/\\r?\\n/).filter(Boolean);
  if (lines.length < 2) return [];
  const headers = lines[0].split(',');
  return lines.slice(1).map((line) => {{
    const cols = line.split(',');
    const row = {{}};
    headers.forEach((h, i) => {{ row[h] = cols[i] ?? ''; }});
    return row;
  }});
}}

function shortSha(s) {{
  if (!s) return '';
  if (s.length <= 12) return s;
  return s.slice(0, 12) + '…';
}}

function renderDataQuality() {{
  const body = document.getElementById('dq-rows');
  const hint = document.getElementById('dq-hint');
  if (!dataQualityRows || dataQualityRows.length === 0) {{
    hint.textContent = 'data_quality_report.csv not found';
    body.innerHTML = '';
    return;
  }}
  hint.textContent = 'data_quality_report.csv';
  body.innerHTML = dataQualityRows.map((r) => {{
    const market = (r.market || '');
    const status = (r.status || '');
    const cls = status === 'PASS' ? 'ok' : (status === 'WARN' ? 'warn' : 'bad');
    const rows = Number(r.rows || 0);
    const issues = (r.issues || '');
    return '<tr>'
      + '<td>' + market + '</td>'
      + '<td><span class=\"pill ' + cls + '\">' + status + '</span></td>'
      + '<td>' + rows + '</td>'
      + '<td class=\"sub\">' + issues + '</td>'
      + '</tr>';
  }}).join('');
}}

function renderAudit() {{
  const body = document.getElementById('audit-markets');
  const hint = document.getElementById('audit-hint');
  if (!auditMarkets || auditMarkets.length === 0) {{
    hint.textContent = 'audit_snapshot.json not found';
    body.innerHTML = '';
    return;
  }}
  hint.textContent = auditConfigSha
    ? ('audit_snapshot.json | config_sha256=' + shortSha(auditConfigSha))
    : 'audit_snapshot.json';
  body.innerHTML = auditMarkets.map((r) => {{
    const market = (r.market || '');
    const dataFile = (r.data_file || '');
    const dataSha = shortSha(r.data_sha256 || '');
    const indFile = (r.industry_file || '');
    const indSha = shortSha(r.industry_sha256 || '');
    const holFile = (r.holiday_file || '');
    const holSha = shortSha(r.holiday_sha256 || '');
    const indCell = indFile
      ? (indFile + '<div class=\"sub\">' + indSha + '</div>')
      : '<span class=\"sub\">-</span>';
    const holCell = holFile
      ? (holFile + '<div class=\"sub\">' + holSha + '</div>')
      : '<span class=\"sub\">-</span>';
    return '<tr>'
      + '<td>' + market + '</td>'
      + '<td class=\"sub\">' + dataFile + '</td>'
      + '<td class=\"sub\">' + dataSha + '</td>'
      + '<td class=\"sub\">' + indCell + '</td>'
      + '<td class=\"sub\">' + holCell + '</td>'
      + '</tr>';
  }}).join('');
}}

async function refreshFromFiles() {{
  try {{
    const [summaryResp, equityResp, tradesResp, rejResp, factorResp, auditResp, researchSummaryResp, researchSummaryResp2, researchJsonResp, researchJsonResp2, dqResp, dq2Resp, dqReportResp, auditJsonResp, registryResp, leaderboardResp] =
      await Promise.all([
      fetch('./summary.txt', {{ cache: 'no-store' }}),
      fetch('./equity_curve.csv', {{ cache: 'no-store' }}),
      fetch('./trades.csv', {{ cache: 'no-store' }}),
      fetch('./rejections.csv', {{ cache: 'no-store' }}),
      fetch('./factor_attribution_summary.txt', {{ cache: 'no-store' }}),
      fetch('./audit_snapshot_summary.txt', {{ cache: 'no-store' }}),
      fetch('./research_report_summary.txt', {{ cache: 'no-store' }}).catch(() => null),
      fetch('./research_report/research_report_summary.txt', {{ cache: 'no-store' }}).catch(() => null),
      fetch('./research_report.json', {{ cache: 'no-store' }}).catch(() => null),
      fetch('./research_report/research_report.json', {{ cache: 'no-store' }}).catch(() => null),
      fetch('./data_quality_summary.txt', {{ cache: 'no-store' }}).catch(() => null),
      fetch('./data_quality/data_quality_summary.txt', {{ cache: 'no-store' }}).catch(() => null),
      fetch('./data_quality_report.csv', {{ cache: 'no-store' }}).catch(() => null),
      fetch('./audit_snapshot.json', {{ cache: 'no-store' }}).catch(() => null),
      fetch(registryRefreshPath, {{ cache: 'no-store' }}).catch(() => null),
      fetch(leaderboardRefreshPath, {{ cache: 'no-store' }}).catch(() => null),
    ]);

    if (summaryResp.ok) {{
      const s = await summaryResp.text();
      summaryBlock.textContent = s;
      summaryKv = parseKv(s);
    }}
    if (auditResp.ok) {{
      const a = await auditResp.text();
      document.getElementById('audit-block').textContent = a;
    }}
    if (researchSummaryResp || researchSummaryResp2) {{
      const s = researchSummaryResp && researchSummaryResp.ok ? await researchSummaryResp.text() : '';
      const s2 = researchSummaryResp2 && researchSummaryResp2.ok ? await researchSummaryResp2.text() : '';
      const textSummary = s || s2 || '';
      document.getElementById('research-summary-block').textContent = textSummary || 'no research report summary';
      researchSummaryKv = parseKv(textSummary);
    }}
    const researchJsonSource = (researchJsonResp && researchJsonResp.ok)
      ? researchJsonResp
      : ((researchJsonResp2 && researchJsonResp2.ok) ? researchJsonResp2 : null);
    if (researchJsonSource) {{
      const t = await researchJsonSource.text();
      try {{
        const obj = JSON.parse(t);
        researchRegimeRows = obj.regime_rows || [];
        researchDecayRows = obj.factor_decay_rows || [];
        researchRollingRows = obj.rolling_ic_rows || [];
      }} catch (e) {{}}
    }}
    if (dqResp || dq2Resp) {{
      const t = dqResp && dqResp.ok ? await dqResp.text() : '';
      const t2 = dq2Resp && dq2Resp.ok ? await dq2Resp.text() : '';
      document.getElementById('data-quality-block').textContent = t || t2 || 'no data quality summary';
    }}
    if (dqReportResp && dqReportResp.ok) {{
      const t = await dqReportResp.text();
      const rows = parseCsv(t);
      dataQualityRows = rows.map((r) => {{
        const issues =
          (r.duplicate_rows && Number(r.duplicate_rows) > 0 ? ('dup=' + r.duplicate_rows + ' ') : '') +
          (r.invalid_close_rows && Number(r.invalid_close_rows) > 0 ? ('bad_close=' + r.invalid_close_rows + ' ') : '') +
          (r.invalid_volume_rows && Number(r.invalid_volume_rows) > 0 ? ('bad_vol=' + r.invalid_volume_rows + ' ') : '') +
          (r.date_order_violations && Number(r.date_order_violations) > 0 ? ('date_order=' + r.date_order_violations + ' ') : '') +
          (r.return_outliers && Number(r.return_outliers) > 0 ? ('outliers=' + r.return_outliers + ' ') : '') +
          (r.large_gaps && Number(r.large_gaps) > 0 ? ('gaps=' + r.large_gaps + ' ') : '') +
          (r.non_trading_day_rows && Number(r.non_trading_day_rows) > 0 ? ('non_trading=' + r.non_trading_day_rows + ' ') : '');
        return {{
          market: r.market || '',
          status: r.status || '',
          rows: Number(r.rows || 0),
          issues: issues.trim(),
        }};
      }});
    }}
    if (auditJsonResp && auditJsonResp.ok) {{
      const t = await auditJsonResp.text();
      try {{
        const obj = JSON.parse(t);
        auditConfigSha = obj.config_sha256 || '';
        auditMarkets = (obj.markets || []).map((m) => {{
          return {{
            market: m.market || '',
            currency: m.currency || '',
            fx_to_base: Number(m.fx_to_base || 0),
            data_file: (m.data_file && m.data_file.path) ? m.data_file.path : '',
            data_sha256: (m.data_file && m.data_file.sha256) ? m.data_file.sha256 : '',
            industry_file: (m.industry_file && m.industry_file.path) ? m.industry_file.path : '',
            industry_sha256: (m.industry_file && m.industry_file.sha256) ? m.industry_file.sha256 : '',
            holiday_file: (m.holiday_file && m.holiday_file.path) ? m.holiday_file.path : '',
            holiday_sha256: (m.holiday_file && m.holiday_file.sha256) ? m.holiday_file.sha256 : '',
          }};
        }});
      }} catch (e) {{}}
    }}
    if (equityResp.ok) {{
      const equityText = await equityResp.text();
      const rows = parseCsv(equityText);
      equityRows = rows.map(r => ({{
        date: r.date || '',
        equity: Number(r.equity || 0),
        cash: Number(r.cash || 0),
        gross_exposure: Number(r.gross_exposure || 0),
        net_exposure: Number(r.net_exposure || 0),
      }}));
      labels = equityRows.map(r => r.date || '');
      points = extractSeries(seriesSelect.value);
    }}
    if (tradesResp.ok) {{
      const tradesText = await tradesResp.text();
      const tradeRows = parseCsv(tradesText);
      trades = tradeRows.map((r) => ({{
        date: r.date || '',
        market: r.market || '',
        symbol: r.symbol || '',
        side: r.side || '',
        qty: Number(r.qty || 0),
        price: Number(r.price || 0),
        fees: Number(r.fees || 0),
      }}));
    }}
    if (rejResp.ok) {{
      const rejText = await rejResp.text();
      const rejRows = parseCsv(rejText);
      rejections = rejRows.map((r) => ({{
        date: r.date || '',
        market: r.market || '',
        symbol: r.symbol || '',
        side: r.side || '',
        qty: Number(r.qty || 0),
        reason: r.reason || '',
      }}));
    }}
    if (factorResp.ok) {{
      const f = await factorResp.text();
      factorKv = parseKv(f);
    }}
    if (registryResp && registryResp.ok) {{
      const csv = await registryResp.text();
      registryRows = parseRegistryRows(parseCsv(csv));
    }}
    if (leaderboardResp && leaderboardResp.ok) {{
      const csv = await leaderboardResp.text();
      leaderboardRows = parseLeaderboardRows(parseCsv(csv));
    }}

    const t = getText(langSwitch.value);
    liveStatus.textContent = t.live_on;
    liveDot.classList.add('ok');
    applyLanguage(langSwitch.value);
  }} catch (e) {{
    const t = getText(langSwitch.value);
    liveStatus.textContent = t.live_fallback;
    liveDot.classList.remove('ok');
  }}
}}

let activeLang = defaultLang;
if (!(activeLang in i18n)) {{
  activeLang = 'en';
}}
langSwitch.value = activeLang;
langSwitch.addEventListener('change', (event) => {{
  applyLanguage(event.target.value);
}});
seriesSelect.addEventListener('change', () => {{
  points = extractSeries(seriesSelect.value);
  applyLanguage(langSwitch.value);
}});
document.getElementById('research-decay-scope').addEventListener('change', () => renderResearchCharts(getText(langSwitch.value)));
document.getElementById('research-decay-metric').addEventListener('change', () => renderResearchCharts(getText(langSwitch.value)));
document.getElementById('research-rolling-horizon-select').addEventListener('change', () => renderResearchCharts(getText(langSwitch.value)));
document.getElementById('research-regime-market').addEventListener('change', () => renderRegime(getText(langSwitch.value)));
document.getElementById('strategy-time-select').addEventListener('change', () => renderStrategyComparison(getText(langSwitch.value)));
document.getElementById('leaderboard-source-select').addEventListener('change', () => renderPublicLeaderboard(getText(langSwitch.value)));
document.getElementById('leaderboard-time-select').addEventListener('change', () => renderPublicLeaderboard(getText(langSwitch.value)));
symbolInput.addEventListener('input', () => applyLanguage(langSwitch.value));
marketSel.addEventListener('change', () => applyLanguage(langSwitch.value));
sideSel.addEventListener('change', () => applyLanguage(langSwitch.value));

window.addEventListener('resize', () => applyLanguage(langSwitch.value));
liveStatus.textContent = getText(activeLang).live_init;
attachChartHover();
points = extractSeries(seriesSelect.value);
applyLanguage(activeLang);
setInterval(refreshFromFiles, 10000);
refreshFromFiles();
</script>
</body>
</html>
"#,
        html_lang = language.html_lang(),
        title = text.title,
        subtitle = text.subtitle,
        generated_from = text.generated_from,
        overview = text.overview,
        series = text.series,
        equity_curve = text.equity_curve,
        run_summary = text.run_summary,
        audit = text.audit,
        data_quality = text.data_quality,
        status = text.status,
        rows = text.rows,
        issues = text.issues,
        data_file = text.data_file,
        industry_file = text.industry_file,
        holiday_file = text.holiday_file,
        sha256 = text.sha256,
        recent_trades = text.recent_trades,
        date = text.date,
        market = text.market,
        symbol = text.symbol,
        side = text.side,
        qty = text.qty,
        price = text.price,
        fees = text.fees,
        runs_label = text.runs_label,
        rejections = text.rejections,
        reason = text.reason,
        factors = text.factors,
        strategy_comparison = text.strategy_comparison,
        public_leaderboard = text.public_leaderboard,
        plugin = text.plugin,
        method = text.method,
        source = text.source,
        time_range = text.time_range,
        all_time = text.all_time,
        recent_7d = text.recent_7d,
        recent_30d = text.recent_30d,
        scenario = text.scenario,
        score = text.score,
        command_label = text.command_label,
        rank = text.rank,
        avg_score = text.avg_score,
        best_score = text.best_score,
        avg_pnl_short = text.avg_pnl_short,
        avg_sharpe = text.avg_sharpe,
        top_runs = text.top_runs,
        composite_score = text.composite_score,
        latest_vs_selected = text.latest_vs_selected,
        run_details = text.run_details,
        time_label = text.time_label,
        notes_label = text.notes_label,
        compare_runs = text.compare_runs,
        baseline_run = text.baseline_run,
        candidate_run = text.candidate_run,
        output_dir_label = text.output_dir_label,
        copy_command_label = text.copy_command_label,
        compare_hint = text.compare_hint,
        recent_compare = text.recent_compare,
        research = text.research,
        decay_overview = text.decay_overview,
        rolling_ic = text.rolling_ic,
        regime_split = text.regime_split,
        horizon_days = text.horizon_days,
        ic_short = text.ic_short,
        spread = text.spread,
        scope = text.scope,
        metric = text.metric,
        observations = text.observations,
        composite_alpha = text.composite_alpha,
        regime_bucket = text.regime_bucket,
        summary_html = summary_html,
        research_summary_html = research_summary_html,
        audit_html = audit_html,
        data_quality_html = data_quality_html,
        equity_rows_json = equity_rows_json,
        trade_json = trade_json,
        rejection_json = rejection_json,
        summary_kv_json = summary_kv_json,
        factor_kv_json = factor_kv_json,
        registry_rows_json = registry_rows_json,
        strategy_compare_json = strategy_compare_json,
        research_summary_kv_json = research_summary_kv_json,
        research_regime_json = research_regime_json,
        research_decay_json = research_decay_json,
        research_rolling_json = research_rolling_json,
        data_quality_json = data_quality_json,
        audit_markets_json = audit_markets_json,
        audit_config_sha_json = audit_config_sha_json,
        recent_compare_json = recent_compare_json,
        registry_refresh_path_json = registry_refresh_path_json,
        i18n_json = i18n_json,
        default_lang_json = default_lang_json,
    );

    let share_html = render_share_dashboard(
        language,
        &summary_kv,
        &research_summary_kv,
        &strategy_compare_rows,
        &leaderboard_rows,
        &data_quality_rows,
    );
    let mut dashboard_path = output_dir.join("dashboard.html");
    fs::write(&dashboard_path, html)?;
    fs::write(output_dir.join("dashboard_share.html"), share_html)?;
    for theme in [
        ShareCoverTheme::Default,
        ShareCoverTheme::Github,
        ShareCoverTheme::X,
        ShareCoverTheme::Xiaohongshu,
    ] {
        let cover_html = render_share_cover_dashboard(
            language,
            &summary_kv,
            &research_summary_kv,
            &strategy_compare_rows,
            &leaderboard_rows,
            &data_quality_rows,
            theme,
        );
        fs::write(output_dir.join(theme.file_name()), cover_html)?;
    }
    dashboard_path = fs::canonicalize(dashboard_path)?;

    Ok(dashboard_path)
}

fn escape_html(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(ch),
        }
    }
    out
}

fn render_share_dashboard(
    language: Language,
    summary_kv: &serde_json::Value,
    research_summary_kv: &serde_json::Value,
    strategy_compare_rows: &[StrategyCompareRowUi],
    leaderboard_rows: &[LeaderboardRowUi],
    data_quality_rows: &[DataQualityRowUi],
) -> String {
    let text = dashboard_text(language);
    let end_equity = kv_string(summary_kv, "end_equity");
    let pnl_ratio = kv_string(summary_kv, "pnl_ratio");
    let sharpe = kv_string(summary_kv, "sharpe");
    let max_drawdown = kv_string(summary_kv, "max_drawdown");
    let best_decay_factor = kv_string(research_summary_kv, "best_decay_factor");
    let best_decay_horizon = kv_string(research_summary_kv, "best_decay_horizon_days");
    let best_decay_ic = kv_string(research_summary_kv, "best_decay_ic");
    let latest_rolling_factor = kv_string(research_summary_kv, "latest_rolling_factor");
    let latest_rolling_horizon = kv_string(research_summary_kv, "latest_rolling_horizon_days");
    let latest_rolling_ic = kv_string(research_summary_kv, "latest_rolling_ic");
    let top_combo = strategy_compare_rows.first();
    let top_leaderboard = leaderboard_rows.first();
    let pass = data_quality_rows
        .iter()
        .filter(|r| r.status == "PASS")
        .count();
    let warn = data_quality_rows
        .iter()
        .filter(|r| r.status == "WARN")
        .count();
    let fail = data_quality_rows
        .iter()
        .filter(|r| r.status == "FAIL")
        .count();
    let markets = data_quality_rows
        .iter()
        .map(|r| r.market.clone())
        .collect::<Vec<_>>()
        .join(" / ");
    let generated_at = chrono::Utc::now().format("%Y-%m-%d %H:%M UTC").to_string();

    let top_combo_html = if let Some(row) = top_combo {
        format!(
            "<div class=\"sub\">{plugin}: <strong>{}</strong></div>\
             <div class=\"sub\">{method}: <strong>{}</strong></div>\
             <div class=\"sub\">{best_score}: <strong>{:.3}</strong></div>\
             <div class=\"sub\">{avg_score}: <strong>{:.3}</strong></div>",
            escape_html(&row.strategy_plugin),
            escape_html(&row.portfolio_method),
            row.best_score,
            row.avg_score,
            plugin = text.plugin,
            method = text.method,
            best_score = text.best_score,
            avg_score = text.avg_score
        )
    } else {
        "<div class=\"sub\">run_registry.csv not found</div>".to_string()
    };

    let leaderboard_html = if leaderboard_rows.is_empty() {
        "<div class=\"sub\">leaderboard_public.csv not found</div>".to_string()
    } else {
        let rows = leaderboard_rows
            .iter()
            .take(5)
            .map(|row| {
                format!(
                    "<tr><td>{}</td><td>{}</td><td>{}</td><td>{:.3}</td></tr>",
                    row.rank,
                    escape_html(&row.source),
                    escape_html(&row.strategy_plugin),
                    row.score
                )
            })
            .collect::<Vec<_>>()
            .join("");
        format!(
            "<table><thead><tr><th>{rank}</th><th>{source}</th><th>{plugin}</th><th>{score}</th></tr></thead><tbody>{rows}</tbody></table>",
            rank = text.rank,
            source = text.source,
            plugin = text.plugin,
            score = text.score,
            rows = rows
        )
    };

    let headline = if let Some(row) = top_leaderboard {
        format!(
            "{} #{}, {}={:.3}, {}={}",
            text.public_leaderboard,
            row.rank,
            text.score,
            row.score,
            text.source,
            escape_html(&row.source)
        )
    } else {
        text.subtitle.to_string()
    };

    format!(
        r#"<!doctype html>
<html lang="{lang}">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>{title} Share</title>
<style>
html, body {{ -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
body {{ margin:0; font-family:"Avenir Next","Helvetica Neue",sans-serif; color:#102033; background:
  radial-gradient(900px 500px at 10% 10%, rgba(245,158,11,.18), transparent 60%),
  radial-gradient(900px 600px at 85% 15%, rgba(2,132,199,.16), transparent 58%),
  linear-gradient(180deg, #fffaf0 0%, #f0fdf4 100%); }}
.wrap {{ max-width: 1180px; margin: 0 auto; padding: 28px 18px 36px; }}
.export-bar {{ position: sticky; top: 0; z-index: 10; display:flex; align-items:center; justify-content:space-between; gap:12px; padding: 10px 0 16px; }}
.export-actions {{ display:flex; gap:10px; flex-wrap:wrap; }}
.export-btn {{ border:1px solid rgba(15,23,42,.10); background: rgba(255,255,255,.82); color:#102033; border-radius: 999px; padding: 10px 14px; font-size: 13px; font-weight: 700; text-decoration:none; cursor:pointer; }}
.hero {{ display:grid; grid-template-columns: 1.25fr .95fr; gap:16px; }}
.panel {{ background: rgba(255,255,255,.86); border:1px solid rgba(15,23,42,.10); border-radius: 24px; padding: 18px; box-shadow: 0 18px 40px rgba(15,23,42,.08); }}
.eyebrow {{ display:inline-flex; gap:8px; flex-wrap:wrap; margin-bottom:12px; }}
.chip {{ border:1px solid rgba(15,23,42,.10); border-radius:999px; padding:6px 10px; font-size:12px; background:rgba(255,255,255,.7); }}
.title {{ font-size: 34px; font-weight: 900; line-height: 1.05; margin-bottom: 10px; }}
.sub {{ color: rgba(16,32,51,.68); font-size: 14px; line-height: 1.5; }}
.metrics {{ display:grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap:12px; margin-top:16px; }}
.metric {{ background: rgba(255,255,255,.72); border:1px solid rgba(15,23,42,.10); border-radius:18px; padding:14px; }}
.k {{ color: rgba(16,32,51,.58); font-size:12px; }}
.v {{ font-size: 24px; font-weight: 900; margin-top: 6px; }}
.grid {{ display:grid; grid-template-columns: repeat(3, minmax(0,1fr)); gap:16px; margin-top:16px; }}
.section-title {{ font-size: 16px; font-weight: 800; margin-bottom: 10px; }}
.footer-note {{ color: rgba(16,32,51,.58); font-size: 12px; margin-top: 14px; }}
table {{ width:100%; border-collapse: collapse; font-size: 13px; }}
th, td {{ text-align:left; padding:8px; border-bottom:1px solid rgba(15,23,42,.08); }}
th {{ color: rgba(16,32,51,.58); }}
@page {{ size: A4 landscape; margin: 10mm; }}
@media print {{
  body {{ background: #ffffff; }}
  .export-bar {{ display:none; }}
  .wrap {{ max-width: none; padding: 0; }}
  .panel {{ box-shadow:none; background:#fff; break-inside: avoid; }}
  .hero, .grid {{ gap: 10px; }}
  .title {{ font-size: 30px; }}
}}
@media (max-width: 960px) {{
  .hero, .grid, .metrics {{ grid-template-columns: 1fr; }}
}}
</style>
</head>
<body>
  <div class="wrap">
    <div class="export-bar">
      <div class="sub">share-ready snapshot | generated {generated_at} | paper-only</div>
      <div class="export-actions">
        <button id="print-btn" class="export-btn" type="button">Print / Save PDF</button>
        <a class="export-btn" href="./dashboard.html">Open Full Dashboard</a>
      </div>
    </div>
    <div class="hero">
      <section class="panel">
        <div class="eyebrow">
          <span class="chip">{subtitle}</span>
          <span class="chip">{markets}</span>
          <span class="chip">paper-only</span>
        </div>
        <div class="title">{title}</div>
        <div class="sub">{headline}</div>
        <div class="metrics">
          <div class="metric"><div class="k">{kpi_end_equity}</div><div class="v">{end_equity}</div></div>
          <div class="metric"><div class="k">{kpi_pnl_ratio}</div><div class="v">{pnl_ratio}</div></div>
          <div class="metric"><div class="k">{kpi_sharpe}</div><div class="v">{sharpe}</div></div>
          <div class="metric"><div class="k">{kpi_max_drawdown}</div><div class="v">{max_drawdown}</div></div>
        </div>
      </section>
      <section class="panel">
        <div class="section-title">{research}</div>
        <div class="sub">{best_decay}: <strong>{best_decay_factor}</strong> / {best_decay_horizon}d / IC={best_decay_ic}</div>
        <div class="sub" style="margin-top:8px;">{latest_rolling}: <strong>{latest_rolling_factor}</strong> / {latest_rolling_horizon}d / IC={latest_rolling_ic}</div>
        <div class="sub" style="margin-top:14px;">{data_quality}: PASS={pass} | WARN={warn} | FAIL={fail}</div>
      </section>
    </div>

    <div class="grid">
      <section class="panel">
        <div class="section-title">{strategy_comparison}</div>
        {top_combo_html}
      </section>
      <section class="panel">
        <div class="section-title">{public_leaderboard}</div>
        {leaderboard_html}
      </section>
      <section class="panel">
        <div class="section-title">{overview}</div>
        <div class="sub">{generated_from}</div>
        <div class="sub" style="margin-top:8px;">dashboard.html</div>
        <div class="sub">dashboard_share.html</div>
        <div class="footer-note">Tip: use browser Print to export a clean PDF.</div>
      </section>
    </div>
  </div>
<script>
const printBtn = document.getElementById('print-btn');
if (printBtn) {{
  printBtn.addEventListener('click', () => window.print());
}}
</script>
</body>
</html>"#,
        lang = language.html_lang(),
        title = escape_html(text.title),
        subtitle = escape_html(text.subtitle),
        headline = headline,
        markets = if markets.is_empty() {
            "US / A-share / JP".to_string()
        } else {
            escape_html(&markets)
        },
        generated_from = escape_html(text.generated_from),
        overview = escape_html(text.overview),
        research = escape_html(text.research),
        strategy_comparison = escape_html(text.strategy_comparison),
        public_leaderboard = escape_html(text.public_leaderboard),
        data_quality = escape_html(text.data_quality),
        best_decay = escape_html(text.best_decay),
        latest_rolling = escape_html(text.latest_rolling),
        kpi_end_equity = escape_html(text.kpi_end_equity),
        kpi_pnl_ratio = escape_html(text.kpi_pnl_ratio),
        kpi_sharpe = escape_html(text.kpi_sharpe),
        kpi_max_drawdown = escape_html(text.kpi_max_drawdown),
        end_equity = escape_html(&end_equity),
        pnl_ratio = escape_html(&pnl_ratio),
        sharpe = escape_html(&sharpe),
        max_drawdown = escape_html(&max_drawdown),
        best_decay_factor = escape_html(&best_decay_factor),
        best_decay_horizon = escape_html(&best_decay_horizon),
        best_decay_ic = escape_html(&best_decay_ic),
        latest_rolling_factor = escape_html(&latest_rolling_factor),
        latest_rolling_horizon = escape_html(&latest_rolling_horizon),
        latest_rolling_ic = escape_html(&latest_rolling_ic),
        generated_at = escape_html(&generated_at),
        pass = pass,
        warn = warn,
        fail = fail,
        top_combo_html = top_combo_html,
        leaderboard_html = leaderboard_html
    )
}

#[derive(Clone, Copy)]
enum ShareCoverTheme {
    Default,
    Github,
    X,
    Xiaohongshu,
}

impl ShareCoverTheme {
    fn file_name(self) -> &'static str {
        match self {
            ShareCoverTheme::Default => "dashboard_cover.html",
            ShareCoverTheme::Github => "dashboard_cover_github.html",
            ShareCoverTheme::X => "dashboard_cover_x.html",
            ShareCoverTheme::Xiaohongshu => "dashboard_cover_xiaohongshu.html",
        }
    }

    fn badge(self) -> &'static str {
        match self {
            ShareCoverTheme::Default => "Studio Cover",
            ShareCoverTheme::Github => "GitHub Social Cover",
            ShareCoverTheme::X => "X Wide Cover",
            ShareCoverTheme::Xiaohongshu => "Xiaohongshu Poster",
        }
    }

    fn page_size(self) -> &'static str {
        match self {
            ShareCoverTheme::Default | ShareCoverTheme::Github => "1600px 900px",
            ShareCoverTheme::X => "1500px 840px",
            ShareCoverTheme::Xiaohongshu => "1242px 1660px",
        }
    }

    fn max_width(self) -> &'static str {
        match self {
            ShareCoverTheme::Default | ShareCoverTheme::Github => "1600px",
            ShareCoverTheme::X => "1500px",
            ShareCoverTheme::Xiaohongshu => "1242px",
        }
    }

    fn min_height(self) -> &'static str {
        match self {
            ShareCoverTheme::Xiaohongshu => "1660px",
            ShareCoverTheme::X => "840px",
            ShareCoverTheme::Default | ShareCoverTheme::Github => "900px",
        }
    }

    fn padding(self) -> &'static str {
        match self {
            ShareCoverTheme::Xiaohongshu => "56px",
            ShareCoverTheme::X => "36px",
            ShareCoverTheme::Default | ShareCoverTheme::Github => "40px",
        }
    }

    fn hero_columns(self) -> &'static str {
        match self {
            ShareCoverTheme::Xiaohongshu => "1fr",
            ShareCoverTheme::X => "1.35fr .65fr",
            ShareCoverTheme::Default | ShareCoverTheme::Github => "1.2fr .8fr",
        }
    }

    fn grid_columns(self) -> &'static str {
        match self {
            ShareCoverTheme::Xiaohongshu => "1fr",
            _ => "repeat(3, minmax(0,1fr))",
        }
    }

    fn title_size(self) -> &'static str {
        match self {
            ShareCoverTheme::Xiaohongshu => "74px",
            ShareCoverTheme::X => "52px",
            ShareCoverTheme::Default | ShareCoverTheme::Github => "58px",
        }
    }

    fn title_width(self) -> &'static str {
        match self {
            ShareCoverTheme::Xiaohongshu => "none",
            _ => "10ch",
        }
    }

    fn background(self) -> &'static str {
        match self {
            ShareCoverTheme::Default => {
                "radial-gradient(900px 520px at 8% 12%, rgba(245,158,11,.32), transparent 55%), radial-gradient(900px 600px at 84% 18%, rgba(14,165,233,.30), transparent 55%), linear-gradient(135deg, #0f172a 0%, #102033 52%, #132a42 100%)"
            }
            ShareCoverTheme::Github => {
                "radial-gradient(980px 560px at 10% 10%, rgba(34,197,94,.28), transparent 55%), radial-gradient(860px 600px at 88% 14%, rgba(59,130,246,.28), transparent 58%), linear-gradient(135deg, #0b1220 0%, #111827 48%, #18263d 100%)"
            }
            ShareCoverTheme::X => {
                "radial-gradient(760px 420px at 8% 16%, rgba(249,115,22,.28), transparent 52%), radial-gradient(860px 480px at 80% 24%, rgba(236,72,153,.22), transparent 56%), linear-gradient(135deg, #111827 0%, #1f2937 46%, #0f172a 100%)"
            }
            ShareCoverTheme::Xiaohongshu => {
                "radial-gradient(820px 580px at 14% 12%, rgba(251,113,133,.28), transparent 56%), radial-gradient(900px 640px at 82% 16%, rgba(45,212,191,.20), transparent 58%), linear-gradient(180deg, #1f1633 0%, #2d1f4d 42%, #111827 100%)"
            }
        }
    }

    fn footer_note(self) -> &'static str {
        match self {
            ShareCoverTheme::Default => "Open the full analytics in dashboard.html",
            ShareCoverTheme::Github => "Built for README hero shots and repo social previews",
            ShareCoverTheme::X => "Built for fast reposting and wide social previews",
            ShareCoverTheme::Xiaohongshu => "Built for poster-style sharing and mobile screenshots",
        }
    }
}

fn render_share_cover_dashboard(
    language: Language,
    summary_kv: &serde_json::Value,
    research_summary_kv: &serde_json::Value,
    strategy_compare_rows: &[StrategyCompareRowUi],
    leaderboard_rows: &[LeaderboardRowUi],
    data_quality_rows: &[DataQualityRowUi],
    theme: ShareCoverTheme,
) -> String {
    let text = dashboard_text(language);
    let pnl_ratio = kv_string(summary_kv, "pnl_ratio");
    let sharpe = kv_string(summary_kv, "sharpe");
    let max_drawdown = kv_string(summary_kv, "max_drawdown");
    let end_equity = kv_string(summary_kv, "end_equity");
    let best_decay_factor = kv_string(research_summary_kv, "best_decay_factor");
    let best_decay_horizon = kv_string(research_summary_kv, "best_decay_horizon_days");
    let best_decay_ic = kv_string(research_summary_kv, "best_decay_ic");
    let latest_rolling_factor = kv_string(research_summary_kv, "latest_rolling_factor");
    let latest_rolling_horizon = kv_string(research_summary_kv, "latest_rolling_horizon_days");
    let latest_rolling_ic = kv_string(research_summary_kv, "latest_rolling_ic");
    let top_combo = strategy_compare_rows.first();
    let top_leaderboard = leaderboard_rows.first();
    let markets = data_quality_rows
        .iter()
        .map(|r| r.market.clone())
        .collect::<Vec<_>>()
        .join(" / ");
    let cover_sub = if let Some(row) = top_combo {
        format!(
            "{}: {} / {} | {}={:.3}",
            text.strategy_comparison,
            escape_html(&row.strategy_plugin),
            escape_html(&row.portfolio_method),
            text.best_score,
            row.best_score
        )
    } else {
        "paper-only multi-market research stack".to_string()
    };
    let leaderboard_note = if let Some(row) = top_leaderboard {
        format!(
            "{} #{} | {}={:.3} | {}={}",
            text.public_leaderboard,
            row.rank,
            text.score,
            row.score,
            text.source,
            escape_html(&row.source)
        )
    } else {
        text.public_leaderboard.to_string()
    };

    format!(
        r#"<!doctype html>
<html lang="{lang}">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>{title} Cover</title>
<style>
html, body {{ margin:0; padding:0; -webkit-print-color-adjust: exact; print-color-adjust: exact; }}
body {{
  font-family: "Avenir Next", "Helvetica Neue", sans-serif;
  color: #f8fafc;
  background: {background};
}}
.cover {{ width: 100%; max-width: {max_width}; min-height: {min_height}; margin: 0 auto; padding: {padding}; display:grid; grid-template-rows: auto 1fr auto; gap: 22px; box-sizing: border-box; }}
.chips {{ display:flex; gap:10px; flex-wrap:wrap; }}
.chip {{ border:1px solid rgba(255,255,255,.18); background: rgba(255,255,255,.08); border-radius:999px; padding:8px 12px; font-size:13px; }}
.hero {{ display:grid; grid-template-columns: {hero_columns}; gap: 22px; align-items:stretch; }}
.title {{ font-size: {title_size}; font-weight: 900; line-height: .94; letter-spacing: -0.03em; max-width: {title_width}; }}
.subtitle {{ margin-top: 16px; color: rgba(241,245,249,.76); font-size: 17px; max-width: 48ch; line-height: 1.45; }}
.panel {{ background: rgba(255,255,255,.08); border:1px solid rgba(255,255,255,.12); border-radius: 28px; padding: 24px; backdrop-filter: blur(10px); }}
.metrics {{ display:grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap:12px; }}
.metric {{ background: rgba(255,255,255,.07); border-radius: 18px; padding: 16px; }}
.k {{ color: rgba(241,245,249,.68); font-size: 12px; text-transform: uppercase; letter-spacing: .08em; }}
.v {{ font-size: 28px; font-weight: 900; margin-top: 8px; }}
.grid {{ display:grid; grid-template-columns: {grid_columns}; gap: 18px; }}
.label {{ color: rgba(241,245,249,.65); font-size: 12px; text-transform: uppercase; letter-spacing: .08em; margin-bottom: 10px; }}
.main {{ font-size: 20px; font-weight: 800; line-height: 1.3; }}
.sub {{ color: rgba(241,245,249,.72); font-size: 14px; line-height: 1.45; margin-top: 8px; }}
.footer {{ display:flex; justify-content:space-between; gap:14px; align-items:flex-end; color: rgba(241,245,249,.66); font-size: 13px; }}
@page {{ size: {page_size}; margin: 0; }}
@media print {{
  .cover {{ min-height: auto; height: 100vh; }}
}}
@media (max-width: 1100px) {{
  .hero, .grid {{ grid-template-columns: 1fr; }}
  .title {{ max-width: none; font-size: 42px; }}
}}
</style>
</head>
<body>
  <main class="cover">
    <div class="chips">
      <span class="chip">{subtitle}</span>
      <span class="chip">{theme_badge}</span>
      <span class="chip">{markets}</span>
      <span class="chip">paper-only</span>
      <span class="chip">{file_name}</span>
    </div>

    <section class="hero">
      <div class="panel">
        <div class="title">{title}</div>
        <div class="subtitle">{cover_sub}</div>
      </div>
      <div class="panel">
        <div class="metrics">
          <div class="metric"><div class="k">{kpi_pnl_ratio}</div><div class="v">{pnl_ratio}</div></div>
          <div class="metric"><div class="k">{kpi_sharpe}</div><div class="v">{sharpe}</div></div>
          <div class="metric"><div class="k">{kpi_max_drawdown}</div><div class="v">{max_drawdown}</div></div>
          <div class="metric"><div class="k">{kpi_end_equity}</div><div class="v">{end_equity}</div></div>
        </div>
      </div>
    </section>

    <section class="grid">
      <div class="panel">
        <div class="label">{research}</div>
        <div class="main">{best_decay_factor} / {best_decay_horizon}d / IC={best_decay_ic}</div>
        <div class="sub">{best_decay}</div>
      </div>
      <div class="panel">
        <div class="label">{rolling_ic}</div>
        <div class="main">{latest_rolling_factor} / {latest_rolling_horizon}d / IC={latest_rolling_ic}</div>
        <div class="sub">{latest_rolling}</div>
      </div>
      <div class="panel">
        <div class="label">{public_leaderboard}</div>
        <div class="main">{leaderboard_note}</div>
        <div class="sub">{generated_from}</div>
      </div>
    </section>

    <div class="footer">
      <div>{title} | Rust paper trading research stack</div>
      <div>{footer_note}</div>
    </div>
  </main>
</body>
</html>"#,
        lang = language.html_lang(),
        background = theme.background(),
        max_width = theme.max_width(),
        min_height = theme.min_height(),
        padding = theme.padding(),
        hero_columns = theme.hero_columns(),
        title_size = theme.title_size(),
        title_width = theme.title_width(),
        grid_columns = theme.grid_columns(),
        page_size = theme.page_size(),
        title = escape_html(text.title),
        subtitle = escape_html(text.subtitle),
        theme_badge = theme.badge(),
        markets = if markets.is_empty() {
            "US / A-share / JP".to_string()
        } else {
            escape_html(&markets)
        },
        file_name = theme.file_name(),
        cover_sub = cover_sub,
        research = escape_html(text.research),
        rolling_ic = escape_html(text.rolling_ic),
        best_decay = escape_html(text.best_decay),
        latest_rolling = escape_html(text.latest_rolling),
        public_leaderboard = escape_html(text.public_leaderboard),
        generated_from = escape_html(text.generated_from),
        leaderboard_note = leaderboard_note,
        kpi_pnl_ratio = escape_html(text.kpi_pnl_ratio),
        kpi_sharpe = escape_html(text.kpi_sharpe),
        kpi_max_drawdown = escape_html(text.kpi_max_drawdown),
        kpi_end_equity = escape_html(text.kpi_end_equity),
        pnl_ratio = escape_html(&pnl_ratio),
        sharpe = escape_html(&sharpe),
        max_drawdown = escape_html(&max_drawdown),
        end_equity = escape_html(&end_equity),
        best_decay_factor = escape_html(&best_decay_factor),
        best_decay_horizon = escape_html(&best_decay_horizon),
        best_decay_ic = escape_html(&best_decay_ic),
        latest_rolling_factor = escape_html(&latest_rolling_factor),
        latest_rolling_horizon = escape_html(&latest_rolling_horizon),
        latest_rolling_ic = escape_html(&latest_rolling_ic),
        footer_note = theme.footer_note(),
    )
}

fn kv_string(value: &serde_json::Value, key: &str) -> String {
    value
        .get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("-")
        .to_string()
}

fn discover_recent_compare(output_dir: &Path) -> Option<RecentCompareUi> {
    let mut candidate_dirs = Vec::<PathBuf>::new();
    if output_dir.join("compare_report.json").exists() {
        candidate_dirs.push(output_dir.to_path_buf());
    }
    if let Ok(read_dir) = fs::read_dir(output_dir) {
        for entry in read_dir.flatten() {
            let path = entry.path();
            if path.is_dir() && path.join("compare_report.json").exists() {
                candidate_dirs.push(path);
            }
        }
    }
    if let Some(parent) = output_dir.parent() {
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
            .unwrap_or(SystemTime::UNIX_EPOCH)
    })?;

    let report_text = fs::read_to_string(latest_dir.join("compare_report.json")).ok()?;
    let report: CompareReportCompat = serde_json::from_str(&report_text).ok()?;
    Some(RecentCompareUi {
        output_dir: latest_dir.display().to_string(),
        html_href: relative_href(output_dir, &latest_dir.join("compare_report.html")),
        json_href: relative_href(output_dir, &latest_dir.join("compare_report.json")),
        baseline_dir: report.baseline_dir,
        candidate_dir: report.candidate_dir,
        metric_changes: report.metric_rows.iter().filter(|r| r.changed).count(),
        audit_changes: report.audit_rows.iter().filter(|r| r.changed).count(),
        data_quality_changes: report
            .data_quality_rows
            .iter()
            .filter(|r| r.changed)
            .count(),
    })
}

fn relative_href(from_dir: &Path, to_path: &Path) -> String {
    let from_components: Vec<_> = from_dir.components().collect();
    let to_components: Vec<_> = to_path.components().collect();
    let mut common = 0usize;
    while common < from_components.len()
        && common < to_components.len()
        && from_components[common] == to_components[common]
    {
        common += 1;
    }
    let mut out = PathBuf::new();
    for _ in common..from_components.len() {
        out.push("..");
    }
    for comp in &to_components[common..] {
        out.push(comp.as_os_str());
    }
    let s = out.to_string_lossy().replace('\\', "/");
    if s.is_empty() {
        ".".to_string()
    } else {
        s
    }
}

fn read_equity_rows(path: &Path) -> Result<Vec<EquityRow>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    let mut rdr = csv::Reader::from_path(path)?;
    for rec in rdr.records() {
        let rec = rec?;
        let date = rec.get(0).unwrap_or_default().to_string();
        let equity = rec.get(1).unwrap_or("0").parse::<f64>().unwrap_or(0.0);
        let cash = rec.get(2).unwrap_or("0").parse::<f64>().unwrap_or(0.0);
        let gross_exposure = rec.get(3).unwrap_or("0").parse::<f64>().unwrap_or(0.0);
        let net_exposure = rec.get(4).unwrap_or("0").parse::<f64>().unwrap_or(0.0);
        rows.push(EquityRow {
            date,
            equity,
            cash,
            gross_exposure,
            net_exposure,
        });
    }
    Ok(rows)
}

fn read_trade_rows(path: &Path) -> Result<Vec<TradeRow>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    let mut rdr = csv::Reader::from_path(path)?;
    for rec in rdr.records() {
        let rec = rec?;
        rows.push(TradeRow {
            date: rec.get(0).unwrap_or_default().to_string(),
            market: rec.get(1).unwrap_or_default().to_string(),
            symbol: rec.get(2).unwrap_or_default().to_string(),
            side: rec.get(3).unwrap_or_default().to_string(),
            qty: rec.get(4).unwrap_or("0").parse::<i64>().unwrap_or(0),
            price: rec.get(5).unwrap_or("0").parse::<f64>().unwrap_or(0.0),
            fees: rec.get(6).unwrap_or("0").parse::<f64>().unwrap_or(0.0),
        });
    }
    Ok(rows)
}

fn read_rejection_rows(path: &Path) -> Result<Vec<RejectionRow>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    let mut rdr = csv::Reader::from_path(path)?;
    for rec in rdr.records() {
        let rec = rec?;
        rows.push(RejectionRow {
            date: rec.get(0).unwrap_or_default().to_string(),
            market: rec.get(1).unwrap_or_default().to_string(),
            symbol: rec.get(2).unwrap_or_default().to_string(),
            side: rec.get(3).unwrap_or_default().to_string(),
            qty: rec.get(4).unwrap_or("0").parse::<i64>().unwrap_or(0),
            reason: rec.get(5).unwrap_or_default().to_string(),
        });
    }
    Ok(rows)
}

fn parse_kv_lines(text: &str) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        map.insert(
            k.trim().to_string(),
            serde_json::Value::String(v.trim().to_string()),
        );
    }
    serde_json::Value::Object(map)
}

fn read_data_quality_rows(path: &Path) -> Result<Vec<DataQualityRowUi>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut rdr = csv::Reader::from_path(path)?;
    let headers = rdr
        .headers()
        .map(|h| h.iter().map(|s| s.to_string()).collect::<Vec<_>>())
        .unwrap_or_default();

    let idx = |name: &str| headers.iter().position(|h| h == name);
    let n64 = |s: &str| s.parse::<i64>().unwrap_or(0);
    fn get(rec: &csv::StringRecord, i: Option<usize>) -> &str {
        i.and_then(|x| rec.get(x)).unwrap_or("")
    }

    let mut out = Vec::new();
    for rec in rdr.records() {
        let rec = rec?;
        let market = get(&rec, idx("market")).to_string();
        let status = get(&rec, idx("status")).to_string();
        let rows = n64(get(&rec, idx("rows")));
        let unique_symbols = n64(get(&rec, idx("unique_symbols")));
        let duplicate_rows = n64(get(&rec, idx("duplicate_rows")));
        let invalid_close_rows = n64(get(&rec, idx("invalid_close_rows")));
        let invalid_volume_rows = n64(get(&rec, idx("invalid_volume_rows")));
        let date_order_violations = n64(get(&rec, idx("date_order_violations")));
        let return_outliers = n64(get(&rec, idx("return_outliers")));
        let large_gaps = n64(get(&rec, idx("large_gaps")));
        let non_trading_day_rows = n64(get(&rec, idx("non_trading_day_rows")));

        let mut issues = Vec::new();
        if duplicate_rows > 0 {
            issues.push(format!("dup={duplicate_rows}"));
        }
        if invalid_close_rows > 0 {
            issues.push(format!("bad_close={invalid_close_rows}"));
        }
        if invalid_volume_rows > 0 {
            issues.push(format!("bad_vol={invalid_volume_rows}"));
        }
        if date_order_violations > 0 {
            issues.push(format!("date_order={date_order_violations}"));
        }
        if return_outliers > 0 {
            issues.push(format!("outliers={return_outliers}"));
        }
        if large_gaps > 0 {
            issues.push(format!("gaps={large_gaps}"));
        }
        if non_trading_day_rows > 0 {
            issues.push(format!("non_trading={non_trading_day_rows}"));
        }

        out.push(DataQualityRowUi {
            market,
            rows,
            unique_symbols,
            duplicate_rows,
            invalid_close_rows,
            invalid_volume_rows,
            date_order_violations,
            return_outliers,
            large_gaps,
            non_trading_day_rows,
            status,
            issues: issues.join(" "),
        });
    }
    out.sort_by(|a, b| a.market.cmp(&b.market));
    Ok(out)
}

fn read_audit_snapshot(path: &Path) -> (String, Vec<AuditMarketUi>) {
    let Ok(s) = fs::read_to_string(path) else {
        return (String::new(), Vec::new());
    };
    let snap: AuditSnapshotCompat = serde_json::from_str(&s).unwrap_or_default();
    let mut out = Vec::new();
    for m in snap.markets {
        let industry_file = m
            .industry_file
            .as_ref()
            .map(|f| f.path.clone())
            .unwrap_or_default();
        let industry_sha256 = m
            .industry_file
            .as_ref()
            .map(|f| f.sha256.clone())
            .unwrap_or_default();
        let holiday_file = m
            .holiday_file
            .as_ref()
            .map(|f| f.path.clone())
            .unwrap_or_default();
        let holiday_sha256 = m
            .holiday_file
            .as_ref()
            .map(|f| f.sha256.clone())
            .unwrap_or_default();
        out.push(AuditMarketUi {
            market: m.market,
            currency: m.currency,
            fx_to_base: m.fx_to_base,
            data_file: m.data_file.path,
            data_sha256: m.data_file.sha256,
            industry_file,
            industry_sha256,
            holiday_file,
            holiday_sha256,
        });
    }
    out.sort_by(|a, b| a.market.cmp(&b.market));
    (snap.config_sha256, out)
}

fn read_research_report(
    path: &Path,
) -> (
    Vec<RegimeSplitRowUi>,
    Vec<FactorDecayRowUi>,
    Vec<RollingIcRowUi>,
) {
    let Ok(s) = fs::read_to_string(path) else {
        return (Vec::new(), Vec::new(), Vec::new());
    };
    let report: ResearchReportCompat = serde_json::from_str(&s).unwrap_or_default();
    (
        report.regime_rows,
        report.factor_decay_rows,
        report.rolling_ic_rows,
    )
}

fn read_registry_rows(path: &Path) -> Result<Vec<RunRegistryEntry>> {
    let mut rows = read_run_registry(path)?;
    rows.retain(|r| !r.strategy_plugin.is_empty() || !r.portfolio_method.is_empty());
    Ok(rows)
}

fn build_strategy_compare_rows(rows: &[RunRegistryEntry]) -> Vec<StrategyCompareRowUi> {
    let mut grouped: std::collections::BTreeMap<(String, String), Vec<&RunRegistryEntry>> =
        std::collections::BTreeMap::new();
    for row in rows {
        let plugin = if row.strategy_plugin.is_empty() {
            "-".to_string()
        } else {
            row.strategy_plugin.clone()
        };
        let method = if row.portfolio_method.is_empty() {
            "-".to_string()
        } else {
            row.portfolio_method.clone()
        };
        grouped.entry((plugin, method)).or_default().push(row);
    }

    let mut out = grouped
        .into_iter()
        .map(|((strategy_plugin, portfolio_method), group)| {
            let runs = group.len();
            let avg_score =
                group.iter().map(|r| r.composite_score).sum::<f64>() / runs.max(1) as f64;
            let best_score = group
                .iter()
                .map(|r| r.composite_score)
                .fold(f64::NEG_INFINITY, f64::max);
            let avg_pnl_ratio = group.iter().map(|r| r.pnl_ratio).sum::<f64>() / runs.max(1) as f64;
            let avg_max_drawdown =
                group.iter().map(|r| r.max_drawdown).sum::<f64>() / runs.max(1) as f64;
            let avg_sharpe = group.iter().map(|r| r.sharpe).sum::<f64>() / runs.max(1) as f64;
            StrategyCompareRowUi {
                strategy_plugin,
                portfolio_method,
                runs,
                avg_score,
                best_score,
                avg_pnl_ratio,
                avg_max_drawdown,
                avg_sharpe,
            }
        })
        .collect::<Vec<_>>();

    out.sort_by(|a, b| {
        b.best_score
            .total_cmp(&a.best_score)
            .then_with(|| b.avg_score.total_cmp(&a.avg_score))
            .then_with(|| a.strategy_plugin.cmp(&b.strategy_plugin))
            .then_with(|| a.portfolio_method.cmp(&b.portfolio_method))
    });
    out
}

fn read_leaderboard_rows(path: &Path) -> Result<Vec<LeaderboardRowUi>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let mut rdr = csv::Reader::from_path(path)?;
    for rec in rdr.deserialize() {
        out.push(rec?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::build_dashboard_with_language;
    use crate::i18n::Language;

    #[test]
    fn build_dashboard_escapes_summary_and_sets_lang_tag() {
        let output_dir = make_temp_output_dir("ui_lang_escape");
        fs::write(
            output_dir.join("summary.txt"),
            "<script>alert('x')</script>\nline2",
        )
        .expect("write summary");
        fs::write(
            output_dir.join("equity_curve.csv"),
            "date,equity\n2026-01-01,1.0\n2026-01-02,1.2\n",
        )
        .expect("write equity");
        fs::write(
            output_dir.join("trades.csv"),
            "date,market,symbol,side,qty,price\n2026-01-02,US,AAPL,BUY,1,100.0\n",
        )
        .expect("write trades");

        let path =
            build_dashboard_with_language(&output_dir, Language::Zh).expect("build dashboard");
        let html = fs::read_to_string(path).expect("read dashboard");
        assert!(html.contains(r#"<html lang="zh-CN">"#));
        assert!(html.contains("&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt;"));
    }

    #[test]
    fn build_dashboard_embeds_research_section() {
        let output_dir = make_temp_output_dir("ui_research_embed");
        fs::write(output_dir.join("summary.txt"), "pnl=1.0\n").expect("write summary");
        fs::write(
            output_dir.join("equity_curve.csv"),
            "date,equity,cash,gross_exposure,net_exposure\n2026-01-01,1.0,1.0,0.0,0.0\n2026-01-02,1.2,1.1,0.2,0.1\n",
        )
        .expect("write equity");
        fs::write(
            output_dir.join("research_report_summary.txt"),
            "folds=3\navg_test_sharpe=1.2345\nbest_decay_factor=momentum\nbest_decay_horizon_days=5\nbest_decay_ic=0.2222\nlatest_rolling_factor=volume\nlatest_rolling_horizon_days=3\nlatest_rolling_ic=0.1111\n",
        )
        .expect("write research summary");
        fs::write(
            output_dir.join("research_report.json"),
            r#"{
  "regime_rows":[
    {"market":"US","regime_bucket":"trend_up_low_vol","observations":12,"avg_factor_momentum":0.12,"avg_factor_mean_reversion":-0.01,"avg_factor_low_vol":0.08,"avg_factor_volume":0.03,"avg_composite_alpha":0.09,"avg_selected_symbols":4.0}
  ],
  "factor_decay_rows":[
    {"scope":"ALL","factor":"momentum","horizon_days":5,"observations":10,"ic":0.2222,"top_quintile_avg_return":0.01,"bottom_quintile_avg_return":-0.01,"long_short_spread":0.02}
  ],
  "rolling_ic_rows":[
    {"date":"2026-01-02","factor":"volume","horizon_days":3,"observations":9,"ic":0.1111}
  ]
}"#,
        )
        .expect("write research json");
        fs::write(
            output_dir.join("run_registry.csv"),
            "run_id,timestamp_utc,command,output_dir,strategy_plugin,portfolio_method,markets,primary_metric_name,primary_metric_value,composite_score,pnl_ratio,max_drawdown,sharpe,sortino,calmar,trades,rejections,notes\nrun-1,2026-01-02T00:00:00Z,run,outputs_rust,my_alpha,risk_parity,US|JP,end_equity,100.0,1.2345,0.1000,0.0500,1.2000,1.3000,1.4000,12,0,first\nrun-2,2026-01-03T00:00:00Z,run,outputs_rust,my_alpha,hrp,US|JP,end_equity,101.0,1.4567,0.1200,0.0400,1.5000,1.6000,1.7000,10,1,second\n",
        )
        .expect("write registry");
        fs::create_dir_all(output_dir.join("leaderboard")).expect("leaderboard dir");
        fs::write(
            output_dir.join("leaderboard").join("leaderboard_public.csv"),
            "rank,source,timestamp_utc,command,scenario,strategy_plugin,portfolio_method,score,pnl_ratio,max_drawdown,sharpe,notes\n1,registry,2026-01-03T00:00:00Z,run,default,my_alpha,hrp,1.4567,0.1200,0.0400,1.5000,top row\n2,research,,research,walk_forward,my_alpha,risk_parity,1.2222,0.0900,0.0500,1.2000,research row\n",
        )
        .expect("write leaderboard");
        let compare_dir = output_dir.join("compare_demo");
        fs::create_dir_all(&compare_dir).expect("compare dir");
        fs::write(
            compare_dir.join("compare_report.json"),
            r#"{
  "baseline_dir":"outputs_rust/run_a",
  "candidate_dir":"outputs_rust/run_b",
  "metric_rows":[{"changed":true},{"changed":false}],
  "audit_rows":[{"changed":true}],
  "data_quality_rows":[{"changed":false},{"changed":true}]
}"#,
        )
        .expect("write compare json");
        fs::write(
            compare_dir.join("compare_report.html"),
            "<html><body>compare html</body></html>",
        )
        .expect("write compare html");

        let path =
            build_dashboard_with_language(&output_dir, Language::En).expect("build dashboard");
        let html = fs::read_to_string(path).expect("read dashboard");
        let share_html =
            fs::read_to_string(output_dir.join("dashboard_share.html")).expect("read share");
        let cover_html =
            fs::read_to_string(output_dir.join("dashboard_cover.html")).expect("read cover");
        let github_cover_html = fs::read_to_string(output_dir.join("dashboard_cover_github.html"))
            .expect("read github cover");
        let x_cover_html =
            fs::read_to_string(output_dir.join("dashboard_cover_x.html")).expect("read x cover");
        let xiaohongshu_cover_html =
            fs::read_to_string(output_dir.join("dashboard_cover_xiaohongshu.html"))
                .expect("read xiaohongshu cover");
        assert!(html.contains("Research"));
        assert!(html.contains("Strategy Comparison"));
        assert!(html.contains("Public Leaderboard"));
        assert!(html.contains("strategy-detail-rows"));
        assert!(html.contains("compare-baseline-select"));
        assert!(html.contains("compare-candidate-select"));
        assert!(html.contains("compare-copy-btn"));
        assert!(html.contains("recent-compare-block"));
        assert!(html.contains("leaderboard-detail-rows"));
        assert!(html.contains("researchDecayRows"));
        assert!(html.contains("registryRows"));
        assert!(html.contains("leaderboardRows"));
        assert!(html.contains("renderCompareShortcut"));
        assert!(html.contains("renderRecentCompare"));
        assert!(html.contains("cargo run --bin compare -- --baseline-dir"));
        assert!(html.contains("compare_demo/compare_report.html"));
        assert!(html.contains("Metric Changes"));
        assert!(html.contains("momentum"));
        assert!(html.contains("researchRollingRows"));
        assert!(html.contains("research-decay-chart"));
        assert!(html.contains("research-rolling-chart"));
        assert!(html.contains("research-regime-cards"));
        assert!(html.contains("trend_up_low_vol"));
        assert!(html.contains("my_alpha"));
        assert!(share_html.contains("dashboard_share.html"));
        assert!(share_html.contains("Public Leaderboard"));
        assert!(share_html.contains("paper-only"));
        assert!(share_html.contains("@media print"));
        assert!(share_html.contains("window.print()"));
        assert!(cover_html.contains("dashboard_cover.html"));
        assert!(cover_html.contains("paper-only"));
        assert!(cover_html.contains("Studio Cover"));
        assert!(cover_html.contains("Open the full analytics in dashboard.html"));
        assert!(github_cover_html.contains("dashboard_cover_github.html"));
        assert!(github_cover_html.contains("GitHub Social Cover"));
        assert!(github_cover_html.contains("Built for README hero shots and repo social previews"));
        assert!(x_cover_html.contains("dashboard_cover_x.html"));
        assert!(x_cover_html.contains("X Wide Cover"));
        assert!(x_cover_html.contains("Built for fast reposting and wide social previews"));
        assert!(xiaohongshu_cover_html.contains("dashboard_cover_xiaohongshu.html"));
        assert!(xiaohongshu_cover_html.contains("Xiaohongshu Poster"));
        assert!(xiaohongshu_cover_html
            .contains("Built for poster-style sharing and mobile screenshots"));
    }

    fn make_temp_output_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("private_quant_bot_{prefix}_{nanos}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }
}
