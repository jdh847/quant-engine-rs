use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::i18n::{dashboard_text, DashboardText, Language};

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

#[derive(Debug, Clone, Default, Deserialize)]
struct ResearchReportCompat {
    #[serde(default)]
    factor_decay_rows: Vec<FactorDecayRowUi>,
    #[serde(default)]
    rolling_ic_rows: Vec<RollingIcRowUi>,
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
    rejections: String,
    reason: String,
    factors: String,
    research: String,
    decay_overview: String,
    rolling_ic: String,
    folds: String,
    avg_test_sharpe_short: String,
    best_decay: String,
    latest_rolling: String,
    horizon_days: String,
    ic_short: String,
    spread: String,
    scope: String,
    metric: String,
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
        rejections: t.rejections.to_string(),
        reason: t.reason.to_string(),
        factors: t.factors.to_string(),
        research: t.research.to_string(),
        decay_overview: t.decay_overview.to_string(),
        rolling_ic: t.rolling_ic.to_string(),
        folds: t.folds.to_string(),
        avg_test_sharpe_short: t.avg_test_sharpe_short.to_string(),
        best_decay: t.best_decay.to_string(),
        latest_rolling: t.latest_rolling.to_string(),
        horizon_days: t.horizon_days.to_string(),
        ic_short: t.ic_short.to_string(),
        spread: t.spread.to_string(),
        scope: t.scope.to_string(),
        metric: t.metric.to_string(),
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
    let (research_decay_rows, research_rolling_rows) = read_research_report(&research_json_path);

    let trade_json = serde_json::to_string(&trade_rows)?;
    let rejection_json = serde_json::to_string(&rejection_rows)?;
    let equity_rows_json = serde_json::to_string(&equity_rows)?;
    let summary_kv_json = serde_json::to_string(&summary_kv)?;
    let factor_kv_json = serde_json::to_string(&factor_kv)?;
    let research_summary_kv_json = serde_json::to_string(&research_summary_kv)?;
    let research_decay_json = serde_json::to_string(&research_decay_rows)?;
    let research_rolling_json = serde_json::to_string(&research_rolling_rows)?;
    let data_quality_json = serde_json::to_string(&data_quality_rows)?;
    let audit_markets_json = serde_json::to_string(&audit_markets)?;
    let audit_config_sha_json = serde_json::to_string(&audit_config_sha)?;
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
@keyframes rise {{ from {{ transform: translateY(8px); opacity: 0; }} to {{ transform: translateY(0); opacity: 1; }} }}
@media (max-width: 960px) {{
  .grid {{ grid-template-columns: 1fr; }}
  #chart {{ height: 280px; }}
  .filters {{ grid-template-columns: 1fr; }}
  .bar-row {{ grid-template-columns: 120px 1fr 56px; }}
  .mini-grid {{ grid-template-columns: 1fr; }}
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
let researchSummaryKv = {research_summary_kv_json};
let researchDecayRows = {research_decay_json};
let researchRollingRows = {research_rolling_json};
let dataQualityRows = {data_quality_json};
let auditMarkets = {audit_markets_json};
let auditConfigSha = {audit_config_sha_json};
const i18n = {i18n_json};
const defaultLang = {default_lang_json};

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
  document.getElementById('research-title').textContent = text.research;
  document.getElementById('decay-chart-title').textContent = text.decay_overview;
  document.getElementById('decay-title').textContent = text.decay_overview;
  document.getElementById('rolling-chart-title').textContent = text.rolling_ic;
  document.getElementById('rolling-title').textContent = text.rolling_ic;
  document.getElementById('research-decay-factor').textContent = text.factors;
  document.getElementById('research-decay-scope-th').textContent = text.scope;
  document.getElementById('research-decay-horizon').textContent = text.horizon_days;
  document.getElementById('research-decay-ic').textContent = text.ic_short;
  document.getElementById('research-decay-spread').textContent = text.spread;
  document.getElementById('research-rolling-date').textContent = text.date;
  document.getElementById('research-rolling-factor').textContent = text.factors;
  document.getElementById('research-rolling-horizon').textContent = text.horizon_days;
  document.getElementById('research-rolling-ic').textContent = text.ic_short;
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
    const [summaryResp, equityResp, tradesResp, rejResp, factorResp, auditResp, researchSummaryResp, researchSummaryResp2, researchJsonResp, researchJsonResp2, dqResp, dq2Resp, dqReportResp, auditJsonResp] =
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
        rejections = text.rejections,
        reason = text.reason,
        factors = text.factors,
        research = text.research,
        decay_overview = text.decay_overview,
        rolling_ic = text.rolling_ic,
        horizon_days = text.horizon_days,
        ic_short = text.ic_short,
        spread = text.spread,
        scope = text.scope,
        metric = text.metric,
        summary_html = summary_html,
        research_summary_html = research_summary_html,
        audit_html = audit_html,
        data_quality_html = data_quality_html,
        equity_rows_json = equity_rows_json,
        trade_json = trade_json,
        rejection_json = rejection_json,
        summary_kv_json = summary_kv_json,
        factor_kv_json = factor_kv_json,
        research_summary_kv_json = research_summary_kv_json,
        research_decay_json = research_decay_json,
        research_rolling_json = research_rolling_json,
        data_quality_json = data_quality_json,
        audit_markets_json = audit_markets_json,
        audit_config_sha_json = audit_config_sha_json,
        i18n_json = i18n_json,
        default_lang_json = default_lang_json,
    );

    let mut dashboard_path = output_dir.join("dashboard.html");
    fs::write(&dashboard_path, html)?;
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

fn read_research_report(path: &Path) -> (Vec<FactorDecayRowUi>, Vec<RollingIcRowUi>) {
    let Ok(s) = fs::read_to_string(path) else {
        return (Vec::new(), Vec::new());
    };
    let report: ResearchReportCompat = serde_json::from_str(&s).unwrap_or_default();
    (report.factor_decay_rows, report.rolling_ic_rows)
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
  "factor_decay_rows":[
    {"scope":"ALL","factor":"momentum","horizon_days":5,"observations":10,"ic":0.2222,"top_quintile_avg_return":0.01,"bottom_quintile_avg_return":-0.01,"long_short_spread":0.02}
  ],
  "rolling_ic_rows":[
    {"date":"2026-01-02","factor":"volume","horizon_days":3,"observations":9,"ic":0.1111}
  ]
}"#,
        )
        .expect("write research json");

        let path =
            build_dashboard_with_language(&output_dir, Language::En).expect("build dashboard");
        let html = fs::read_to_string(path).expect("read dashboard");
        assert!(html.contains("Research"));
        assert!(html.contains("researchDecayRows"));
        assert!(html.contains("momentum"));
        assert!(html.contains("researchRollingRows"));
        assert!(html.contains("research-decay-chart"));
        assert!(html.contains("research-rolling-chart"));
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
