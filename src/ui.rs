use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::Result;
use serde::Serialize;

use crate::i18n::{dashboard_text, Language};

#[derive(Debug, Serialize)]
struct EquityRow {
    date: String,
    equity: f64,
}

#[derive(Debug, Serialize)]
struct TradeRow {
    date: String,
    market: String,
    symbol: String,
    side: String,
    qty: i64,
    price: f64,
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

    let summary = fs::read_to_string(summary_path).unwrap_or_else(|_| "no summary".to_string());
    let summary_html = escape_html(&summary);
    let equity_rows = read_equity_rows(&equity_path)?;
    let trade_rows = read_trade_rows(&trades_path)?;

    let labels: Vec<String> = equity_rows.iter().map(|r| r.date.clone()).collect();
    let points: Vec<f64> = equity_rows.iter().map(|r| r.equity).collect();

    let trade_json = serde_json::to_string(&trade_rows)?;
    let labels_json = serde_json::to_string(&labels)?;
    let points_json = serde_json::to_string(&points)?;
    let text = dashboard_text(language);
    let text_en = dashboard_text(Language::En);
    let text_zh = dashboard_text(Language::Zh);
    let text_ja = dashboard_text(Language::Ja);
    let i18n_json = serde_json::to_string(&serde_json::json!({
        "en": {
            "title": text_en.title,
            "subtitle": text_en.subtitle,
            "generated_from": text_en.generated_from,
            "equity_curve": text_en.equity_curve,
            "run_summary": text_en.run_summary,
            "recent_trades": text_en.recent_trades,
            "date": text_en.date,
            "market": text_en.market,
            "symbol": text_en.symbol,
            "side": text_en.side,
            "qty": text_en.qty,
            "price": text_en.price,
            "start": text_en.start,
            "end": text_en.end,
            "buy": text_en.buy,
            "sell": text_en.sell,
        },
        "zh-CN": {
            "title": text_zh.title,
            "subtitle": text_zh.subtitle,
            "generated_from": text_zh.generated_from,
            "equity_curve": text_zh.equity_curve,
            "run_summary": text_zh.run_summary,
            "recent_trades": text_zh.recent_trades,
            "date": text_zh.date,
            "market": text_zh.market,
            "symbol": text_zh.symbol,
            "side": text_zh.side,
            "qty": text_zh.qty,
            "price": text_zh.price,
            "start": text_zh.start,
            "end": text_zh.end,
            "buy": text_zh.buy,
            "sell": text_zh.sell,
        },
        "ja": {
            "title": text_ja.title,
            "subtitle": text_ja.subtitle,
            "generated_from": text_ja.generated_from,
            "equity_curve": text_ja.equity_curve,
            "run_summary": text_ja.run_summary,
            "recent_trades": text_ja.recent_trades,
            "date": text_ja.date,
            "market": text_ja.market,
            "symbol": text_ja.symbol,
            "side": text_ja.side,
            "qty": text_ja.qty,
            "price": text_ja.price,
            "start": text_ja.start,
            "end": text_ja.end,
            "buy": text_ja.buy,
            "sell": text_ja.sell,
        }
    }))?;
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
  --bg: #f5f7fb;
  --panel: #ffffff;
  --ink: #111827;
  --muted: #6b7280;
  --line: #d1d5db;
  --accent: #0f766e;
  --accent-soft: #ccfbf1;
  --danger: #b91c1c;
}}
* {{ box-sizing: border-box; }}
body {{ margin: 0; font-family: "Avenir Next", "Segoe UI", sans-serif; background: linear-gradient(160deg, #f7fafc 0%, #ecfeff 60%, #f0fdf4 100%); color: var(--ink); }}
.wrap {{ max-width: 1200px; margin: 24px auto; padding: 0 16px 24px; }}
.head {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; }}
.title {{ font-size: 28px; font-weight: 700; letter-spacing: 0.2px; }}
.sub {{ color: var(--muted); font-size: 14px; }}
.head-right {{ display: flex; align-items: center; gap: 10px; }}
.lang-switch {{ border: 1px solid #cbd5e1; background: #ffffff; border-radius: 10px; padding: 6px 10px; font-size: 13px; }}
.grid {{ display: grid; grid-template-columns: 1.2fr 1fr; gap: 16px; }}
.panel {{ background: var(--panel); border: 1px solid var(--line); border-radius: 16px; padding: 16px; box-shadow: 0 8px 20px rgba(15, 23, 42, 0.05); }}
.panel h3 {{ margin: 0 0 12px 0; font-size: 16px; }}
#chart {{ width: 100%; height: 360px; border-radius: 10px; background: linear-gradient(180deg, #ecfeff 0%, #ffffff 70%); border: 1px solid #a5f3fc; }}
.summary {{ white-space: pre-wrap; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; background: #f9fafb; padding: 10px; border-radius: 8px; border: 1px solid #e5e7eb; }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th, td {{ text-align: left; padding: 8px; border-bottom: 1px solid #eef2f7; }}
th {{ color: var(--muted); font-weight: 600; }}
.tag-buy {{ color: #065f46; background: #d1fae5; padding: 2px 6px; border-radius: 999px; font-weight: 600; }}
.tag-sell {{ color: #7f1d1d; background: #fee2e2; padding: 2px 6px; border-radius: 999px; font-weight: 600; }}
@media (max-width: 960px) {{
  .grid {{ grid-template-columns: 1fr; }}
  #chart {{ height: 280px; }}
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
        <div class="sub" id="live-status">live refresh: init</div>
      </div>
    </div>

    <div class="grid">
      <section class="panel">
        <h3 id="equity-curve">{equity_curve}</h3>
        <canvas id="chart"></canvas>
      </section>

      <section class="panel">
        <h3 id="run-summary">{run_summary}</h3>
        <div class="summary" id="summary-block">{summary_html}</div>
      </section>
    </div>

    <section class="panel" style="margin-top: 16px;">
      <h3 id="recent-trades">{recent_trades}</h3>
      <table>
        <thead>
          <tr>
            <th id="th-date">{date}</th><th id="th-market">{market}</th><th id="th-symbol">{symbol}</th><th id="th-side">{side}</th><th id="th-qty">{qty}</th><th id="th-price">{price}</th>
          </tr>
        </thead>
        <tbody id="trades"></tbody>
      </table>
    </section>
  </div>

<script>
let labels = {labels_json};
let points = {points_json};
let trades = {trade_json};
const i18n = {i18n_json};
const defaultLang = {default_lang_json};

const c = document.getElementById('chart');
const ctx = c.getContext('2d');
const langSwitch = document.getElementById('lang-switch');
const summaryBlock = document.getElementById('summary-block');
const liveStatus = document.getElementById('live-status');

function getText(lang) {{
  return i18n[lang] || i18n['en'];
}}

function renderChart(text) {{
  const dpr = window.devicePixelRatio || 1;
  const w = c.clientWidth;
  const h = c.clientHeight;
  c.width = Math.floor(w * dpr);
  c.height = Math.floor(h * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);
  ctx.lineWidth = 2;
  ctx.strokeStyle = '#0f766e';
  ctx.fillStyle = 'rgba(15,118,110,0.12)';

  if (points.length <= 1) {{
    return;
  }}

  const min = Math.min(...points);
  const max = Math.max(...points);
  const span = Math.max(max - min, 1);
  const pad = 24;
  const stepX = (w - pad * 2) / (points.length - 1);

  ctx.beginPath();
  points.forEach((v, i) => {{
    const x = pad + i * stepX;
    const y = h - pad - ((v - min) / span) * (h - pad * 2);
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  }});
  ctx.stroke();

  ctx.lineTo(w - pad, h - pad);
  ctx.lineTo(pad, h - pad);
  ctx.closePath();
  ctx.fill();

  ctx.fillStyle = '#334155';
  ctx.font = '12px Avenir Next, sans-serif';
  ctx.fillText(text.start + ': ' + points[0].toFixed(2), pad, 16);
  ctx.fillText(text.end + ': ' + points[points.length - 1].toFixed(2), w - 160, 16);
}}

function renderTrades(text) {{
  const tbody = document.getElementById('trades');
  tbody.innerHTML = '';
  trades.slice(-30).reverse().forEach(t => {{
    const tr = document.createElement('tr');
    const sideTag = t.side === 'BUY'
      ? '<span class="tag-buy">' + text.buy + '</span>'
      : '<span class="tag-sell">' + text.sell + '</span>';
    tr.innerHTML = `<td>${{t.date}}</td><td>${{t.market}}</td><td>${{t.symbol}}</td><td>${{sideTag}}</td><td>${{t.qty}}</td><td>${{t.price.toFixed(4)}}</td>`;
    tbody.appendChild(tr);
  }});
}}

function applyLanguage(lang) {{
  const text = getText(lang);
  document.documentElement.lang = lang;
  document.title = text.title + ' | ' + text.subtitle;
  document.getElementById('title').textContent = text.title;
  document.getElementById('subtitle').textContent = text.subtitle;
  document.getElementById('generated-from').textContent = text.generated_from;
  document.getElementById('equity-curve').textContent = text.equity_curve;
  document.getElementById('run-summary').textContent = text.run_summary;
  document.getElementById('recent-trades').textContent = text.recent_trades;
  document.getElementById('th-date').textContent = text.date;
  document.getElementById('th-market').textContent = text.market;
  document.getElementById('th-symbol').textContent = text.symbol;
  document.getElementById('th-side').textContent = text.side;
  document.getElementById('th-qty').textContent = text.qty;
  document.getElementById('th-price').textContent = text.price;
  renderChart(text);
  renderTrades(text);
}}

function parseCsv(text) {{
  const lines = text.trim().split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) return [];
  const headers = lines[0].split(',');
  return lines.slice(1).map((line) => {{
    const cols = line.split(',');
    const row = {{}};
    headers.forEach((h, i) => {{ row[h] = cols[i] ?? ''; }});
    return row;
  }});
}}

async function refreshFromFiles() {{
  try {{
    const [summaryResp, equityResp, tradesResp] = await Promise.all([
      fetch('./summary.txt', {{ cache: 'no-store' }}),
      fetch('./equity_curve.csv', {{ cache: 'no-store' }}),
      fetch('./trades.csv', {{ cache: 'no-store' }}),
    ]);

    if (summaryResp.ok) {{
      summaryBlock.textContent = await summaryResp.text();
    }}
    if (equityResp.ok) {{
      const equityText = await equityResp.text();
      const equityRows = parseCsv(equityText);
      labels = equityRows.map((r) => r.date || '');
      points = equityRows.map((r) => Number(r.equity || 0));
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
      }}));
    }}

    liveStatus.textContent = 'live refresh: on';
    applyLanguage(langSwitch.value);
  }} catch (e) {{
    liveStatus.textContent = 'live refresh: fallback';
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
window.addEventListener('resize', () => applyLanguage(langSwitch.value));
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
        equity_curve = text.equity_curve,
        run_summary = text.run_summary,
        recent_trades = text.recent_trades,
        date = text.date,
        market = text.market,
        symbol = text.symbol,
        side = text.side,
        qty = text.qty,
        price = text.price,
        summary_html = summary_html,
        labels_json = labels_json,
        points_json = points_json,
        trade_json = trade_json,
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
        rows.push(EquityRow { date, equity });
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
        });
    }
    Ok(rows)
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
