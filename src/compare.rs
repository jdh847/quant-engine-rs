use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use serde::Serialize;

#[derive(Debug, Clone)]
pub struct CompareRequest {
    pub baseline_dir: PathBuf,
    pub candidate_dir: PathBuf,
    pub output_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub struct CompareField {
    pub key: String,
    pub baseline: String,
    pub candidate: String,
    pub changed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct CompareReport {
    pub baseline_dir: String,
    pub candidate_dir: String,
    pub metric_rows: Vec<CompareField>,
    pub audit_rows: Vec<CompareField>,
    pub data_quality_rows: Vec<CompareField>,
}

pub fn compare_runs(req: &CompareRequest) -> Result<CompareReport> {
    fs::create_dir_all(&req.output_dir)
        .with_context(|| format!("create compare output dir {}", req.output_dir.display()))?;

    let baseline = load_run_snapshot(&req.baseline_dir)?;
    let candidate = load_run_snapshot(&req.candidate_dir)?;

    let metric_rows = build_rows(&baseline.summary_kv, &candidate.summary_kv);
    let audit_rows = build_rows(&baseline.audit_kv, &candidate.audit_kv);
    let data_quality_rows = build_rows(&baseline.data_quality_kv, &candidate.data_quality_kv);

    let report = CompareReport {
        baseline_dir: req.baseline_dir.display().to_string(),
        candidate_dir: req.candidate_dir.display().to_string(),
        metric_rows,
        audit_rows,
        data_quality_rows,
    };

    write_compare_outputs(&req.output_dir, &report)?;
    Ok(report)
}

#[derive(Debug, Clone)]
struct RunSnapshot {
    summary_kv: BTreeMap<String, String>,
    audit_kv: BTreeMap<String, String>,
    data_quality_kv: BTreeMap<String, String>,
}

fn load_run_snapshot(dir: &Path) -> Result<RunSnapshot> {
    let summary_path = dir.join("summary.txt");
    let audit_path = dir.join("audit_snapshot.json");
    let dq_report = dir.join("data_quality_report.csv");

    if !summary_path.exists() {
        return Err(anyhow!("missing summary.txt in {}", dir.display()));
    }
    if !audit_path.exists() {
        return Err(anyhow!("missing audit_snapshot.json in {}", dir.display()));
    }

    let summary = fs::read_to_string(&summary_path)
        .with_context(|| format!("read {}", summary_path.display()))?;
    let audit = fs::read_to_string(&audit_path)
        .with_context(|| format!("read {}", audit_path.display()))?;

    let summary_kv = parse_kv_lines(&summary);
    let audit_kv = parse_audit_json(&audit)?;
    let data_quality_kv = if dq_report.exists() {
        parse_data_quality_csv(&dq_report)?
    } else {
        BTreeMap::from([("status".to_string(), "missing".to_string())])
    };

    Ok(RunSnapshot {
        summary_kv,
        audit_kv,
        data_quality_kv,
    })
}

fn parse_kv_lines(text: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        out.insert(k.trim().to_string(), v.trim().to_string());
    }
    out
}

fn parse_audit_json(text: &str) -> Result<BTreeMap<String, String>> {
    let v: serde_json::Value = serde_json::from_str(text).context("parse audit json")?;
    let mut out = BTreeMap::new();

    for key in [
        "command",
        "note",
        "config_sha256",
        "broker_mode",
        "paper_only",
        "base_currency",
        "strategy_plugin",
        "portfolio_method",
    ] {
        if let Some(val) = v.get(key) {
            out.insert(key.to_string(), json_value_to_string(val));
        }
    }

    if let Some(stats) = v.get("stats").and_then(|x| x.as_object()) {
        for key in [
            "end_equity",
            "pnl_ratio",
            "max_drawdown",
            "trades",
            "rejections",
            "sharpe",
            "sortino",
            "calmar",
        ] {
            if let Some(val) = stats.get(key) {
                out.insert(format!("stats.{key}"), json_value_to_string(val));
            }
        }
    }

    if let Some(markets) = v.get("markets").and_then(|x| x.as_array()) {
        for market in markets {
            let name = market
                .get("market")
                .and_then(|x| x.as_str())
                .unwrap_or("UNKNOWN");
            if let Some(data_sha) = market
                .get("data_file")
                .and_then(|x| x.get("sha256"))
                .and_then(|x| x.as_str())
            {
                out.insert(format!("market.{name}.data_sha256"), data_sha.to_string());
            }
            if let Some(ind_sha) = market
                .get("industry_file")
                .and_then(|x| x.get("sha256"))
                .and_then(|x| x.as_str())
            {
                out.insert(
                    format!("market.{name}.industry_sha256"),
                    ind_sha.to_string(),
                );
            }
            if let Some(hol_sha) = market
                .get("holiday_file")
                .and_then(|x| x.get("sha256"))
                .and_then(|x| x.as_str())
            {
                out.insert(format!("market.{name}.holiday_sha256"), hol_sha.to_string());
            }
        }
    }

    Ok(out)
}

fn parse_data_quality_csv(path: &Path) -> Result<BTreeMap<String, String>> {
    let mut rdr = csv::Reader::from_path(path)
        .with_context(|| format!("open data quality csv {}", path.display()))?;
    let mut pass = 0usize;
    let mut warn = 0usize;
    let mut fail = 0usize;
    let mut non_trading = 0i64;
    let mut outliers = 0i64;
    let mut gaps = 0i64;

    for rec in rdr.deserialize::<BTreeMap<String, String>>() {
        let rec = rec.with_context(|| format!("parse data quality row {}", path.display()))?;
        match rec.get("status").map(String::as_str).unwrap_or("") {
            "PASS" => pass += 1,
            "WARN" => warn += 1,
            "FAIL" => fail += 1,
            _ => {}
        }
        non_trading += rec
            .get("non_trading_day_rows")
            .and_then(|x| x.parse::<i64>().ok())
            .unwrap_or(0);
        outliers += rec
            .get("return_outliers")
            .and_then(|x| x.parse::<i64>().ok())
            .unwrap_or(0);
        gaps += rec
            .get("large_gaps")
            .and_then(|x| x.parse::<i64>().ok())
            .unwrap_or(0);
    }

    Ok(BTreeMap::from([
        ("pass_markets".to_string(), pass.to_string()),
        ("warn_markets".to_string(), warn.to_string()),
        ("fail_markets".to_string(), fail.to_string()),
        ("non_trading_day_rows".to_string(), non_trading.to_string()),
        ("return_outliers".to_string(), outliers.to_string()),
        ("large_gaps".to_string(), gaps.to_string()),
    ]))
}

fn build_rows(
    baseline: &BTreeMap<String, String>,
    candidate: &BTreeMap<String, String>,
) -> Vec<CompareField> {
    let mut keys = baseline.keys().cloned().collect::<Vec<_>>();
    for key in candidate.keys() {
        if !keys.iter().any(|x| x == key) {
            keys.push(key.clone());
        }
    }
    keys.sort();
    keys.into_iter()
        .map(|key| {
            let base = baseline
                .get(&key)
                .cloned()
                .unwrap_or_else(|| "-".to_string());
            let cand = candidate
                .get(&key)
                .cloned()
                .unwrap_or_else(|| "-".to_string());
            let changed = base != cand;
            CompareField {
                key,
                baseline: base,
                candidate: cand,
                changed,
            }
        })
        .collect()
}

fn write_compare_outputs(output_dir: &Path, report: &CompareReport) -> Result<()> {
    fs::write(
        output_dir.join("compare_report.json"),
        serde_json::to_string_pretty(report).context("serialize compare json")?,
    )
    .context("write compare json")?;

    fs::write(
        output_dir.join("compare_report.md"),
        render_markdown(report),
    )
    .context("write compare markdown")?;
    fs::write(output_dir.join("compare_report.html"), render_html(report))
        .context("write compare html")?;
    Ok(())
}

fn render_markdown(report: &CompareReport) -> String {
    let mut out = String::new();
    out.push_str("# Run Compare Report\n\n");
    out.push_str(&format!("- baseline: `{}`\n", report.baseline_dir));
    out.push_str(&format!("- candidate: `{}`\n\n", report.candidate_dir));
    out.push_str(&render_md_table("Metrics", &report.metric_rows));
    out.push('\n');
    out.push_str(&render_md_table("Audit", &report.audit_rows));
    out.push('\n');
    out.push_str(&render_md_table("Data Quality", &report.data_quality_rows));
    out
}

fn render_md_table(title: &str, rows: &[CompareField]) -> String {
    let mut out = String::new();
    out.push_str(&format!("## {title}\n\n"));
    out.push_str("| key | baseline | candidate | changed |\n");
    out.push_str("| --- | --- | --- | --- |\n");
    for row in rows {
        out.push_str(&format!(
            "| {} | {} | {} | {} |\n",
            row.key,
            row.baseline.replace('|', "\\|"),
            row.candidate.replace('|', "\\|"),
            if row.changed { "yes" } else { "no" }
        ));
    }
    out
}

fn render_html(report: &CompareReport) -> String {
    let metrics = render_html_rows(&report.metric_rows);
    let audit = render_html_rows(&report.audit_rows);
    let dq = render_html_rows(&report.data_quality_rows);
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Run Compare Report</title>
<style>
:root {{
  --bg: #f5f7f0;
  --panel: rgba(255,255,255,0.88);
  --ink: #102018;
  --muted: rgba(16,32,24,0.64);
  --line: rgba(16,32,24,0.12);
  --accent: #0f766e;
  --warn: #b45309;
}}
* {{ box-sizing: border-box; }}
body {{
  margin: 0;
  color: var(--ink);
  font-family: "Avenir Next", "Helvetica Neue", sans-serif;
  background:
    radial-gradient(900px 500px at 10% 10%, rgba(15,118,110,0.16), transparent 60%),
    radial-gradient(900px 600px at 80% 15%, rgba(180,83,9,0.10), transparent 60%),
    linear-gradient(180deg, #f8faf5 0%, #eef7f2 100%);
}}
.wrap {{ max-width: 1220px; margin: 24px auto; padding: 0 16px 40px; }}
.hero {{ background: var(--panel); border: 1px solid var(--line); border-radius: 22px; padding: 20px; }}
.title {{ font-size: 32px; font-weight: 800; }}
.sub {{ color: var(--muted); margin-top: 8px; }}
.grid {{ display: grid; grid-template-columns: 1fr; gap: 16px; margin-top: 16px; }}
.panel {{ background: var(--panel); border: 1px solid var(--line); border-radius: 18px; padding: 16px; overflow: auto; }}
table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
th, td {{ text-align: left; padding: 10px 8px; border-bottom: 1px solid rgba(16,32,24,0.08); vertical-align: top; }}
th {{ color: var(--muted); font-weight: 700; }}
.chg-yes {{ color: var(--warn); font-weight: 800; }}
.chg-no {{ color: var(--accent); font-weight: 700; }}
</style>
</head>
<body>
  <div class="wrap">
    <div class="hero">
      <div class="title">Run Compare Report</div>
      <div class="sub">baseline: {baseline}</div>
      <div class="sub">candidate: {candidate}</div>
    </div>
    <div class="grid">
      <section class="panel"><h2>Metrics</h2><table><thead><tr><th>key</th><th>baseline</th><th>candidate</th><th>changed</th></tr></thead><tbody>{metrics}</tbody></table></section>
      <section class="panel"><h2>Audit</h2><table><thead><tr><th>key</th><th>baseline</th><th>candidate</th><th>changed</th></tr></thead><tbody>{audit}</tbody></table></section>
      <section class="panel"><h2>Data Quality</h2><table><thead><tr><th>key</th><th>baseline</th><th>candidate</th><th>changed</th></tr></thead><tbody>{dq}</tbody></table></section>
    </div>
  </div>
</body>
</html>"#,
        baseline = escape_html(&report.baseline_dir),
        candidate = escape_html(&report.candidate_dir),
        metrics = metrics,
        audit = audit,
        dq = dq
    )
}

fn render_html_rows(rows: &[CompareField]) -> String {
    let mut out = String::new();
    for row in rows {
        let cls = if row.changed { "chg-yes" } else { "chg-no" };
        let changed = if row.changed { "yes" } else { "no" };
        out.push_str(&format!(
            "<tr><td>{}</td><td>{}</td><td>{}</td><td class=\"{}\">{}</td></tr>",
            escape_html(&row.key),
            escape_html(&row.baseline),
            escape_html(&row.candidate),
            cls,
            changed
        ));
    }
    out
}

fn json_value_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "null".to_string(),
        serde_json::Value::Bool(x) => x.to_string(),
        serde_json::Value::Number(x) => x.to_string(),
        serde_json::Value::String(x) => x.clone(),
        _ => v.to_string(),
    }
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

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{compare_runs, CompareRequest};

    #[test]
    fn compare_generates_outputs() {
        let root = std::env::temp_dir().join("pqbot_compare_test");
        let _ = fs::remove_dir_all(&root);
        let base = root.join("base");
        let cand = root.join("cand");
        let out = root.join("out");
        fs::create_dir_all(&base).expect("mkdir base");
        fs::create_dir_all(&cand).expect("mkdir cand");

        fs::write(base.join("summary.txt"), "end_equity=100\ntrades=1\n").expect("summary");
        fs::write(cand.join("summary.txt"), "end_equity=120\ntrades=2\n").expect("summary");
        fs::write(
            base.join("audit_snapshot.json"),
            r#"{"config_sha256":"aaa","strategy_plugin":"x","portfolio_method":"risk_parity","stats":{"end_equity":100,"trades":1,"rejections":0},"markets":[{"market":"US","data_file":{"sha256":"111"}}]}"#,
        )
        .expect("audit");
        fs::write(
            cand.join("audit_snapshot.json"),
            r#"{"config_sha256":"bbb","strategy_plugin":"x","portfolio_method":"hrp","stats":{"end_equity":120,"trades":2,"rejections":0},"markets":[{"market":"US","data_file":{"sha256":"222"}}]}"#,
        )
        .expect("audit");
        fs::write(
            base.join("data_quality_report.csv"),
            "market,rows,status,return_outliers,large_gaps,non_trading_day_rows\nUS,10,PASS,0,0,0\n",
        )
        .expect("dq");
        fs::write(
            cand.join("data_quality_report.csv"),
            "market,rows,status,return_outliers,large_gaps,non_trading_day_rows\nUS,10,WARN,1,0,2\n",
        )
        .expect("dq");

        let report = compare_runs(&CompareRequest {
            baseline_dir: base,
            candidate_dir: cand,
            output_dir: out.clone(),
        })
        .expect("compare");

        assert!(!report.metric_rows.is_empty());
        assert!(out.join("compare_report.md").exists());
        assert!(out.join("compare_report.html").exists());
        assert!(out.join("compare_report.json").exists());
    }
}
