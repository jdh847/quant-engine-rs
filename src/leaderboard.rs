use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Result};

use crate::registry::read_run_registry;

#[derive(Debug, Clone)]
pub struct LeaderboardRequest {
    pub output_dir: PathBuf,
    pub top: usize,
}

#[derive(Debug, Clone)]
pub struct LeaderboardReport {
    pub csv_path: PathBuf,
    pub markdown_path: PathBuf,
    pub html_path: PathBuf,
    pub rows: usize,
}

#[derive(Debug, Clone)]
struct LeaderboardRow {
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

pub fn build_public_leaderboard(req: &LeaderboardRequest) -> Result<LeaderboardReport> {
    if req.top == 0 {
        return Err(anyhow!("top must be > 0"));
    }

    let root = req.output_dir.as_path();
    let out_dir = root.join("leaderboard");
    fs::create_dir_all(&out_dir)?;

    let mut rows = Vec::new();
    rows.extend(load_registry_rows(root)?);
    rows.extend(load_benchmark_rows(root)?);
    rows.extend(load_research_rows(root)?);

    if rows.is_empty() {
        return Err(anyhow!(
            "no sources found; expected run_registry.csv and/or benchmark/research outputs under {}",
            root.display()
        ));
    }

    rows.sort_by(|a, b| b.score.total_cmp(&a.score));
    for (idx, row) in rows.iter_mut().enumerate() {
        row.rank = idx + 1;
    }
    if rows.len() > req.top {
        rows.truncate(req.top);
    }

    let csv_path = out_dir.join("leaderboard_public.csv");
    let markdown_path = out_dir.join("leaderboard_public.md");
    let html_path = out_dir.join("leaderboard_public.html");

    write_csv(&csv_path, &rows)?;
    write_markdown(&markdown_path, &rows)?;
    write_html(&html_path, &rows)?;

    Ok(LeaderboardReport {
        csv_path,
        markdown_path,
        html_path,
        rows: rows.len(),
    })
}

fn load_registry_rows(root: &Path) -> Result<Vec<LeaderboardRow>> {
    let path = root.join("run_registry.csv");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let entries = read_run_registry(&path)?;
    Ok(entries
        .into_iter()
        .map(|row| LeaderboardRow {
            rank: 0,
            source: "registry".to_string(),
            timestamp_utc: row.timestamp_utc,
            command: row.command,
            scenario: String::new(),
            strategy_plugin: row.strategy_plugin,
            portfolio_method: row.portfolio_method,
            score: row.composite_score,
            pnl_ratio: row.pnl_ratio,
            max_drawdown: row.max_drawdown,
            sharpe: row.sharpe,
            notes: row.notes,
        })
        .collect())
}

fn load_benchmark_rows(root: &Path) -> Result<Vec<LeaderboardRow>> {
    let path = first_existing_path(&[
        root.join("benchmark").join("baseline_results.csv"),
        root.join("baseline_results.csv"),
    ]);
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let mut rdr = csv::Reader::from_path(path)?;
    let mut rows = Vec::new();
    for rec in rdr.records() {
        let rec = rec?;
        rows.push(LeaderboardRow {
            rank: 0,
            source: "benchmark".to_string(),
            timestamp_utc: String::new(),
            command: "benchmark".to_string(),
            scenario: rec.get(1).unwrap_or_default().to_string(),
            strategy_plugin: rec.get(3).unwrap_or_default().to_string(),
            portfolio_method: rec.get(4).unwrap_or_default().to_string(),
            score: parse_f64(rec.get(2)).unwrap_or(0.0),
            pnl_ratio: parse_f64(rec.get(10)).unwrap_or(0.0),
            max_drawdown: parse_f64(rec.get(11)).unwrap_or(0.0),
            sharpe: parse_f64(rec.get(13)).unwrap_or(0.0),
            notes: "benchmark scenario".to_string(),
        });
    }
    Ok(rows)
}

fn load_research_rows(root: &Path) -> Result<Vec<LeaderboardRow>> {
    let path = first_existing_path(&[
        root.join("research").join("research_leaderboard.csv"),
        root.join("research_leaderboard.csv"),
    ]);
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let mut rdr = csv::Reader::from_path(path)?;
    let mut rows = Vec::new();
    for rec in rdr.records() {
        let rec = rec?;
        rows.push(LeaderboardRow {
            rank: 0,
            source: "research".to_string(),
            timestamp_utc: String::new(),
            command: "research".to_string(),
            scenario: rec.get(1).unwrap_or_default().to_string(),
            strategy_plugin: rec.get(7).unwrap_or_default().to_string(),
            portfolio_method: rec.get(8).unwrap_or_default().to_string(),
            score: parse_f64(rec.get(9)).unwrap_or(0.0),
            pnl_ratio: parse_f64(rec.get(10)).unwrap_or(0.0),
            max_drawdown: parse_f64(rec.get(11)).unwrap_or(0.0),
            sharpe: parse_f64(rec.get(12)).unwrap_or(0.0),
            notes: "research leaderboard".to_string(),
        });
    }
    Ok(rows)
}

fn write_csv(path: &Path, rows: &[LeaderboardRow]) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;
    wtr.write_record([
        "rank",
        "source",
        "timestamp_utc",
        "command",
        "scenario",
        "strategy_plugin",
        "portfolio_method",
        "score",
        "pnl_ratio",
        "max_drawdown",
        "sharpe",
        "notes",
    ])?;
    for row in rows {
        wtr.write_record([
            row.rank.to_string(),
            row.source.clone(),
            row.timestamp_utc.clone(),
            row.command.clone(),
            row.scenario.clone(),
            row.strategy_plugin.clone(),
            row.portfolio_method.clone(),
            format!("{:.8}", row.score),
            format!("{:.8}", row.pnl_ratio),
            format!("{:.8}", row.max_drawdown),
            format!("{:.8}", row.sharpe),
            row.notes.clone(),
        ])?;
    }
    wtr.flush()?;
    Ok(())
}

fn write_markdown(path: &Path, rows: &[LeaderboardRow]) -> Result<()> {
    let mut lines = vec![
        "# Public Leaderboard".to_string(),
        String::new(),
        "| Rank | Source | Command | Scenario | Plugin | Method | Score | PnL | MaxDD | Sharpe |"
            .to_string(),
        "|---:|---|---|---|---|---|---:|---:|---:|---:|".to_string(),
    ];
    for row in rows {
        lines.push(format!(
            "| {} | {} | {} | {} | {} | {} | {:.4} | {:.2}% | {:.2}% | {:.3} |",
            row.rank,
            safe_md(&row.source),
            safe_md(&row.command),
            safe_md(&row.scenario),
            safe_md(&row.strategy_plugin),
            safe_md(&row.portfolio_method),
            row.score,
            row.pnl_ratio * 100.0,
            row.max_drawdown * 100.0,
            row.sharpe
        ));
    }
    fs::write(path, lines.join("\n") + "\n")?;
    Ok(())
}

fn write_html(path: &Path, rows: &[LeaderboardRow]) -> Result<()> {
    let rows_json = serde_json::to_string(
        &rows
            .iter()
            .map(|r| {
                serde_json::json!({
                    "rank": r.rank,
                    "source": r.source,
                    "command": r.command,
                    "scenario": r.scenario,
                    "strategy_plugin": r.strategy_plugin,
                    "portfolio_method": r.portfolio_method,
                    "score": r.score,
                    "pnl_ratio": r.pnl_ratio,
                    "max_drawdown": r.max_drawdown,
                    "sharpe": r.sharpe
                })
            })
            .collect::<Vec<_>>(),
    )?;

    let html = format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Private Quant Bot Public Leaderboard</title>
<style>
body {{ margin: 0; font-family: "Avenir Next", "Segoe UI", sans-serif; background: linear-gradient(160deg, #f8fafc 0%, #ecfeff 60%, #f0fdf4 100%); color: #0f172a; }}
.wrap {{ max-width: 1080px; margin: 32px auto; padding: 0 16px; }}
.panel {{ background: #fff; border: 1px solid #e2e8f0; border-radius: 14px; padding: 14px; }}
h1 {{ margin: 0 0 8px; }}
p {{ color: #475569; }}
select {{ border: 1px solid #cbd5e1; border-radius: 8px; padding: 6px 8px; margin-bottom: 10px; }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th, td {{ text-align: left; border-bottom: 1px solid #eef2f7; padding: 8px; }}
th {{ color: #475569; }}
</style>
</head>
<body>
  <div class="wrap">
    <h1>Public Leaderboard</h1>
    <p>Combined ranking from run registry, benchmark matrix, and research leaderboard.</p>
    <div class="panel">
      <select id="source">
        <option value="ALL">ALL</option>
        <option value="registry">registry</option>
        <option value="benchmark">benchmark</option>
        <option value="research">research</option>
      </select>
      <table>
        <thead>
          <tr>
            <th>Rank</th><th>Source</th><th>Command</th><th>Scenario</th><th>Plugin</th><th>Method</th><th>Score</th><th>PnL</th><th>MaxDD</th><th>Sharpe</th>
          </tr>
        </thead>
        <tbody id="rows"></tbody>
      </table>
    </div>
  </div>
<script>
const rows = {rows_json};
const source = document.getElementById('source');
const body = document.getElementById('rows');

function render() {{
  const src = source.value;
  const filtered = rows.filter((r) => src === 'ALL' || r.source === src);
  body.innerHTML = '';
  filtered.forEach((r) => {{
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${{r.rank}}</td><td>${{r.source}}</td><td>${{r.command}}</td><td>${{r.scenario}}</td><td>${{r.strategy_plugin}}</td><td>${{r.portfolio_method}}</td><td>${{r.score.toFixed(4)}}</td><td>${{(r.pnl_ratio * 100).toFixed(2)}}%</td><td>${{(r.max_drawdown * 100).toFixed(2)}}%</td><td>${{r.sharpe.toFixed(3)}}</td>`;
    body.appendChild(tr);
  }});
}}
source.addEventListener('change', render);
render();
</script>
</body>
</html>
"#
    );
    fs::write(path, html)?;
    Ok(())
}

fn parse_f64(value: Option<&str>) -> Option<f64> {
    value?.trim().parse::<f64>().ok()
}

fn first_existing_path(paths: &[PathBuf]) -> Option<PathBuf> {
    paths.iter().find(|p| p.exists()).cloned()
}

fn safe_md(input: &str) -> String {
    if input.is_empty() {
        "-".to_string()
    } else {
        input.replace('|', "/")
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use crate::{
        engine::BacktestStats,
        registry::{append_run_registry, RunRegistryBacktestInput, RunRegistryEntry},
    };

    use super::{build_public_leaderboard, LeaderboardRequest};

    #[test]
    fn leaderboard_builds_files() {
        let root = std::env::temp_dir().join("private_quant_bot_leaderboard_test");
        if root.exists() {
            fs::remove_dir_all(&root).ok();
        }
        fs::create_dir_all(root.join("benchmark")).expect("benchmark dir");
        fs::create_dir_all(root.join("research")).expect("research dir");

        let entry = RunRegistryEntry::from_backtest_input(RunRegistryBacktestInput {
            command: "run".to_string(),
            output_dir: PathBuf::from("outputs_rust"),
            strategy_plugin: "layered_multi_factor".to_string(),
            portfolio_method: "risk_parity".to_string(),
            markets: "A|JP|US".to_string(),
            primary_metric_name: "pnl_ratio".to_string(),
            primary_metric_value: 0.02,
            stats: BacktestStats {
                pnl_ratio: 0.02,
                max_drawdown: 0.01,
                sharpe: 1.5,
                ..BacktestStats::default()
            },
            notes: "test".to_string(),
        });
        append_run_registry(&root, &entry).expect("append");

        fs::write(
            root.join("benchmark").join("baseline_results.csv"),
            "rank,scenario,score,strategy_plugin,portfolio_method,short_window,long_window,vol_window,top_n,min_momentum,pnl_ratio,max_drawdown,cagr,sharpe,sortino,calmar,daily_win_rate,profit_factor,trades,rejections\n1,global_baseline,0.12,layered_multi_factor,risk_parity,3,7,5,1,0.0,0.02,0.01,0.01,1.2,1.4,1.1,0.6,1.2,22,2\n",
        )
        .expect("write benchmark");
        fs::write(
            root.join("research").join("research_leaderboard.csv"),
            "rank,scenario,short_window,long_window,vol_window,top_n,min_momentum,strategy_plugin,portfolio_method,score,pnl_ratio,max_drawdown,sharpe,sortino,calmar,daily_win_rate,profit_factor,trades,rejections\n1,GLOBAL,3,7,5,1,0.0,layered_multi_factor,risk_parity,0.10,0.018,0.011,1.1,1.3,1.0,0.58,1.1,20,2\n",
        )
        .expect("write research");

        let report = build_public_leaderboard(&LeaderboardRequest {
            output_dir: root,
            top: 10,
        })
        .expect("build leaderboard");
        assert!(report.csv_path.exists());
        assert!(report.markdown_path.exists());
        assert!(report.html_path.exists());
        assert!(report.rows >= 3);
    }
}
