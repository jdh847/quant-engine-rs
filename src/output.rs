use std::{fs, path::Path};

use anyhow::Result;

use crate::engine::{summarize_result, RunResult};

pub fn write_outputs(path: impl AsRef<Path>, result: &RunResult) -> Result<()> {
    let output_dir = path.as_ref();
    fs::create_dir_all(output_dir)?;

    write_equity(output_dir.join("equity_curve.csv"), result)?;
    write_trades(output_dir.join("trades.csv"), result)?;
    write_rejections(output_dir.join("rejections.csv"), result)?;
    write_summary(output_dir.join("summary.txt"), result)?;

    Ok(())
}

fn write_equity(path: impl AsRef<Path>, result: &RunResult) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;
    wtr.write_record(["date", "equity", "cash", "gross_exposure", "net_exposure"])?;
    for p in &result.equity_curve {
        wtr.write_record([
            p.date.format("%Y-%m-%d").to_string(),
            format!("{:.4}", p.equity),
            format!("{:.4}", p.cash),
            format!("{:.4}", p.gross_exposure),
            format!("{:.4}", p.net_exposure),
        ])?;
    }
    wtr.flush()?;
    Ok(())
}

fn write_trades(path: impl AsRef<Path>, result: &RunResult) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;
    wtr.write_record(["date", "market", "symbol", "side", "qty", "price", "fees"])?;
    for t in &result.trades {
        wtr.write_record([
            t.date.format("%Y-%m-%d").to_string(),
            t.market.clone(),
            t.symbol.clone(),
            t.side.as_str().to_string(),
            t.qty.to_string(),
            format!("{:.4}", t.price),
            format!("{:.4}", t.fees),
        ])?;
    }
    wtr.flush()?;
    Ok(())
}

fn write_rejections(path: impl AsRef<Path>, result: &RunResult) -> Result<()> {
    let mut wtr = csv::Writer::from_path(path)?;
    wtr.write_record(["date", "market", "symbol", "side", "qty", "reason"])?;
    for r in &result.rejections {
        wtr.write_record([
            r.date.format("%Y-%m-%d").to_string(),
            r.market.clone(),
            r.symbol.clone(),
            r.side.as_str().to_string(),
            r.qty.to_string(),
            r.reason.clone(),
        ])?;
    }
    wtr.flush()?;
    Ok(())
}

fn write_summary(path: impl AsRef<Path>, result: &RunResult) -> Result<()> {
    let stats = summarize_result(result);
    if result.equity_curve.is_empty() {
        fs::write(path, "no data\n")?;
        return Ok(());
    }

    let text = format!(
        "start_equity={:.2}\nend_equity={:.2}\npnl={:.2}\npnl_ratio={:.4}%\nmax_drawdown={:.4}%\ntrades={}\nrejections={}\ncagr={:.4}%\nsharpe={:.4}\nsortino={:.4}\ncalmar={:.4}\ndaily_win_rate={:.4}%\nprofit_factor={:.4}\n",
        stats.start_equity,
        stats.end_equity,
        stats.pnl,
        stats.pnl_ratio * 100.0,
        stats.max_drawdown * 100.0,
        stats.trades,
        stats.rejections,
        stats.cagr * 100.0,
        stats.sharpe,
        stats.sortino,
        stats.calmar,
        stats.daily_win_rate * 100.0,
        stats.profit_factor
    );
    fs::write(path, text)?;
    Ok(())
}
