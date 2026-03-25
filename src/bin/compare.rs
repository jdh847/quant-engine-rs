use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

#[path = "../compare.rs"]
mod compare;

use compare::{compare_runs, CompareRequest};

#[derive(Parser, Debug)]
#[command(
    name = "compare",
    about = "Compare two paper-only quant run directories and generate metrics/audit/data-quality/research diff reports"
)]
struct Cli {
    #[arg(long)]
    baseline_dir: PathBuf,
    #[arg(long)]
    candidate_dir: PathBuf,
    #[arg(long, default_value = "outputs_rust/compare")]
    output_dir: PathBuf,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let report = compare_runs(&CompareRequest {
        baseline_dir: cli.baseline_dir,
        candidate_dir: cli.candidate_dir,
        output_dir: cli.output_dir.clone(),
    })?;

    println!(
        "compare complete: metrics={} audit={} data_quality={} research={}",
        report.metric_rows.len(),
        report.audit_rows.len(),
        report.data_quality_rows.len(),
        report.research_rows.len()
    );
    println!(
        "artifacts: {}/compare_report.md, {}/compare_report.html, {}/compare_report.json, {}/compare_report.csv, {}/research_compare.csv",
        cli.output_dir.display(),
        cli.output_dir.display(),
        cli.output_dir.display(),
        cli.output_dir.display(),
        cli.output_dir.display(),
    );
    Ok(())
}
