use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use private_quant_bot::{
    attribution::write_factor_attribution_report,
    audit::write_audit_snapshot,
    benchmark::{run_benchmark_suite, BenchmarkRequest},
    bundle::{create_run_bundle, BundleRequest},
    config::{load_config, BotConfig},
    control_center::{run_control_center, ControlCenterRequest},
    daemon::{run_paper_daemon, PaperDaemonRequest},
    data::CsvDataPortal,
    data_quality::{run_data_quality_check, DataQualityRequest},
    dataset_manifest::{write_dataset_manifest, DatasetManifestRequest},
    doctor::{run_doctor, DoctorRequest},
    engine::{summarize_result, BacktestStats, QuantBotEngine},
    i18n::{
        msg_benchmark_completed, msg_dashboard, msg_demo_completed, msg_open_dashboard_hint,
        msg_replay_completed, msg_research_completed, msg_run_completed, msg_server_ctrl_c,
        msg_server_root, msg_server_started, msg_server_url, msg_walk_forward_completed, Language,
    },
    leaderboard::{build_public_leaderboard, LeaderboardRequest},
    optimize::{run_walk_forward, WalkForwardRequest},
    output::write_outputs,
    registry::{
        append_run_registry, infer_registry_root, read_run_registry, top_registry_entries,
        write_registry_views, RunRegistryBacktestInput, RunRegistryEntry,
        RunRegistryOperationInput,
    },
    replay::run_event_replay,
    research::{run_cross_market_research, ResearchRequest},
    research_report::{write_research_report, ResearchReportRequest},
    robustness::{run_robustness_assessment, RobustnessRequest},
    scaffold::create_strategy_plugin_scaffold,
    sdk::{
        check_strategy_sdk, create_strategy_sdk, register_strategy_sdk, SdkInitRequest,
        SdkRegisterRequest,
    },
    serve::{serve_forever, start_dashboard_server, ServeRequest},
    strategy::{is_supported_strategy_plugin, runtime_strategy_plugin_catalog},
    synth_data::{generate_synth_dataset, SynthDatasetRequest},
    ui::build_dashboard_with_language,
};

#[derive(Debug, Parser)]
#[command(name = "private-quant-bot")]
#[command(about = "Rust paper-trading quant bot for US/A/JP")]
struct Cli {
    #[arg(long, default_value = "en", global = true)]
    lang: String,
    #[arg(long, global = true)]
    strategy_plugin: Option<String>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Run {
        #[arg(long, default_value = "config/bot.toml")]
        config: String,
        #[arg(long, default_value = "outputs_rust")]
        output_dir: String,
        #[arg(long, default_value_t = false)]
        skip_validate_data: bool,
    },
    Demo {
        #[arg(long, default_value = "config/bot.toml")]
        config: String,
        #[arg(long, default_value = "outputs_rust/demo")]
        output_root: String,
        #[arg(long, default_value_t = false)]
        skip_validate_data: bool,
    },
    Serve {
        #[arg(long, default_value = "outputs_rust")]
        root: String,
        #[arg(long, default_value = "127.0.0.1:8787")]
        bind: String,
        #[arg(long, default_value_t = true)]
        prefer_latest: bool,
    },
    Doctor {
        #[arg(long, default_value = "config/bot.toml")]
        config: String,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    DatasetManifest {
        #[arg(long, default_value = "config/bot.toml")]
        config: String,
        #[arg(long, default_value = "data/DATASET_MANIFEST.json")]
        output_path: String,
    },
    Optimize {
        #[arg(long, default_value = "config/bot.toml")]
        config: String,
        #[arg(long, default_value = "outputs_rust/optimize")]
        output_dir: String,
        #[arg(long, default_value_t = 12)]
        train_days: usize,
        #[arg(long, default_value_t = 5)]
        test_days: usize,
        #[arg(long, default_value = "3,4,5")]
        short_windows: String,
        #[arg(long, default_value = "7,9,11")]
        long_windows: String,
        #[arg(long, default_value = "5,7")]
        vol_windows: String,
        #[arg(long, default_value = "1,2")]
        top_ns: String,
        #[arg(long, default_value = "0.001,0.002,0.003", allow_hyphen_values = true)]
        min_momentums: String,
        #[arg(long, default_value = "")]
        strategy_plugins: String,
        #[arg(long, default_value = "risk_parity,hrp")]
        portfolio_methods: String,
    },
    ResearchReport {
        #[arg(long, default_value = "config/bot.toml")]
        config: String,
        #[arg(long, default_value = "outputs_rust/research_report")]
        output_dir: String,
        #[arg(long, default_value_t = 12)]
        train_days: usize,
        #[arg(long, default_value_t = 5)]
        test_days: usize,
        #[arg(long, default_value = "3,4,5")]
        short_windows: String,
        #[arg(long, default_value = "7,9,11")]
        long_windows: String,
        #[arg(long, default_value = "5,7")]
        vol_windows: String,
        #[arg(long, default_value = "1,2")]
        top_ns: String,
        #[arg(long, default_value = "0.001,0.002,0.003", allow_hyphen_values = true)]
        min_momentums: String,
        #[arg(long, default_value = "")]
        strategy_plugins: String,
        #[arg(long, default_value = "risk_parity,hrp")]
        portfolio_methods: String,
        #[arg(long, default_value = "1,3,5,10")]
        factor_decay_horizons: String,
        #[arg(long, default_value_t = 10)]
        regime_vol_window: usize,
        #[arg(long, default_value_t = 5)]
        regime_fast_window: usize,
        #[arg(long, default_value_t = 20)]
        regime_slow_window: usize,
    },
    Research {
        #[arg(long, default_value = "config/bot.toml")]
        config: String,
        #[arg(long, default_value = "outputs_rust/research")]
        output_dir: String,
        #[arg(long, default_value = "US,A,JP")]
        markets: String,
        #[arg(long, default_value = "3,4,5")]
        short_windows: String,
        #[arg(long, default_value = "7,9,11")]
        long_windows: String,
        #[arg(long, default_value = "5,7")]
        vol_windows: String,
        #[arg(long, default_value = "1,2")]
        top_ns: String,
        #[arg(long, default_value = "0.001,0.002,0.003", allow_hyphen_values = true)]
        min_momentums: String,
        #[arg(long, default_value = "")]
        strategy_plugins: String,
        #[arg(long, default_value = "risk_parity,hrp")]
        portfolio_methods: String,
    },
    Dashboard {
        #[arg(long, default_value = "outputs_rust")]
        output_dir: String,
    },
    Benchmark {
        #[arg(long, default_value = "config/bot.toml")]
        config: String,
        #[arg(long, default_value = "outputs_rust/benchmark")]
        output_dir: String,
        #[arg(long, default_value = "")]
        strategy_plugins: String,
        #[arg(long, default_value = "")]
        portfolio_methods: String,
    },
    Replay {
        #[arg(long, default_value = "config/bot.toml")]
        config: String,
        #[arg(long, default_value = "outputs_rust/replay")]
        output_dir: String,
    },
    Robustness {
        #[arg(long, default_value = "config/bot.toml")]
        config: String,
        #[arg(long, default_value = "outputs_rust/robustness")]
        output_dir: String,
        #[arg(long, default_value_t = 12)]
        train_days: usize,
        #[arg(long, default_value_t = 5)]
        test_days: usize,
        #[arg(long, default_value = "3,4,5")]
        short_windows: String,
        #[arg(long, default_value = "7,9,11")]
        long_windows: String,
        #[arg(long, default_value = "5,7")]
        vol_windows: String,
        #[arg(long, default_value = "1,2")]
        top_ns: String,
        #[arg(long, default_value = "0.001,0.002,0.003", allow_hyphen_values = true)]
        min_momentums: String,
        #[arg(long, default_value = "")]
        strategy_plugins: String,
        #[arg(long, default_value = "risk_parity,hrp")]
        portfolio_methods: String,
    },
    ValidateData {
        #[arg(long, default_value = "config/bot.toml")]
        config: String,
        #[arg(long, default_value = "outputs_rust/data_quality")]
        output_dir: String,
        #[arg(long, default_value_t = 0.35)]
        return_outlier_threshold: f64,
        #[arg(long, default_value_t = 10)]
        gap_days_threshold: i64,
    },
    PaperDaemon {
        #[arg(long, default_value = "config/bot.toml")]
        config: String,
        #[arg(long, default_value = "outputs_rust/daemon")]
        output_dir: String,
        #[arg(long, default_value_t = 3)]
        cycles: usize,
        #[arg(long, default_value_t = 1)]
        sleep_secs: u64,
        #[arg(long, default_value_t = 0.03)]
        alert_drawdown_ratio: f64,
    },
    Registry {
        #[arg(long, default_value = "outputs_rust")]
        output_dir: String,
        #[arg(long, default_value_t = 20)]
        top: usize,
    },
    ControlCenter {
        #[arg(long, default_value = "outputs_rust")]
        output_dir: String,
        #[arg(long, default_value_t = 2)]
        refresh_secs: u64,
        #[arg(long, default_value_t = 30)]
        cycles: usize,
    },
    Leaderboard {
        #[arg(long, default_value = "outputs_rust")]
        output_dir: String,
        #[arg(long, default_value_t = 50)]
        top: usize,
    },
    SdkInit {
        #[arg(long)]
        id: String,
        #[arg(long, default_value = "plugins_sdk")]
        output_dir: String,
        #[arg(long, default_value_t = false)]
        force: bool,
    },
    SdkCheck {
        #[arg(long)]
        package_dir: String,
    },
    SdkRegister {
        #[arg(long)]
        package_dir: String,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        description: Option<String>,
    },
    Plugins,
    ScaffoldPlugin {
        #[arg(long)]
        id: String,
        #[arg(long, default_value = "plugins")]
        output_dir: String,
        #[arg(long, default_value_t = false)]
        force: bool,
    },
    GenSynthData {
        #[arg(long, default_value = "data_synth")]
        output_dir: String,
        #[arg(long, default_value = "2025-01-02")]
        start_date: String,
        #[arg(long, default_value = "2026-12-31")]
        end_date: String,
        #[arg(long, default_value_t = 42)]
        seed: u64,
        #[arg(long, default_value_t = 6)]
        us_symbols: usize,
        #[arg(long, default_value_t = 6)]
        a_symbols: usize,
        #[arg(long, default_value_t = 6)]
        jp_symbols: usize,
        #[arg(long, default_value_t = 3)]
        industries_per_market: usize,
        #[arg(long, default_value_t = false)]
        force: bool,
    },
    Bundle {
        #[arg(long, default_value = "outputs_rust")]
        output_dir: String,
        #[arg(long, default_value = "")]
        bundle_path: String,
    },
    BundleVerify {
        #[arg(long)]
        bundle_path: String,
    },
    BundleExtract {
        #[arg(long)]
        bundle_path: String,
        #[arg(long)]
        output_dir: String,
        #[arg(long, default_value_t = false)]
        force: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let language = Language::from_tag(&cli.lang);

    match cli.command.unwrap_or(Command::Run {
        config: "config/bot.toml".to_string(),
        output_dir: "outputs_rust".to_string(),
        skip_validate_data: false,
    }) {
        Command::Run {
            config,
            output_dir,
            skip_validate_data,
        } => run_command(
            &config,
            &output_dir,
            skip_validate_data,
            language,
            cli.strategy_plugin.as_deref(),
        ),
        Command::Demo {
            config,
            output_root,
            skip_validate_data,
        } => demo_command(
            &config,
            &output_root,
            skip_validate_data,
            language,
            cli.strategy_plugin.as_deref(),
        ),
        Command::Serve {
            root,
            bind,
            prefer_latest,
        } => serve_command(&root, &bind, prefer_latest, language),
        Command::Doctor { config, json } => doctor_command(&config, json),
        Command::DatasetManifest {
            config,
            output_path,
        } => dataset_manifest_command(&config, &output_path),
        Command::Optimize {
            config,
            output_dir,
            train_days,
            test_days,
            short_windows,
            long_windows,
            vol_windows,
            top_ns,
            min_momentums,
            strategy_plugins,
            portfolio_methods,
        } => optimize_command(
            &config,
            &output_dir,
            train_days,
            test_days,
            &short_windows,
            &long_windows,
            &vol_windows,
            &top_ns,
            &min_momentums,
            &strategy_plugins,
            &portfolio_methods,
            language,
            cli.strategy_plugin.as_deref(),
        ),
        Command::Dashboard { output_dir } => {
            let path = build_dashboard_with_language(&output_dir, language)?;
            println!("{}: {}", msg_dashboard(language), path.display());
            Ok(())
        }
        Command::Research {
            config,
            output_dir,
            markets,
            short_windows,
            long_windows,
            vol_windows,
            top_ns,
            min_momentums,
            strategy_plugins,
            portfolio_methods,
        } => research_command(
            &config,
            &output_dir,
            &markets,
            &short_windows,
            &long_windows,
            &vol_windows,
            &top_ns,
            &min_momentums,
            &strategy_plugins,
            &portfolio_methods,
            language,
            cli.strategy_plugin.as_deref(),
        ),
        Command::ResearchReport {
            config,
            output_dir,
            train_days,
            test_days,
            short_windows,
            long_windows,
            vol_windows,
            top_ns,
            min_momentums,
            strategy_plugins,
            portfolio_methods,
            factor_decay_horizons,
            regime_vol_window,
            regime_fast_window,
            regime_slow_window,
        } => research_report_command(
            &config,
            &output_dir,
            train_days,
            test_days,
            &short_windows,
            &long_windows,
            &vol_windows,
            &top_ns,
            &min_momentums,
            &strategy_plugins,
            &portfolio_methods,
            &factor_decay_horizons,
            regime_vol_window,
            regime_fast_window,
            regime_slow_window,
            cli.strategy_plugin.as_deref(),
        ),
        Command::Benchmark {
            config,
            output_dir,
            strategy_plugins,
            portfolio_methods,
        } => benchmark_command(
            &config,
            &output_dir,
            &strategy_plugins,
            &portfolio_methods,
            language,
            cli.strategy_plugin.as_deref(),
        ),
        Command::Replay { config, output_dir } => replay_command(
            &config,
            &output_dir,
            language,
            cli.strategy_plugin.as_deref(),
        ),
        Command::Robustness {
            config,
            output_dir,
            train_days,
            test_days,
            short_windows,
            long_windows,
            vol_windows,
            top_ns,
            min_momentums,
            strategy_plugins,
            portfolio_methods,
        } => robustness_command(
            &config,
            &output_dir,
            train_days,
            test_days,
            &short_windows,
            &long_windows,
            &vol_windows,
            &top_ns,
            &min_momentums,
            &strategy_plugins,
            &portfolio_methods,
            cli.strategy_plugin.as_deref(),
        ),
        Command::ValidateData {
            config,
            output_dir,
            return_outlier_threshold,
            gap_days_threshold,
        } => validate_data_command(
            &config,
            &output_dir,
            return_outlier_threshold,
            gap_days_threshold,
            cli.strategy_plugin.as_deref(),
        ),
        Command::PaperDaemon {
            config,
            output_dir,
            cycles,
            sleep_secs,
            alert_drawdown_ratio,
        } => paper_daemon_command(
            &config,
            &output_dir,
            cycles,
            sleep_secs,
            alert_drawdown_ratio,
            cli.strategy_plugin.as_deref(),
        ),
        Command::Registry { output_dir, top } => registry_command(&output_dir, top),
        Command::ControlCenter {
            output_dir,
            refresh_secs,
            cycles,
        } => control_center_command(&output_dir, refresh_secs, cycles),
        Command::Leaderboard { output_dir, top } => leaderboard_command(&output_dir, top),
        Command::SdkInit {
            id,
            output_dir,
            force,
        } => sdk_init_command(&id, &output_dir, force),
        Command::SdkCheck { package_dir } => sdk_check_command(&package_dir),
        Command::SdkRegister {
            package_dir,
            name,
            description,
        } => sdk_register_command(&package_dir, name.as_deref(), description.as_deref()),
        Command::Plugins => plugins_command(),
        Command::ScaffoldPlugin {
            id,
            output_dir,
            force,
        } => scaffold_plugin_command(&id, &output_dir, force),
        Command::GenSynthData {
            output_dir,
            start_date,
            end_date,
            seed,
            us_symbols,
            a_symbols,
            jp_symbols,
            industries_per_market,
            force,
        } => gen_synth_data_command(GenSynthDataArgs {
            output_dir,
            start_date,
            end_date,
            seed,
            us_symbols,
            a_symbols,
            jp_symbols,
            industries_per_market,
            force,
        }),
        Command::Bundle {
            output_dir,
            bundle_path,
        } => bundle_command(&output_dir, &bundle_path),
        Command::BundleVerify { bundle_path } => bundle_verify_command(&bundle_path),
        Command::BundleExtract {
            bundle_path,
            output_dir,
            force,
        } => bundle_extract_command(&bundle_path, &output_dir, force),
    }
}

#[derive(Debug, Clone)]
struct GenSynthDataArgs {
    output_dir: String,
    start_date: String,
    end_date: String,
    seed: u64,
    us_symbols: usize,
    a_symbols: usize,
    jp_symbols: usize,
    industries_per_market: usize,
    force: bool,
}

fn gen_synth_data_command(args: GenSynthDataArgs) -> Result<()> {
    let start =
        chrono::NaiveDate::parse_from_str(&args.start_date, "%Y-%m-%d").with_context(|| {
            format!(
                "invalid start_date: {} (expected YYYY-MM-DD)",
                args.start_date
            )
        })?;
    let end = chrono::NaiveDate::parse_from_str(&args.end_date, "%Y-%m-%d")
        .with_context(|| format!("invalid end_date: {} (expected YYYY-MM-DD)", args.end_date))?;

    generate_synth_dataset(&SynthDatasetRequest {
        output_dir: PathBuf::from(&args.output_dir),
        start_date: start,
        end_date: end,
        seed: args.seed,
        us_symbols: args.us_symbols,
        a_symbols: args.a_symbols,
        jp_symbols: args.jp_symbols,
        industries_per_market: args.industries_per_market,
        force: args.force,
    })?;

    println!(
        "synthetic dataset generated: {} (US={} A={} JP={} seed={})",
        args.output_dir, args.us_symbols, args.a_symbols, args.jp_symbols, args.seed
    );
    Ok(())
}

fn bundle_command(output_dir: &str, bundle_path: &str) -> Result<()> {
    let out = PathBuf::from(output_dir);
    let bundle_path = if bundle_path.trim().is_empty() {
        let ts = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
        out.join(format!("run_bundle_{ts}.tar.gz"))
    } else {
        PathBuf::from(bundle_path)
    };

    let report = create_run_bundle(&BundleRequest {
        output_dir: out,
        bundle_path,
    })?;
    println!("bundle written: {}", report.bundle_path.display());
    println!(
        "bundle files: included={} missing={}",
        report.manifest.files.len(),
        report.manifest.missing.len()
    );
    Ok(())
}

fn bundle_verify_command(bundle_path: &str) -> Result<()> {
    let report = private_quant_bot::bundle::verify_run_bundle(bundle_path)?;
    println!("bundle verified: {}", bundle_path);
    println!(
        "checked_files={} missing_in_manifest={}",
        report.checked_files,
        report.manifest.missing.len()
    );
    Ok(())
}

fn bundle_extract_command(bundle_path: &str, output_dir: &str, force: bool) -> Result<()> {
    private_quant_bot::bundle::extract_run_bundle(
        &private_quant_bot::bundle::BundleExtractRequest {
            bundle_path: PathBuf::from(bundle_path),
            output_dir: PathBuf::from(output_dir),
            force,
        },
    )?;
    println!("bundle extracted: {} -> {}", bundle_path, output_dir);
    Ok(())
}

fn serve_command(root: &str, bind: &str, prefer_latest: bool, language: Language) -> Result<()> {
    let req = ServeRequest {
        root_dir: PathBuf::from(root),
        bind: bind.to_string(),
        prefer_latest,
    };
    let (report, listener) = start_dashboard_server(&req)?;
    println!(
        "{} | {}={} | {}={}",
        msg_server_started(language),
        msg_server_root(language),
        report.root_dir.display(),
        msg_server_url(language),
        report.base_url
    );
    if let Some(rel) = &report.default_doc_rel {
        println!("default: /{rel}");
    } else {
        println!("default: (none)");
    }
    println!("{}", msg_server_ctrl_c(language));
    serve_forever(
        listener,
        &report.root_dir,
        report.default_doc_rel.as_deref(),
    );
}

fn doctor_command(config_path: &str, json: bool) -> Result<()> {
    let report = run_doctor(&DoctorRequest {
        config_path: PathBuf::from(config_path),
    })?;

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    let range = match (&report.first_date, &report.last_date) {
        (Some(a), Some(b)) => format!("{a}..{b}"),
        _ => "-".to_string(),
    };
    println!(
        "doctor ok | config={} paper_only={} broker_mode={} dates={} range={}",
        report.config_path.display(),
        report.paper_only,
        report.broker_mode,
        report.dates,
        range
    );
    for m in &report.markets {
        let span = match (&m.first_date, &m.last_date) {
            (Some(a), Some(b)) => format!("{a}..{b}"),
            _ => "-".to_string(),
        };
        println!(
            "market={} alloc={:.3} ccy={} fx_to_base={:.6} industry_symbols={} span={} last_day_bars={}",
            m.market,
            m.allocation,
            m.currency,
            m.fx_to_base,
            m.industry_map_symbols,
            span,
            m.last_day_bars
        );
    }
    Ok(())
}

fn dataset_manifest_command(config_path: &str, output_path: &str) -> Result<()> {
    let report = write_dataset_manifest(&DatasetManifestRequest {
        config_path: PathBuf::from(config_path),
        output_path: PathBuf::from(output_path),
    })?;
    println!(
        "dataset manifest written | files={} output={}",
        report.files.len(),
        output_path
    );
    Ok(())
}

fn demo_command(
    config_path: &str,
    output_root: &str,
    skip_validate_data: bool,
    language: Language,
    strategy_plugin_override: Option<&str>,
) -> Result<()> {
    let mut cfg = load_config(config_path)?;
    apply_strategy_plugin_override(&mut cfg, strategy_plugin_override)?;
    let data = load_data_for_config(&cfg)?;

    let output_dir = make_demo_output_dir(output_root)?;
    write_config_snapshot_redacted(Path::new(config_path), &output_dir)?;
    let output_dir_str = output_dir.to_string_lossy();
    let result = QuantBotEngine::from_config(cfg.clone(), data.clone())?.run();
    write_outputs(output_dir_str.as_ref(), &result)?;
    let _ = write_factor_attribution_report(&cfg, &data, output_dir_str.as_ref())?;
    let stats = summarize_result(&result);

    let audit_path = write_audit_snapshot(
        &output_dir,
        "demo",
        "paper-only demo",
        Path::new(config_path),
        &cfg,
        &stats,
    )?;
    if !skip_validate_data {
        let _ = run_data_quality_check(
            &cfg,
            &DataQualityRequest {
                return_outlier_threshold: 0.35,
                gap_days_threshold: 10,
            },
            &output_dir,
        )?;
    }

    let dashboard_path = build_dashboard_with_language(output_dir_str.as_ref(), language)?;
    write_demo_latest(output_root, &output_dir, &dashboard_path)?;

    println!(
        "{} | dates={} trades={} rejections={} final_equity={:.2}",
        msg_demo_completed(language),
        result.equity_curve.len(),
        stats.trades,
        stats.rejections,
        stats.end_equity
    );
    println!("{}: {}", msg_dashboard(language), dashboard_path.display());
    println!(
        "{}: {}",
        msg_open_dashboard_hint(language),
        dashboard_path.display()
    );
    println!("audit_snapshot: {}", audit_path.display());
    let registry = append_registry_backtest(BacktestRegistryLog {
        cfg: &cfg,
        command: "demo",
        output_dir: output_dir_str.as_ref(),
        strategy_plugin: &cfg.strategy.strategy_plugin,
        portfolio_method: &cfg.strategy.portfolio_method,
        primary_metric_name: "pnl_ratio",
        primary_metric_value: stats.pnl_ratio,
        stats: &stats,
        notes: "demo run (paper-only)",
    })?;
    println!(
        "run_registry: {} (runs={})",
        registry.csv_path.display(),
        registry.total_runs
    );

    Ok(())
}

fn make_demo_output_dir(output_root: &str) -> Result<PathBuf> {
    let ts = chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let dir = PathBuf::from(output_root).join(format!("run_{ts}"));
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("create demo output dir {}", dir.display()))?;
    Ok(dir)
}

fn write_demo_latest(output_root: &str, run_dir: &Path, dashboard_path: &Path) -> Result<()> {
    let root = PathBuf::from(output_root);
    std::fs::create_dir_all(&root)
        .with_context(|| format!("create demo output root {}", root.display()))?;

    // Absolute paths avoid cwd confusion for users running from different directories.
    std::fs::write(root.join("LATEST_RUN.txt"), run_dir.display().to_string())
        .context("write LATEST_RUN.txt")?;
    std::fs::write(
        root.join("LATEST_DASHBOARD.txt"),
        dashboard_path.display().to_string(),
    )
    .context("write LATEST_DASHBOARD.txt")?;
    Ok(())
}

fn plugins_command() -> Result<()> {
    println!("strategy plugins:");
    for plugin in runtime_strategy_plugin_catalog() {
        println!(
            "- {} | {} | {} | source={}",
            plugin.id, plugin.name, plugin.description, plugin.source
        );
    }
    Ok(())
}

fn scaffold_plugin_command(plugin_id: &str, output_dir: &str, force: bool) -> Result<()> {
    let out = create_strategy_plugin_scaffold(plugin_id, output_dir, force)?;
    println!("plugin scaffold created:");
    println!("- {}", out.plugin_file.display());
    println!("- {}", out.notes_file.display());
    Ok(())
}

fn registry_command(output_dir: &str, top: usize) -> Result<()> {
    let root = infer_registry_root(output_dir);
    let csv_path = root.join("run_registry.csv");
    let entries = read_run_registry(&csv_path)?;
    if entries.is_empty() {
        println!("run registry is empty: {}", csv_path.display());
        return Ok(());
    }

    let (_, markdown_path) = write_registry_views(&root, &entries)?;
    let top_rows = top_registry_entries(&entries, top.max(1));

    println!(
        "registry refreshed | runs={} csv={} markdown={}",
        entries.len(),
        csv_path.display(),
        markdown_path.display()
    );
    for (idx, row) in top_rows.iter().enumerate() {
        let plugin = if row.strategy_plugin.is_empty() {
            "-"
        } else {
            row.strategy_plugin.as_str()
        };
        let method = if row.portfolio_method.is_empty() {
            "-"
        } else {
            row.portfolio_method.as_str()
        };
        println!(
            "{:>2}. {} {} {}:{:.4} composite={:.4} pnl={:.2}% sharpe={:.3} plugin={} method={}",
            idx + 1,
            row.timestamp_utc,
            row.command,
            row.primary_metric_name,
            row.primary_metric_value,
            row.composite_score,
            row.pnl_ratio * 100.0,
            row.sharpe,
            plugin,
            method
        );
    }
    Ok(())
}

fn control_center_command(output_dir: &str, refresh_secs: u64, cycles: usize) -> Result<()> {
    let report = run_control_center(&ControlCenterRequest {
        output_dir: PathBuf::from(output_dir),
        refresh_secs,
        cycles,
    })?;
    println!(
        "control center completed | ticks={} registry_runs={} last_end_equity={:.2}",
        report.ticks, report.registry_runs, report.last_end_equity
    );
    Ok(())
}

fn leaderboard_command(output_dir: &str, top: usize) -> Result<()> {
    let report = build_public_leaderboard(&LeaderboardRequest {
        output_dir: PathBuf::from(output_dir),
        top: top.max(1),
    })?;
    println!(
        "leaderboard built | rows={} csv={} md={} html={}",
        report.rows,
        report.csv_path.display(),
        report.markdown_path.display(),
        report.html_path.display()
    );
    Ok(())
}

fn sdk_init_command(id: &str, output_dir: &str, force: bool) -> Result<()> {
    let report = create_strategy_sdk(&SdkInitRequest {
        plugin_id: id.to_string(),
        output_dir: PathBuf::from(output_dir),
        force,
        project_root: std::env::current_dir().context("read current_dir failed")?,
    })?;
    println!("sdk package created:");
    println!("- {}", report.package_dir.display());
    println!("- {}", report.cargo_toml.display());
    println!("- {}", report.lib_rs.display());
    println!("- {}", report.manifest_toml.display());
    println!("- {}", report.readme_md.display());
    Ok(())
}

fn sdk_check_command(package_dir: &str) -> Result<()> {
    let report = check_strategy_sdk(package_dir)?;
    println!(
        "sdk check | valid={} package={}",
        report.valid,
        report.package_dir.display()
    );
    for warning in report.warnings {
        println!("warning: {warning}");
    }
    for error in &report.errors {
        println!("error: {error}");
    }
    if !report.valid {
        return Err(anyhow::anyhow!("sdk check failed"));
    }
    Ok(())
}

fn sdk_register_command(
    package_dir: &str,
    name: Option<&str>,
    description: Option<&str>,
) -> Result<()> {
    let report = register_strategy_sdk(&SdkRegisterRequest {
        package_dir: PathBuf::from(package_dir),
        display_name: name.map(|s| s.to_string()),
        description: description.map(|s| s.to_string()),
        registry_path: None,
    })?;
    println!(
        "sdk register | plugin_id={} registry={} created={}",
        report.plugin_id,
        report.registry_path.display(),
        report.created
    );
    Ok(())
}

fn run_command(
    config_path: &str,
    output_dir: &str,
    skip_validate_data: bool,
    language: Language,
    strategy_plugin_override: Option<&str>,
) -> Result<()> {
    let mut cfg = load_config(config_path)?;
    apply_strategy_plugin_override(&mut cfg, strategy_plugin_override)?;
    let data = load_data_for_config(&cfg)?;

    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("create output_dir failed: {output_dir}"))?;
    write_config_snapshot_redacted(Path::new(config_path), Path::new(output_dir))?;

    let result = QuantBotEngine::from_config(cfg.clone(), data.clone())?.run();
    write_outputs(output_dir, &result)?;
    let attribution = write_factor_attribution_report(&cfg, &data, output_dir)?;
    let stats = summarize_result(&result);
    let audit_path = write_audit_snapshot(
        output_dir,
        "run",
        "paper run",
        Path::new(config_path),
        &cfg,
        &stats,
    )?;
    if !skip_validate_data {
        let _ = run_data_quality_check(
            &cfg,
            &DataQualityRequest {
                return_outlier_threshold: 0.35,
                gap_days_threshold: 10,
            },
            output_dir,
        )?;
    }
    let dashboard_path = build_dashboard_with_language(output_dir, language)?;

    println!(
        "{} | dates={} trades={} rejections={} final_equity={:.2}",
        msg_run_completed(language),
        result.equity_curve.len(),
        stats.trades,
        stats.rejections,
        stats.end_equity
    );
    println!("{}: {}", msg_dashboard(language), dashboard_path.display());
    println!("factor_attribution: {}", attribution.csv_path.display());
    println!(
        "factor_attribution_summary: {}",
        attribution.summary_path.display()
    );
    println!("audit_snapshot: {}", audit_path.display());
    let registry = append_registry_backtest(BacktestRegistryLog {
        cfg: &cfg,
        command: "run",
        output_dir,
        strategy_plugin: &cfg.strategy.strategy_plugin,
        portfolio_method: &cfg.strategy.portfolio_method,
        primary_metric_name: "pnl_ratio",
        primary_metric_value: stats.pnl_ratio,
        stats: &stats,
        notes: "paper run",
    })?;
    println!(
        "run_registry: {} (runs={})",
        registry.csv_path.display(),
        registry.total_runs
    );

    Ok(())
}

fn write_config_snapshot_redacted(config_path: &Path, output_dir: &Path) -> Result<PathBuf> {
    let raw = std::fs::read_to_string(config_path)
        .with_context(|| format!("read config for snapshot failed: {}", config_path.display()))?;
    let redacted = redact_toml_secrets(&raw);
    let out = output_dir.join("config_used_redacted.toml");
    std::fs::write(&out, redacted)
        .with_context(|| format!("write config snapshot failed: {}", out.display()))?;
    Ok(out)
}

fn redact_toml_secrets(input: &str) -> String {
    // Best-effort redaction. Users should still avoid putting secrets in configs.
    // We redact common keys while preserving formatting and comments as much as possible.
    let sensitive_keys = [
        "account_id",
        "token",
        "secret",
        "password",
        "passwd",
        "api_key",
        "apikey",
        "client_secret",
        "access_key",
        "private_key",
    ];

    let mut out = String::with_capacity(input.len());
    for line in input.lines() {
        let trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            out.push_str(line);
            out.push('\n');
            continue;
        }

        let (code, comment) = match line.split_once('#') {
            Some((a, b)) => (a, Some(b)),
            None => (line, None),
        };
        let Some((left, right)) = code.split_once('=') else {
            out.push_str(line);
            out.push('\n');
            continue;
        };

        let key = left.trim().to_ascii_lowercase();
        let val = right.trim();
        let is_sensitive = sensitive_keys.iter().any(|k| key.contains(k));
        if is_sensitive && !val.is_empty() && val != "\"\"" && val != "''" {
            // Preserve leading indentation and original key formatting.
            let prefix_len = line.len().saturating_sub(trimmed.len());
            let indent = &line[..prefix_len];
            out.push_str(indent);
            out.push_str(left.trim_end());
            out.push_str(" = \"REDACTED\"");
            if let Some(c) = comment {
                out.push_str(" #");
                out.push_str(c);
            }
            out.push('\n');
            continue;
        }

        out.push_str(line);
        out.push('\n');
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn optimize_command(
    config_path: &str,
    output_dir: &str,
    train_days: usize,
    test_days: usize,
    short_windows: &str,
    long_windows: &str,
    vol_windows: &str,
    top_ns: &str,
    min_momentums: &str,
    strategy_plugins: &str,
    portfolio_methods: &str,
    language: Language,
    strategy_plugin_override: Option<&str>,
) -> Result<()> {
    let mut cfg = load_config(config_path)?;
    apply_strategy_plugin_override(&mut cfg, strategy_plugin_override)?;
    let data = load_data_for_config(&cfg)?;

    let request = WalkForwardRequest {
        train_days,
        test_days,
        strategy_plugins: resolve_strategy_plugins(strategy_plugins, &cfg)?,
        short_windows: parse_usize_list(short_windows)?,
        long_windows: parse_usize_list(long_windows)?,
        vol_windows: parse_usize_list(vol_windows)?,
        top_ns: parse_usize_list(top_ns)?,
        min_momentums: parse_f64_list(min_momentums)?,
        portfolio_methods: parse_portfolio_methods(portfolio_methods)?,
    };

    let report = run_walk_forward(&cfg, &data, &request, output_dir)?;
    let summary_path = Path::new(output_dir).join("walk_forward_summary.txt");
    println!(
        "{} | folds={} summary={} ",
        msg_walk_forward_completed(language),
        report.folds.len(),
        summary_path.display()
    );
    let fold_test_stats = report
        .folds
        .iter()
        .map(|f| f.test_stats.clone())
        .collect::<Vec<_>>();
    let avg_stats = average_backtest_stats(&fold_test_stats);
    let avg_test_pnl = avg_stats.pnl_ratio;
    let strategy_plugin = if request.strategy_plugins.len() == 1 {
        request.strategy_plugins[0].clone()
    } else {
        "mixed".to_string()
    };
    let portfolio_method = if request.portfolio_methods.len() == 1 {
        request.portfolio_methods[0].clone()
    } else {
        "mixed".to_string()
    };
    let registry = append_registry_backtest(BacktestRegistryLog {
        cfg: &cfg,
        command: "optimize",
        output_dir,
        strategy_plugin: &strategy_plugin,
        portfolio_method: &portfolio_method,
        primary_metric_name: "avg_test_pnl_ratio",
        primary_metric_value: avg_test_pnl,
        stats: &avg_stats,
        notes: &format!("folds={}", report.folds.len()),
    })?;
    println!(
        "run_registry: {} (runs={})",
        registry.csv_path.display(),
        registry.total_runs
    );

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn research_command(
    config_path: &str,
    output_dir: &str,
    markets: &str,
    short_windows: &str,
    long_windows: &str,
    vol_windows: &str,
    top_ns: &str,
    min_momentums: &str,
    strategy_plugins: &str,
    portfolio_methods: &str,
    language: Language,
    strategy_plugin_override: Option<&str>,
) -> Result<()> {
    let mut cfg = load_config(config_path)?;
    apply_strategy_plugin_override(&mut cfg, strategy_plugin_override)?;
    let data = load_data_for_config(&cfg)?;

    let req = ResearchRequest {
        target_markets: parse_string_list(markets),
        strategy_plugins: resolve_strategy_plugins(strategy_plugins, &cfg)?,
        short_windows: parse_usize_list(short_windows)?,
        long_windows: parse_usize_list(long_windows)?,
        vol_windows: parse_usize_list(vol_windows)?,
        top_ns: parse_usize_list(top_ns)?,
        min_momentums: parse_f64_list(min_momentums)?,
        portfolio_methods: parse_portfolio_methods(portfolio_methods)?,
    };

    let report = run_cross_market_research(&cfg, &data, &req, output_dir)?;
    println!(
        "{} | rows={} leaderboard={}/research_leaderboard.csv",
        msg_research_completed(language),
        report.rows.len(),
        output_dir
    );
    if let Some(top) = report.rows.first() {
        let registry = append_registry_backtest(BacktestRegistryLog {
            cfg: &cfg,
            command: "research",
            output_dir,
            strategy_plugin: &top.strategy.strategy_plugin,
            portfolio_method: &top.strategy.portfolio_method,
            primary_metric_name: "top_score",
            primary_metric_value: top.score,
            stats: &top.stats,
            notes: &format!("scenario={}", top.scenario),
        })?;
        println!(
            "run_registry: {} (runs={})",
            registry.csv_path.display(),
            registry.total_runs
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn research_report_command(
    config_path: &str,
    output_dir: &str,
    train_days: usize,
    test_days: usize,
    short_windows: &str,
    long_windows: &str,
    vol_windows: &str,
    top_ns: &str,
    min_momentums: &str,
    strategy_plugins: &str,
    portfolio_methods: &str,
    factor_decay_horizons: &str,
    regime_vol_window: usize,
    regime_fast_window: usize,
    regime_slow_window: usize,
    strategy_plugin_override: Option<&str>,
) -> Result<()> {
    let mut cfg = load_config(config_path)?;
    apply_strategy_plugin_override(&mut cfg, strategy_plugin_override)?;
    let data = load_data_for_config(&cfg)?;

    let req = ResearchReportRequest {
        walk_forward: WalkForwardRequest {
            train_days,
            test_days,
            strategy_plugins: resolve_strategy_plugins(strategy_plugins, &cfg)?,
            short_windows: parse_usize_list(short_windows)?,
            long_windows: parse_usize_list(long_windows)?,
            vol_windows: parse_usize_list(vol_windows)?,
            top_ns: parse_usize_list(top_ns)?,
            min_momentums: parse_f64_list(min_momentums)?,
            portfolio_methods: parse_portfolio_methods(portfolio_methods)?,
        },
        factor_decay_horizons: parse_usize_list(factor_decay_horizons)?,
        regime_vol_window,
        regime_fast_window,
        regime_slow_window,
    };

    let (report, artifacts) = write_research_report(&cfg, &data, &req, output_dir)?;
    println!(
        "research report completed | folds={} regime_rows={} factor_decay_rows={}",
        report.walk_forward_rows.len(),
        report.regime_rows.len(),
        report.factor_decay_rows.len()
    );
    println!(
        "research_report: {} | {} | {}",
        artifacts.markdown_path.display(),
        artifacts.html_path.display(),
        artifacts.json_path.display()
    );
    let registry = append_registry_operation(
        &cfg,
        "research-report",
        output_dir,
        "avg_test_sharpe",
        report.walk_forward_summary.avg_test_sharpe,
        &format!(
            "folds={} regime_rows={} factor_decay_rows={}",
            report.walk_forward_rows.len(),
            report.regime_rows.len(),
            report.factor_decay_rows.len()
        ),
    )?;
    println!(
        "run_registry: {} (runs={})",
        registry.csv_path.display(),
        registry.total_runs
    );
    Ok(())
}

fn load_data_for_config(cfg: &private_quant_bot::config::BotConfig) -> Result<CsvDataPortal> {
    CsvDataPortal::new(
        cfg.markets
            .values()
            .map(|m| (m.name.clone(), m.data_file.clone()))
            .collect(),
    )
}

fn replay_command(
    config_path: &str,
    output_dir: &str,
    language: Language,
    strategy_plugin_override: Option<&str>,
) -> Result<()> {
    let mut cfg = load_config(config_path)?;
    apply_strategy_plugin_override(&mut cfg, strategy_plugin_override)?;
    let data = load_data_for_config(&cfg)?;
    let summary = run_event_replay(&cfg, &data, output_dir)?;
    println!(
        "{} | events={} dates={} markets={} output={}",
        msg_replay_completed(language),
        summary.events,
        summary.dates,
        summary.markets,
        output_dir
    );
    let registry = append_registry_operation(
        &cfg,
        "replay",
        output_dir,
        "events",
        summary.events as f64,
        &format!("dates={} markets={}", summary.dates, summary.markets),
    )?;
    println!(
        "run_registry: {} (runs={})",
        registry.csv_path.display(),
        registry.total_runs
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn robustness_command(
    config_path: &str,
    output_dir: &str,
    train_days: usize,
    test_days: usize,
    short_windows: &str,
    long_windows: &str,
    vol_windows: &str,
    top_ns: &str,
    min_momentums: &str,
    strategy_plugins: &str,
    portfolio_methods: &str,
    strategy_plugin_override: Option<&str>,
) -> Result<()> {
    let mut cfg = load_config(config_path)?;
    apply_strategy_plugin_override(&mut cfg, strategy_plugin_override)?;
    let data = load_data_for_config(&cfg)?;

    let req = RobustnessRequest {
        train_days,
        test_days,
        strategy_plugins: resolve_strategy_plugins(strategy_plugins, &cfg)?,
        short_windows: parse_usize_list(short_windows)?,
        long_windows: parse_usize_list(long_windows)?,
        vol_windows: parse_usize_list(vol_windows)?,
        top_ns: parse_usize_list(top_ns)?,
        min_momentums: parse_f64_list(min_momentums)?,
        portfolio_methods: parse_portfolio_methods(portfolio_methods)?,
    };

    let report = run_robustness_assessment(&cfg, &data, &req, output_dir)?;
    println!(
        "robustness completed | folds={} summary={}/robustness_summary.txt",
        report.folds.len(),
        output_dir
    );
    let selected_stats = report
        .folds
        .iter()
        .map(|f| f.selected_test_stats.clone())
        .collect::<Vec<_>>();
    let avg_stats = average_backtest_stats(&selected_stats);
    let avg_deflated_sharpe_proxy = if report.folds.is_empty() {
        0.0
    } else {
        report
            .folds
            .iter()
            .map(|f| f.deflated_sharpe_proxy)
            .sum::<f64>()
            / report.folds.len() as f64
    };
    let strategy_plugin = if req.strategy_plugins.len() == 1 {
        req.strategy_plugins[0].clone()
    } else {
        "mixed".to_string()
    };
    let portfolio_method = if req.portfolio_methods.len() == 1 {
        req.portfolio_methods[0].clone()
    } else {
        "mixed".to_string()
    };
    let registry = append_registry_backtest(BacktestRegistryLog {
        cfg: &cfg,
        command: "robustness",
        output_dir,
        strategy_plugin: &strategy_plugin,
        portfolio_method: &portfolio_method,
        primary_metric_name: "avg_deflated_sharpe_proxy",
        primary_metric_value: avg_deflated_sharpe_proxy,
        stats: &avg_stats,
        notes: &format!("folds={}", report.folds.len()),
    })?;
    println!(
        "run_registry: {} (runs={})",
        registry.csv_path.display(),
        registry.total_runs
    );
    Ok(())
}

fn validate_data_command(
    config_path: &str,
    output_dir: &str,
    return_outlier_threshold: f64,
    gap_days_threshold: i64,
    strategy_plugin_override: Option<&str>,
) -> Result<()> {
    let mut cfg = load_config(config_path)?;
    apply_strategy_plugin_override(&mut cfg, strategy_plugin_override)?;
    let report = run_data_quality_check(
        &cfg,
        &DataQualityRequest {
            return_outlier_threshold,
            gap_days_threshold,
        },
        output_dir,
    )?;
    println!(
        "data validation completed | markets={} report={}/data_quality_report.csv",
        report.rows.len(),
        output_dir
    );
    let pass_markets = report.rows.iter().filter(|r| r.status == "PASS").count();
    let warn_markets = report.rows.iter().filter(|r| r.status == "WARN").count();
    let fail_markets = report.rows.iter().filter(|r| r.status == "FAIL").count();
    let pass_rate = if report.rows.is_empty() {
        0.0
    } else {
        pass_markets as f64 / report.rows.len() as f64
    };
    let registry = append_registry_operation(
        &cfg,
        "validate-data",
        output_dir,
        "pass_rate",
        pass_rate,
        &format!(
            "pass={} warn={} fail={}",
            pass_markets, warn_markets, fail_markets
        ),
    )?;
    println!(
        "run_registry: {} (runs={})",
        registry.csv_path.display(),
        registry.total_runs
    );
    Ok(())
}

fn paper_daemon_command(
    config_path: &str,
    output_dir: &str,
    cycles: usize,
    sleep_secs: u64,
    alert_drawdown_ratio: f64,
    strategy_plugin_override: Option<&str>,
) -> Result<()> {
    let mut cfg = load_config(config_path)?;
    apply_strategy_plugin_override(&mut cfg, strategy_plugin_override)?;
    let data = load_data_for_config(&cfg)?;
    let report = run_paper_daemon(
        &cfg,
        &data,
        output_dir,
        &PaperDaemonRequest {
            cycles,
            sleep_secs,
            alert_drawdown_ratio,
        },
    )?;
    println!(
        "paper daemon completed | cycles={} alerts={} state={}/paper_daemon_state.json",
        report.cycles_run, report.alerts, output_dir
    );
    let stability_score = report.cycles_run as f64 / (1.0 + report.alerts as f64);
    let registry = append_registry_operation(
        &cfg,
        "paper-daemon",
        output_dir,
        "stability_score",
        stability_score,
        &format!("cycles={} alerts={}", report.cycles_run, report.alerts),
    )?;
    println!(
        "run_registry: {} (runs={})",
        registry.csv_path.display(),
        registry.total_runs
    );
    Ok(())
}

fn parse_usize_list(text: &str) -> Result<Vec<usize>> {
    let out: Vec<usize> = text
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| {
            v.parse::<usize>()
                .with_context(|| format!("invalid usize: {v}"))
        })
        .collect::<Result<Vec<_>>>()?;

    if out.is_empty() {
        return Err(anyhow::anyhow!("empty usize list"));
    }
    Ok(out)
}

fn parse_f64_list(text: &str) -> Result<Vec<f64>> {
    let out: Vec<f64> = text
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| {
            v.parse::<f64>()
                .with_context(|| format!("invalid f64: {v}"))
        })
        .collect::<Result<Vec<_>>>()?;

    if out.is_empty() {
        return Err(anyhow::anyhow!("empty f64 list"));
    }
    Ok(out)
}

fn parse_string_list(text: &str) -> Vec<String> {
    text.split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_uppercase())
        .collect()
}

fn parse_portfolio_methods(text: &str) -> Result<Vec<String>> {
    let methods: Vec<String> = text
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_lowercase())
        .collect();

    if methods.is_empty() {
        return Err(anyhow::anyhow!("empty portfolio method list"));
    }
    for method in &methods {
        if method != "risk_parity" && method != "hrp" {
            return Err(anyhow::anyhow!(
                "unsupported portfolio method: {method}; expected risk_parity or hrp"
            ));
        }
    }
    Ok(methods)
}

fn resolve_portfolio_methods(text: &str, cfg: &BotConfig) -> Result<Vec<String>> {
    if text.trim().is_empty() {
        return Ok(vec![cfg.strategy.portfolio_method.clone()]);
    }
    parse_portfolio_methods(text)
}

fn parse_strategy_plugins(text: &str) -> Vec<String> {
    text.split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_lowercase())
        .collect()
}

fn resolve_strategy_plugins(text: &str, cfg: &BotConfig) -> Result<Vec<String>> {
    let plugins = if text.trim().is_empty() {
        vec![cfg.strategy.strategy_plugin.clone()]
    } else {
        parse_strategy_plugins(text)
    };

    if plugins.is_empty() {
        return Err(anyhow::anyhow!("empty strategy plugin list"));
    }
    for plugin in &plugins {
        if !is_supported_strategy_plugin(plugin) {
            return Err(anyhow::anyhow!(
                "unsupported strategy plugin: {plugin}; run `cargo run -- plugins` to list available plugins"
            ));
        }
    }
    Ok(plugins)
}

fn apply_strategy_plugin_override(
    cfg: &mut BotConfig,
    strategy_plugin_override: Option<&str>,
) -> Result<()> {
    let Some(raw) = strategy_plugin_override else {
        return Ok(());
    };

    let plugin = raw.trim().to_lowercase();
    if plugin.is_empty() {
        return Ok(());
    }
    if !is_supported_strategy_plugin(&plugin) {
        return Err(anyhow::anyhow!(
            "unsupported strategy plugin: {plugin}; run `cargo run -- plugins` to list available plugins"
        ));
    }
    cfg.strategy.strategy_plugin = plugin;
    Ok(())
}

fn benchmark_command(
    config_path: &str,
    output_dir: &str,
    strategy_plugins: &str,
    portfolio_methods: &str,
    language: Language,
    strategy_plugin_override: Option<&str>,
) -> Result<()> {
    let mut cfg = load_config(config_path)?;
    apply_strategy_plugin_override(&mut cfg, strategy_plugin_override)?;
    let data = load_data_for_config(&cfg)?;
    let request = BenchmarkRequest {
        strategy_plugins: resolve_strategy_plugins(strategy_plugins, &cfg)?,
        portfolio_methods: resolve_portfolio_methods(portfolio_methods, &cfg)?,
    };
    let report = run_benchmark_suite(&cfg, &data, output_dir, &request)?;
    println!(
        "{} | scenarios={} report={}/baseline_results.csv",
        msg_benchmark_completed(language),
        report.rows.len(),
        output_dir
    );
    if let Some(top) = report.rows.first() {
        let registry = append_registry_backtest(BacktestRegistryLog {
            cfg: &cfg,
            command: "benchmark",
            output_dir,
            strategy_plugin: &top.strategy_plugin,
            portfolio_method: &top.portfolio_method,
            primary_metric_name: "top_score",
            primary_metric_value: top.score,
            stats: &top.stats,
            notes: &format!("scenario={}", top.scenario),
        })?;
        println!(
            "run_registry: {} (runs={})",
            registry.csv_path.display(),
            registry.total_runs
        );
    }
    Ok(())
}

struct BacktestRegistryLog<'a> {
    cfg: &'a BotConfig,
    command: &'a str,
    output_dir: &'a str,
    strategy_plugin: &'a str,
    portfolio_method: &'a str,
    primary_metric_name: &'a str,
    primary_metric_value: f64,
    stats: &'a BacktestStats,
    notes: &'a str,
}

fn append_registry_backtest(
    log: BacktestRegistryLog<'_>,
) -> Result<private_quant_bot::registry::RegistryWriteReport> {
    let registry_root = infer_registry_root(log.output_dir);
    let entry = RunRegistryEntry::from_backtest_input(RunRegistryBacktestInput {
        command: log.command.to_string(),
        output_dir: PathBuf::from(log.output_dir),
        strategy_plugin: log.strategy_plugin.to_string(),
        portfolio_method: log.portfolio_method.to_string(),
        markets: markets_for_registry(log.cfg),
        primary_metric_name: log.primary_metric_name.to_string(),
        primary_metric_value: log.primary_metric_value,
        stats: log.stats.clone(),
        notes: log.notes.to_string(),
    });
    append_run_registry(registry_root, &entry)
}

fn append_registry_operation(
    cfg: &BotConfig,
    command: &str,
    output_dir: &str,
    primary_metric_name: &str,
    primary_metric_value: f64,
    notes: &str,
) -> Result<private_quant_bot::registry::RegistryWriteReport> {
    let registry_root = infer_registry_root(output_dir);
    let entry = RunRegistryEntry::from_operation_input(RunRegistryOperationInput {
        command: command.to_string(),
        output_dir: PathBuf::from(output_dir),
        markets: markets_for_registry(cfg),
        primary_metric_name: primary_metric_name.to_string(),
        primary_metric_value,
        notes: notes.to_string(),
    });
    append_run_registry(registry_root, &entry)
}

fn markets_for_registry(cfg: &BotConfig) -> String {
    let mut markets = cfg.markets.keys().cloned().collect::<Vec<_>>();
    markets.sort();
    markets.join("|")
}

fn average_backtest_stats(rows: &[BacktestStats]) -> BacktestStats {
    if rows.is_empty() {
        return BacktestStats::default();
    }

    let n = rows.len() as f64;
    BacktestStats {
        pnl_ratio: rows.iter().map(|r| r.pnl_ratio).sum::<f64>() / n,
        max_drawdown: rows.iter().map(|r| r.max_drawdown).sum::<f64>() / n,
        sharpe: rows.iter().map(|r| r.sharpe).sum::<f64>() / n,
        sortino: rows.iter().map(|r| r.sortino).sum::<f64>() / n,
        calmar: rows.iter().map(|r| r.calmar).sum::<f64>() / n,
        daily_win_rate: rows.iter().map(|r| r.daily_win_rate).sum::<f64>() / n,
        profit_factor: rows.iter().map(|r| r.profit_factor).sum::<f64>() / n,
        trades: rows.iter().map(|r| r.trades).sum::<usize>() / rows.len(),
        rejections: rows.iter().map(|r| r.rejections).sum::<usize>() / rows.len(),
        ..BacktestStats::default()
    }
}
