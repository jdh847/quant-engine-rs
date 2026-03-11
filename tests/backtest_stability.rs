use private_quant_bot::{
    config::load_config,
    data::CsvDataPortal,
    engine::{summarize_result, QuantBotEngine},
};

#[test]
fn paper_backtest_is_deterministic_on_sample_dataset() {
    let cfg = load_config("config/bot.toml").expect("config should load");
    let data = CsvDataPortal::new(
        cfg.markets
            .values()
            .map(|m| (m.name.clone(), m.data_file.clone()))
            .collect(),
    )
    .expect("csv should load");

    let result = QuantBotEngine::from_config_force_sim(cfg, data).run();
    let stats = summarize_result(&result);

    // Golden numbers for the repo's bundled sample dataset.
    assert_eq!(stats.trades, 23);
    assert_eq!(stats.rejections, 7);
    assert!((stats.end_equity - 1_011_913.552_0).abs() < 0.05);
    assert!((stats.max_drawdown - 0.001871).abs() < 1e-6);
}
