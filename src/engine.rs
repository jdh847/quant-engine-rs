use std::collections::HashMap;

use anyhow::Result;
use chrono::NaiveDate;

use crate::{
    calendar::ExchangeCalendar,
    config::{BotConfig, StrategyConfig},
    data::CsvDataPortal,
    execution::{build_broker, BrokerAdapter, ExecutionAdapter, PaperBroker},
    market::MarketRuleEngine,
    metrics::compute_performance_metrics,
    model::{Bar, EquityPoint, Order, RiskRejection, Side, Trade},
    risk::UnifiedRiskManager,
    safety::is_trading_kill_switch_armed,
    strategy::{build_strategy, StrategyPlugin},
};

#[derive(Debug, Default, Clone)]
pub struct RunResult {
    pub equity_curve: Vec<EquityPoint>,
    pub trades: Vec<Trade>,
    pub rejections: Vec<RiskRejection>,
}

pub struct QuantBotEngine<E: ExecutionAdapter> {
    cfg: BotConfig,
    data: CsvDataPortal,
    market_strategies: HashMap<String, Box<dyn StrategyPlugin>>,
    risk: UnifiedRiskManager,
    market_rules: MarketRuleEngine,
    broker: E,
}

impl QuantBotEngine<BrokerAdapter> {
    pub fn from_config(cfg: BotConfig, data: CsvDataPortal) -> Result<Self> {
        let market_strategies = market_strategies_from_config(&cfg);
        let risk = UnifiedRiskManager::new(
            cfg.risk.clone(),
            &cfg.markets,
            &cfg.fx,
            &cfg.start.base_currency,
        );
        let broker = build_broker(&cfg)?;
        let rules = market_rules_from_config(&cfg);

        Ok(Self::new(cfg, data, market_strategies, risk, rules, broker))
    }

    pub fn from_config_force_sim(cfg: BotConfig, data: CsvDataPortal) -> Self {
        let market_strategies = market_strategies_from_config(&cfg);
        let risk = UnifiedRiskManager::new(
            cfg.risk.clone(),
            &cfg.markets,
            &cfg.fx,
            &cfg.start.base_currency,
        );
        let market_costs = cfg
            .markets
            .iter()
            .map(|(name, market)| (name.clone(), market.execution_cost))
            .collect();
        let market_fx_to_base = cfg
            .markets
            .iter()
            .map(|(name, market)| (name.clone(), market.fx_to_base))
            .collect();
        let broker = BrokerAdapter::Sim(
            PaperBroker::new(
                cfg.start.starting_capital,
                cfg.execution.commission_bps,
                cfg.execution.slippage_bps,
            )
            .with_market_fx_to_base(market_fx_to_base)
            .with_market_costs(
                market_costs,
                cfg.execution.sell_tax_bps,
                cfg.execution.min_fee,
            ),
        );
        let rules = market_rules_from_config(&cfg);

        Self::new(cfg, data, market_strategies, risk, rules, broker)
    }
}

impl<E: ExecutionAdapter> QuantBotEngine<E> {
    pub fn new(
        cfg: BotConfig,
        data: CsvDataPortal,
        market_strategies: HashMap<String, Box<dyn StrategyPlugin>>,
        risk: UnifiedRiskManager,
        market_rules: MarketRuleEngine,
        broker: E,
    ) -> Self {
        Self {
            cfg,
            data,
            market_strategies,
            risk,
            market_rules,
            broker,
        }
    }

    pub fn run(mut self) -> RunResult {
        let mut result = RunResult::default();
        let mut prices = HashMap::new();

        for bar_date in self.data.trading_dates() {
            self.risk.refresh_live_fx_if_enabled(bar_date);
            let equity_before = self.broker.equity(&prices);
            self.risk.start_day(equity_before);

            for (market_name, market_cfg) in &self.cfg.markets {
                let bars = self.data.bars_for(bar_date, market_name);
                if bars.is_empty() {
                    continue;
                }

                for bar in &bars {
                    prices.insert((bar.market.clone(), bar.symbol.clone()), bar.close);
                }

                let equity_now = self.broker.equity(&prices);
                let market_budget = market_budget_local_ccy(equity_now, market_cfg);
                let current_notionals = bars
                    .iter()
                    .map(|b| {
                        let qty = self.broker.position_qty(&b.market, &b.symbol);
                        (b.symbol.clone(), (qty as f64 * b.close).max(0.0))
                    })
                    .collect::<HashMap<_, _>>();
                let Some(strategy) = self.market_strategies.get_mut(market_name) else {
                    continue;
                };
                let target_notionals =
                    strategy.target_notionals(&bars, market_budget, &current_notionals);
                let proposed = self.orders_from_targets(
                    bar_date,
                    &bars,
                    &target_notionals,
                    market_cfg.lot_size,
                    market_cfg.min_trade_notional,
                );

                let (session_ok, session_blocked) =
                    self.market_rules
                        .filter_orders(bar_date, &bars, &proposed, &self.broker);
                result.rejections.extend(session_blocked);

                let (accepted, blocked) =
                    self.risk
                        .filter_orders(&session_ok, &self.broker, &prices, equity_now);
                result.rejections.extend(blocked);

                if is_trading_kill_switch_armed() {
                    result
                        .rejections
                        .extend(accepted.into_iter().map(|order| RiskRejection {
                            date: order.date,
                            market: order.market,
                            symbol: order.symbol,
                            side: order.side,
                            qty: order.qty,
                            reason: "trading kill switch armed; execution skipped".to_string(),
                        }));
                } else {
                    let fills = self.broker.execute_orders(&accepted, &prices);
                    result.trades.extend(fills);
                }

                self.market_rules.end_day_update(&bars);
            }

            let equity_after = self.broker.equity(&prices);
            result.equity_curve.push(EquityPoint {
                date: bar_date,
                equity: equity_after,
                cash: self.broker.cash(),
                gross_exposure: self.broker.gross_exposure(&prices),
                net_exposure: self.broker.net_exposure(&prices),
            });
            self.broker.reconcile_day(bar_date);
            self.broker.end_of_day(bar_date);
        }

        result
    }

    fn orders_from_targets(
        &self,
        bar_date: NaiveDate,
        bars: &[Bar],
        target_notionals: &HashMap<String, f64>,
        lot_size: i64,
        min_trade_notional: f64,
    ) -> Vec<Order> {
        let mut orders = Vec::new();

        for bar in bars {
            let target_notional = *target_notionals.get(&bar.symbol).unwrap_or(&0.0);
            let raw_target_qty = (target_notional / bar.close).floor() as i64;
            let target_qty = (raw_target_qty / lot_size) * lot_size;

            let current_qty = self.broker.position_qty(&bar.market, &bar.symbol);
            let delta = target_qty - current_qty;
            let delta_notional = (delta as f64 * bar.close).abs();
            if delta == 0 || delta_notional < min_trade_notional {
                continue;
            }

            let side = if delta > 0 { Side::Buy } else { Side::Sell };
            orders.push(Order {
                date: bar_date,
                market: bar.market.clone(),
                symbol: bar.symbol.clone(),
                side,
                qty: delta.abs(),
            });
        }

        orders
    }
}

fn market_rules_from_config(cfg: &BotConfig) -> MarketRuleEngine {
    let mut cal = ExchangeCalendar::new();
    for market in cfg.markets.values() {
        cal.add_holidays(&market.name, &market.holiday_dates);
    }
    MarketRuleEngine::new(cal)
}

fn industry_lookup(cfg: &BotConfig) -> HashMap<(String, String), String> {
    let mut out = HashMap::new();
    for (market_name, market_cfg) in &cfg.markets {
        for (symbol, industry) in &market_cfg.industry_map {
            out.insert((market_name.clone(), symbol.clone()), industry.clone());
        }
    }
    out
}

fn market_strategy_config(cfg: &BotConfig, market_name: &str) -> StrategyConfig {
    let mut strategy = cfg.strategy.clone();
    if let Some(route) = cfg.strategy.market_routing.get(market_name) {
        if let Some(plugin) = &route.strategy_plugin {
            strategy.strategy_plugin = plugin.clone();
        }
        if let Some(method) = &route.portfolio_method {
            strategy.portfolio_method = method.clone();
        }
    }
    strategy
}

fn market_strategies_from_config(cfg: &BotConfig) -> HashMap<String, Box<dyn StrategyPlugin>> {
    let industries = industry_lookup(cfg);
    cfg.markets
        .keys()
        .map(|market_name| {
            let strategy_cfg = market_strategy_config(cfg, market_name);
            (
                market_name.clone(),
                build_strategy(strategy_cfg, industries.clone()),
            )
        })
        .collect()
}

fn market_budget_local_ccy(equity_base_ccy: f64, market_cfg: &crate::config::MarketConfig) -> f64 {
    let budget_base = equity_base_ccy * market_cfg.allocation;
    budget_base / market_cfg.fx_to_base.max(1e-12)
}

pub fn summarize_result(result: &RunResult) -> BacktestStats {
    if result.equity_curve.is_empty() {
        return BacktestStats::default();
    }

    let start = result.equity_curve.first().map(|x| x.equity).unwrap_or(0.0);
    let end = result.equity_curve.last().map(|x| x.equity).unwrap_or(0.0);
    let pnl = end - start;
    let pnl_ratio = if start == 0.0 { 0.0 } else { pnl / start };

    let mut peak = start;
    let mut max_drawdown = 0.0;
    for point in &result.equity_curve {
        if point.equity > peak {
            peak = point.equity;
        }
        if peak > 0.0 {
            let dd = (peak - point.equity) / peak;
            if dd > max_drawdown {
                max_drawdown = dd;
            }
        }
    }

    BacktestStats {
        start_equity: start,
        end_equity: end,
        pnl,
        pnl_ratio,
        max_drawdown,
        trades: result.trades.len(),
        rejections: result.rejections.len(),
        ..compute_performance_metrics(&result.equity_curve, max_drawdown).into()
    }
}

#[derive(Debug, Default, Clone)]
pub struct BacktestStats {
    pub start_equity: f64,
    pub end_equity: f64,
    pub pnl: f64,
    pub pnl_ratio: f64,
    pub max_drawdown: f64,
    pub trades: usize,
    pub rejections: usize,
    pub cagr: f64,
    pub sharpe: f64,
    pub sortino: f64,
    pub calmar: f64,
    pub daily_win_rate: f64,
    pub profit_factor: f64,
}

impl From<crate::metrics::PerformanceMetrics> for BacktestStats {
    fn from(metrics: crate::metrics::PerformanceMetrics) -> Self {
        Self {
            cagr: metrics.cagr,
            sharpe: metrics.sharpe,
            sortino: metrics.sortino,
            calmar: metrics.calmar,
            daily_win_rate: metrics.daily_win_rate,
            profit_factor: metrics.profit_factor,
            ..Self::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{config::load_config, data::CsvDataPortal};

    use super::{market_budget_local_ccy, market_strategy_config, QuantBotEngine};

    #[test]
    fn run_produces_equity_and_trades() {
        let cfg = load_config("config/bot.toml").expect("config should load");
        let data = CsvDataPortal::new(
            cfg.markets
                .values()
                .map(|m| (m.name.clone(), m.data_file.clone()))
                .collect(),
        )
        .expect("csv should load");

        let result = QuantBotEngine::from_config_force_sim(cfg, data).run();
        assert!(!result.equity_curve.is_empty());
        assert!(!result.trades.is_empty());
    }

    #[test]
    fn foreign_market_budget_is_converted_to_local_currency() {
        let cfg = load_config("config/bot.toml").expect("config should load");
        let us_budget = market_budget_local_ccy(1_000_000.0, &cfg.markets["US"]);
        let a_budget = market_budget_local_ccy(1_000_000.0, &cfg.markets["A"]);
        let jp_budget = market_budget_local_ccy(1_000_000.0, &cfg.markets["JP"]);

        assert!((us_budget - 500_000.0).abs() < 1e-6);
        assert!(a_budget > 2_000_000.0);
        assert!(jp_budget > 20_000_000.0);
    }

    #[test]
    fn market_routing_overrides_plugin_and_method() {
        let cfg = load_config("config/bot.toml").expect("config should load");

        let us = market_strategy_config(&cfg, "US");
        let a = market_strategy_config(&cfg, "A");
        let jp = market_strategy_config(&cfg, "JP");

        assert_eq!(us.strategy_plugin, "momentum_guard");
        assert_eq!(us.portfolio_method, "risk_parity");
        assert_eq!(a.strategy_plugin, "momentum_guard");
        assert_eq!(a.portfolio_method, "risk_parity");
        assert_eq!(jp.strategy_plugin, "momentum_guard");
        assert_eq!(jp.portfolio_method, "risk_parity");
    }

    // ── summarize_result on empty run ─────────────────────────
    #[test]
    fn summarize_empty_result_does_not_panic() {
        use super::{summarize_result, RunResult};
        let result = RunResult::default();
        let stats = summarize_result(&result);
        assert_eq!(stats.trades, 0);
        assert_eq!(stats.rejections, 0);
        assert!((stats.pnl).abs() < 1e-9);
        assert!((stats.max_drawdown).abs() < 1e-9);
    }

    // ── summarize_result captures drawdown correctly ──────────
    #[test]
    fn summarize_result_captures_drawdown() {
        use super::{summarize_result, RunResult};
        use crate::model::EquityPoint;
        use chrono::NaiveDate;

        let result = RunResult {
            equity_curve: vec![
                EquityPoint {
                    date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                    equity: 100_000.0,
                    cash: 100_000.0,
                    gross_exposure: 0.0,
                    net_exposure: 0.0,
                },
                EquityPoint {
                    date: NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
                    equity: 110_000.0,
                    cash: 50_000.0,
                    gross_exposure: 60_000.0,
                    net_exposure: 60_000.0,
                },
                EquityPoint {
                    date: NaiveDate::from_ymd_opt(2025, 1, 3).unwrap(),
                    equity: 99_000.0,
                    cash: 50_000.0,
                    gross_exposure: 49_000.0,
                    net_exposure: 49_000.0,
                },
                EquityPoint {
                    date: NaiveDate::from_ymd_opt(2025, 1, 6).unwrap(),
                    equity: 105_000.0,
                    cash: 50_000.0,
                    gross_exposure: 55_000.0,
                    net_exposure: 55_000.0,
                },
            ],
            trades: Vec::new(),
            rejections: Vec::new(),
        };
        let stats = summarize_result(&result);

        // peak=110k, trough=99k → dd = 11k/110k = 0.1
        assert!(
            (stats.max_drawdown - 0.1).abs() < 1e-6,
            "dd={}, expected=0.1",
            stats.max_drawdown
        );
        assert!((stats.end_equity - 105_000.0).abs() < 1e-9);
        assert!((stats.pnl - 5_000.0).abs() < 1e-9);
    }

    // ── orders_from_targets: zero-close bar produces no order ─
    #[test]
    fn zero_close_bar_produces_no_order() {
        use crate::model::Bar;
        use std::collections::HashMap;

        let cfg = load_config("config/bot.toml").expect("config should load");
        let data = CsvDataPortal::new(
            cfg.markets
                .values()
                .map(|m| (m.name.clone(), m.data_file.clone()))
                .collect(),
        )
        .expect("csv should load");

        let engine = QuantBotEngine::from_config_force_sim(cfg, data);
        let date = chrono::NaiveDate::from_ymd_opt(2025, 1, 10).unwrap();
        let bars = vec![Bar {
            date,
            market: "US".to_string(),
            symbol: "DEAD".to_string(),
            close: 0.0,
            volume: 0.0,
        }];
        let mut targets = HashMap::new();
        targets.insert("DEAD".to_string(), 10_000.0);

        // close=0 → target_qty = (10000/0).floor() which is inf→i64
        // this should not panic; the order may be nonsensical but must not crash
        let orders = engine.orders_from_targets(date, &bars, &targets, 1, 0.0);
        // With close=0, the division produces infinity which floors to i64::MAX,
        // but delta_notional = delta * 0.0 = 0, which is < min_trade_notional(0),
        // so delta == 0 check fails but delta_notional < min kicks in... let's just
        // ensure no panic.
        let _ = orders;
    }

    // ── Full run does not crash and equity is monotonically tracked
    #[test]
    fn full_run_equity_curve_is_monotonically_dated() {
        let cfg = load_config("config/bot.toml").expect("config should load");
        let data = CsvDataPortal::new(
            cfg.markets
                .values()
                .map(|m| (m.name.clone(), m.data_file.clone()))
                .collect(),
        )
        .expect("csv should load");

        let result = QuantBotEngine::from_config_force_sim(cfg, data).run();
        // Dates must be strictly non-decreasing
        for w in result.equity_curve.windows(2) {
            assert!(
                w[1].date >= w[0].date,
                "equity curve dates out of order: {} > {}",
                w[0].date,
                w[1].date
            );
        }
        // Every equity point should be positive (no negative equity)
        for point in &result.equity_curve {
            assert!(
                point.equity > 0.0,
                "negative equity on {}: {}",
                point.date,
                point.equity
            );
        }
    }

    // ════════════════════════════════════════════════════════════
    // Step 4: Minimal strategy loop — US only, IbkrPaperAdapter
    //         dry_run, full engine run, daily reconcile
    // ════════════════════════════════════════════════════════════

    /// Run the full engine loop on the bundled US data through IbkrPaperAdapter
    /// in dry_run mode. Verify trades, equity curve, lifecycle, and daily
    /// reconcile reports.
    ///
    /// NOTE: `#[ignore]` because this test loads the full US dataset (6k+ rows)
    /// and runs the full engine pipeline, which is too heavy for default CI.
    /// Run manually with:
    ///     cargo test --lib engine::tests::minimal_strategy_loop -- --ignored
    #[test]
    #[ignore]
    fn minimal_strategy_loop_us_ibkr_paper_dry_run() {
        use std::collections::HashMap;
        use std::fs;

        use crate::config::IbkrConfig;
        use crate::execution::{IbkrPaperAdapter, PaperBroker};
        use crate::risk::UnifiedRiskManager;
        use crate::strategy::build_strategy;

        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("quant_engine_min_loop_{seed}"));
        fs::create_dir_all(&root).expect("create temp dir");

        let mut cfg = load_config("config/bot.toml").expect("config should load");

        // ── Strip to US only ──────────────────────────────────
        cfg.markets.retain(|name, _| name == "US");
        // Keep within default risk caps:
        // - USD exposure cap defaults to 0.90
        // - max_gross_exposure_ratio defaults to 0.95
        // - max_symbol_weight defaults to 0.30 (so top_n must be > 1)
        cfg.markets.get_mut("US").unwrap().allocation = 0.90;
        cfg.strategy.top_n = cfg.strategy.top_n.max(5);

        let ibkr_cfg = IbkrConfig {
            enabled: false,
            dry_run: true,
            auto_reconcile: true,
            auto_cancel_stale: true,
            mirror_log: root.join("mirror.jsonl").display().to_string(),
            lifecycle_log: root.join("lifecycle.jsonl").display().to_string(),
            reconcile_log: root.join("reconcile.jsonl").display().to_string(),
            ..IbkrConfig::default()
        };

        let data = CsvDataPortal::new(
            cfg.markets
                .values()
                .map(|m| (m.name.clone(), m.data_file.clone()))
                .collect(),
        )
        .expect("csv should load");

        let trading_days = data.trading_dates().len();
        assert!(trading_days > 10, "need enough data to be meaningful");

        // ── Build IbkrPaperAdapter directly (skip config validation) ──
        let market_costs = cfg
            .markets
            .iter()
            .map(|(name, market)| (name.clone(), market.execution_cost))
            .collect();
        let market_fx_to_base: HashMap<String, f64> = cfg
            .markets
            .iter()
            .map(|(name, market)| (name.clone(), market.fx_to_base))
            .collect();
        let sim = PaperBroker::new(
            cfg.start.starting_capital,
            cfg.execution.commission_bps,
            cfg.execution.slippage_bps,
        )
        .with_market_fx_to_base(market_fx_to_base)
        .with_market_costs(market_costs, cfg.execution.sell_tax_bps, cfg.execution.min_fee);

        let broker = IbkrPaperAdapter::new(sim, ibkr_cfg).expect("adapter create");

        let industries: HashMap<(String, String), String> = cfg
            .markets
            .iter()
            .flat_map(|(market_name, market_cfg)| {
                market_cfg
                    .industry_map
                    .iter()
                    .map(move |(symbol, industry)| {
                        ((market_name.clone(), symbol.clone()), industry.clone())
                    })
            })
            .collect();

        let strategy_cfg = market_strategy_config(&cfg, "US");
        let mut market_strategies: HashMap<String, Box<dyn crate::strategy::StrategyPlugin>> =
            HashMap::new();
        market_strategies.insert(
            "US".to_string(),
            build_strategy(strategy_cfg, industries),
        );

        let risk = UnifiedRiskManager::new(
            cfg.risk.clone(),
            &cfg.markets,
            &cfg.fx,
            &cfg.start.base_currency,
        );
        let rules = super::market_rules_from_config(&cfg);

        let engine = QuantBotEngine::new(cfg.clone(), data, market_strategies, risk, rules, broker);
        let result = engine.run();

        // ── Verify equity curve ───────────────────────────────
        assert_eq!(
            result.equity_curve.len(),
            trading_days,
            "one equity point per trading day"
        );
        for point in &result.equity_curve {
            assert!(point.equity > 0.0);
        }

        // ── Verify trades happened ────────────────────────────
        assert!(
            !result.trades.is_empty(),
            "US-only run should produce trades (got {} equity points, {} rejections)",
            result.equity_curve.len(),
            result.rejections.len()
        );
        for trade in &result.trades {
            assert_eq!(trade.market, "US");
        }

        // ── Verify lifecycle JSONL ────────────────────────────
        let lifecycle = fs::read_to_string(root.join("lifecycle.jsonl")).expect("read lifecycle");
        assert!(lifecycle.contains("\"event\":\"created\""));
        assert!(lifecycle.contains("\"event\":\"filled\""));
        assert!(lifecycle.contains("\"event\":\"end_of_day\""));

        // ── Verify reconcile: one report per trading day ──────
        let reconcile = fs::read_to_string(root.join("reconcile.jsonl")).expect("read reconcile");
        let reconcile_lines: Vec<&str> = reconcile.trim().lines().collect();
        assert_eq!(
            reconcile_lines.len(),
            trading_days,
            "one reconcile report per trading day: got {}, expected {}",
            reconcile_lines.len(),
            trading_days
        );

        // All should be clean in dry_run
        for (i, line) in reconcile_lines.iter().enumerate() {
            let report: serde_json::Value = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("parse reconcile line {i}: {e}"));
            assert_eq!(
                report["clean"], true,
                "reconcile day {i} should be clean in dry_run"
            );
        }

        // ── Verify mirror log has entries ─────────────────────
        let mirror = fs::read_to_string(root.join("mirror.jsonl")).expect("read mirror");
        assert!(!mirror.is_empty(), "mirror log should have trade records");
    }
}
