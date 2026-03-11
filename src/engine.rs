use std::collections::HashMap;

use anyhow::Result;
use chrono::NaiveDate;

use crate::{
    calendar::ExchangeCalendar,
    config::BotConfig,
    data::CsvDataPortal,
    execution::{build_broker, BrokerAdapter, ExecutionAdapter, PaperBroker},
    market::MarketRuleEngine,
    metrics::compute_performance_metrics,
    model::{Bar, EquityPoint, Order, RiskRejection, Side, Trade},
    risk::UnifiedRiskManager,
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
    strategy: Box<dyn StrategyPlugin>,
    risk: UnifiedRiskManager,
    market_rules: MarketRuleEngine,
    broker: E,
}

impl QuantBotEngine<BrokerAdapter> {
    pub fn from_config(cfg: BotConfig, data: CsvDataPortal) -> Result<Self> {
        let strategy = build_strategy(cfg.strategy.clone(), industry_lookup(&cfg));
        let risk = UnifiedRiskManager::new(
            cfg.risk.clone(),
            &cfg.markets,
            &cfg.fx,
            &cfg.start.base_currency,
        );
        let broker = build_broker(&cfg)?;
        let rules = market_rules_from_config(&cfg);

        Ok(Self::new(cfg, data, strategy, risk, rules, broker))
    }

    pub fn from_config_force_sim(cfg: BotConfig, data: CsvDataPortal) -> Self {
        let strategy = build_strategy(cfg.strategy.clone(), industry_lookup(&cfg));
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
        let broker = BrokerAdapter::Sim(
            PaperBroker::new(
                cfg.start.starting_capital,
                cfg.execution.commission_bps,
                cfg.execution.slippage_bps,
            )
            .with_market_costs(
                market_costs,
                cfg.execution.sell_tax_bps,
                cfg.execution.min_fee,
            ),
        );
        let rules = market_rules_from_config(&cfg);

        Self::new(cfg, data, strategy, risk, rules, broker)
    }
}

impl<E: ExecutionAdapter> QuantBotEngine<E> {
    pub fn new(
        cfg: BotConfig,
        data: CsvDataPortal,
        strategy: Box<dyn StrategyPlugin>,
        risk: UnifiedRiskManager,
        market_rules: MarketRuleEngine,
        broker: E,
    ) -> Self {
        Self {
            cfg,
            data,
            strategy,
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
                let market_budget = equity_now * market_cfg.allocation;
                let current_notionals = bars
                    .iter()
                    .map(|b| {
                        let qty = self.broker.position_qty(&b.market, &b.symbol);
                        (b.symbol.clone(), (qty as f64 * b.close).max(0.0))
                    })
                    .collect::<HashMap<_, _>>();
                let target_notionals =
                    self.strategy
                        .target_notionals(&bars, market_budget, &current_notionals);
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

                let fills = self.broker.execute_orders(&accepted, &prices);
                result.trades.extend(fills);

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

    use super::QuantBotEngine;

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
}
