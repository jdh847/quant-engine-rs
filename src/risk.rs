use std::collections::{BTreeMap, HashMap};

use chrono::NaiveDate;

use crate::{
    config::{FxConfig, MarketConfig, RiskConfig},
    execution::ExecutionAdapter,
    fx::fetch_live_fx_to_base,
    model::{Order, PriceMap, RiskRejection, Side},
};

#[derive(Debug)]
pub struct UnifiedRiskManager {
    cfg: RiskConfig,
    market_currency: HashMap<String, String>,
    static_market_fx_to_base: HashMap<String, f64>,
    market_fx_to_base: HashMap<String, f64>,
    fx_runtime: Option<FxRuntime>,
    start_day_equity: f64,
    daily_locked: bool,
}

#[derive(Debug, Clone)]
struct FxRuntime {
    cfg: FxConfig,
    base_currency: String,
    last_refresh_date: Option<NaiveDate>,
    last_failure_date: Option<NaiveDate>,
}

impl UnifiedRiskManager {
    pub fn new(
        mut cfg: RiskConfig,
        markets: &BTreeMap<String, MarketConfig>,
        fx_cfg: &FxConfig,
        base_currency: &str,
    ) -> Self {
        cfg.currency_max_net_exposure_ratio = cfg
            .currency_max_net_exposure_ratio
            .into_iter()
            .map(|(ccy, limit)| (ccy.to_uppercase(), limit))
            .collect();

        let market_currency = markets
            .iter()
            .map(|(name, market)| (name.clone(), market.currency.to_uppercase()))
            .collect::<HashMap<_, _>>();
        let static_market_fx_to_base = markets
            .iter()
            .map(|(name, market)| (name.clone(), market.fx_to_base))
            .collect::<HashMap<_, _>>();
        let mut market_fx_to_base = static_market_fx_to_base.clone();

        let fx_runtime = if fx_cfg.live_enabled {
            let currencies: Vec<String> = market_currency.values().cloned().collect();
            if let Ok(live) = fetch_live_fx_to_base(fx_cfg, base_currency, &currencies) {
                for (market, ccy) in &market_currency {
                    if let Some(rate) = live.get(ccy).copied() {
                        market_fx_to_base.insert(market.clone(), rate);
                    }
                }
            }
            Some(FxRuntime {
                cfg: fx_cfg.clone(),
                base_currency: base_currency.to_uppercase(),
                last_refresh_date: None,
                last_failure_date: None,
            })
        } else {
            None
        };

        Self {
            cfg,
            market_currency,
            static_market_fx_to_base,
            market_fx_to_base,
            fx_runtime,
            start_day_equity: 0.0,
            daily_locked: false,
        }
    }

    pub fn start_day(&mut self, equity: f64) {
        self.start_day_equity = equity;
        self.daily_locked = false;
    }

    pub fn refresh_live_fx_if_enabled(&mut self, date: NaiveDate) {
        let Some(runtime) = self.fx_runtime.as_mut() else {
            return;
        };

        if let Some(last_refresh) = runtime.last_refresh_date {
            let delta = (date - last_refresh).num_days();
            if delta >= 0 && delta < runtime.cfg.refresh_interval_days as i64 {
                return;
            }
        }

        if let Some(last_failure) = runtime.last_failure_date {
            let delta = (date - last_failure).num_days();
            if delta >= 0 && delta < runtime.cfg.failure_cooldown_days as i64 {
                return;
            }
        }

        if runtime.last_refresh_date == Some(date) {
            return;
        }

        let currencies: Vec<String> = self.market_currency.values().cloned().collect();
        match fetch_live_fx_to_base(&runtime.cfg, &runtime.base_currency, &currencies) {
            Ok(live) => {
                for (market, ccy) in &self.market_currency {
                    if let Some(rate) = live.get(ccy).copied() {
                        self.market_fx_to_base.insert(market.clone(), rate);
                    } else if let Some(static_rate) = self.static_market_fx_to_base.get(market) {
                        self.market_fx_to_base.insert(market.clone(), *static_rate);
                    }
                }
                runtime.last_failure_date = None;
            }
            Err(_) => {
                self.market_fx_to_base = self.static_market_fx_to_base.clone();
                runtime.last_failure_date = Some(date);
            }
        }
        runtime.last_refresh_date = Some(date);
    }

    fn update_daily_lock(&mut self, current_equity: f64) {
        if self.start_day_equity <= 0.0 {
            return;
        }
        let drawdown_ratio = 1.0 - (current_equity / self.start_day_equity);
        if drawdown_ratio >= self.cfg.daily_loss_limit_ratio {
            self.daily_locked = true;
        }
    }

    pub fn filter_orders<E: ExecutionAdapter>(
        &mut self,
        orders: &[Order],
        broker: &E,
        prices: &PriceMap,
        equity: f64,
    ) -> (Vec<Order>, Vec<RiskRejection>) {
        self.update_daily_lock(equity);

        let mut accepted = Vec::new();
        let mut rejected = Vec::new();
        let mut currency_exposure = self.currency_exposure_in_base(broker, prices);

        let gross_limit = self.cfg.max_gross_exposure_ratio * equity;
        let symbol_limit = self.cfg.max_symbol_weight * equity;

        for order in orders {
            let key = (order.market.clone(), order.symbol.clone());
            let px = prices.get(&key).copied().unwrap_or(0.0);

            if self.daily_locked && order.side == Side::Buy {
                rejected.push(RiskRejection {
                    date: order.date,
                    market: order.market.clone(),
                    symbol: order.symbol.clone(),
                    side: order.side,
                    qty: order.qty,
                    reason: "daily loss lock: only reducing trades allowed".to_string(),
                });
                continue;
            }

            let current_qty = broker.position_qty(&order.market, &order.symbol);
            let delta = if order.side == Side::Buy {
                order.qty
            } else {
                -order.qty
            };
            let projected_qty = (current_qty + delta).max(0);
            let projected_symbol_notional = (projected_qty as f64 * px).abs();
            if projected_symbol_notional > symbol_limit {
                rejected.push(RiskRejection {
                    date: order.date,
                    market: order.market.clone(),
                    symbol: order.symbol.clone(),
                    side: order.side,
                    qty: order.qty,
                    reason: "symbol weight breach".to_string(),
                });
                continue;
            }

            let projected_gross = broker.projected_gross_after_order(order, prices);
            if projected_gross > gross_limit {
                rejected.push(RiskRejection {
                    date: order.date,
                    market: order.market.clone(),
                    symbol: order.symbol.clone(),
                    side: order.side,
                    qty: order.qty,
                    reason: "gross exposure breach".to_string(),
                });
                continue;
            }

            let ccy = self
                .market_currency
                .get(&order.market)
                .cloned()
                .unwrap_or_else(|| order.market.to_uppercase());
            let fx = self
                .market_fx_to_base
                .get(&order.market)
                .copied()
                .unwrap_or(1.0);
            let delta_qty = if order.side == Side::Buy {
                order.qty
            } else {
                -order.qty
            };
            let current_ccy = currency_exposure.get(&ccy).copied().unwrap_or(0.0);
            let projected_ccy = current_ccy + delta_qty as f64 * px * fx;

            if let Some(limit_ratio) = self.cfg.currency_max_net_exposure_ratio.get(&ccy) {
                let ccy_limit = limit_ratio * equity;
                if projected_ccy.abs() > ccy_limit {
                    rejected.push(RiskRejection {
                        date: order.date,
                        market: order.market.clone(),
                        symbol: order.symbol.clone(),
                        side: order.side,
                        qty: order.qty,
                        reason: format!("currency exposure breach: {ccy}"),
                    });
                    continue;
                }
            }

            currency_exposure.insert(ccy, projected_ccy);
            accepted.push(order.clone());
        }

        (accepted, rejected)
    }

    fn currency_exposure_in_base<E: ExecutionAdapter>(
        &self,
        broker: &E,
        prices: &PriceMap,
    ) -> HashMap<String, f64> {
        let mut exposure = HashMap::new();
        for ((market, symbol), px) in prices {
            let qty = broker.position_qty(market, symbol);
            if qty == 0 {
                continue;
            }
            let ccy = self
                .market_currency
                .get(market)
                .cloned()
                .unwrap_or_else(|| market.to_uppercase());
            let fx = self.market_fx_to_base.get(market).copied().unwrap_or(1.0);
            let notional_base = qty as f64 * px * fx;
            *exposure.entry(ccy).or_insert(0.0) += notional_base;
        }
        exposure
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use crate::{
        config::load_config,
        execution::PaperBroker,
        model::{Order, PriceMap, Side},
    };

    use super::UnifiedRiskManager;

    #[test]
    fn currency_exposure_limit_blocks_order() {
        let cfg = load_config("config/bot.toml").expect("load cfg");
        let mut risk_cfg = cfg.risk.clone();
        risk_cfg
            .currency_max_net_exposure_ratio
            .insert("USD".to_string(), 0.01);

        let mut rm =
            UnifiedRiskManager::new(risk_cfg, &cfg.markets, &cfg.fx, &cfg.start.base_currency);
        rm.start_day(100_000.0);

        let broker = PaperBroker::new(100_000.0, 0.0, 0.0);
        let date = NaiveDate::from_ymd_opt(2025, 1, 10).expect("date");
        let order = Order {
            date,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Buy,
            qty: 100,
        };
        let mut prices = PriceMap::new();
        prices.insert(("US".to_string(), "AAPL".to_string()), 100.0);

        let (accepted, rejected) = rm.filter_orders(&[order], &broker, &prices, 100_000.0);
        assert!(accepted.is_empty());
        assert_eq!(rejected.len(), 1);
        assert!(rejected[0].reason.contains("currency exposure breach"));
    }
}
