use std::collections::HashMap;

use chrono::NaiveDate;

use crate::{
    calendar::ExchangeCalendar,
    execution::ExecutionAdapter,
    model::{Bar, Order, PriceKey, RiskRejection, Side},
};

#[derive(Debug, Clone, Copy)]
struct MarketPolicy {
    t_plus_one: bool,
    price_limit_pct: Option<f64>,
}

impl MarketPolicy {
    fn by_market(market: &str) -> Self {
        match market {
            "A" => Self {
                t_plus_one: true,
                price_limit_pct: Some(0.10),
            },
            "JP" => Self {
                t_plus_one: false,
                price_limit_pct: None,
            },
            _ => Self {
                t_plus_one: false,
                price_limit_pct: None,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct MarketRuleEngine {
    previous_closes: HashMap<PriceKey, f64>,
    calendar: ExchangeCalendar,
}

impl MarketRuleEngine {
    pub fn new(calendar: ExchangeCalendar) -> Self {
        Self {
            previous_closes: HashMap::new(),
            calendar,
        }
    }

    pub fn filter_orders<E: ExecutionAdapter>(
        &self,
        date: NaiveDate,
        bars: &[Bar],
        orders: &[Order],
        broker: &E,
    ) -> (Vec<Order>, Vec<RiskRejection>) {
        let mut accepted = Vec::new();
        let mut rejected = Vec::new();

        let close_map: HashMap<PriceKey, f64> = bars
            .iter()
            .map(|b| ((b.market.clone(), b.symbol.clone()), b.close))
            .collect();
        let volume_map: HashMap<PriceKey, f64> = bars
            .iter()
            .map(|b| ((b.market.clone(), b.symbol.clone()), b.volume))
            .collect();

        for order in orders {
            let key = (order.market.clone(), order.symbol.clone());
            let px = close_map.get(&key).copied().unwrap_or(0.0);
            let vol = volume_map.get(&key).copied().unwrap_or(0.0);
            if !(px.is_finite() && px > 0.0) {
                rejected.push(RiskRejection {
                    date,
                    market: order.market.clone(),
                    symbol: order.symbol.clone(),
                    side: order.side,
                    qty: order.qty,
                    reason: "missing/invalid price for symbol today (halted or data gap)"
                        .to_string(),
                });
                continue;
            }
            if !(vol.is_finite() && vol > 0.0) {
                rejected.push(RiskRejection {
                    date,
                    market: order.market.clone(),
                    symbol: order.symbol.clone(),
                    side: order.side,
                    qty: order.qty,
                    reason: "zero volume today (halted/illiquid)".to_string(),
                });
                continue;
            }

            if !self.calendar.is_trading_day(&order.market, date) {
                rejected.push(RiskRejection {
                    date,
                    market: order.market.clone(),
                    symbol: order.symbol.clone(),
                    side: order.side,
                    qty: order.qty,
                    reason: "market holiday/session closed".to_string(),
                });
                continue;
            }

            let policy = MarketPolicy::by_market(&order.market);
            if order.side == Side::Sell {
                let sellable =
                    broker.sellable_qty(date, &order.market, &order.symbol, policy.t_plus_one);
                if order.qty > sellable {
                    rejected.push(RiskRejection {
                        date,
                        market: order.market.clone(),
                        symbol: order.symbol.clone(),
                        side: order.side,
                        qty: order.qty,
                        reason: if policy.t_plus_one {
                            "T+1 rule: insufficient sellable quantity".to_string()
                        } else {
                            "insufficient sellable quantity".to_string()
                        },
                    });
                    continue;
                }
            }

            if let Some(limit) = policy.price_limit_pct {
                if let (Some(prev_close), Some(current_close)) =
                    (self.previous_closes.get(&key), close_map.get(&key))
                {
                    let change = (current_close - prev_close) / prev_close;
                    if order.side == Side::Buy && change >= limit * 0.98 {
                        rejected.push(RiskRejection {
                            date,
                            market: order.market.clone(),
                            symbol: order.symbol.clone(),
                            side: order.side,
                            qty: order.qty,
                            reason: "A-share limit-up day: avoid buy chase".to_string(),
                        });
                        continue;
                    }
                    if order.side == Side::Sell && change <= -limit * 0.98 {
                        rejected.push(RiskRejection {
                            date,
                            market: order.market.clone(),
                            symbol: order.symbol.clone(),
                            side: order.side,
                            qty: order.qty,
                            reason: "A-share limit-down day: avoid forced illiquid sell"
                                .to_string(),
                        });
                        continue;
                    }
                }
            }

            accepted.push(order.clone());
        }

        (accepted, rejected)
    }

    pub fn end_day_update(&mut self, bars: &[Bar]) {
        for bar in bars {
            self.previous_closes
                .insert((bar.market.clone(), bar.symbol.clone()), bar.close);
        }
    }
}

impl Default for MarketRuleEngine {
    fn default() -> Self {
        Self::new(ExchangeCalendar::new())
    }
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use crate::{
        calendar::ExchangeCalendar,
        execution::PaperBroker,
        model::{Bar, Order, PriceMap, Side},
    };

    use super::MarketRuleEngine;

    #[test]
    fn a_share_t_plus_one_blocks_same_day_sell() {
        let date = NaiveDate::from_ymd_opt(2025, 1, 10).expect("valid date");
        let mut broker = PaperBroker::new(100_000.0, 0.0, 0.0);
        let mut prices = PriceMap::new();
        prices.insert(("A".to_string(), "000001".to_string()), 10.0);

        let buy = Order {
            date,
            market: "A".to_string(),
            symbol: "000001".to_string(),
            side: Side::Buy,
            qty: 1000,
        };
        broker.execute_orders(&[buy], &prices);

        let sell_order = Order {
            date,
            market: "A".to_string(),
            symbol: "000001".to_string(),
            side: Side::Sell,
            qty: 1000,
        };
        let bar = Bar {
            date,
            market: "A".to_string(),
            symbol: "000001".to_string(),
            close: 10.0,
            volume: 1_000_000.0,
        };

        let rules = MarketRuleEngine::default();
        let (_accepted, rejected) = rules.filter_orders(date, &[bar], &[sell_order], &broker);
        assert_eq!(rejected.len(), 1);
        assert!(rejected[0].reason.contains("T+1"));
    }

    #[test]
    fn holiday_blocks_orders() {
        let date = NaiveDate::from_ymd_opt(2025, 1, 1).expect("valid date");
        let broker = PaperBroker::new(100_000.0, 0.0, 0.0);

        let order = Order {
            date,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Buy,
            qty: 10,
        };

        let bar = Bar {
            date,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            close: 100.0,
            volume: 1_000_000.0,
        };

        let rules = MarketRuleEngine::default();
        let (_accepted, rejected) = rules.filter_orders(date, &[bar], &[order], &broker);
        assert_eq!(rejected.len(), 1);
        assert!(rejected[0].reason.contains("holiday"));
    }

    #[test]
    fn custom_holiday_file_dates_can_block_orders() {
        let date = NaiveDate::from_ymd_opt(2025, 1, 2).expect("valid date");
        let broker = PaperBroker::new(100_000.0, 0.0, 0.0);
        let order = Order {
            date,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Buy,
            qty: 10,
        };
        let bar = Bar {
            date,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            close: 100.0,
            volume: 1_000_000.0,
        };

        let mut cal = ExchangeCalendar::new();
        let mut extras = std::collections::HashSet::new();
        extras.insert(date);
        cal.add_holidays("US", &extras);
        let rules = MarketRuleEngine::new(cal);

        let (_accepted, rejected) = rules.filter_orders(date, &[bar], &[order], &broker);
        assert_eq!(rejected.len(), 1);
        assert!(rejected[0].reason.contains("holiday"));
    }
}
