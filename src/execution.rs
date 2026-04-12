use std::{
    collections::{HashMap, HashSet},
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

use anyhow::{Context, Result};
use chrono::NaiveDate;
use reqwest::blocking::Client;
use serde::Serialize;
use serde_json::Value;

use crate::{
    config::{BotConfig, IbkrConfig, MarketExecutionCost},
    model::{Order, Position, PriceKey, PriceMap, Side, Trade},
    reconcile::{ReconcileReport, ReconcileSnapshot},
    safety::{ensure_network_allowed, is_trading_kill_switch_armed},
};

pub trait ExecutionAdapter {
    fn cash(&self) -> f64;
    fn position_qty(&self, market: &str, symbol: &str) -> i64;
    fn sellable_qty(&self, date: NaiveDate, market: &str, symbol: &str, t_plus_one: bool) -> i64;
    fn projected_gross_after_order(&self, order: &Order, prices: &PriceMap) -> f64;
    fn execute_orders(&mut self, orders: &[Order], prices: &PriceMap) -> Vec<Trade>;
    fn equity(&self, prices: &PriceMap) -> f64;
    fn gross_exposure(&self, prices: &PriceMap) -> f64;
    fn net_exposure(&self, prices: &PriceMap) -> f64;
    fn reconcile_day(&mut self, date: NaiveDate);
    fn end_of_day(&mut self, date: NaiveDate);
}

#[derive(Debug)]
pub enum BrokerAdapter {
    Sim(PaperBroker),
    IbkrPaper(IbkrPaperAdapter),
}

impl ExecutionAdapter for BrokerAdapter {
    fn cash(&self) -> f64 {
        match self {
            BrokerAdapter::Sim(inner) => inner.cash,
            BrokerAdapter::IbkrPaper(inner) => inner.sim.cash,
        }
    }

    fn position_qty(&self, market: &str, symbol: &str) -> i64 {
        match self {
            BrokerAdapter::Sim(inner) => inner.position_qty(market, symbol),
            BrokerAdapter::IbkrPaper(inner) => inner.sim.position_qty(market, symbol),
        }
    }

    fn sellable_qty(&self, date: NaiveDate, market: &str, symbol: &str, t_plus_one: bool) -> i64 {
        match self {
            BrokerAdapter::Sim(inner) => inner.sellable_qty(date, market, symbol, t_plus_one),
            BrokerAdapter::IbkrPaper(inner) => {
                inner.sim.sellable_qty(date, market, symbol, t_plus_one)
            }
        }
    }

    fn projected_gross_after_order(&self, order: &Order, prices: &PriceMap) -> f64 {
        match self {
            BrokerAdapter::Sim(inner) => inner.projected_gross_after_order(order, prices),
            BrokerAdapter::IbkrPaper(inner) => inner.sim.projected_gross_after_order(order, prices),
        }
    }

    fn execute_orders(&mut self, orders: &[Order], prices: &PriceMap) -> Vec<Trade> {
        match self {
            BrokerAdapter::Sim(inner) => inner.execute_orders(orders, prices),
            BrokerAdapter::IbkrPaper(inner) => inner.execute_orders(orders, prices),
        }
    }

    fn equity(&self, prices: &PriceMap) -> f64 {
        match self {
            BrokerAdapter::Sim(inner) => inner.equity(prices),
            BrokerAdapter::IbkrPaper(inner) => inner.sim.equity(prices),
        }
    }

    fn gross_exposure(&self, prices: &PriceMap) -> f64 {
        match self {
            BrokerAdapter::Sim(inner) => inner.gross_exposure(prices),
            BrokerAdapter::IbkrPaper(inner) => inner.sim.gross_exposure(prices),
        }
    }

    fn net_exposure(&self, prices: &PriceMap) -> f64 {
        match self {
            BrokerAdapter::Sim(inner) => inner.net_exposure(prices),
            BrokerAdapter::IbkrPaper(inner) => inner.sim.net_exposure(prices),
        }
    }

    fn reconcile_day(&mut self, date: NaiveDate) {
        match self {
            BrokerAdapter::Sim(inner) => inner.reconcile_day(date),
            BrokerAdapter::IbkrPaper(inner) => inner.reconcile_day(date),
        }
    }

    fn end_of_day(&mut self, date: NaiveDate) {
        match self {
            BrokerAdapter::Sim(inner) => inner.end_of_day(date),
            BrokerAdapter::IbkrPaper(inner) => inner.end_of_day(date),
        }
    }
}

pub fn build_broker(cfg: &BotConfig) -> Result<BrokerAdapter> {
    let market_costs: HashMap<String, MarketExecutionCost> = cfg
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
    .with_market_costs(
        market_costs,
        cfg.execution.sell_tax_bps,
        cfg.execution.min_fee,
    );

    match cfg.broker.mode.as_str() {
        "sim" => Ok(BrokerAdapter::Sim(sim)),
        "ibkr_paper" => Ok(BrokerAdapter::IbkrPaper(IbkrPaperAdapter::new(
            sim,
            cfg.ibkr.clone(),
        )?)),
        _ => unreachable!("validated in config loader"),
    }
}

#[derive(Debug)]
pub struct PaperBroker {
    pub cash: f64,
    default_cost: MarketExecutionCost,
    market_costs: HashMap<String, MarketExecutionCost>,
    market_fx_to_base: HashMap<String, f64>,
    positions: HashMap<PriceKey, Position>,
    buys_today: HashMap<PriceKey, (NaiveDate, i64)>,
    current_day: Option<NaiveDate>,
}

impl PaperBroker {
    pub fn new(starting_cash: f64, commission_bps: f64, slippage_bps: f64) -> Self {
        Self {
            cash: starting_cash,
            default_cost: MarketExecutionCost {
                commission_bps,
                slippage_bps,
                sell_tax_bps: 0.0,
                min_fee: 0.0,
            },
            market_costs: HashMap::new(),
            market_fx_to_base: HashMap::new(),
            positions: HashMap::new(),
            buys_today: HashMap::new(),
            current_day: None,
        }
    }

    pub fn with_market_fx_to_base(mut self, market_fx_to_base: HashMap<String, f64>) -> Self {
        self.market_fx_to_base = market_fx_to_base;
        self
    }

    pub fn with_market_costs(
        mut self,
        market_costs: HashMap<String, MarketExecutionCost>,
        default_sell_tax_bps: f64,
        default_min_fee: f64,
    ) -> Self {
        self.market_costs = market_costs;
        self.default_cost.sell_tax_bps = default_sell_tax_bps;
        self.default_cost.min_fee = default_min_fee;
        self
    }

    fn roll_day_if_needed(&mut self, date: NaiveDate) {
        if self.current_day != Some(date) {
            self.current_day = Some(date);
            self.buys_today.clear();
        }
    }

    fn market_cost(&self, market: &str) -> MarketExecutionCost {
        self.market_costs
            .get(market)
            .copied()
            .unwrap_or(self.default_cost)
    }

    fn market_fx_to_base(&self, market: &str) -> f64 {
        self.market_fx_to_base.get(market).copied().unwrap_or(1.0)
    }

    fn fill_price(&self, close: f64, side: Side, cost: MarketExecutionCost) -> f64 {
        let slip = cost.slippage_bps / 10_000.0;
        match side {
            Side::Buy => close * (1.0 + slip),
            Side::Sell => close * (1.0 - slip),
        }
    }

    fn fees(&self, notional: f64, side: Side, cost: MarketExecutionCost) -> f64 {
        let mut bps = cost.commission_bps;
        if side == Side::Sell {
            bps += cost.sell_tax_bps;
        }
        let variable_fee = notional * (bps / 10_000.0);
        if notional > 0.0 {
            variable_fee.max(cost.min_fee)
        } else {
            0.0
        }
    }

    /// All (market, symbol) keys with a non-zero position.
    pub fn position_keys(&self) -> Vec<(String, String)> {
        self.positions
            .iter()
            .filter(|(_, p)| p.qty != 0)
            .map(|(k, _)| k.clone())
            .collect()
    }

    pub fn position_qty(&self, market: &str, symbol: &str) -> i64 {
        self.positions
            .get(&(market.to_owned(), symbol.to_owned()))
            .map(|p| p.qty)
            .unwrap_or(0)
    }

    pub fn sellable_qty(
        &self,
        date: NaiveDate,
        market: &str,
        symbol: &str,
        t_plus_one: bool,
    ) -> i64 {
        let key = (market.to_owned(), symbol.to_owned());
        let qty = self.position_qty(market, symbol);
        if !t_plus_one {
            return qty;
        }

        if let Some((buy_date, bought_qty)) = self.buys_today.get(&key) {
            if *buy_date == date {
                return (qty - *bought_qty).max(0);
            }
        }

        qty
    }

    pub fn projected_gross_after_order(&self, order: &Order, prices: &PriceMap) -> f64 {
        let current = self.gross_exposure(prices);
        let key = (order.market.clone(), order.symbol.clone());
        let Some(price) = prices.get(&key).copied() else {
            return current;
        };
        let fx = self.market_fx_to_base(&order.market);

        let current_qty = self.positions.get(&key).map(|p| p.qty).unwrap_or(0);
        let current_symbol = (current_qty as f64 * price * fx).abs();

        let delta = if order.side == Side::Buy {
            order.qty
        } else {
            -order.qty
        };
        let projected_qty = (current_qty + delta).max(0);
        let projected_symbol = (projected_qty as f64 * price * fx).abs();

        current - current_symbol + projected_symbol
    }

    pub fn execute_orders(&mut self, orders: &[Order], prices: &PriceMap) -> Vec<Trade> {
        if is_trading_kill_switch_armed() {
            return Vec::new();
        }
        let mut trades = Vec::new();

        for order in orders {
            self.roll_day_if_needed(order.date);
            let key = (order.market.clone(), order.symbol.clone());
            let Some(close) = prices.get(&key).copied() else {
                continue;
            };
            let cost = self.market_cost(&order.market);
            let fx = self.market_fx_to_base(&order.market);
            let fill = self.fill_price(close, order.side, cost);

            if order.side == Side::Buy {
                let notional_local = fill * order.qty as f64;
                let fees_local = self.fees(notional_local, order.side, cost);
                let notional = notional_local * fx;
                let fees = fees_local * fx;
                let total = notional + fees;
                if total > self.cash {
                    continue;
                }

                let position = self.positions.entry(key.clone()).or_default();
                let new_qty = position.qty + order.qty;
                if new_qty > 0 {
                    position.avg_price = (position.avg_price * position.qty as f64
                        + notional_local)
                        / new_qty as f64;
                }
                position.qty = new_qty;

                self.cash -= total;
                let buy_state = self
                    .buys_today
                    .entry(key.clone())
                    .or_insert((order.date, 0));
                if buy_state.0 != order.date {
                    *buy_state = (order.date, 0);
                }
                buy_state.1 += order.qty;

                trades.push(Trade {
                    date: order.date,
                    market: order.market.clone(),
                    symbol: order.symbol.clone(),
                    side: order.side,
                    qty: order.qty,
                    price: fill,
                    fees,
                });
            } else {
                let available = self.positions.get(&key).map(|p| p.qty).unwrap_or(0);
                let sell_qty = order.qty.min(available);
                if sell_qty <= 0 {
                    continue;
                }

                let notional_local = fill * sell_qty as f64;
                let fees_local = self.fees(notional_local, order.side, cost);
                let notional = notional_local * fx;
                let fees = fees_local * fx;
                if let Some(position) = self.positions.get_mut(&key) {
                    position.qty -= sell_qty;
                    if position.qty == 0 {
                        position.avg_price = 0.0;
                    }
                }

                self.cash += notional - fees;
                trades.push(Trade {
                    date: order.date,
                    market: order.market.clone(),
                    symbol: order.symbol.clone(),
                    side: order.side,
                    qty: sell_qty,
                    price: fill,
                    fees,
                });
            }
        }

        trades
    }

    pub fn equity(&self, prices: &PriceMap) -> f64 {
        self.cash + self.net_exposure(prices)
    }

    pub fn gross_exposure(&self, prices: &PriceMap) -> f64 {
        self.positions
            .iter()
            .map(|((market, symbol), pos)| {
                prices
                    .get(&(market.clone(), symbol.clone()))
                    .copied()
                    .unwrap_or(0.0)
                    * pos.qty as f64
                    * self.market_fx_to_base(market)
            })
            .map(f64::abs)
            .sum()
    }

    pub fn net_exposure(&self, prices: &PriceMap) -> f64 {
        self.positions
            .iter()
            .map(|((market, symbol), pos)| {
                prices
                    .get(&(market.clone(), symbol.clone()))
                    .copied()
                    .unwrap_or(0.0)
                    * pos.qty as f64
                    * self.market_fx_to_base(market)
            })
            .sum()
    }

    pub fn end_of_day(&mut self, date: NaiveDate) {
        self.current_day = Some(date);
    }

    pub fn reconcile_day(&mut self, _date: NaiveDate) {}
}

impl ExecutionAdapter for PaperBroker {
    fn cash(&self) -> f64 {
        self.cash
    }

    fn position_qty(&self, market: &str, symbol: &str) -> i64 {
        PaperBroker::position_qty(self, market, symbol)
    }

    fn sellable_qty(&self, date: NaiveDate, market: &str, symbol: &str, t_plus_one: bool) -> i64 {
        PaperBroker::sellable_qty(self, date, market, symbol, t_plus_one)
    }

    fn projected_gross_after_order(&self, order: &Order, prices: &PriceMap) -> f64 {
        PaperBroker::projected_gross_after_order(self, order, prices)
    }

    fn execute_orders(&mut self, orders: &[Order], prices: &PriceMap) -> Vec<Trade> {
        PaperBroker::execute_orders(self, orders, prices)
    }

    fn equity(&self, prices: &PriceMap) -> f64 {
        PaperBroker::equity(self, prices)
    }

    fn gross_exposure(&self, prices: &PriceMap) -> f64 {
        PaperBroker::gross_exposure(self, prices)
    }

    fn net_exposure(&self, prices: &PriceMap) -> f64 {
        PaperBroker::net_exposure(self, prices)
    }

    fn reconcile_day(&mut self, date: NaiveDate) {
        PaperBroker::reconcile_day(self, date);
    }

    fn end_of_day(&mut self, date: NaiveDate) {
        PaperBroker::end_of_day(self, date);
    }
}

#[derive(Debug)]
pub struct IbkrPaperAdapter {
    pub sim: PaperBroker,
    cfg: IbkrConfig,
    client: Option<Client>,
    next_local_order_id: u64,
    tracked: HashMap<u64, IbkrTrackedOrder>,
    last_prices: PriceMap,
}

impl IbkrPaperAdapter {
    pub fn new(sim: PaperBroker, cfg: IbkrConfig) -> Result<Self> {
        let client = if cfg.enabled {
            ensure_network_allowed("ibkr_http_client")?;
            Some(
                Client::builder()
                    .danger_accept_invalid_certs(true)
                    .build()
                    .context("build IBKR HTTP client failed")?,
            )
        } else {
            None
        };

        for file in [&cfg.mirror_log, &cfg.lifecycle_log] {
            if let Some(parent) = Path::new(file).parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent).with_context(|| {
                        format!("create IBKR log directory failed: {}", parent.display())
                    })?;
                }
            }
        }

        Ok(Self {
            sim,
            cfg,
            client,
            next_local_order_id: 1,
            tracked: HashMap::new(),
            last_prices: PriceMap::new(),
        })
    }

    fn execute_orders(&mut self, orders: &[Order], prices: &PriceMap) -> Vec<Trade> {
        if is_trading_kill_switch_armed() {
            return Vec::new();
        }
        self.last_prices.clone_from(prices);
        for order in orders {
            self.register_and_submit(order.clone());
        }

        let fills = self.sim.execute_orders(orders, prices);
        for fill in &fills {
            self.apply_fill(fill);
            let _ = self.write_mirror_record(fill);
        }
        let _ = self.write_lifecycle_summary(
            orders
                .first()
                .map(|o| o.date)
                .or_else(|| self.sim.current_day)
                .unwrap_or_else(|| NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid")),
            "post_execute",
        );

        fills
    }

    fn end_of_day(&mut self, date: NaiveDate) {
        self.sim.end_of_day(date);
        if self.cfg.auto_reconcile {
            let _ = self.reconcile_with_ibkr();
            let _ = self.write_lifecycle_summary(date, "reconcile");
        }
        if self.cfg.auto_cancel_stale {
            let _ = self.cancel_stale_orders(date);
        }
        let _ = self.write_reconcile_report(date);
        let _ = self.write_lifecycle_summary(date, "end_of_day");
    }

    fn register_and_submit(&mut self, order: Order) {
        let local_order_id = self.next_local_order_id;
        self.next_local_order_id += 1;

        let mut tracked = IbkrTrackedOrder {
            local_order_id,
            ibkr_order_id: None,
            date: order.date,
            market: order.market.clone(),
            symbol: order.symbol.clone(),
            side: order.side,
            qty: order.qty,
            filled_qty: 0,
            remaining_qty: order.qty,
            status: IbkrOrderStatus::Created,
            message: "created".to_string(),
        };
        let _ = self.write_lifecycle_event(&tracked, "created");

        tracked.status = IbkrOrderStatus::Submitted;
        tracked.message = "submitted_to_paper_route".to_string();

        if self.cfg.enabled && !self.cfg.dry_run && !self.cfg.account_id.is_empty() {
            match self.submit_to_ibkr(&order) {
                Ok(remote_id) => {
                    tracked.ibkr_order_id = remote_id;
                    tracked.status = IbkrOrderStatus::Acknowledged;
                    tracked.message = "acknowledged_by_ibkr".to_string();
                    let _ = self.write_lifecycle_event(&tracked, "ack");
                }
                Err(err) => {
                    tracked.status = IbkrOrderStatus::Rejected;
                    tracked.message = format!("submission_error: {err}");
                    let _ = self.write_lifecycle_event(&tracked, "submission_error");
                }
            }
        } else {
            tracked.status = IbkrOrderStatus::Acknowledged;
            tracked.message = "local_dry_run_ack".to_string();
            let _ = self.write_lifecycle_event(&tracked, "ack");
        }

        self.tracked.insert(local_order_id, tracked);
    }

    fn submit_to_ibkr(&self, order: &Order) -> Result<Option<String>> {
        let Some(client) = &self.client else {
            return Ok(None);
        };
        ensure_network_allowed("ibkr_submit")?;

        let side = if order.side == Side::Buy {
            "BUY"
        } else {
            "SELL"
        };
        let payload = serde_json::json!({
            "orders": [
                {
                    "acctId": self.cfg.account_id,
                    "ticker": order.symbol,
                    "secType": "STK",
                    "side": side,
                    "orderType": "MKT",
                    "quantity": order.qty,
                    "tif": "DAY"
                }
            ]
        });

        let url = format!(
            "{}/iserver/account/{}/orders",
            self.cfg.gateway_url.trim_end_matches('/'),
            self.cfg.account_id
        );
        let response = client
            .post(url)
            .json(&payload)
            .send()
            .context("submit paper order to IBKR failed")?
            .error_for_status()
            .context("IBKR rejected order submit")?;
        let body: Value = response
            .json()
            .context("parse IBKR submit response failed")?;

        Ok(extract_first_order_id(&body))
    }

    fn apply_fill(&mut self, fill: &Trade) {
        let mut candidates: Vec<u64> = self
            .tracked
            .iter()
            .filter_map(|(id, tracked)| {
                let open_status = matches!(
                    tracked.status,
                    IbkrOrderStatus::Acknowledged
                        | IbkrOrderStatus::Submitted
                        | IbkrOrderStatus::PartiallyFilled
                );
                if open_status
                    && tracked.market == fill.market
                    && tracked.symbol == fill.symbol
                    && tracked.side == fill.side
                    && tracked.remaining_qty > 0
                {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();
        candidates.sort_unstable();

        let Some(id) = candidates.first().copied() else {
            return;
        };

        let mut event_snapshot: Option<(IbkrTrackedOrder, &'static str)> = None;
        if let Some(tracked) = self.tracked.get_mut(&id) {
            let fill_qty = fill.qty.min(tracked.remaining_qty);
            tracked.filled_qty += fill_qty;
            tracked.remaining_qty -= fill_qty;
            if tracked.remaining_qty == 0 {
                tracked.status = IbkrOrderStatus::Filled;
                tracked.message = "filled".to_string();
                event_snapshot = Some((tracked.clone(), "filled"));
            } else {
                tracked.status = IbkrOrderStatus::PartiallyFilled;
                tracked.message = "partially_filled".to_string();
                event_snapshot = Some((tracked.clone(), "partial_fill"));
            }
        }
        if let Some((snapshot, event)) = event_snapshot {
            let _ = self.write_lifecycle_event(&snapshot, event);
        }
    }

    fn reconcile_with_ibkr(&mut self) -> Result<()> {
        if !self.cfg.enabled || self.cfg.dry_run {
            return Ok(());
        }
        ensure_network_allowed("ibkr_reconcile")?;

        let open_ids = self.fetch_open_order_ids()?;
        let mut snapshots: Vec<IbkrTrackedOrder> = Vec::new();
        for tracked in self.tracked.values_mut() {
            if let Some(remote_id) = &tracked.ibkr_order_id {
                let is_open = open_ids.contains(remote_id);
                if !is_open
                    && tracked.remaining_qty > 0
                    && !matches!(
                        tracked.status,
                        IbkrOrderStatus::Filled | IbkrOrderStatus::Canceled
                    )
                {
                    tracked.status = IbkrOrderStatus::Reconciled;
                    tracked.message = "reconciled_remote_closed".to_string();
                    snapshots.push(tracked.clone());
                }
            }
        }
        for snapshot in snapshots {
            let _ = self.write_lifecycle_event(&snapshot, "reconciled");
        }

        Ok(())
    }

    fn fetch_open_order_ids(&self) -> Result<HashSet<String>> {
        let Some(client) = &self.client else {
            return Ok(HashSet::new());
        };

        let url = format!(
            "{}/iserver/account/orders",
            self.cfg.gateway_url.trim_end_matches('/')
        );
        let response = client
            .get(url)
            .send()
            .context("fetch IBKR open orders failed")?
            .error_for_status()
            .context("IBKR open orders status error")?;

        let body: Value = response
            .json()
            .context("parse IBKR open orders response failed")?;
        Ok(extract_all_order_ids(&body))
    }

    fn cancel_stale_orders(&mut self, current_date: NaiveDate) -> Result<()> {
        let stale_ids: Vec<u64> = self
            .tracked
            .iter()
            .filter_map(|(id, tracked)| {
                let is_open = matches!(
                    tracked.status,
                    IbkrOrderStatus::Submitted
                        | IbkrOrderStatus::Acknowledged
                        | IbkrOrderStatus::PartiallyFilled
                        | IbkrOrderStatus::Reconciled
                );
                if tracked.date < current_date && tracked.remaining_qty > 0 && is_open {
                    Some(*id)
                } else {
                    None
                }
            })
            .collect();

        let mut cancel_snapshots: Vec<IbkrTrackedOrder> = Vec::new();
        for id in stale_ids {
            let submit_cancel = self
                .tracked
                .get(&id)
                .map(|tracked| {
                    self.cfg.enabled
                        && !self.cfg.dry_run
                        && !self.cfg.account_id.is_empty()
                        && tracked.ibkr_order_id.is_some()
                })
                .unwrap_or(false);

            if submit_cancel {
                if let Some(snapshot) = self.tracked.get(&id).cloned() {
                    let _ = self.cancel_ibkr_order(&snapshot);
                }
            }

            if let Some(tracked) = self.tracked.get_mut(&id) {
                tracked.status = IbkrOrderStatus::Canceled;
                tracked.message = "auto_canceled_end_of_day".to_string();
                cancel_snapshots.push(tracked.clone());
            }
        }
        for snapshot in cancel_snapshots {
            let _ = self.write_lifecycle_event(&snapshot, "canceled");
        }

        Ok(())
    }

    fn cancel_ibkr_order(&self, tracked: &IbkrTrackedOrder) -> Result<()> {
        let Some(client) = &self.client else {
            return Ok(());
        };
        ensure_network_allowed("ibkr_cancel")?;
        let Some(remote_id) = &tracked.ibkr_order_id else {
            return Ok(());
        };

        let url = format!(
            "{}/iserver/account/{}/order/{}",
            self.cfg.gateway_url.trim_end_matches('/'),
            self.cfg.account_id,
            remote_id
        );

        let _ = client
            .delete(url)
            .send()
            .context("cancel IBKR order failed")?;

        Ok(())
    }

    fn write_mirror_record(&self, trade: &Trade) -> Result<()> {
        let payload = IbkrMirrorOrder {
            date: trade.date.format("%Y-%m-%d").to_string(),
            market: trade.market.clone(),
            symbol: trade.symbol.clone(),
            side: trade.side.as_str().to_string(),
            qty: trade.qty,
            price: trade.price,
            fees: trade.fees,
            mode: if self.cfg.dry_run {
                "dry_run".to_string()
            } else {
                "submit".to_string()
            },
        };

        let line = serde_json::to_string(&payload)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.cfg.mirror_log)
            .with_context(|| format!("open mirror log failed: {}", self.cfg.mirror_log))?;
        writeln!(file, "{}", line)?;
        Ok(())
    }

    fn write_lifecycle_event(&self, tracked: &IbkrTrackedOrder, event: &str) -> Result<()> {
        let payload = IbkrLifecycleEvent {
            date: tracked.date.format("%Y-%m-%d").to_string(),
            event: event.to_string(),
            local_order_id: tracked.local_order_id,
            ibkr_order_id: tracked.ibkr_order_id.clone(),
            market: tracked.market.clone(),
            symbol: tracked.symbol.clone(),
            side: tracked.side.as_str().to_string(),
            qty: tracked.qty,
            filled_qty: tracked.filled_qty,
            remaining_qty: tracked.remaining_qty,
            status: tracked.status.as_str().to_string(),
            message: tracked.message.clone(),
        };

        let line = serde_json::to_string(&payload)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.cfg.lifecycle_log)
            .with_context(|| format!("open lifecycle log failed: {}", self.cfg.lifecycle_log))?;
        writeln!(file, "{}", line)?;
        Ok(())
    }

    fn write_lifecycle_summary(&self, date: NaiveDate, event: &str) -> Result<()> {
        let mut created = 0usize;
        let mut submitted = 0usize;
        let mut acknowledged = 0usize;
        let mut partial = 0usize;
        let mut filled = 0usize;
        let mut canceled = 0usize;
        let mut rejected = 0usize;
        let mut reconciled = 0usize;
        let mut open = 0usize;
        for tracked in self.tracked.values() {
            match tracked.status {
                IbkrOrderStatus::Created => created += 1,
                IbkrOrderStatus::Submitted => submitted += 1,
                IbkrOrderStatus::Acknowledged => acknowledged += 1,
                IbkrOrderStatus::PartiallyFilled => partial += 1,
                IbkrOrderStatus::Filled => filled += 1,
                IbkrOrderStatus::Canceled => canceled += 1,
                IbkrOrderStatus::Rejected => rejected += 1,
                IbkrOrderStatus::Reconciled => reconciled += 1,
            }
            if matches!(
                tracked.status,
                IbkrOrderStatus::Created
                    | IbkrOrderStatus::Submitted
                    | IbkrOrderStatus::Acknowledged
                    | IbkrOrderStatus::PartiallyFilled
                    | IbkrOrderStatus::Reconciled
            ) && tracked.remaining_qty > 0
            {
                open += 1;
            }
        }
        let payload = IbkrLifecycleSummary {
            date: date.format("%Y-%m-%d").to_string(),
            event: event.to_string(),
            tracked_orders: self.tracked.len(),
            created,
            submitted,
            acknowledged,
            partial,
            filled,
            canceled,
            rejected,
            reconciled,
            open,
        };
        let line = serde_json::to_string(&payload)?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.cfg.lifecycle_log)
            .with_context(|| format!("open lifecycle log failed: {}", self.cfg.lifecycle_log))?;
        writeln!(file, "{}", line)?;
        Ok(())
    }

    fn write_reconcile_report(&self, date: NaiveDate) -> Result<()> {
        let symbols = self.sim.position_keys();
        let expected =
            ReconcileSnapshot::capture(&self.sim, &self.last_prices, date, "internal", &symbols);

        // In dry_run / offline mode, the "actual" snapshot is also the sim.
        // When connected to real IBKR, this would come from the broker positions API.
        let actual = if self.cfg.enabled && !self.cfg.dry_run {
            // TODO: fetch real positions from IBKR API and build snapshot
            expected.clone()
        } else {
            expected.clone()
        };

        let report = ReconcileReport::diff(&expected, &actual);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.cfg.reconcile_log)
            .with_context(|| {
                format!("open reconcile log failed: {}", self.cfg.reconcile_log)
            })?;
        report.write_jsonl(&mut file)?;
        Ok(())
    }

}

impl ExecutionAdapter for IbkrPaperAdapter {
    fn cash(&self) -> f64 {
        self.sim.cash
    }

    fn position_qty(&self, market: &str, symbol: &str) -> i64 {
        self.sim.position_qty(market, symbol)
    }

    fn sellable_qty(&self, date: NaiveDate, market: &str, symbol: &str, t_plus_one: bool) -> i64 {
        self.sim.sellable_qty(date, market, symbol, t_plus_one)
    }

    fn projected_gross_after_order(&self, order: &Order, prices: &PriceMap) -> f64 {
        self.sim.projected_gross_after_order(order, prices)
    }

    fn execute_orders(&mut self, orders: &[Order], prices: &PriceMap) -> Vec<Trade> {
        IbkrPaperAdapter::execute_orders(self, orders, prices)
    }

    fn equity(&self, prices: &PriceMap) -> f64 {
        self.sim.equity(prices)
    }

    fn gross_exposure(&self, prices: &PriceMap) -> f64 {
        self.sim.gross_exposure(prices)
    }

    fn net_exposure(&self, prices: &PriceMap) -> f64 {
        self.sim.net_exposure(prices)
    }

    fn reconcile_day(&mut self, date: NaiveDate) {
        if self.cfg.auto_reconcile {
            let _ = self.reconcile_with_ibkr();
            let _ = self.write_lifecycle_summary(date, "reconcile");
        }
    }

    fn end_of_day(&mut self, date: NaiveDate) {
        IbkrPaperAdapter::end_of_day(self, date);
    }
}

#[derive(Debug, Clone)]
struct IbkrTrackedOrder {
    local_order_id: u64,
    ibkr_order_id: Option<String>,
    date: NaiveDate,
    market: String,
    symbol: String,
    side: Side,
    qty: i64,
    filled_qty: i64,
    remaining_qty: i64,
    status: IbkrOrderStatus,
    message: String,
}

#[derive(Debug, Clone, Copy)]
enum IbkrOrderStatus {
    Created,
    Submitted,
    Acknowledged,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Reconciled,
}

impl IbkrOrderStatus {
    fn as_str(self) -> &'static str {
        match self {
            IbkrOrderStatus::Created => "created",
            IbkrOrderStatus::Submitted => "submitted",
            IbkrOrderStatus::Acknowledged => "acknowledged",
            IbkrOrderStatus::PartiallyFilled => "partially_filled",
            IbkrOrderStatus::Filled => "filled",
            IbkrOrderStatus::Canceled => "canceled",
            IbkrOrderStatus::Rejected => "rejected",
            IbkrOrderStatus::Reconciled => "reconciled",
        }
    }
}

#[derive(Debug, Serialize)]
struct IbkrMirrorOrder {
    date: String,
    market: String,
    symbol: String,
    side: String,
    qty: i64,
    price: f64,
    fees: f64,
    mode: String,
}

#[derive(Debug, Serialize)]
struct IbkrLifecycleEvent {
    date: String,
    event: String,
    local_order_id: u64,
    ibkr_order_id: Option<String>,
    market: String,
    symbol: String,
    side: String,
    qty: i64,
    filled_qty: i64,
    remaining_qty: i64,
    status: String,
    message: String,
}

#[derive(Debug, Serialize)]
struct IbkrLifecycleSummary {
    date: String,
    event: String,
    tracked_orders: usize,
    created: usize,
    submitted: usize,
    acknowledged: usize,
    partial: usize,
    filled: usize,
    canceled: usize,
    rejected: usize,
    reconciled: usize,
    open: usize,
}

fn extract_first_order_id(value: &Value) -> Option<String> {
    extract_all_order_ids(value).into_iter().next()
}

fn extract_all_order_ids(value: &Value) -> HashSet<String> {
    let mut ids = HashSet::new();
    collect_order_ids(value, &mut ids);
    ids
}

fn collect_order_ids(value: &Value, out: &mut HashSet<String>) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let key = k.to_ascii_lowercase();
                if key == "orderid" || key == "order_id" || key == "id" {
                    if let Some(s) = value_to_string_id(v) {
                        out.insert(s);
                    }
                }
                collect_order_ids(v, out);
            }
        }
        Value::Array(arr) => {
            for item in arr {
                collect_order_ids(item, out);
            }
        }
        _ => {}
    }
}

fn value_to_string_id(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use chrono::NaiveDate;

    use crate::{config::IbkrConfig, model::PriceMap};

    use super::{ExecutionAdapter, IbkrPaperAdapter, Order, PaperBroker, Side};

    #[test]
    fn foreign_market_positions_are_marked_in_base_currency() {
        let mut broker = PaperBroker::new(100_000.0, 0.0, 0.0)
            .with_market_fx_to_base(HashMap::from([("JP".to_string(), 0.01)]));

        let date = NaiveDate::from_ymd_opt(2025, 1, 10).expect("valid");
        let order = Order {
            date,
            market: "JP".to_string(),
            symbol: "7203".to_string(),
            side: Side::Buy,
            qty: 100,
        };
        let mut prices = PriceMap::new();
        prices.insert(("JP".to_string(), "7203".to_string()), 1_000.0);

        let fills = broker.execute_orders(&[order], &prices);
        assert_eq!(fills.len(), 1);
        assert!((broker.cash - 99_000.0).abs() < 1e-9);
        assert!((broker.net_exposure(&prices) - 1_000.0).abs() < 1e-9);
        assert!((broker.gross_exposure(&prices) - 1_000.0).abs() < 1e-9);
        assert!((broker.equity(&prices) - 100_000.0).abs() < 1e-9);
    }

    #[test]
    fn ibkr_lifecycle_logs_are_written_in_dry_run() {
        let (mirror_log, lifecycle_log) = temp_logs();
        let cfg = IbkrConfig {
            enabled: false,
            dry_run: true,
            mirror_log: mirror_log.display().to_string(),
            lifecycle_log: lifecycle_log.display().to_string(),
            ..IbkrConfig::default()
        };
        let mut adapter = IbkrPaperAdapter::new(PaperBroker::new(100_000.0, 0.0, 0.0), cfg)
            .expect("adapter create");

        let date = NaiveDate::from_ymd_opt(2025, 1, 10).expect("valid");
        let order = Order {
            date,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Buy,
            qty: 10,
        };
        let mut prices = PriceMap::new();
        prices.insert(("US".to_string(), "AAPL".to_string()), 100.0);

        let fills = adapter.execute_orders(&[order], &prices);
        assert_eq!(fills.len(), 1);
        adapter.reconcile_day(date);

        let lifecycle = fs::read_to_string(lifecycle_log).expect("read lifecycle");
        let mirror = fs::read_to_string(mirror_log).expect("read mirror");
        assert!(lifecycle.contains("\"event\":\"ack\""));
        assert!(lifecycle.contains("\"event\":\"filled\""));
        assert!(lifecycle.contains("\"event\":\"post_execute\""));
        assert!(lifecycle.contains("\"event\":\"reconcile\""));
        assert!(lifecycle.contains("\"tracked_orders\":1"));
        assert!(mirror.contains("\"symbol\":\"AAPL\""));
    }

    #[test]
    fn stale_open_order_gets_auto_canceled() {
        let (_mirror_log, lifecycle_log) = temp_logs();
        let cfg = IbkrConfig {
            enabled: false,
            dry_run: true,
            auto_cancel_stale: true,
            lifecycle_log: lifecycle_log.display().to_string(),
            ..IbkrConfig::default()
        };
        let mut adapter =
            IbkrPaperAdapter::new(PaperBroker::new(100.0, 0.0, 0.0), cfg).expect("adapter create");

        let d1 = NaiveDate::from_ymd_opt(2025, 1, 10).expect("valid");
        let d2 = NaiveDate::from_ymd_opt(2025, 1, 13).expect("valid");
        let mut prices = PriceMap::new();
        prices.insert(("US".to_string(), "AAPL".to_string()), 100.0);

        let order = Order {
            date: d1,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Buy,
            qty: 1000,
        };
        let fills = adapter.execute_orders(&[order], &prices);
        assert!(fills.is_empty());
        adapter.end_of_day(d2);

        let lifecycle = fs::read_to_string(lifecycle_log).expect("read lifecycle");
        assert!(lifecycle.contains("\"event\":\"reconcile\""));
        assert!(lifecycle.contains("\"event\":\"canceled\""));
        assert!(lifecycle.contains("\"event\":\"end_of_day\""));
    }

    // ── Buy skipped when insufficient cash ──────────────────────
    #[test]
    fn buy_skipped_when_cash_insufficient() {
        let mut broker = PaperBroker::new(500.0, 0.0, 0.0);
        let date = NaiveDate::from_ymd_opt(2025, 1, 10).expect("valid");
        let order = Order {
            date,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Buy,
            qty: 10,
        };
        let mut prices = PriceMap::new();
        prices.insert(("US".to_string(), "AAPL".to_string()), 100.0);

        let fills = broker.execute_orders(&[order], &prices);
        assert!(fills.is_empty(), "should not fill when cash < notional");
        assert!((broker.cash - 500.0).abs() < 1e-9, "cash unchanged");
        assert_eq!(broker.position_qty("US", "AAPL"), 0);
    }

    // ── Sell clamped to available position ────────────────────
    #[test]
    fn sell_clamped_to_available_position() {
        let mut broker = PaperBroker::new(100_000.0, 0.0, 0.0);
        let date = NaiveDate::from_ymd_opt(2025, 1, 10).expect("valid");
        let mut prices = PriceMap::new();
        prices.insert(("US".to_string(), "AAPL".to_string()), 100.0);

        // buy 5
        let buy = Order {
            date,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Buy,
            qty: 5,
        };
        broker.execute_orders(&[buy], &prices);

        // try to sell 10, should only sell 5
        let sell = Order {
            date,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Sell,
            qty: 10,
        };
        let fills = broker.execute_orders(&[sell], &prices);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].qty, 5, "sell clamped to held qty");
        assert_eq!(broker.position_qty("US", "AAPL"), 0);
    }

    // ── Sell with zero position is a no-op ────────────────────
    #[test]
    fn sell_with_no_position_is_noop() {
        let mut broker = PaperBroker::new(100_000.0, 0.0, 0.0);
        let date = NaiveDate::from_ymd_opt(2025, 1, 10).expect("valid");
        let mut prices = PriceMap::new();
        prices.insert(("US".to_string(), "AAPL".to_string()), 100.0);

        let sell = Order {
            date,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Sell,
            qty: 10,
        };
        let fills = broker.execute_orders(&[sell], &prices);
        assert!(fills.is_empty());
    }

    // ── Position avg_price resets to 0 when fully sold ────────
    #[test]
    fn position_avg_price_resets_on_full_sell() {
        let mut broker = PaperBroker::new(100_000.0, 0.0, 0.0);
        let date = NaiveDate::from_ymd_opt(2025, 1, 10).expect("valid");
        let mut prices = PriceMap::new();
        prices.insert(("US".to_string(), "AAPL".to_string()), 100.0);

        let buy = Order {
            date,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Buy,
            qty: 10,
        };
        broker.execute_orders(&[buy], &prices);
        assert_eq!(broker.position_qty("US", "AAPL"), 10);

        let sell = Order {
            date,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Sell,
            qty: 10,
        };
        broker.execute_orders(&[sell], &prices);
        assert_eq!(broker.position_qty("US", "AAPL"), 0);

        // equity should be original minus round-trip friction (none here)
        let eq = broker.equity(&prices);
        assert!((eq - 100_000.0).abs() < 1e-9);
    }

    // ── T+1 sellable_qty blocks same-day sells ────────────────
    #[test]
    fn t_plus_one_blocks_same_day_sell() {
        let mut broker = PaperBroker::new(100_000.0, 0.0, 0.0);
        let d1 = NaiveDate::from_ymd_opt(2025, 1, 10).expect("valid");
        let d2 = NaiveDate::from_ymd_opt(2025, 1, 13).expect("valid");
        let mut prices = PriceMap::new();
        prices.insert(("A".to_string(), "600519".to_string()), 100.0);

        let buy = Order {
            date: d1,
            market: "A".to_string(),
            symbol: "600519".to_string(),
            side: Side::Buy,
            qty: 100,
        };
        broker.execute_orders(&[buy], &prices);

        // same day, T+1: nothing sellable
        assert_eq!(broker.sellable_qty(d1, "A", "600519", true), 0);
        // same day, non-T+1: all sellable
        assert_eq!(broker.sellable_qty(d1, "A", "600519", false), 100);
        // next day, T+1: all sellable
        assert_eq!(broker.sellable_qty(d2, "A", "600519", true), 100);
    }

    // ── Multi-market FX accounting ────────────────────────────
    #[test]
    fn multi_market_fx_accounting_is_coherent() {
        let mut broker = PaperBroker::new(100_000.0, 0.0, 0.0).with_market_fx_to_base(
            HashMap::from([("JP".to_string(), 0.007), ("A".to_string(), 0.14)]),
        );
        let date = NaiveDate::from_ymd_opt(2025, 1, 10).expect("valid");
        let mut prices = PriceMap::new();
        prices.insert(("JP".to_string(), "7203".to_string()), 2_500.0); // ¥2500
        prices.insert(("A".to_string(), "600519".to_string()), 1_800.0); // ¥1800
        prices.insert(("US".to_string(), "AAPL".to_string()), 200.0);

        let orders = vec![
            Order {
                date,
                market: "JP".to_string(),
                symbol: "7203".to_string(),
                side: Side::Buy,
                qty: 100,
            },
            Order {
                date,
                market: "A".to_string(),
                symbol: "600519".to_string(),
                side: Side::Buy,
                qty: 10,
            },
            Order {
                date,
                market: "US".to_string(),
                symbol: "AAPL".to_string(),
                side: Side::Buy,
                qty: 5,
            },
        ];
        let fills = broker.execute_orders(&orders, &prices);
        assert_eq!(fills.len(), 3);

        // JP: 100 * 2500 * 0.007 = $1,750
        // A:  10 * 1800 * 0.14  = $2,520
        // US: 5 * 200 * 1.0     = $1,000
        let expected_exposure = 1_750.0 + 2_520.0 + 1_000.0;
        let gross = broker.gross_exposure(&prices);
        assert!(
            (gross - expected_exposure).abs() < 1.0,
            "gross={gross}, expected={expected_exposure}"
        );

        // equity should be cash + net_exposure
        let eq = broker.equity(&prices);
        assert!(
            (eq - (broker.cash + broker.net_exposure(&prices))).abs() < 1e-9,
            "equity = cash + net"
        );
    }

    // ── Kill switch blocks all execution ──────────────────────
    #[test]
    fn kill_switch_blocks_execution() {
        std::env::set_var("PQBOT_KILL_SWITCH", "1");
        let mut broker = PaperBroker::new(100_000.0, 0.0, 0.0);
        let date = NaiveDate::from_ymd_opt(2025, 1, 10).expect("valid");
        let mut prices = PriceMap::new();
        prices.insert(("US".to_string(), "AAPL".to_string()), 100.0);

        let order = Order {
            date,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Buy,
            qty: 10,
        };
        let fills = broker.execute_orders(&[order], &prices);
        assert!(fills.is_empty(), "kill switch should block fills");
        assert!((broker.cash - 100_000.0).abs() < 1e-9, "cash unchanged");
        std::env::remove_var("PQBOT_KILL_SWITCH");
    }

    // ── Fees and slippage deducted correctly ──────────────────
    #[test]
    fn fees_and_slippage_affect_cash_correctly() {
        // 10 bps commission, 5 bps slippage
        let mut broker = PaperBroker::new(100_000.0, 10.0, 5.0);
        let date = NaiveDate::from_ymd_opt(2025, 1, 10).expect("valid");
        let mut prices = PriceMap::new();
        prices.insert(("US".to_string(), "AAPL".to_string()), 100.0);

        let buy = Order {
            date,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Buy,
            qty: 100,
        };
        let fills = broker.execute_orders(&[buy], &prices);
        assert_eq!(fills.len(), 1);

        // fill price = 100 * (1 + 5/10000) = 100.05
        // notional = 100.05 * 100 = 10_005
        // fees = 10_005 * 10/10000 = 10.005
        // total deducted = 10_005 + 10.005 = 10_015.005
        let expected_cash = 100_000.0 - 10_015.005;
        assert!(
            (broker.cash - expected_cash).abs() < 0.01,
            "cash={}, expected={expected_cash}",
            broker.cash
        );
        assert!(fills[0].fees > 0.0, "fees should be non-zero");
    }

    // ── Missing price silently skips order ─────────────────────
    #[test]
    fn missing_price_skips_order() {
        let mut broker = PaperBroker::new(100_000.0, 0.0, 0.0);
        let date = NaiveDate::from_ymd_opt(2025, 1, 10).expect("valid");
        let prices = PriceMap::new(); // empty — no prices

        let order = Order {
            date,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Buy,
            qty: 10,
        };
        let fills = broker.execute_orders(&[order], &prices);
        assert!(fills.is_empty());
    }

    fn temp_logs() -> (PathBuf, PathBuf) {
        let (_, mirror, lifecycle, _) = temp_test_dir();
        (mirror, lifecycle)
    }

    fn temp_test_dir() -> (PathBuf, PathBuf, PathBuf, PathBuf) {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("quant_engine_rs_exec_test_{seed}"));
        fs::create_dir_all(&root).expect("create temp dir");
        let mirror = root.join("mirror.jsonl");
        let lifecycle = root.join("lifecycle.jsonl");
        let reconcile = root.join("reconcile.jsonl");
        (root, mirror, lifecycle, reconcile)
    }

    // ════════════════════════════════════════════════════════════
    // IBKR Paper Adapter — single order full lifecycle tests
    // ════════════════════════════════════════════════════════════

    /// Full single-order lifecycle: buy 10 AAPL, verify every phase.
    #[test]
    fn ibkr_single_us_buy_full_lifecycle() {
        let (_root, mirror_log, lifecycle_log, reconcile_log) = temp_test_dir();

        let cfg = IbkrConfig {
            enabled: false,
            dry_run: true,
            auto_reconcile: true,
            auto_cancel_stale: true,
            mirror_log: mirror_log.display().to_string(),
            lifecycle_log: lifecycle_log.display().to_string(),
            reconcile_log: reconcile_log.display().to_string(),
            ..IbkrConfig::default()
        };

        let sim = PaperBroker::new(100_000.0, 10.0, 5.0);
        let mut adapter = IbkrPaperAdapter::new(sim, cfg).expect("adapter create");

        let date = NaiveDate::from_ymd_opt(2025, 6, 10).expect("valid");
        let mut prices = PriceMap::new();
        prices.insert(("US".to_string(), "AAPL".to_string()), 200.0);

        // Phase 1: Submit + Execute
        let order = Order {
            date,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Buy,
            qty: 10,
        };
        let fills = adapter.execute_orders(&[order], &prices);

        assert_eq!(fills.len(), 1, "one fill");
        assert_eq!(fills[0].symbol, "AAPL");
        assert_eq!(fills[0].qty, 10);
        assert!(fills[0].price > 200.0, "buy fill includes slippage");
        assert!(fills[0].fees > 0.0, "fees charged");
        assert_eq!(adapter.position_qty("US", "AAPL"), 10);
        assert!(adapter.cash() < 100_000.0);

        // Verify lifecycle events
        let lifecycle = fs::read_to_string(&lifecycle_log).expect("read lifecycle");
        assert!(lifecycle.contains("\"event\":\"created\""));
        assert!(lifecycle.contains("\"event\":\"ack\""));
        assert!(lifecycle.contains("\"event\":\"filled\""));
        assert!(lifecycle.contains("\"event\":\"post_execute\""));

        // Verify mirror log
        let mirror = fs::read_to_string(&mirror_log).expect("read mirror");
        assert!(mirror.contains("\"symbol\":\"AAPL\""));
        assert!(mirror.contains("\"side\":\"BUY\""));

        // Phase 2: Reconcile + End of Day
        adapter.reconcile_day(date);
        adapter.end_of_day(date);

        let lifecycle_after = fs::read_to_string(&lifecycle_log).expect("read lifecycle");
        assert!(lifecycle_after.contains("\"event\":\"reconcile\""));
        assert!(lifecycle_after.contains("\"event\":\"end_of_day\""));

        // Verify reconcile report was written and is clean
        let reconcile = fs::read_to_string(&reconcile_log).expect("read reconcile");
        let report: serde_json::Value =
            serde_json::from_str(reconcile.trim()).expect("parse reconcile json");
        assert_eq!(report["clean"], true, "dry_run reconcile should be clean");
        assert_eq!(report["expected_source"], "internal");

        // Phase 3: Position survives overnight, sell half on day 2
        let d2 = NaiveDate::from_ymd_opt(2025, 6, 11).expect("valid");
        assert_eq!(adapter.position_qty("US", "AAPL"), 10);

        let sell = Order {
            date: d2,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Sell,
            qty: 5,
        };
        let sell_fills = adapter.execute_orders(&[sell], &prices);
        assert_eq!(sell_fills.len(), 1);
        assert_eq!(sell_fills[0].qty, 5);
        assert_eq!(adapter.position_qty("US", "AAPL"), 5);

        adapter.end_of_day(d2);

        // Two reconcile reports (one per end_of_day)
        let reconcile_final = fs::read_to_string(&reconcile_log).expect("read reconcile");
        let lines: Vec<&str> = reconcile_final.trim().lines().collect();
        assert_eq!(lines.len(), 2, "two reconcile reports");
    }

    /// Order that can't fill still gets lifecycle tracking + stale cancel.
    #[test]
    fn ibkr_unfillable_order_gets_tracked_and_canceled() {
        let (_root, mirror_log, lifecycle_log, reconcile_log) = temp_test_dir();

        let cfg = IbkrConfig {
            enabled: false,
            dry_run: true,
            auto_reconcile: true,
            auto_cancel_stale: true,
            mirror_log: mirror_log.display().to_string(),
            lifecycle_log: lifecycle_log.display().to_string(),
            reconcile_log: reconcile_log.display().to_string(),
            ..IbkrConfig::default()
        };

        let sim = PaperBroker::new(100.0, 0.0, 0.0); // only $100
        let mut adapter = IbkrPaperAdapter::new(sim, cfg).expect("adapter create");

        let d1 = NaiveDate::from_ymd_opt(2025, 6, 10).expect("valid");
        let d2 = NaiveDate::from_ymd_opt(2025, 6, 11).expect("valid");
        let mut prices = PriceMap::new();
        prices.insert(("US".to_string(), "AAPL".to_string()), 200.0);

        // $20k order with $100 cash → no fill
        let order = Order {
            date: d1,
            market: "US".to_string(),
            symbol: "AAPL".to_string(),
            side: Side::Buy,
            qty: 100,
        };
        let fills = adapter.execute_orders(&[order], &prices);
        assert!(fills.is_empty());

        let lifecycle = fs::read_to_string(&lifecycle_log).expect("read lifecycle");
        assert!(lifecycle.contains("\"event\":\"created\""));
        assert!(lifecycle.contains("\"event\":\"ack\""));
        assert!(!lifecycle.contains("\"event\":\"filled\""));

        // End of day on a later date triggers stale cancel
        adapter.end_of_day(d2);

        let lifecycle_final = fs::read_to_string(&lifecycle_log).expect("read lifecycle");
        assert!(lifecycle_final.contains("\"event\":\"canceled\""));
    }

    /// Multi-day: buy day 1, price changes, sell day 2. Reconcile each day.
    #[test]
    fn ibkr_multi_day_buy_sell_with_price_change() {
        let (_root, mirror_log, lifecycle_log, reconcile_log) = temp_test_dir();

        let cfg = IbkrConfig {
            enabled: false,
            dry_run: true,
            auto_reconcile: true,
            auto_cancel_stale: true,
            mirror_log: mirror_log.display().to_string(),
            lifecycle_log: lifecycle_log.display().to_string(),
            reconcile_log: reconcile_log.display().to_string(),
            ..IbkrConfig::default()
        };

        let sim = PaperBroker::new(50_000.0, 0.0, 0.0);
        let mut adapter = IbkrPaperAdapter::new(sim, cfg).expect("adapter create");

        let d1 = NaiveDate::from_ymd_opt(2025, 6, 10).expect("valid");
        let d2 = NaiveDate::from_ymd_opt(2025, 6, 11).expect("valid");
        let mut prices = PriceMap::new();
        prices.insert(("US".to_string(), "MSFT".to_string()), 400.0);

        // Day 1: buy 20 MSFT
        let buy = Order {
            date: d1,
            market: "US".to_string(),
            symbol: "MSFT".to_string(),
            side: Side::Buy,
            qty: 20,
        };
        adapter.execute_orders(&[buy], &prices);
        adapter.end_of_day(d1);

        let cash_after_buy = adapter.cash();
        assert_eq!(adapter.position_qty("US", "MSFT"), 20);

        // Day 2: price up, sell all
        prices.insert(("US".to_string(), "MSFT".to_string()), 420.0);
        let sell = Order {
            date: d2,
            market: "US".to_string(),
            symbol: "MSFT".to_string(),
            side: Side::Sell,
            qty: 20,
        };
        adapter.execute_orders(&[sell], &prices);
        assert_eq!(adapter.position_qty("US", "MSFT"), 0);
        assert!(adapter.cash() > cash_after_buy, "profit from price increase");

        adapter.end_of_day(d2);

        // Two reconcile reports, both clean
        let reconcile = fs::read_to_string(&reconcile_log).expect("read reconcile");
        let lines: Vec<&str> = reconcile.trim().lines().collect();
        assert_eq!(lines.len(), 2);
        for line in &lines {
            let report: serde_json::Value = serde_json::from_str(line).expect("parse");
            assert_eq!(report["clean"], true);
        }
    }
}
