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

    let sim = PaperBroker::new(
        cfg.start.starting_capital,
        cfg.execution.commission_bps,
        cfg.execution.slippage_bps,
    )
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
            positions: HashMap::new(),
            buys_today: HashMap::new(),
            current_day: None,
        }
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

        let current_qty = self.positions.get(&key).map(|p| p.qty).unwrap_or(0);
        let current_symbol = (current_qty as f64 * price).abs();

        let delta = if order.side == Side::Buy {
            order.qty
        } else {
            -order.qty
        };
        let projected_qty = (current_qty + delta).max(0);
        let projected_symbol = (projected_qty as f64 * price).abs();

        current - current_symbol + projected_symbol
    }

    pub fn execute_orders(&mut self, orders: &[Order], prices: &PriceMap) -> Vec<Trade> {
        let mut trades = Vec::new();

        for order in orders {
            self.roll_day_if_needed(order.date);
            let key = (order.market.clone(), order.symbol.clone());
            let Some(close) = prices.get(&key).copied() else {
                continue;
            };
            let cost = self.market_cost(&order.market);
            let fill = self.fill_price(close, order.side, cost);

            if order.side == Side::Buy {
                let notional = fill * order.qty as f64;
                let fees = self.fees(notional, order.side, cost);
                let total = notional + fees;
                if total > self.cash {
                    continue;
                }

                let position = self.positions.entry(key.clone()).or_default();
                let new_qty = position.qty + order.qty;
                if new_qty > 0 {
                    position.avg_price =
                        (position.avg_price * position.qty as f64 + notional) / new_qty as f64;
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

                let notional = fill * sell_qty as f64;
                let fees = self.fees(notional, order.side, cost);
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
            .map(|(key, pos)| prices.get(key).copied().unwrap_or(0.0) * pos.qty as f64)
            .map(f64::abs)
            .sum()
    }

    pub fn net_exposure(&self, prices: &PriceMap) -> f64 {
        self.positions
            .iter()
            .map(|(key, pos)| prices.get(key).copied().unwrap_or(0.0) * pos.qty as f64)
            .sum()
    }

    pub fn end_of_day(&mut self, date: NaiveDate) {
        self.current_day = Some(date);
    }
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
}

impl IbkrPaperAdapter {
    fn new(sim: PaperBroker, cfg: IbkrConfig) -> Result<Self> {
        let client = if cfg.enabled {
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
        })
    }

    fn execute_orders(&mut self, orders: &[Order], prices: &PriceMap) -> Vec<Trade> {
        for order in orders {
            self.register_and_submit(order.clone());
        }

        let fills = self.sim.execute_orders(orders, prices);
        for fill in &fills {
            self.apply_fill(fill);
            let _ = self.write_mirror_record(fill);
        }

        if self.cfg.auto_reconcile {
            let _ = self.reconcile_with_ibkr();
        }

        fills
    }

    fn end_of_day(&mut self, date: NaiveDate) {
        self.sim.end_of_day(date);
        if self.cfg.auto_cancel_stale {
            let _ = self.cancel_stale_orders(date);
        }
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
        fs,
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use chrono::NaiveDate;

    use crate::{config::IbkrConfig, model::PriceMap};

    use super::{IbkrPaperAdapter, Order, PaperBroker, Side};

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

        let lifecycle = fs::read_to_string(lifecycle_log).expect("read lifecycle");
        let mirror = fs::read_to_string(mirror_log).expect("read mirror");
        assert!(lifecycle.contains("\"event\":\"ack\""));
        assert!(lifecycle.contains("\"event\":\"filled\""));
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
        assert!(lifecycle.contains("\"event\":\"canceled\""));
    }

    fn temp_logs() -> (PathBuf, PathBuf) {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("private_quant_bot_exec_test_{seed}"));
        fs::create_dir_all(&root).expect("create temp dir");
        let mirror = root.join("mirror.jsonl");
        let lifecycle = root.join("lifecycle.jsonl");
        (mirror, lifecycle)
    }
}
