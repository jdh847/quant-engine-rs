use chrono::NaiveDate;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub fn as_str(self) -> &'static str {
        match self {
            Side::Buy => "BUY",
            Side::Sell => "SELL",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Bar {
    pub date: NaiveDate,
    pub market: String,
    pub symbol: String,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub date: NaiveDate,
    pub market: String,
    pub symbol: String,
    pub side: Side,
    pub qty: i64,
}

#[derive(Debug, Clone, Default)]
pub struct Position {
    pub qty: i64,
    pub avg_price: f64,
}

#[derive(Debug, Clone)]
pub struct Trade {
    pub date: NaiveDate,
    pub market: String,
    pub symbol: String,
    pub side: Side,
    pub qty: i64,
    pub price: f64,
    pub fees: f64,
}

#[derive(Debug, Clone)]
pub struct RiskRejection {
    pub date: NaiveDate,
    pub market: String,
    pub symbol: String,
    pub side: Side,
    pub qty: i64,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct EquityPoint {
    pub date: NaiveDate,
    pub equity: f64,
    pub cash: f64,
    pub gross_exposure: f64,
    pub net_exposure: f64,
}

pub type PriceKey = (String, String);
pub type PriceMap = std::collections::HashMap<PriceKey, f64>;
