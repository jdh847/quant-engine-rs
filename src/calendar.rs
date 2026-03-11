use std::collections::{HashMap, HashSet};

use chrono::{Datelike, NaiveDate, Weekday};

#[derive(Debug, Clone)]
pub struct ExchangeCalendar {
    holidays: HashMap<String, HashSet<NaiveDate>>,
}

impl Default for ExchangeCalendar {
    fn default() -> Self {
        Self::new()
    }
}

impl ExchangeCalendar {
    pub fn new() -> Self {
        let mut holidays: HashMap<String, HashSet<NaiveDate>> = HashMap::new();
        holidays.insert("US".to_string(), us_holidays());
        holidays.insert("JP".to_string(), jp_holidays());
        holidays.insert("A".to_string(), a_share_holidays());

        Self { holidays }
    }

    pub fn is_trading_day(&self, market: &str, date: NaiveDate) -> bool {
        if matches!(date.weekday(), Weekday::Sat | Weekday::Sun) {
            return false;
        }

        let key = market.to_uppercase();
        if let Some(days) = self.holidays.get(&key) {
            return !days.contains(&date);
        }

        true
    }
}

fn us_holidays() -> HashSet<NaiveDate> {
    [
        (2025, 1, 1),
        (2025, 1, 20),
        (2025, 2, 17),
        (2025, 4, 18),
        (2025, 5, 26),
        (2025, 6, 19),
        (2025, 7, 4),
        (2025, 9, 1),
        (2025, 11, 27),
        (2025, 12, 25),
        (2026, 1, 1),
        (2026, 1, 19),
        (2026, 2, 16),
        (2026, 4, 3),
        (2026, 5, 25),
        (2026, 6, 19),
        (2026, 7, 3),
        (2026, 9, 7),
        (2026, 11, 26),
        (2026, 12, 25),
    ]
    .into_iter()
    .filter_map(|(y, m, d)| NaiveDate::from_ymd_opt(y, m, d))
    .collect()
}

fn jp_holidays() -> HashSet<NaiveDate> {
    [
        (2025, 1, 1),
        (2025, 1, 13),
        (2025, 2, 11),
        (2025, 2, 24),
        (2025, 3, 20),
        (2025, 4, 29),
        (2025, 5, 5),
        (2025, 5, 6),
        (2025, 7, 21),
        (2025, 8, 11),
        (2025, 9, 15),
        (2025, 9, 23),
        (2025, 10, 13),
        (2025, 11, 3),
        (2025, 11, 24),
        (2026, 1, 1),
        (2026, 1, 12),
        (2026, 2, 11),
        (2026, 2, 23),
        (2026, 3, 20),
        (2026, 4, 29),
        (2026, 5, 4),
        (2026, 5, 5),
        (2026, 5, 6),
        (2026, 7, 20),
        (2026, 8, 11),
        (2026, 9, 21),
        (2026, 9, 22),
        (2026, 10, 12),
        (2026, 11, 3),
        (2026, 11, 23),
    ]
    .into_iter()
    .filter_map(|(y, m, d)| NaiveDate::from_ymd_opt(y, m, d))
    .collect()
}

fn a_share_holidays() -> HashSet<NaiveDate> {
    [
        (2025, 1, 1),
        (2025, 1, 28),
        (2025, 1, 29),
        (2025, 1, 30),
        (2025, 1, 31),
        (2025, 2, 3),
        (2025, 2, 4),
        (2025, 4, 4),
        (2025, 4, 7),
        (2025, 5, 1),
        (2025, 5, 2),
        (2025, 5, 5),
        (2025, 6, 2),
        (2025, 10, 1),
        (2025, 10, 2),
        (2025, 10, 3),
        (2025, 10, 6),
        (2025, 10, 7),
        (2025, 10, 8),
        (2026, 1, 1),
        (2026, 2, 16),
        (2026, 2, 17),
        (2026, 2, 18),
        (2026, 2, 19),
        (2026, 2, 20),
        (2026, 4, 6),
        (2026, 5, 1),
        (2026, 6, 22),
        (2026, 10, 1),
        (2026, 10, 2),
        (2026, 10, 5),
        (2026, 10, 6),
        (2026, 10, 7),
        (2026, 10, 8),
    ]
    .into_iter()
    .filter_map(|(y, m, d)| NaiveDate::from_ymd_opt(y, m, d))
    .collect()
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;

    use super::ExchangeCalendar;

    #[test]
    fn known_holidays_are_closed() {
        let cal = ExchangeCalendar::new();

        let us_new_year = NaiveDate::from_ymd_opt(2025, 1, 1).expect("valid date");
        let jp_holiday = NaiveDate::from_ymd_opt(2025, 1, 13).expect("valid date");
        let cn_national = NaiveDate::from_ymd_opt(2025, 10, 1).expect("valid date");

        assert!(!cal.is_trading_day("US", us_new_year));
        assert!(!cal.is_trading_day("JP", jp_holiday));
        assert!(!cal.is_trading_day("A", cn_national));
    }

    #[test]
    fn normal_weekday_is_open() {
        let cal = ExchangeCalendar::new();
        let d = NaiveDate::from_ymd_opt(2025, 1, 15).expect("valid date");
        assert!(cal.is_trading_day("US", d));
        assert!(cal.is_trading_day("JP", d));
    }
}
