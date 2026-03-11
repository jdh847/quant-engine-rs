use std::{collections::HashMap, time::Duration};

use anyhow::{Context, Result};
use reqwest::blocking::Client;
use serde::Deserialize;

use crate::config::FxConfig;

#[derive(Debug, Deserialize)]
struct FxResponse {
    rates: HashMap<String, f64>,
}

pub fn fetch_live_fx_to_base(
    fx_cfg: &FxConfig,
    base_currency: &str,
    currencies: &[String],
) -> Result<HashMap<String, f64>> {
    let base = base_currency.to_uppercase();
    let mut targets: Vec<String> = currencies
        .iter()
        .map(|c| c.to_uppercase())
        .filter(|c| c != &base)
        .collect();
    targets.sort();
    targets.dedup();

    if targets.is_empty() {
        let mut only_base = HashMap::new();
        only_base.insert(base, 1.0);
        return Ok(only_base);
    }

    let client = Client::builder()
        .timeout(Duration::from_millis(fx_cfg.timeout_ms))
        .build()
        .context("build FX HTTP client failed")?;

    let url = format!(
        "{}?from={}&to={}",
        fx_cfg.provider_url.trim_end_matches('/'),
        base,
        targets.join(",")
    );

    let body = client
        .get(url)
        .send()
        .context("fetch FX rates failed")?
        .error_for_status()
        .context("FX provider returned error status")?
        .json::<FxResponse>()
        .context("parse FX response failed")?;

    let mut fx_to_base = HashMap::new();
    fx_to_base.insert(base.clone(), 1.0);
    for (ccy, rate_base_to_ccy) in body.rates {
        if rate_base_to_ccy > 0.0 {
            fx_to_base.insert(ccy.to_uppercase(), 1.0 / rate_base_to_ccy);
        }
    }
    Ok(fx_to_base)
}

#[cfg(test)]
mod tests {
    use crate::config::FxConfig;

    use super::fetch_live_fx_to_base;

    #[test]
    fn empty_targets_returns_base_only() {
        let cfg = FxConfig {
            live_enabled: false,
            provider_url: "https://example.invalid".to_string(),
            timeout_ms: 100,
            refresh_interval_days: 1,
            failure_cooldown_days: 1,
        };
        let out = fetch_live_fx_to_base(&cfg, "USD", &["USD".to_string()]).expect("should succeed");
        assert_eq!(out.get("USD").copied().unwrap_or(0.0), 1.0);
    }
}
