use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use serde::Deserialize;

use crate::safety::{ensure_ibkr_paper_allowed, ensure_network_allowed};
use crate::sdk::is_registered_sdk_plugin;

#[derive(Debug, Clone, Deserialize)]
pub struct StartConfig {
    pub starting_capital: f64,
    pub base_currency: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StrategyConfig {
    #[serde(default = "default_strategy_plugin")]
    pub strategy_plugin: String,
    pub short_window: usize,
    pub long_window: usize,
    pub vol_window: usize,
    pub top_n: usize,
    pub min_momentum: f64,
    #[serde(default = "default_mean_reversion_window")]
    pub mean_reversion_window: usize,
    #[serde(default = "default_volume_window")]
    pub volume_window: usize,
    #[serde(default = "default_factor_momentum_weight")]
    pub factor_momentum_weight: f64,
    #[serde(default = "default_factor_mean_reversion_weight")]
    pub factor_mean_reversion_weight: f64,
    #[serde(default = "default_factor_low_vol_weight")]
    pub factor_low_vol_weight: f64,
    #[serde(default = "default_factor_volume_weight")]
    pub factor_volume_weight: f64,
    #[serde(default = "default_risk_parity_blend")]
    pub risk_parity_blend: f64,
    #[serde(default = "default_max_turnover_ratio")]
    pub max_turnover_ratio: f64,
    #[serde(default = "default_portfolio_method")]
    pub portfolio_method: String,
    #[serde(default = "default_hrp_lookback")]
    pub hrp_lookback: usize,
    #[serde(default = "default_winsorize_pct")]
    pub winsorize_pct: f64,
    #[serde(default = "default_layer1_select_ratio")]
    pub layer1_select_ratio: f64,
    #[serde(default = "default_industry_neutral_strength")]
    pub industry_neutral_strength: f64,
    #[serde(default = "default_regime_vol_window")]
    pub regime_vol_window: usize,
    #[serde(default = "default_regime_target_vol")]
    pub regime_target_vol: f64,
    #[serde(default = "default_regime_floor_scale")]
    pub regime_floor_scale: f64,
    #[serde(default = "default_regime_ceiling_scale")]
    pub regime_ceiling_scale: f64,
}

fn default_strategy_plugin() -> String {
    "layered_multi_factor".to_string()
}

fn default_regime_vol_window() -> usize {
    8
}

fn default_regime_target_vol() -> f64 {
    0.02
}

fn default_regime_floor_scale() -> f64 {
    0.35
}

fn default_regime_ceiling_scale() -> f64 {
    1.10
}

fn default_mean_reversion_window() -> usize {
    3
}

fn default_volume_window() -> usize {
    5
}

fn default_factor_momentum_weight() -> f64 {
    0.45
}

fn default_factor_mean_reversion_weight() -> f64 {
    0.20
}

fn default_factor_low_vol_weight() -> f64 {
    0.25
}

fn default_factor_volume_weight() -> f64 {
    0.10
}

fn default_risk_parity_blend() -> f64 {
    0.60
}

fn default_max_turnover_ratio() -> f64 {
    0.35
}

fn default_portfolio_method() -> String {
    "risk_parity".to_string()
}

fn default_hrp_lookback() -> usize {
    20
}

fn default_winsorize_pct() -> f64 {
    0.05
}

fn default_layer1_select_ratio() -> f64 {
    0.60
}

fn default_industry_neutral_strength() -> f64 {
    1.0
}

#[derive(Debug, Clone, Deserialize)]
pub struct RiskConfig {
    pub max_gross_exposure_ratio: f64,
    pub max_symbol_weight: f64,
    pub daily_loss_limit_ratio: f64,
    #[serde(default)]
    pub currency_max_net_exposure_ratio: BTreeMap<String, f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExecutionConfig {
    pub commission_bps: f64,
    pub slippage_bps: f64,
    #[serde(default)]
    pub sell_tax_bps: f64,
    #[serde(default)]
    pub min_fee: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FxConfig {
    #[serde(default)]
    pub live_enabled: bool,
    #[serde(default = "default_fx_provider_url")]
    pub provider_url: String,
    #[serde(default = "default_fx_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_fx_refresh_interval_days")]
    pub refresh_interval_days: u64,
    #[serde(default = "default_fx_failure_cooldown_days")]
    pub failure_cooldown_days: u64,
}

fn default_fx_provider_url() -> String {
    "https://api.frankfurter.app/latest".to_string()
}

fn default_fx_timeout_ms() -> u64 {
    1500
}

fn default_fx_refresh_interval_days() -> u64 {
    1
}

fn default_fx_failure_cooldown_days() -> u64 {
    3
}

impl Default for FxConfig {
    fn default() -> Self {
        Self {
            live_enabled: false,
            provider_url: default_fx_provider_url(),
            timeout_ms: default_fx_timeout_ms(),
            refresh_interval_days: default_fx_refresh_interval_days(),
            failure_cooldown_days: default_fx_failure_cooldown_days(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MarketExecutionCost {
    pub commission_bps: f64,
    pub slippage_bps: f64,
    pub sell_tax_bps: f64,
    pub min_fee: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BrokerConfig {
    pub mode: String,
    pub paper_only: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IbkrConfig {
    pub enabled: bool,
    pub gateway_url: String,
    pub account_id: String,
    pub dry_run: bool,
    pub mirror_log: String,
    pub lifecycle_log: String,
    pub auto_reconcile: bool,
    pub auto_cancel_stale: bool,
    pub allow_remote_paper: bool,
}

impl Default for IbkrConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            gateway_url: "https://127.0.0.1:5000/v1/api".to_string(),
            account_id: String::new(),
            dry_run: true,
            mirror_log: "outputs_rust/ibkr_mirror_orders.jsonl".to_string(),
            lifecycle_log: "outputs_rust/ibkr_lifecycle_events.jsonl".to_string(),
            auto_reconcile: true,
            auto_cancel_stale: true,
            allow_remote_paper: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RawMarketConfig {
    pub allocation: f64,
    pub data_file: String,
    pub lot_size: i64,
    #[serde(default)]
    pub min_trade_notional: f64,
    #[serde(default)]
    pub currency: Option<String>,
    #[serde(default)]
    pub fx_to_base: Option<f64>,
    #[serde(default)]
    pub industry_file: Option<String>,
    #[serde(default)]
    pub holiday_file: Option<String>,
    #[serde(default)]
    pub commission_bps: Option<f64>,
    #[serde(default)]
    pub slippage_bps: Option<f64>,
    #[serde(default)]
    pub sell_tax_bps: Option<f64>,
    #[serde(default)]
    pub min_fee: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct MarketConfig {
    pub name: String,
    pub allocation: f64,
    pub data_file: PathBuf,
    pub lot_size: i64,
    pub min_trade_notional: f64,
    pub currency: String,
    pub fx_to_base: f64,
    pub industry_file: Option<PathBuf>,
    pub industry_map: HashMap<String, String>,
    pub holiday_file: Option<PathBuf>,
    pub holiday_dates: HashSet<NaiveDate>,
    pub execution_cost: MarketExecutionCost,
}

#[derive(Debug, Clone)]
pub struct BotConfig {
    pub start: StartConfig,
    pub strategy: StrategyConfig,
    pub risk: RiskConfig,
    pub execution: ExecutionConfig,
    pub fx: FxConfig,
    pub broker: BrokerConfig,
    pub ibkr: IbkrConfig,
    pub markets: BTreeMap<String, MarketConfig>,
}

#[derive(Debug, Deserialize)]
struct RawBotConfig {
    start: StartConfig,
    strategy: StrategyConfig,
    risk: RiskConfig,
    execution: ExecutionConfig,
    #[serde(default)]
    fx: FxConfig,
    broker: BrokerConfig,
    #[serde(default)]
    ibkr: IbkrConfig,
    markets: BTreeMap<String, RawMarketConfig>,
}

pub fn load_config(path: impl AsRef<Path>) -> Result<BotConfig> {
    let config_path = path.as_ref();
    let content = fs::read_to_string(config_path)
        .with_context(|| format!("read config failed: {}", config_path.display()))?;
    let raw: RawBotConfig = toml::from_str(&content).context("parse config TOML failed")?;

    validate_strategy(&raw.strategy)?;
    validate_execution(&raw.execution)?;
    validate_risk(&raw.risk)?;
    validate_fx(&raw.fx)?;
    validate_broker(&raw.broker, &raw.ibkr)?;

    let abs_config = fs::canonicalize(config_path)
        .with_context(|| format!("canonicalize config failed: {}", config_path.display()))?;
    let project_root = detect_project_root(&abs_config);

    let mut markets = BTreeMap::new();
    for (name, market) in raw.markets {
        if market.lot_size <= 0 {
            return Err(anyhow!("lot_size must be positive for market {name}"));
        }
        let currency = market
            .currency
            .unwrap_or_else(|| default_market_currency(&name).to_string())
            .to_uppercase();
        let fx_to_base = market.fx_to_base.unwrap_or(1.0);
        if fx_to_base <= 0.0 {
            return Err(anyhow!("markets.{name}.fx_to_base must be > 0"));
        }

        let execution_cost = MarketExecutionCost {
            commission_bps: market
                .commission_bps
                .unwrap_or(raw.execution.commission_bps),
            slippage_bps: market.slippage_bps.unwrap_or(raw.execution.slippage_bps),
            sell_tax_bps: market.sell_tax_bps.unwrap_or(raw.execution.sell_tax_bps),
            min_fee: market.min_fee.unwrap_or(raw.execution.min_fee),
        };
        validate_market_execution(&name, execution_cost)?;
        let industry_file_path = market.industry_file.as_ref().map(|p| project_root.join(p));
        let industry_map = if let Some(industry_file) = &industry_file_path {
            load_industry_map(industry_file)
                .with_context(|| format!("failed loading industry file for market {name}"))?
        } else {
            HashMap::new()
        };
        let holiday_file_path = market.holiday_file.as_ref().map(|p| project_root.join(p));
        let holiday_dates = if let Some(holiday_file) = &holiday_file_path {
            load_holiday_dates(holiday_file)
                .with_context(|| format!("failed loading holiday file for market {name}"))?
        } else {
            HashSet::new()
        };

        markets.insert(
            name.clone(),
            MarketConfig {
                name,
                allocation: market.allocation,
                data_file: project_root.join(market.data_file),
                lot_size: market.lot_size,
                min_trade_notional: market.min_trade_notional,
                currency,
                fx_to_base,
                industry_file: industry_file_path,
                industry_map,
                holiday_file: holiday_file_path,
                holiday_dates,
                execution_cost,
            },
        );
    }

    let allocation_sum: f64 = markets.values().map(|m| m.allocation).sum();
    if (allocation_sum - 1.0).abs() > 1e-6 {
        return Err(anyhow!(
            "market allocations must sum to 1.0, got {allocation_sum:.6}"
        ));
    }

    Ok(BotConfig {
        start: raw.start,
        strategy: raw.strategy,
        risk: raw.risk,
        execution: raw.execution,
        fx: raw.fx,
        broker: raw.broker,
        ibkr: IbkrConfig {
            mirror_log: project_root.join(raw.ibkr.mirror_log).display().to_string(),
            lifecycle_log: project_root
                .join(raw.ibkr.lifecycle_log)
                .display()
                .to_string(),
            ..raw.ibkr
        },
        markets,
    })
}

fn load_industry_map(path: &Path) -> Result<HashMap<String, String>> {
    let mut reader = csv::Reader::from_path(path)
        .with_context(|| format!("open industry csv failed: {}", path.display()))?;
    let mut map = HashMap::new();
    for rec in reader.records() {
        let rec = rec.with_context(|| format!("parse industry row failed: {}", path.display()))?;
        let symbol = rec
            .get(0)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| anyhow!("industry csv missing symbol column in {}", path.display()))?
            .to_string();
        let industry = rec
            .get(1)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| anyhow!("industry csv missing industry column in {}", path.display()))?
            .to_string();
        map.insert(symbol, industry);
    }
    Ok(map)
}

fn load_holiday_dates(path: &Path) -> Result<HashSet<NaiveDate>> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("read holiday file failed: {}", path.display()))?;
    let mut out = HashSet::new();
    for (idx, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Allow either "YYYY-MM-DD" or "date,..." CSV style. Also tolerate a header line "date".
        let first = line.split(',').next().unwrap_or("").trim();
        if first.eq_ignore_ascii_case("date") {
            continue;
        }
        let date = NaiveDate::parse_from_str(first, "%Y-%m-%d").with_context(|| {
            format!(
                "invalid holiday date at {}:{}: '{}'",
                path.display(),
                idx + 1,
                first
            )
        })?;
        out.insert(date);
    }
    Ok(out)
}

fn default_market_currency(market: &str) -> &'static str {
    match market {
        "A" => "CNY",
        "JP" => "JPY",
        _ => "USD",
    }
}

fn validate_execution(execution: &ExecutionConfig) -> Result<()> {
    if execution.commission_bps < 0.0 {
        return Err(anyhow!("execution.commission_bps must be >= 0"));
    }
    if execution.slippage_bps < 0.0 {
        return Err(anyhow!("execution.slippage_bps must be >= 0"));
    }
    if execution.sell_tax_bps < 0.0 {
        return Err(anyhow!("execution.sell_tax_bps must be >= 0"));
    }
    if execution.min_fee < 0.0 {
        return Err(anyhow!("execution.min_fee must be >= 0"));
    }
    Ok(())
}

fn validate_market_execution(name: &str, cost: MarketExecutionCost) -> Result<()> {
    if cost.commission_bps < 0.0 {
        return Err(anyhow!(
            "markets.{name}.commission_bps must be >= 0 when provided"
        ));
    }
    if cost.slippage_bps < 0.0 {
        return Err(anyhow!(
            "markets.{name}.slippage_bps must be >= 0 when provided"
        ));
    }
    if cost.sell_tax_bps < 0.0 {
        return Err(anyhow!(
            "markets.{name}.sell_tax_bps must be >= 0 when provided"
        ));
    }
    if cost.min_fee < 0.0 {
        return Err(anyhow!("markets.{name}.min_fee must be >= 0 when provided"));
    }
    Ok(())
}

fn validate_strategy(strategy: &StrategyConfig) -> Result<()> {
    let plugin = strategy.strategy_plugin.trim();
    let built_in = plugin == "layered_multi_factor" || plugin == "momentum_guard";
    if !built_in && !is_registered_sdk_plugin(plugin) {
        return Err(anyhow!(
            "strategy.strategy_plugin '{}' is not registered; run `cargo run -- plugins` or `cargo run -- sdk-register --package-dir <path>`",
            strategy.strategy_plugin
        ));
    }
    if strategy.short_window >= strategy.long_window {
        return Err(anyhow!(
            "strategy short_window must be smaller than long_window"
        ));
    }
    if strategy.vol_window < 2 {
        return Err(anyhow!("strategy vol_window must be >= 2"));
    }
    if strategy.top_n == 0 {
        return Err(anyhow!("strategy top_n must be >= 1"));
    }
    if strategy.mean_reversion_window < 1 {
        return Err(anyhow!("strategy mean_reversion_window must be >= 1"));
    }
    if strategy.volume_window < 2 {
        return Err(anyhow!("strategy volume_window must be >= 2"));
    }
    if strategy.risk_parity_blend < 0.0 || strategy.risk_parity_blend > 1.0 {
        return Err(anyhow!("strategy risk_parity_blend must be in [0, 1]"));
    }
    if strategy.max_turnover_ratio <= 0.0 {
        return Err(anyhow!("strategy max_turnover_ratio must be > 0"));
    }
    if strategy.portfolio_method != "risk_parity" && strategy.portfolio_method != "hrp" {
        return Err(anyhow!(
            "strategy.portfolio_method must be risk_parity or hrp"
        ));
    }
    if strategy.hrp_lookback < 4 {
        return Err(anyhow!("strategy.hrp_lookback must be >= 4"));
    }
    if !(0.0..0.5).contains(&strategy.winsorize_pct) {
        return Err(anyhow!("strategy.winsorize_pct must be in [0.0, 0.5)"));
    }
    if !(0.0..=1.0).contains(&strategy.layer1_select_ratio) || strategy.layer1_select_ratio == 0.0 {
        return Err(anyhow!("strategy.layer1_select_ratio must be in (0, 1]"));
    }
    if !(0.0..=1.0).contains(&strategy.industry_neutral_strength) {
        return Err(anyhow!(
            "strategy.industry_neutral_strength must be in [0, 1]"
        ));
    }
    if strategy.regime_vol_window < 2 {
        return Err(anyhow!("strategy regime_vol_window must be >= 2"));
    }
    if strategy.regime_target_vol <= 0.0 {
        return Err(anyhow!("strategy regime_target_vol must be > 0"));
    }
    if strategy.regime_floor_scale <= 0.0 {
        return Err(anyhow!("strategy regime_floor_scale must be > 0"));
    }
    if strategy.regime_ceiling_scale < strategy.regime_floor_scale {
        return Err(anyhow!(
            "strategy regime_ceiling_scale must be >= regime_floor_scale"
        ));
    }
    let weight_sum = strategy.factor_momentum_weight
        + strategy.factor_mean_reversion_weight
        + strategy.factor_low_vol_weight
        + strategy.factor_volume_weight;
    if weight_sum <= 0.0 {
        return Err(anyhow!("strategy factor weights sum must be > 0"));
    }
    Ok(())
}

fn validate_risk(risk: &RiskConfig) -> Result<()> {
    if risk.max_gross_exposure_ratio <= 0.0 {
        return Err(anyhow!("risk.max_gross_exposure_ratio must be > 0"));
    }
    if risk.max_symbol_weight <= 0.0 {
        return Err(anyhow!("risk.max_symbol_weight must be > 0"));
    }
    if risk.daily_loss_limit_ratio <= 0.0 {
        return Err(anyhow!("risk.daily_loss_limit_ratio must be > 0"));
    }
    for (ccy, limit) in &risk.currency_max_net_exposure_ratio {
        if *limit < 0.0 {
            return Err(anyhow!(
                "risk.currency_max_net_exposure_ratio.{ccy} must be >= 0"
            ));
        }
    }
    Ok(())
}

fn validate_fx(fx: &FxConfig) -> Result<()> {
    if fx.provider_url.trim().is_empty() {
        return Err(anyhow!("fx.provider_url cannot be empty"));
    }
    if fx.timeout_ms == 0 {
        return Err(anyhow!("fx.timeout_ms must be > 0"));
    }
    if fx.refresh_interval_days == 0 {
        return Err(anyhow!("fx.refresh_interval_days must be > 0"));
    }
    if fx.failure_cooldown_days == 0 {
        return Err(anyhow!("fx.failure_cooldown_days must be > 0"));
    }
    Ok(())
}

fn detect_project_root(abs_config_path: &Path) -> PathBuf {
    for ancestor in abs_config_path.ancestors().skip(1) {
        if ancestor.join("Cargo.toml").is_file() {
            return ancestor.to_path_buf();
        }
    }
    abs_config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}

fn validate_broker(broker: &BrokerConfig, ibkr: &IbkrConfig) -> Result<()> {
    // This repo is intentionally paper-only. Enforce it for all broker modes.
    if !broker.paper_only {
        return Err(anyhow!(
            "broker.paper_only must stay true (paper-only repository)"
        ));
    }

    if ibkr.enabled && broker.mode.as_str() != "ibkr_paper" {
        return Err(anyhow!(
            "ibkr.enabled=true requires broker.mode=ibkr_paper (paper adapter)"
        ));
    }

    match broker.mode.as_str() {
        "sim" => Ok(()),
        "ibkr_paper" => {
            ensure_ibkr_paper_allowed()?;
            if ibkr.enabled && !ibkr.allow_remote_paper {
                let safe_hosts = ["127.0.0.1", "localhost"];
                if !safe_hosts
                    .iter()
                    .any(|host| ibkr.gateway_url.contains(host))
                {
                    return Err(anyhow!(
                        "ibkr gateway_url must target localhost unless allow_remote_paper=true"
                    ));
                }
            }
            if ibkr.enabled && !ibkr.dry_run {
                // Any real network call requires explicit opt-in, even though it's still paper.
                ensure_network_allowed("ibkr_paper")?;
            }
            Ok(())
        }
        other => Err(anyhow!(
            "unsupported broker.mode {other}; expected sim or ibkr_paper"
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use super::detect_project_root;

    #[test]
    fn detect_project_root_finds_workspace_cargo() {
        let cfg = PathBuf::from("config/bot.toml");
        let abs = fs::canonicalize(cfg).expect("canonical config");
        let root = detect_project_root(&abs);
        assert!(root.join("Cargo.toml").is_file());
    }

    #[test]
    fn detect_project_root_falls_back_to_config_parent() {
        let temp = std::env::temp_dir().join("private_quant_bot_cfg_root_test");
        fs::create_dir_all(&temp).expect("create temp");
        let cfg = temp.join("bot.toml");
        fs::write(&cfg, "").expect("write file");
        let abs = fs::canonicalize(&cfg).expect("canonical temp config");
        let root = detect_project_root(&abs);
        let temp_canon = fs::canonicalize(temp).expect("canonical temp root");
        assert_eq!(root, temp_canon);
    }
}
