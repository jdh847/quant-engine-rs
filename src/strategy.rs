use std::collections::{HashMap, VecDeque};

use crate::{
    config::StrategyConfig,
    model::Bar,
    portfolio::{optimize_targets, PortfolioMethod, PortfolioOptimizerConfig, SignalCandidate},
    sdk::{is_registered_sdk_plugin, list_registered_sdk_plugins_or_empty, RegisteredSdkPlugin},
};

pub trait StrategyPlugin {
    fn id(&self) -> &'static str;
    fn target_notionals(
        &mut self,
        bars: &[Bar],
        market_budget: f64,
        current_notionals: &HashMap<String, f64>,
    ) -> HashMap<String, f64>;
}

const LAYERED_MULTI_FACTOR: &str = "layered_multi_factor";
const MOMENTUM_GUARD: &str = "momentum_guard";

pub struct StrategyPluginInfo {
    pub id: &'static str,
    pub name: &'static str,
    pub description: &'static str,
}

pub struct RuntimeStrategyPluginInfo {
    pub id: String,
    pub name: String,
    pub description: String,
    pub source: String,
}

pub fn strategy_plugin_catalog() -> &'static [StrategyPluginInfo] {
    &[
        StrategyPluginInfo {
            id: LAYERED_MULTI_FACTOR,
            name: "Layered Multi-Factor",
            description:
                "Two-stage ranking with winsorization and industry-neutralized multi-factor alpha.",
        },
        StrategyPluginInfo {
            id: MOMENTUM_GUARD,
            name: "Momentum Guard",
            description:
                "Lightweight momentum/volatility strategy with trend and regime guardrails.",
        },
    ]
}

pub fn available_strategy_plugins() -> Vec<String> {
    let mut out = strategy_plugin_catalog()
        .iter()
        .map(|p| p.id.to_string())
        .collect::<Vec<_>>();
    for plugin in list_registered_sdk_plugins_or_empty() {
        if plugin.enabled && !out.iter().any(|x| x == &plugin.plugin_id) {
            out.push(plugin.plugin_id);
        }
    }
    out.sort();
    out
}

pub fn is_supported_strategy_plugin(plugin: &str) -> bool {
    strategy_plugin_catalog()
        .iter()
        .any(|item| item.id == plugin)
        || is_registered_sdk_plugin(plugin)
}

pub fn runtime_strategy_plugin_catalog() -> Vec<RuntimeStrategyPluginInfo> {
    let mut out = strategy_plugin_catalog()
        .iter()
        .map(|p| RuntimeStrategyPluginInfo {
            id: p.id.to_string(),
            name: p.name.to_string(),
            description: p.description.to_string(),
            source: "builtin".to_string(),
        })
        .collect::<Vec<_>>();
    for plugin in list_registered_sdk_plugins_or_empty() {
        if !plugin.enabled {
            continue;
        }
        if out.iter().any(|row| row.id == plugin.plugin_id) {
            continue;
        }
        out.push(RuntimeStrategyPluginInfo {
            id: plugin.plugin_id,
            name: plugin.name,
            description: plugin.description,
            source: "sdk".to_string(),
        });
    }
    out.sort_by(|a, b| a.id.cmp(&b.id));
    out
}

pub fn build_strategy(
    cfg: StrategyConfig,
    industries: HashMap<(String, String), String>,
) -> Box<dyn StrategyPlugin> {
    match cfg.strategy_plugin.as_str() {
        MOMENTUM_GUARD => Box::new(MomentumOnlyStrategy::new(cfg)),
        LAYERED_MULTI_FACTOR => Box::new(MomentumTrendStrategy::new(cfg, industries)),
        plugin_id => {
            if let Some(plugin_cfg) = list_registered_sdk_plugins_or_empty()
                .into_iter()
                .find(|p| p.enabled && p.plugin_id == plugin_id)
            {
                Box::new(SdkTemplateStrategy::new(cfg, plugin_cfg))
            } else {
                Box::new(MomentumTrendStrategy::new(cfg, industries))
            }
        }
    }
}

#[derive(Debug)]
pub struct MomentumTrendStrategy {
    cfg: StrategyConfig,
    history: HashMap<(String, String), SymbolHistory>,
    industries: HashMap<(String, String), String>,
}

#[derive(Debug, Default)]
struct SymbolHistory {
    closes: VecDeque<f64>,
    volumes: VecDeque<f64>,
}

#[derive(Debug, Clone)]
struct FactorSnapshot {
    market: String,
    symbol: String,
    momentum: f64,
    mean_reversion: f64,
    volatility: f64,
    volume_signal: f64,
    returns: Vec<f64>,
    trend_ok: bool,
}

#[derive(Debug)]
pub struct SdkTemplateStrategy {
    plugin_id: &'static str,
    inner: MomentumTrendStrategy,
    min_price: f64,
    alpha_volume_scale: f64,
}

impl SdkTemplateStrategy {
    pub fn new(cfg: StrategyConfig, plugin: RegisteredSdkPlugin) -> Self {
        let leaked_id: &'static str = Box::leak(plugin.plugin_id.into_boxed_str());
        Self {
            plugin_id: leaked_id,
            inner: MomentumTrendStrategy::new(cfg, HashMap::new()),
            min_price: plugin.min_price.max(0.0),
            alpha_volume_scale: plugin.alpha_volume_scale,
        }
    }
}

impl StrategyPlugin for SdkTemplateStrategy {
    fn id(&self) -> &'static str {
        self.plugin_id
    }

    fn target_notionals(
        &mut self,
        bars: &[Bar],
        market_budget: f64,
        current_notionals: &HashMap<String, f64>,
    ) -> HashMap<String, f64> {
        let mut targets: HashMap<String, f64> =
            bars.iter().map(|b| (b.symbol.clone(), 0.0)).collect();
        if bars.is_empty() {
            return targets;
        }

        let prepared = bars
            .iter()
            .filter(|bar| bar.close >= self.min_price)
            .map(|bar| {
                let mut shaped = bar.clone();
                if self.alpha_volume_scale.abs() > f64::EPSILON {
                    let boost = 1.0 + (bar.volume + 1.0).ln() * self.alpha_volume_scale;
                    shaped.volume = (bar.volume * boost.max(0.0)).max(0.0);
                }
                shaped
            })
            .collect::<Vec<_>>();
        if prepared.is_empty() {
            return targets;
        }

        let sdk_targets = self
            .inner
            .target_notionals(&prepared, market_budget, current_notionals);
        for (symbol, target) in sdk_targets {
            targets.insert(symbol, target);
        }
        targets
    }
}

impl MomentumTrendStrategy {
    pub fn new(cfg: StrategyConfig, industries: HashMap<(String, String), String>) -> Self {
        Self {
            cfg,
            history: HashMap::new(),
            industries,
        }
    }

    fn factor_snapshot(&mut self, bar: &Bar) -> Option<FactorSnapshot> {
        let key = (bar.market.clone(), bar.symbol.clone());
        let history = self.history.entry(key).or_default();
        let max_len = self
            .cfg
            .long_window
            .max(self.cfg.vol_window + 1)
            .max(self.cfg.regime_vol_window + 1)
            .max(self.cfg.mean_reversion_window + 1)
            .max(self.cfg.volume_window)
            .max(self.cfg.hrp_lookback + 1);

        history.closes.push_back(bar.close);
        history.volumes.push_back(bar.volume);
        while history.closes.len() > max_len {
            history.closes.pop_front();
        }
        while history.volumes.len() > max_len {
            history.volumes.pop_front();
        }

        if history.closes.len()
            < self
                .cfg
                .long_window
                .max(self.cfg.vol_window + 1)
                .max(self.cfg.mean_reversion_window + 1)
            || history.volumes.len() < self.cfg.volume_window
        {
            return None;
        }

        let len = history.closes.len();
        let contiguous = history.closes.make_contiguous();
        let short_slice = &contiguous[len - self.cfg.short_window..len];
        let long_slice = &contiguous[len - self.cfg.long_window..len];

        let short_ma = short_slice.iter().sum::<f64>() / short_slice.len() as f64;
        let long_ma = long_slice.iter().sum::<f64>() / long_slice.len() as f64;
        let trend_ok = short_ma > long_ma;

        let momentum = bar.close / long_slice[0] - 1.0;

        let mr_base_idx = len - 1 - self.cfg.mean_reversion_window;
        let mr_base = contiguous[mr_base_idx];
        let mean_reversion = -(bar.close / mr_base - 1.0);

        let vol_slice = &contiguous[len - (self.cfg.vol_window + 1)..];
        let mut returns = Vec::with_capacity(self.cfg.vol_window);
        for pair in vol_slice.windows(2) {
            let r = pair[1] / pair[0] - 1.0;
            returns.push(r);
        }
        let vol = stddev(&returns).max(1e-6);

        let volume_contiguous = history.volumes.make_contiguous();
        let volume_slice = &volume_contiguous[volume_contiguous.len() - self.cfg.volume_window..];
        let avg_volume = volume_slice.iter().sum::<f64>() / volume_slice.len() as f64;
        let volume_signal = if avg_volume > 0.0 {
            bar.volume / avg_volume - 1.0
        } else {
            0.0
        };
        let hrp_returns = trailing_returns(contiguous, self.cfg.hrp_lookback);

        Some(FactorSnapshot {
            market: bar.market.clone(),
            symbol: bar.symbol.clone(),
            momentum,
            mean_reversion,
            volatility: vol,
            volume_signal,
            returns: hrp_returns,
            trend_ok,
        })
    }

    pub fn target_notionals(
        &mut self,
        bars: &[Bar],
        market_budget: f64,
        current_notionals: &HashMap<String, f64>,
    ) -> HashMap<String, f64> {
        let mut snapshots = Vec::new();

        for bar in bars {
            if let Some(snapshot) = self.factor_snapshot(bar) {
                snapshots.push(snapshot);
            }
        }

        let mut targets: HashMap<String, f64> =
            bars.iter().map(|b| (b.symbol.clone(), 0.0)).collect();
        if snapshots.is_empty() {
            return targets;
        }

        let mut scored = if snapshots.len() == 1 {
            let s = &snapshots[0];
            let score = self.cfg.factor_momentum_weight * s.momentum
                + self.cfg.factor_mean_reversion_weight * s.mean_reversion
                + self.cfg.factor_low_vol_weight * (1.0 / s.volatility.max(1e-6))
                + self.cfg.factor_volume_weight * s.volume_signal;
            if score.is_finite() && score > 0.0 && s.momentum >= self.cfg.min_momentum && s.trend_ok
            {
                vec![(
                    s.market.clone(),
                    s.symbol.clone(),
                    score,
                    s.volatility,
                    s.returns.clone(),
                )]
            } else {
                Vec::new()
            }
        } else {
            let momentum_z = winsorized_zscores(
                &snapshots
                    .iter()
                    .map(|s| (s.symbol.clone(), s.momentum))
                    .collect::<Vec<_>>(),
                self.cfg.winsorize_pct,
            );
            let mean_reversion_z = winsorized_zscores(
                &snapshots
                    .iter()
                    .map(|s| (s.symbol.clone(), s.mean_reversion))
                    .collect::<Vec<_>>(),
                self.cfg.winsorize_pct,
            );
            let volatility_z = winsorized_zscores(
                &snapshots
                    .iter()
                    .map(|s| (s.symbol.clone(), s.volatility))
                    .collect::<Vec<_>>(),
                self.cfg.winsorize_pct,
            );
            let volume_z = winsorized_zscores(
                &snapshots
                    .iter()
                    .map(|s| (s.symbol.clone(), s.volume_signal))
                    .collect::<Vec<_>>(),
                self.cfg.winsorize_pct,
            );

            let mut layer1 = snapshots
                .iter()
                .map(|s| {
                    let score = self.cfg.factor_momentum_weight
                        * momentum_z.get(&s.symbol).copied().unwrap_or(0.0)
                        + self.cfg.factor_volume_weight
                            * volume_z.get(&s.symbol).copied().unwrap_or(0.0);
                    (s.market.clone(), s.symbol.clone(), score)
                })
                .collect::<Vec<_>>();
            layer1.sort_by(|a, b| b.2.total_cmp(&a.2));

            let keep = ((layer1.len() as f64 * self.cfg.layer1_select_ratio).ceil() as usize)
                .max(self.cfg.top_n.saturating_mul(2))
                .max(1)
                .min(layer1.len());
            let selected: HashMap<String, f64> = layer1
                .iter()
                .take(keep)
                .map(|(_, symbol, score)| (symbol.clone(), *score))
                .collect();

            let mut selected_rows = Vec::new();
            for s in &snapshots {
                let Some(layer1_score) = selected.get(&s.symbol).copied() else {
                    continue;
                };
                if s.momentum < self.cfg.min_momentum || !s.trend_ok {
                    continue;
                }
                let layer2_score = layer1_score
                    + self.cfg.factor_mean_reversion_weight
                        * mean_reversion_z.get(&s.symbol).copied().unwrap_or(0.0)
                    + self.cfg.factor_low_vol_weight
                        * -volatility_z.get(&s.symbol).copied().unwrap_or(0.0);
                selected_rows.push((
                    s.market.clone(),
                    s.symbol.clone(),
                    layer2_score,
                    s.volatility,
                    s.returns.clone(),
                ));
            }

            self.industry_neutralize_scores(selected_rows)
        };

        scored.sort_by(|a, b| b.2.total_cmp(&a.2));
        scored.truncate(self.cfg.top_n);
        if scored.is_empty() {
            return targets;
        }

        let risk_scale = self.market_regime_scale(bars);
        let scaled_budget = market_budget * risk_scale;
        let candidates = scored
            .into_iter()
            .map(|(_, symbol, score, volatility, returns)| SignalCandidate {
                symbol,
                alpha_score: score,
                volatility,
                returns,
            })
            .collect::<Vec<_>>();

        let optimized = optimize_targets(
            &candidates,
            current_notionals,
            scaled_budget,
            PortfolioOptimizerConfig {
                method: parse_portfolio_method(&self.cfg.portfolio_method),
                risk_parity_blend: self.cfg.risk_parity_blend,
                max_turnover_ratio: self.cfg.max_turnover_ratio,
            },
        );
        for (symbol, target) in optimized {
            targets.insert(symbol, target);
        }

        targets
    }

    fn market_regime_scale(&self, bars: &[Bar]) -> f64 {
        let mut symbol_vols = Vec::new();
        for bar in bars {
            let key = (bar.market.clone(), bar.symbol.clone());
            let Some(history) = self.history.get(&key) else {
                continue;
            };

            let needed = self.cfg.regime_vol_window + 1;
            if history.closes.len() < needed {
                continue;
            }

            let prices: Vec<f64> = history.closes.iter().copied().collect();
            let start = prices.len().saturating_sub(needed);
            let mut returns = Vec::with_capacity(self.cfg.regime_vol_window);
            for pair in prices[start..].windows(2) {
                returns.push(pair[1] / pair[0] - 1.0);
            }
            let vol = stddev(&returns);
            if vol.is_finite() && vol > 0.0 {
                symbol_vols.push(vol);
            }
        }

        if symbol_vols.is_empty() {
            return 1.0;
        }

        let market_vol = symbol_vols.iter().sum::<f64>() / symbol_vols.len() as f64;
        let raw_scale = self.cfg.regime_target_vol / market_vol.max(1e-6);
        raw_scale.clamp(self.cfg.regime_floor_scale, self.cfg.regime_ceiling_scale)
    }

    fn industry_neutralize_scores(
        &self,
        rows: Vec<(String, String, f64, f64, Vec<f64>)>,
    ) -> Vec<(String, String, f64, f64, Vec<f64>)> {
        let mut stats: HashMap<String, (usize, f64)> = HashMap::new();
        for (market, symbol, score, _, _) in &rows {
            let industry = self.industry_for(market, symbol);
            let entry = stats.entry(industry).or_insert((0, 0.0));
            entry.0 += 1;
            entry.1 += *score;
        }

        rows.into_iter()
            .map(|(market, symbol, score, vol, returns)| {
                let industry = self.industry_for(&market, &symbol);
                let neutral = if let Some((count, sum)) = stats.get(&industry) {
                    if *count >= 2 {
                        score - self.cfg.industry_neutral_strength * (sum / *count as f64)
                    } else {
                        score
                    }
                } else {
                    score
                };
                (market, symbol, neutral, vol, returns)
            })
            .filter(|(_, _, score, _, _)| score.is_finite() && *score > 0.0)
            .collect()
    }

    fn industry_for(&self, market: &str, symbol: &str) -> String {
        self.industries
            .get(&(market.to_string(), symbol.to_string()))
            .cloned()
            .unwrap_or_else(|| {
                let prefix: String = symbol.chars().take(2).collect();
                format!(
                    "{market}_{}",
                    if prefix.is_empty() { "GEN" } else { &prefix }
                )
            })
    }
}

impl StrategyPlugin for MomentumTrendStrategy {
    fn id(&self) -> &'static str {
        LAYERED_MULTI_FACTOR
    }

    fn target_notionals(
        &mut self,
        bars: &[Bar],
        market_budget: f64,
        current_notionals: &HashMap<String, f64>,
    ) -> HashMap<String, f64> {
        MomentumTrendStrategy::target_notionals(self, bars, market_budget, current_notionals)
    }
}

#[derive(Debug)]
pub struct MomentumOnlyStrategy {
    cfg: StrategyConfig,
    history: HashMap<(String, String), VecDeque<f64>>,
}

#[derive(Debug, Clone)]
struct MomentumOnlySnapshot {
    symbol: String,
    momentum: f64,
    volatility: f64,
    returns: Vec<f64>,
    trend_ok: bool,
}

impl MomentumOnlyStrategy {
    pub fn new(cfg: StrategyConfig) -> Self {
        Self {
            cfg,
            history: HashMap::new(),
        }
    }

    fn snapshot(&mut self, bar: &Bar) -> Option<MomentumOnlySnapshot> {
        let key = (bar.market.clone(), bar.symbol.clone());
        let history = self.history.entry(key).or_default();
        let max_len = self
            .cfg
            .long_window
            .max(self.cfg.vol_window + 1)
            .max(self.cfg.regime_vol_window + 1)
            .max(self.cfg.hrp_lookback + 1);

        history.push_back(bar.close);
        while history.len() > max_len {
            history.pop_front();
        }

        if history.len() < self.cfg.long_window.max(self.cfg.vol_window + 1) {
            return None;
        }

        let len = history.len();
        let prices = history.make_contiguous();
        let short_slice = &prices[len - self.cfg.short_window..len];
        let long_slice = &prices[len - self.cfg.long_window..len];
        let short_ma = short_slice.iter().sum::<f64>() / short_slice.len() as f64;
        let long_ma = long_slice.iter().sum::<f64>() / long_slice.len() as f64;
        let trend_ok = short_ma > long_ma;

        let momentum = bar.close / long_slice[0] - 1.0;
        let vol_slice = &prices[len - (self.cfg.vol_window + 1)..];
        let returns: Vec<f64> = vol_slice.windows(2).map(|w| w[1] / w[0] - 1.0).collect();
        let volatility = stddev(&returns).max(1e-6);
        let hrp_returns = trailing_returns(prices, self.cfg.hrp_lookback);

        Some(MomentumOnlySnapshot {
            symbol: bar.symbol.clone(),
            momentum,
            volatility,
            returns: hrp_returns,
            trend_ok,
        })
    }

    fn market_regime_scale(&self, bars: &[Bar]) -> f64 {
        let mut symbol_vols = Vec::new();
        for bar in bars {
            let key = (bar.market.clone(), bar.symbol.clone());
            let Some(history) = self.history.get(&key) else {
                continue;
            };

            let needed = self.cfg.regime_vol_window + 1;
            if history.len() < needed {
                continue;
            }

            let prices: Vec<f64> = history.iter().copied().collect();
            let start = prices.len().saturating_sub(needed);
            let mut returns = Vec::with_capacity(self.cfg.regime_vol_window);
            for pair in prices[start..].windows(2) {
                returns.push(pair[1] / pair[0] - 1.0);
            }
            let vol = stddev(&returns);
            if vol.is_finite() && vol > 0.0 {
                symbol_vols.push(vol);
            }
        }

        if symbol_vols.is_empty() {
            return 1.0;
        }

        let market_vol = symbol_vols.iter().sum::<f64>() / symbol_vols.len() as f64;
        let raw_scale = self.cfg.regime_target_vol / market_vol.max(1e-6);
        raw_scale.clamp(self.cfg.regime_floor_scale, self.cfg.regime_ceiling_scale)
    }
}

impl StrategyPlugin for MomentumOnlyStrategy {
    fn id(&self) -> &'static str {
        MOMENTUM_GUARD
    }

    fn target_notionals(
        &mut self,
        bars: &[Bar],
        market_budget: f64,
        current_notionals: &HashMap<String, f64>,
    ) -> HashMap<String, f64> {
        let mut targets: HashMap<String, f64> =
            bars.iter().map(|b| (b.symbol.clone(), 0.0)).collect();
        if bars.is_empty() {
            return targets;
        }

        let mut scored = Vec::new();
        for bar in bars {
            let Some(s) = self.snapshot(bar) else {
                continue;
            };
            if s.momentum < self.cfg.min_momentum || !s.trend_ok {
                continue;
            }
            let score = s.momentum / s.volatility.max(1e-6);
            if !score.is_finite() || score <= 0.0 {
                continue;
            }
            scored.push((s.symbol, score, s.volatility, s.returns));
        }
        if scored.is_empty() {
            return targets;
        }

        scored.sort_by(|a, b| b.1.total_cmp(&a.1));
        scored.truncate(self.cfg.top_n);

        let risk_scale = self.market_regime_scale(bars);
        let scaled_budget = market_budget * risk_scale;
        let candidates = scored
            .into_iter()
            .map(|(symbol, score, volatility, returns)| SignalCandidate {
                symbol,
                alpha_score: score,
                volatility,
                returns,
            })
            .collect::<Vec<_>>();

        let optimized = optimize_targets(
            &candidates,
            current_notionals,
            scaled_budget,
            PortfolioOptimizerConfig {
                method: parse_portfolio_method(&self.cfg.portfolio_method),
                risk_parity_blend: self.cfg.risk_parity_blend,
                max_turnover_ratio: self.cfg.max_turnover_ratio,
            },
        );

        for (symbol, target) in optimized {
            targets.insert(symbol, target);
        }

        targets
    }
}

fn parse_portfolio_method(method: &str) -> PortfolioMethod {
    match method {
        "hrp" => PortfolioMethod::Hrp,
        _ => PortfolioMethod::RiskParity,
    }
}

fn winsorized_zscores(values: &[(String, f64)], winsorize_pct: f64) -> HashMap<String, f64> {
    let winsorized = winsorize(values, winsorize_pct);
    zscores(&winsorized)
}

fn winsorize(values: &[(String, f64)], pct: f64) -> Vec<(String, f64)> {
    if values.len() < 4 || pct <= 0.0 {
        return values.to_vec();
    }
    let mut sorted: Vec<f64> = values.iter().map(|(_, v)| *v).collect();
    sorted.sort_by(|a, b| a.total_cmp(b));

    let low_idx = ((sorted.len() as f64 - 1.0) * pct).floor() as usize;
    let high_idx = ((sorted.len() as f64 - 1.0) * (1.0 - pct)).ceil() as usize;
    let low = sorted[low_idx.min(sorted.len() - 1)];
    let high = sorted[high_idx.min(sorted.len() - 1)];

    values
        .iter()
        .map(|(k, v)| (k.clone(), v.clamp(low, high)))
        .collect()
}

fn trailing_returns(closes: &[f64], lookback: usize) -> Vec<f64> {
    if closes.len() < 2 {
        return Vec::new();
    }
    let needed = lookback + 1;
    let start = closes.len().saturating_sub(needed);
    closes[start..]
        .windows(2)
        .map(|w| w[1] / w[0] - 1.0)
        .collect()
}

fn zscores(values: &[(String, f64)]) -> HashMap<String, f64> {
    if values.is_empty() {
        return HashMap::new();
    }
    let mean = values.iter().map(|(_, v)| *v).sum::<f64>() / values.len() as f64;
    let variance = values
        .iter()
        .map(|(_, v)| {
            let d = *v - mean;
            d * d
        })
        .sum::<f64>()
        / values.len() as f64;
    let stdev = variance.sqrt();
    if stdev < 1e-9 {
        return values
            .iter()
            .map(|(symbol, _)| (symbol.clone(), 0.0))
            .collect();
    }

    values
        .iter()
        .map(|(symbol, value)| (symbol.clone(), (*value - mean) / stdev))
        .collect()
}

fn stddev(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }

    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let var = values
        .iter()
        .map(|v| {
            let d = v - mean;
            d * d
        })
        .sum::<f64>()
        / values.len() as f64;
    var.sqrt()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::NaiveDate;

    use crate::{config::StrategyConfig, model::Bar};

    use super::{available_strategy_plugins, build_strategy, MomentumTrendStrategy};

    #[test]
    fn regime_scale_reduces_budget_when_volatility_spikes() {
        let mut strategy = MomentumTrendStrategy::new(
            StrategyConfig {
                strategy_plugin: "layered_multi_factor".to_string(),
                short_window: 2,
                long_window: 4,
                vol_window: 3,
                top_n: 1,
                min_momentum: -1.0,
                mean_reversion_window: 2,
                volume_window: 3,
                factor_momentum_weight: 0.4,
                factor_mean_reversion_weight: 0.2,
                factor_low_vol_weight: 0.3,
                factor_volume_weight: 0.1,
                risk_parity_blend: 0.6,
                max_turnover_ratio: 0.5,
                portfolio_method: "risk_parity".to_string(),
                hrp_lookback: 5,
                winsorize_pct: 0.05,
                layer1_select_ratio: 0.6,
                industry_neutral_strength: 1.0,
                regime_vol_window: 4,
                regime_target_vol: 0.01,
                regime_floor_scale: 0.25,
                regime_ceiling_scale: 1.5,
            },
            HashMap::new(),
        );

        let base_date = NaiveDate::from_ymd_opt(2025, 1, 1).expect("date");
        let calm = [100.0, 100.5, 101.0, 101.4, 101.8, 102.0];
        let panic = [102.0, 95.0, 105.0, 92.0, 110.0, 90.0];

        let mut calm_budget = 0.0;
        for (i, px) in calm.iter().enumerate() {
            let bar = Bar {
                date: base_date + chrono::Duration::days(i as i64),
                market: "US".to_string(),
                symbol: "AAPL".to_string(),
                close: *px,
                volume: 1_000_000.0,
            };
            let target = strategy.target_notionals(&[bar], 100_000.0, &HashMap::new());
            calm_budget = target.values().sum::<f64>();
        }

        let mut panic_budget = 0.0;
        for (i, px) in panic.iter().enumerate() {
            let bar = Bar {
                date: base_date + chrono::Duration::days((i + calm.len()) as i64),
                market: "US".to_string(),
                symbol: "AAPL".to_string(),
                close: *px,
                volume: 1_000_000.0,
            };
            let target = strategy.target_notionals(&[bar], 100_000.0, &HashMap::new());
            panic_budget = target.values().sum::<f64>();
        }

        assert!(panic_budget < calm_budget);
    }

    #[test]
    fn winsorize_caps_extreme_outlier() {
        let input = vec![
            ("A".to_string(), 1.0),
            ("B".to_string(), 1.1),
            ("C".to_string(), 1.2),
            ("D".to_string(), 20.0),
        ];
        let out = super::winsorize(&input, 0.4);
        let d = out
            .into_iter()
            .find(|(k, _)| k == "D")
            .map(|(_, v)| v)
            .unwrap_or(20.0);
        assert!(d < 20.0);
    }

    #[test]
    fn industry_neutralization_reduces_same_industry_bias() {
        let mut industries = HashMap::new();
        industries.insert(("US".to_string(), "AAPL".to_string()), "Tech".to_string());
        industries.insert(("US".to_string(), "MSFT".to_string()), "Tech".to_string());

        let strategy = MomentumTrendStrategy::new(
            StrategyConfig {
                strategy_plugin: "layered_multi_factor".to_string(),
                short_window: 2,
                long_window: 4,
                vol_window: 3,
                top_n: 2,
                min_momentum: -1.0,
                mean_reversion_window: 2,
                volume_window: 3,
                factor_momentum_weight: 0.4,
                factor_mean_reversion_weight: 0.2,
                factor_low_vol_weight: 0.3,
                factor_volume_weight: 0.1,
                risk_parity_blend: 0.6,
                max_turnover_ratio: 0.5,
                portfolio_method: "risk_parity".to_string(),
                hrp_lookback: 5,
                winsorize_pct: 0.05,
                layer1_select_ratio: 0.6,
                industry_neutral_strength: 1.0,
                regime_vol_window: 4,
                regime_target_vol: 0.01,
                regime_floor_scale: 0.25,
                regime_ceiling_scale: 1.5,
            },
            industries,
        );

        let rows = vec![
            (
                "US".to_string(),
                "AAPL".to_string(),
                2.0,
                0.02,
                vec![0.01, 0.02, 0.01],
            ),
            (
                "US".to_string(),
                "MSFT".to_string(),
                1.9,
                0.02,
                vec![0.01, 0.01, 0.02],
            ),
        ];

        let pre_sum = rows.iter().map(|(_, _, s, _, _)| *s).sum::<f64>();
        let neutralized = strategy.industry_neutralize_scores(rows);
        let post_sum = neutralized.iter().map(|(_, _, s, _, _)| *s).sum::<f64>();
        assert!(post_sum < pre_sum);
    }

    #[test]
    fn strategy_registry_builds_momentum_plugin() {
        let strategy = build_strategy(
            StrategyConfig {
                strategy_plugin: "momentum_guard".to_string(),
                short_window: 2,
                long_window: 4,
                vol_window: 3,
                top_n: 1,
                min_momentum: -1.0,
                mean_reversion_window: 2,
                volume_window: 3,
                factor_momentum_weight: 0.4,
                factor_mean_reversion_weight: 0.2,
                factor_low_vol_weight: 0.3,
                factor_volume_weight: 0.1,
                risk_parity_blend: 0.6,
                max_turnover_ratio: 0.5,
                portfolio_method: "risk_parity".to_string(),
                hrp_lookback: 5,
                winsorize_pct: 0.05,
                layer1_select_ratio: 0.6,
                industry_neutral_strength: 1.0,
                regime_vol_window: 4,
                regime_target_vol: 0.01,
                regime_floor_scale: 0.25,
                regime_ceiling_scale: 1.5,
            },
            HashMap::new(),
        );

        assert_eq!(strategy.id(), "momentum_guard");
        assert!(available_strategy_plugins().contains(&"layered_multi_factor".to_string()));
        assert!(available_strategy_plugins().contains(&"momentum_guard".to_string()));
    }
}
