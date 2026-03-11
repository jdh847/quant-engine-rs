use std::collections::{HashMap, VecDeque};

use private_quant_bot::{
    model::Bar,
    portfolio::{optimize_targets, PortfolioMethod, PortfolioOptimizerConfig, SignalCandidate},
    strategy::StrategyPlugin,
};

#[derive(Debug, Clone)]
pub struct MyAlpha {
    top_n: usize,
    min_price: f64,
    min_momentum: f64,
    short_window: usize,
    long_window: usize,
    mean_reversion_window: usize,
    vol_window: usize,
    volume_window: usize,
    layer1_select_ratio: f64,
    risk_parity_blend: f64,
    max_turnover_ratio: f64,
    history: HashMap<String, SymbolHistory>,
}

#[derive(Debug, Clone, Default)]
struct SymbolHistory {
    closes: VecDeque<f64>,
    volumes: VecDeque<f64>,
}

#[derive(Debug, Clone)]
struct FactorSnapshot {
    symbol: String,
    momentum: f64,
    mean_reversion: f64,
    volatility: f64,
    volume_signal: f64,
    returns: Vec<f64>,
    trend_ok: bool,
}

impl MyAlpha {
    pub fn new(top_n: usize, min_price: f64) -> Self {
        Self {
            top_n: top_n.max(1),
            min_price,
            min_momentum: -0.01,
            short_window: 5,
            long_window: 20,
            mean_reversion_window: 3,
            vol_window: 10,
            volume_window: 10,
            layer1_select_ratio: 0.6,
            risk_parity_blend: 0.7,
            max_turnover_ratio: 0.35,
            history: HashMap::new(),
        }
    }

    fn factor_snapshot(&mut self, bar: &Bar) -> Option<FactorSnapshot> {
        let history = self.history.entry(bar.symbol.clone()).or_default();
        let max_len = self
            .long_window
            .max(self.vol_window + 1)
            .max(self.mean_reversion_window + 1)
            .max(self.volume_window)
            .max(24);

        history.closes.push_back(bar.close);
        history.volumes.push_back(bar.volume);
        while history.closes.len() > max_len {
            history.closes.pop_front();
        }
        while history.volumes.len() > max_len {
            history.volumes.pop_front();
        }

        if history.closes.len() < self.long_window.max(self.vol_window + 1)
            || history.closes.len() < self.mean_reversion_window + 1
            || history.volumes.len() < self.volume_window
        {
            return None;
        }

        let len = history.closes.len();
        let closes = history.closes.make_contiguous();
        let short_slice = &closes[len - self.short_window..len];
        let long_slice = &closes[len - self.long_window..len];
        let short_ma = short_slice.iter().sum::<f64>() / short_slice.len() as f64;
        let long_ma = long_slice.iter().sum::<f64>() / long_slice.len() as f64;
        let trend_ok = short_ma > long_ma;

        let momentum = bar.close / long_slice[0] - 1.0;
        let mr_base_idx = len - 1 - self.mean_reversion_window;
        let mr_base = closes[mr_base_idx];
        let mean_reversion = -(bar.close / mr_base - 1.0);

        let vol_slice = &closes[len - (self.vol_window + 1)..];
        let returns: Vec<f64> = vol_slice.windows(2).map(|w| w[1] / w[0] - 1.0).collect();
        let volatility = stddev(&returns).max(1e-6);
        let trailing = trailing_returns(closes, 12);

        let volumes = history.volumes.make_contiguous();
        let volume_slice = &volumes[volumes.len() - self.volume_window..];
        let avg_volume = volume_slice.iter().sum::<f64>() / volume_slice.len() as f64;
        let volume_signal = if avg_volume > 0.0 {
            bar.volume / avg_volume - 1.0
        } else {
            0.0
        };

        Some(FactorSnapshot {
            symbol: bar.symbol.clone(),
            momentum,
            mean_reversion,
            volatility,
            volume_signal,
            returns: trailing,
            trend_ok,
        })
    }
}

impl StrategyPlugin for MyAlpha {
    fn id(&self) -> &'static str {
        "my_alpha"
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

        let mut snapshots = Vec::new();
        for bar in bars {
            if bar.close < self.min_price {
                continue;
            }
            if let Some(snapshot) = self.factor_snapshot(bar) {
                snapshots.push(snapshot);
            }
        }
        if snapshots.is_empty() {
            return targets;
        }

        let momentum_z = winsorized_zscores(
            &snapshots
                .iter()
                .map(|s| (s.symbol.clone(), s.momentum))
                .collect::<Vec<_>>(),
            0.05,
        );
        let mean_reversion_z = winsorized_zscores(
            &snapshots
                .iter()
                .map(|s| (s.symbol.clone(), s.mean_reversion))
                .collect::<Vec<_>>(),
            0.05,
        );
        let volatility_z = winsorized_zscores(
            &snapshots
                .iter()
                .map(|s| (s.symbol.clone(), s.volatility))
                .collect::<Vec<_>>(),
            0.05,
        );
        let volume_z = winsorized_zscores(
            &snapshots
                .iter()
                .map(|s| (s.symbol.clone(), s.volume_signal))
                .collect::<Vec<_>>(),
            0.05,
        );

        let mut layer1 = snapshots
            .iter()
            .map(|s| {
                let score = 0.70 * momentum_z.get(&s.symbol).copied().unwrap_or(0.0)
                    + 0.30 * volume_z.get(&s.symbol).copied().unwrap_or(0.0);
                (s.symbol.clone(), score)
            })
            .collect::<Vec<_>>();
        layer1.sort_by(|a, b| b.1.total_cmp(&a.1));
        let keep = ((layer1.len() as f64 * self.layer1_select_ratio).ceil() as usize)
            .max(self.top_n.saturating_mul(2))
            .max(1)
            .min(layer1.len());
        let selected = layer1
            .iter()
            .take(keep)
            .map(|(symbol, score)| (symbol.clone(), *score))
            .collect::<HashMap<_, _>>();

        let mut candidates = snapshots
            .into_iter()
            .filter_map(|s| {
                let l1 = selected.get(&s.symbol).copied()?;
                if s.momentum < self.min_momentum || !s.trend_ok {
                    return None;
                }
                let layer2 = l1
                    + 0.20 * mean_reversion_z.get(&s.symbol).copied().unwrap_or(0.0)
                    - 0.20 * volatility_z.get(&s.symbol).copied().unwrap_or(0.0);
                if !layer2.is_finite() || layer2 <= 0.0 {
                    return None;
                }
                Some(SignalCandidate {
                    symbol: s.symbol,
                    alpha_score: layer2,
                    volatility: s.volatility,
                    returns: s.returns,
                })
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|a, b| b.alpha_score.total_cmp(&a.alpha_score));
        candidates.truncate(self.top_n);
        if candidates.is_empty() {
            return targets;
        }

        let optimized = optimize_targets(
            &candidates,
            current_notionals,
            market_budget,
            PortfolioOptimizerConfig {
                method: PortfolioMethod::RiskParity,
                risk_parity_blend: self.risk_parity_blend,
                max_turnover_ratio: self.max_turnover_ratio,
            },
        );

        for (symbol, target) in optimized {
            targets.insert(symbol, target);
        }
        targets
    }
}

pub fn build_plugin(top_n: usize) -> Box<dyn StrategyPlugin> {
    Box::new(MyAlpha::new(top_n, 1.0))
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

fn zscores(values: &[(String, f64)]) -> HashMap<String, f64> {
    if values.is_empty() {
        return HashMap::new();
    }
    let mean = values.iter().map(|(_, v)| *v).sum::<f64>() / values.len() as f64;
    let var = values
        .iter()
        .map(|(_, v)| {
            let d = *v - mean;
            d * d
        })
        .sum::<f64>()
        / values.len() as f64;
    let std = var.sqrt().max(1e-9);
    values
        .iter()
        .map(|(k, v)| (k.clone(), (*v - mean) / std))
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

fn stddev(values: &[f64]) -> f64 {
    if values.len() < 2 {
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
