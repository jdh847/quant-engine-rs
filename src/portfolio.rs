use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct SignalCandidate {
    pub symbol: String,
    pub alpha_score: f64,
    pub volatility: f64,
    pub returns: Vec<f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortfolioMethod {
    RiskParity,
    Hrp,
}

#[derive(Debug, Clone, Copy)]
pub struct PortfolioOptimizerConfig {
    pub method: PortfolioMethod,
    pub risk_parity_blend: f64,
    pub max_turnover_ratio: f64,
}

pub fn optimize_targets(
    candidates: &[SignalCandidate],
    current_notionals: &HashMap<String, f64>,
    market_budget: f64,
    cfg: PortfolioOptimizerConfig,
) -> HashMap<String, f64> {
    if candidates.is_empty() || market_budget <= 0.0 {
        return HashMap::new();
    }

    let alpha_weights = alpha_weights(candidates);
    let risk_weights = match cfg.method {
        PortfolioMethod::RiskParity => risk_parity_weights(candidates),
        PortfolioMethod::Hrp => {
            hrp_weights(candidates).unwrap_or_else(|| risk_parity_weights(candidates))
        }
    };

    let mut target_notionals = HashMap::new();
    for c in candidates {
        let alpha = alpha_weights.get(&c.symbol).copied().unwrap_or(0.0);
        let risk = risk_weights.get(&c.symbol).copied().unwrap_or(0.0);
        let w = cfg.risk_parity_blend * risk + (1.0 - cfg.risk_parity_blend) * alpha;
        target_notionals.insert(c.symbol.clone(), (w * market_budget).max(0.0));
    }

    apply_turnover_cap(
        &target_notionals,
        current_notionals,
        market_budget,
        cfg.max_turnover_ratio,
    )
}

fn alpha_weights(candidates: &[SignalCandidate]) -> HashMap<String, f64> {
    let mut positive_scores: Vec<(String, f64)> = candidates
        .iter()
        .map(|c| (c.symbol.clone(), c.alpha_score.max(0.0)))
        .collect();

    let total: f64 = positive_scores.iter().map(|(_, s)| *s).sum();
    if total <= 0.0 {
        let equal = 1.0 / candidates.len() as f64;
        return candidates
            .iter()
            .map(|c| (c.symbol.clone(), equal))
            .collect::<HashMap<_, _>>();
    }

    positive_scores
        .drain(..)
        .map(|(symbol, score)| (symbol, score / total))
        .collect()
}

fn risk_parity_weights(candidates: &[SignalCandidate]) -> HashMap<String, f64> {
    let inv_vols: Vec<(String, f64)> = candidates
        .iter()
        .map(|c| (c.symbol.clone(), 1.0 / c.volatility.max(1e-6)))
        .collect();
    let total: f64 = inv_vols.iter().map(|(_, iv)| *iv).sum();
    if total <= 0.0 {
        let equal = 1.0 / candidates.len() as f64;
        return candidates
            .iter()
            .map(|c| (c.symbol.clone(), equal))
            .collect::<HashMap<_, _>>();
    }

    inv_vols
        .into_iter()
        .map(|(symbol, inv_vol)| (symbol, inv_vol / total))
        .collect()
}

fn hrp_weights(candidates: &[SignalCandidate]) -> Option<HashMap<String, f64>> {
    if candidates.len() < 2 {
        return None;
    }

    let min_len = candidates
        .iter()
        .map(|c| c.returns.len())
        .min()
        .unwrap_or(0);
    if min_len < 4 {
        return None;
    }

    let n = candidates.len();
    let mut cov = vec![vec![0.0; n]; n];
    let mut corr = vec![vec![0.0; n]; n];

    for i in 0..n {
        for j in i..n {
            let a = &candidates[i].returns[candidates[i].returns.len() - min_len..];
            let b = &candidates[j].returns[candidates[j].returns.len() - min_len..];
            let (c_ij, r_ij) = cov_and_corr(a, b);
            cov[i][j] = c_ij;
            cov[j][i] = c_ij;
            corr[i][j] = r_ij;
            corr[j][i] = r_ij;
        }
    }

    let order = hierarchical_order(&corr)?;
    let mut weights = vec![1.0f64; n];
    recursive_bisection(&order, &cov, &mut weights);

    let sum: f64 = weights.iter().sum();
    if sum <= 0.0 {
        return None;
    }

    let mut out = HashMap::new();
    for (idx, w) in weights.iter().enumerate() {
        out.insert(candidates[idx].symbol.clone(), *w / sum);
    }
    Some(out)
}

fn cov_and_corr(a: &[f64], b: &[f64]) -> (f64, f64) {
    let n = a.len().min(b.len());
    if n < 2 {
        return (0.0, 0.0);
    }
    let mean_a = a.iter().take(n).sum::<f64>() / n as f64;
    let mean_b = b.iter().take(n).sum::<f64>() / n as f64;

    let mut cov = 0.0;
    let mut var_a = 0.0;
    let mut var_b = 0.0;
    for k in 0..n {
        let da = a[k] - mean_a;
        let db = b[k] - mean_b;
        cov += da * db;
        var_a += da * da;
        var_b += db * db;
    }
    let denom = (n - 1) as f64;
    cov /= denom;
    var_a /= denom;
    var_b /= denom;

    let corr = if var_a <= 0.0 || var_b <= 0.0 {
        0.0
    } else {
        cov / (var_a.sqrt() * var_b.sqrt())
    };
    (cov, corr.clamp(-1.0, 1.0))
}

fn hierarchical_order(corr: &[Vec<f64>]) -> Option<Vec<usize>> {
    let n = corr.len();
    if n == 0 {
        return None;
    }
    let mut clusters: Vec<Vec<usize>> = (0..n).map(|i| vec![i]).collect();
    while clusters.len() > 1 {
        let mut best_i = 0usize;
        let mut best_j = 1usize;
        let mut best_d = f64::INFINITY;

        for i in 0..clusters.len() {
            for j in i + 1..clusters.len() {
                let d = avg_cluster_distance(&clusters[i], &clusters[j], corr);
                if d < best_d {
                    best_d = d;
                    best_i = i;
                    best_j = j;
                }
            }
        }

        let mut merged = clusters[best_i].clone();
        merged.extend(clusters[best_j].clone());
        if best_i > best_j {
            clusters.swap_remove(best_i);
            clusters.swap_remove(best_j);
        } else {
            clusters.swap_remove(best_j);
            clusters.swap_remove(best_i);
        }
        clusters.push(merged);
    }
    clusters.pop()
}

fn avg_cluster_distance(left: &[usize], right: &[usize], corr: &[Vec<f64>]) -> f64 {
    let mut total = 0.0;
    let mut count = 0usize;
    for i in left {
        for j in right {
            let c = corr[*i][*j];
            let d = (0.5 * (1.0 - c)).max(0.0).sqrt();
            total += d;
            count += 1;
        }
    }
    if count == 0 {
        1.0
    } else {
        total / count as f64
    }
}

fn recursive_bisection(order: &[usize], cov: &[Vec<f64>], weights: &mut [f64]) {
    if order.len() <= 1 {
        return;
    }
    let split = order.len() / 2;
    let left = &order[..split];
    let right = &order[split..];
    let var_left = cluster_variance(left, cov).max(1e-12);
    let var_right = cluster_variance(right, cov).max(1e-12);

    let alloc_left = var_right / (var_left + var_right);
    let alloc_right = 1.0 - alloc_left;

    for idx in left {
        weights[*idx] *= alloc_left;
    }
    for idx in right {
        weights[*idx] *= alloc_right;
    }

    recursive_bisection(left, cov, weights);
    recursive_bisection(right, cov, weights);
}

fn cluster_variance(cluster: &[usize], cov: &[Vec<f64>]) -> f64 {
    if cluster.is_empty() {
        return 0.0;
    }
    if cluster.len() == 1 {
        return cov[cluster[0]][cluster[0]].max(1e-12);
    }

    let inv_diag: Vec<f64> = cluster
        .iter()
        .map(|i| 1.0 / cov[*i][*i].max(1e-12))
        .collect();
    let sum_inv: f64 = inv_diag.iter().sum();
    if sum_inv <= 0.0 {
        return 0.0;
    }
    let w: Vec<f64> = inv_diag.iter().map(|x| x / sum_inv).collect();

    let mut var = 0.0;
    for (a_pos, a) in cluster.iter().enumerate() {
        for (b_pos, b) in cluster.iter().enumerate() {
            var += w[a_pos] * w[b_pos] * cov[*a][*b];
        }
    }
    var.max(1e-12)
}

fn apply_turnover_cap(
    target_notionals: &HashMap<String, f64>,
    current_notionals: &HashMap<String, f64>,
    market_budget: f64,
    max_turnover_ratio: f64,
) -> HashMap<String, f64> {
    if market_budget <= 0.0 {
        return target_notionals.clone();
    }

    let turnover_cap = market_budget * max_turnover_ratio.max(0.0);
    let turnover = target_notionals
        .iter()
        .map(|(symbol, target)| {
            let current = current_notionals.get(symbol).copied().unwrap_or(0.0);
            (target - current).abs()
        })
        .sum::<f64>();

    if turnover <= turnover_cap || turnover <= 0.0 {
        return target_notionals.clone();
    }

    let scale = turnover_cap / turnover;
    target_notionals
        .iter()
        .map(|(symbol, target)| {
            let current = current_notionals.get(symbol).copied().unwrap_or(0.0);
            let adjusted = current + (target - current) * scale;
            (symbol.clone(), adjusted.max(0.0))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{optimize_targets, PortfolioMethod, PortfolioOptimizerConfig, SignalCandidate};

    #[test]
    fn risk_parity_assigns_less_weight_to_high_vol() {
        let candidates = vec![
            SignalCandidate {
                symbol: "LOW".to_string(),
                alpha_score: 1.0,
                volatility: 0.01,
                returns: vec![0.01, 0.02, -0.01, 0.005, 0.003],
            },
            SignalCandidate {
                symbol: "HIGH".to_string(),
                alpha_score: 1.0,
                volatility: 0.05,
                returns: vec![0.03, -0.02, 0.04, -0.03, 0.02],
            },
        ];

        let out = optimize_targets(
            &candidates,
            &HashMap::new(),
            100_000.0,
            PortfolioOptimizerConfig {
                method: PortfolioMethod::RiskParity,
                risk_parity_blend: 1.0,
                max_turnover_ratio: 1.0,
            },
        );

        let low = out.get("LOW").copied().unwrap_or(0.0);
        let high = out.get("HIGH").copied().unwrap_or(0.0);
        assert!(low > high);
    }

    #[test]
    fn turnover_cap_is_enforced() {
        let candidates = vec![
            SignalCandidate {
                symbol: "AAA".to_string(),
                alpha_score: 2.0,
                volatility: 0.02,
                returns: vec![0.01, 0.02, -0.01, 0.005, 0.003],
            },
            SignalCandidate {
                symbol: "BBB".to_string(),
                alpha_score: 1.0,
                volatility: 0.02,
                returns: vec![0.02, -0.01, 0.01, 0.0, 0.004],
            },
        ];

        let mut current = HashMap::new();
        current.insert("AAA".to_string(), 10_000.0);
        current.insert("BBB".to_string(), 90_000.0);

        let out = optimize_targets(
            &candidates,
            &current,
            100_000.0,
            PortfolioOptimizerConfig {
                method: PortfolioMethod::RiskParity,
                risk_parity_blend: 0.0,
                max_turnover_ratio: 0.10,
            },
        );

        let turnover = out
            .iter()
            .map(|(symbol, target)| {
                let c = current.get(symbol).copied().unwrap_or(0.0);
                (target - c).abs()
            })
            .sum::<f64>();

        assert!(turnover <= 10_000.0 + 1e-6);
    }

    #[test]
    fn hrp_branch_returns_valid_weights() {
        let candidates = vec![
            SignalCandidate {
                symbol: "AAA".to_string(),
                alpha_score: 1.0,
                volatility: 0.02,
                returns: vec![0.01, 0.02, -0.01, 0.005, 0.003, 0.004],
            },
            SignalCandidate {
                symbol: "BBB".to_string(),
                alpha_score: 1.0,
                volatility: 0.03,
                returns: vec![0.012, 0.018, -0.008, 0.004, 0.002, 0.006],
            },
            SignalCandidate {
                symbol: "CCC".to_string(),
                alpha_score: 1.0,
                volatility: 0.025,
                returns: vec![-0.005, 0.009, 0.002, -0.001, 0.003, 0.004],
            },
        ];

        let out = optimize_targets(
            &candidates,
            &HashMap::new(),
            120_000.0,
            PortfolioOptimizerConfig {
                method: PortfolioMethod::Hrp,
                risk_parity_blend: 1.0,
                max_turnover_ratio: 1.0,
            },
        );

        let sum = out.values().sum::<f64>();
        assert!((sum - 120_000.0).abs() < 1e-3);
    }
}
