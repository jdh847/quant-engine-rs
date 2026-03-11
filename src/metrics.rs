use crate::model::EquityPoint;

#[derive(Debug, Clone, Default)]
pub struct PerformanceMetrics {
    pub cagr: f64,
    pub sharpe: f64,
    pub sortino: f64,
    pub calmar: f64,
    pub daily_win_rate: f64,
    pub profit_factor: f64,
}

pub fn compute_performance_metrics(
    equity: &[EquityPoint],
    max_drawdown: f64,
) -> PerformanceMetrics {
    if equity.len() < 2 {
        return PerformanceMetrics::default();
    }

    let mut returns = Vec::with_capacity(equity.len() - 1);
    for w in equity.windows(2) {
        let prev = w[0].equity;
        let next = w[1].equity;
        if prev.abs() < 1e-9 {
            returns.push(0.0);
        } else {
            returns.push(next / prev - 1.0);
        }
    }

    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let std = stddev(&returns);
    let down: Vec<f64> = returns.iter().copied().filter(|r| *r < 0.0).collect();
    let downside_std = stddev(&down);

    let start = equity.first().map(|x| x.equity).unwrap_or(0.0);
    let end = equity.last().map(|x| x.equity).unwrap_or(0.0);
    let periods = returns.len() as f64;

    let cagr = if start > 0.0 && end > 0.0 && periods > 0.0 {
        (end / start).powf(252.0 / periods) - 1.0
    } else {
        0.0
    };

    let sharpe = if std > 1e-12 {
        (mean / std) * 252.0_f64.sqrt()
    } else {
        0.0
    };

    let sortino = if downside_std > 1e-12 {
        (mean / downside_std) * 252.0_f64.sqrt()
    } else {
        0.0
    };

    let calmar = if max_drawdown > 1e-12 {
        cagr / max_drawdown
    } else {
        0.0
    };

    let wins = returns.iter().filter(|r| **r > 0.0).count() as f64;
    let daily_win_rate = wins / returns.len() as f64;

    let gross_profit: f64 = returns.iter().copied().filter(|r| *r > 0.0).sum();
    let gross_loss: f64 = returns
        .iter()
        .copied()
        .filter(|r| *r < 0.0)
        .map(f64::abs)
        .sum();
    let profit_factor = if gross_loss > 1e-12 {
        gross_profit / gross_loss
    } else if gross_profit > 0.0 {
        999.0
    } else {
        0.0
    };

    PerformanceMetrics {
        cagr,
        sharpe,
        sortino,
        calmar,
        daily_win_rate,
        profit_factor,
    }
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
