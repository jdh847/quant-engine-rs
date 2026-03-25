use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct PaperHintsDaemonInput {
    pub last_cycle: usize,
    pub last_end_equity: f64,
    pub max_drawdown_observed: f64,
    pub alerts: usize,
}

#[derive(Debug, Clone, Default)]
pub struct PaperHintsCompareInput {
    pub winner: String,
    pub research_changes: usize,
    pub top_research_keys: Vec<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct PaperHintMarketReport {
    pub market: String,
    pub stance: String,
    pub headline: String,
    pub bullets: Vec<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct PaperHintsReport {
    pub stance: String,
    pub headline: String,
    pub watch_markets: Vec<String>,
    pub bullets: Vec<String>,
    pub market_hints: Vec<PaperHintMarketReport>,
}

struct MarketHintsContext<'a> {
    top_market: &'a str,
    top_bucket: &'a str,
    top_factor: &'a str,
    transition_market: &'a str,
    transition_from: &'a str,
    transition_to: &'a str,
    transition_date: &'a str,
    rotation_factor: &'a str,
    rotation_date: &'a str,
    rotation_switches: usize,
    daemon: Option<&'a PaperHintsDaemonInput>,
    compare: Option<&'a PaperHintsCompareInput>,
}

pub fn build_paper_hints(
    research_summary: &HashMap<String, String>,
    daemon: Option<&PaperHintsDaemonInput>,
    compare: Option<&PaperHintsCompareInput>,
) -> PaperHintsReport {
    let top_market = value_or_dash(research_summary.get("top_regime_leader_market"));
    let top_bucket = value_or_dash(research_summary.get("top_regime_leader_bucket"));
    let top_factor = value_or_dash(research_summary.get("top_regime_leader_factor"));
    let rotation_factor = value_or_dash(research_summary.get("current_rotation_leader_factor"));
    let rotation_date = value_or_dash(research_summary.get("current_rotation_date"));
    let rotation_switches = research_summary
        .get("rotation_switches")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0);
    let transition_market = value_or_dash(research_summary.get("latest_regime_transition_market"));
    let transition_date = value_or_dash(research_summary.get("latest_regime_transition_date"));
    let transition_from =
        value_or_dash(research_summary.get("latest_regime_transition_from_bucket"));
    let transition_to = value_or_dash(research_summary.get("latest_regime_transition_to_bucket"));

    let mut watch_markets = Vec::<String>::new();
    for market in [top_market.as_str(), transition_market.as_str()] {
        if market != "-" && !watch_markets.iter().any(|item| item == market) {
            watch_markets.push(market.to_string());
        }
    }

    let compare_changes = compare.map(|item| item.research_changes).unwrap_or(0);
    let daemon_alerts = daemon.map(|item| item.alerts).unwrap_or(0);
    let stance = if daemon_alerts > 0 || compare_changes >= 8 {
        "RISK"
    } else if transition_market != "-" || compare_changes > 0 || rotation_switches > 1 {
        "WATCH"
    } else {
        "HEALTHY"
    }
    .to_string();

    let headline = if transition_market != "-" && rotation_factor != "-" {
        format!(
            "paper-only: {transition_market} shifted {transition_from} -> {transition_to}; watch {rotation_factor}"
        )
    } else if top_market != "-" && top_factor != "-" {
        format!("paper-only: {top_market} leader is {top_factor} in {top_bucket}")
    } else if let Some(daemon) = daemon {
        format!(
            "paper-only: daemon cycle={} alerts={} last_end_equity={:.2}",
            daemon.last_cycle, daemon.alerts, daemon.last_end_equity
        )
    } else {
        "paper-only: waiting for research signals".to_string()
    };

    let mut bullets = Vec::new();
    if top_market != "-" {
        bullets.push(format!(
            "leader: {top_market} / {top_bucket} / {top_factor}"
        ));
    }
    if rotation_factor != "-" {
        bullets.push(format!(
            "rotation: {rotation_factor} on {rotation_date} (switches={rotation_switches})"
        ));
    }
    if transition_market != "-" {
        bullets.push(format!(
            "transition: {transition_market} {transition_from} -> {transition_to} on {transition_date}"
        ));
    }
    if let Some(compare) = compare {
        let top_keys = if compare.top_research_keys.is_empty() {
            "-".to_string()
        } else {
            compare.top_research_keys.join(",")
        };
        bullets.push(format!(
            "compare: winner={} research_changes={} top={}",
            if compare.winner.is_empty() {
                "-"
            } else {
                compare.winner.as_str()
            },
            compare.research_changes,
            top_keys
        ));
    }
    if let Some(daemon) = daemon {
        bullets.push(format!(
            "daemon: alerts={} max_drawdown_observed={:.2}%",
            daemon.alerts,
            daemon.max_drawdown_observed * 100.0
        ));
    }
    if bullets.is_empty() {
        bullets.push("paper-only: no actionable signals yet".to_string());
    }

    let market_hints = build_market_hints(&MarketHintsContext {
        top_market: top_market.as_str(),
        top_bucket: top_bucket.as_str(),
        top_factor: top_factor.as_str(),
        transition_market: transition_market.as_str(),
        transition_from: transition_from.as_str(),
        transition_to: transition_to.as_str(),
        transition_date: transition_date.as_str(),
        rotation_factor: rotation_factor.as_str(),
        rotation_date: rotation_date.as_str(),
        rotation_switches,
        daemon,
        compare,
    });

    PaperHintsReport {
        stance,
        headline,
        watch_markets,
        bullets,
        market_hints,
    }
}

pub fn render_paper_hints_summary(report: &PaperHintsReport) -> String {
    let markets = if report.watch_markets.is_empty() {
        "-".to_string()
    } else {
        report.watch_markets.join("|")
    };
    let mut out = format!(
        "stance={}\nheadline={}\nwatch_markets={}\nbullets_count={}\n",
        report.stance,
        report.headline,
        markets,
        report.bullets.len()
    );
    for (idx, bullet) in report.bullets.iter().enumerate() {
        out.push_str(&format!("bullet_{}={}\n", idx + 1, bullet));
    }
    out.push_str(&format!(
        "market_hints_count={}\n",
        report.market_hints.len()
    ));
    for (idx, market_hint) in report.market_hints.iter().enumerate() {
        out.push_str(&format!(
            "market_hint_{}_market={}\nmarket_hint_{}_stance={}\nmarket_hint_{}_headline={}\n",
            idx + 1,
            market_hint.market,
            idx + 1,
            market_hint.stance,
            idx + 1,
            market_hint.headline
        ));
        for (bullet_idx, bullet) in market_hint.bullets.iter().enumerate() {
            out.push_str(&format!(
                "market_hint_{}_bullet_{}={}\n",
                idx + 1,
                bullet_idx + 1,
                bullet
            ));
        }
    }
    out
}

fn build_market_hints(ctx: &MarketHintsContext<'_>) -> Vec<PaperHintMarketReport> {
    let mut hints = Vec::new();
    for market in [ctx.top_market, ctx.transition_market] {
        if market == "-"
            || hints
                .iter()
                .any(|hint: &PaperHintMarketReport| hint.market == market)
        {
            continue;
        }
        let is_transition_market = market == ctx.transition_market && ctx.transition_market != "-";
        let is_leader_market = market == ctx.top_market && ctx.top_market != "-";
        let compare_changes = ctx.compare.map(|item| item.research_changes).unwrap_or(0);
        let daemon_alerts = ctx.daemon.map(|item| item.alerts).unwrap_or(0);
        let stance = if daemon_alerts > 0 {
            "RISK"
        } else if is_transition_market || compare_changes > 0 || ctx.rotation_switches > 1 {
            "WATCH"
        } else {
            "HEALTHY"
        };
        let headline = if is_transition_market {
            format!(
                "{market}: regime shifted {} -> {} on {}",
                ctx.transition_from, ctx.transition_to, ctx.transition_date
            )
        } else if is_leader_market {
            format!("{market}: leader {} in {}", ctx.top_factor, ctx.top_bucket)
        } else {
            format!("{market}: paper-only monitoring")
        };
        let mut bullets = Vec::new();
        if is_leader_market {
            bullets.push(format!(
                "leader factor={} bucket={}",
                ctx.top_factor, ctx.top_bucket
            ));
        }
        if is_transition_market {
            bullets.push(format!(
                "latest transition={} -> {} on {}",
                ctx.transition_from, ctx.transition_to, ctx.transition_date
            ));
        }
        if ctx.rotation_factor != "-" {
            bullets.push(format!(
                "rotation leader={} on {} switches={}",
                ctx.rotation_factor, ctx.rotation_date, ctx.rotation_switches
            ));
        }
        if let Some(compare) = ctx.compare {
            let winner = if compare.winner.is_empty() {
                "-"
            } else {
                compare.winner.as_str()
            };
            bullets.push(format!(
                "compare winner={winner} research_changes={}",
                compare.research_changes
            ));
        }
        if let Some(daemon) = ctx.daemon {
            bullets.push(format!(
                "daemon alerts={} max_drawdown={:.2}%",
                daemon.alerts,
                daemon.max_drawdown_observed * 100.0
            ));
        }
        hints.push(PaperHintMarketReport {
            market: market.to_string(),
            stance: stance.to_string(),
            headline,
            bullets,
        });
    }
    hints
}

fn value_or_dash(value: Option<&String>) -> String {
    value
        .map(|item| {
            let trimmed = item.trim();
            if trimmed.is_empty() {
                "-".to_string()
            } else {
                trimmed.to_string()
            }
        })
        .unwrap_or_else(|| "-".to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        build_paper_hints, render_paper_hints_summary, PaperHintsCompareInput,
        PaperHintsDaemonInput,
    };

    #[test]
    fn paper_hints_prioritize_transition_and_compare() {
        let mut research = HashMap::new();
        research.insert("top_regime_leader_market".to_string(), "US".to_string());
        research.insert(
            "top_regime_leader_bucket".to_string(),
            "trend_up_low_vol".to_string(),
        );
        research.insert(
            "top_regime_leader_factor".to_string(),
            "momentum".to_string(),
        );
        research.insert(
            "current_rotation_leader_factor".to_string(),
            "volume".to_string(),
        );
        research.insert(
            "current_rotation_date".to_string(),
            "2026-01-04".to_string(),
        );
        research.insert("rotation_switches".to_string(), "2".to_string());
        research.insert(
            "latest_regime_transition_market".to_string(),
            "JP".to_string(),
        );
        research.insert(
            "latest_regime_transition_from_bucket".to_string(),
            "trend_down_low_vol".to_string(),
        );
        research.insert(
            "latest_regime_transition_to_bucket".to_string(),
            "trend_down_high_vol".to_string(),
        );
        research.insert(
            "latest_regime_transition_date".to_string(),
            "2026-01-05".to_string(),
        );

        let report = build_paper_hints(
            &research,
            Some(&PaperHintsDaemonInput {
                alerts: 0,
                last_cycle: 3,
                last_end_equity: 1010000.0,
                max_drawdown_observed: 0.05,
            }),
            Some(&PaperHintsCompareInput {
                winner: "candidate".to_string(),
                research_changes: 3,
                top_research_keys: vec![
                    "top_regime_leader_market".to_string(),
                    "current_rotation_leader_factor".to_string(),
                ],
            }),
        );

        assert_eq!(report.stance, "WATCH");
        assert!(report.headline.contains("JP shifted"));
        assert!(report.watch_markets.iter().any(|item| item == "US"));
        assert!(report.watch_markets.iter().any(|item| item == "JP"));
        assert_eq!(report.market_hints.len(), 2);
        assert!(report.market_hints.iter().any(|item| item.market == "US"));
        assert!(report.market_hints.iter().any(|item| item.market == "JP"));
        let summary = render_paper_hints_summary(&report);
        assert!(summary.contains("watch_markets=US|JP"));
        assert!(summary.contains("market_hints_count=2"));
        assert!(summary.contains("bullet_1="));
    }
}
