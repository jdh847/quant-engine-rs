use anyhow::{anyhow, Result};

fn env_truthy(name: &str) -> bool {
    let Ok(v) = std::env::var(name) else {
        return false;
    };
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "y" | "on"
    )
}

pub fn ensure_network_allowed(context: &str) -> Result<()> {
    if env_truthy("PQBOT_ALLOW_NETWORK") {
        return Ok(());
    }
    Err(anyhow!(
        "network access blocked ({context}); set PQBOT_ALLOW_NETWORK=1 to allow network calls"
    ))
}

pub fn ensure_ibkr_paper_allowed() -> Result<()> {
    if env_truthy("PQBOT_ALLOW_IBKR_PAPER") {
        return Ok(());
    }
    Err(anyhow!(
        "ibkr_paper is disabled by default; set PQBOT_ALLOW_IBKR_PAPER=1 to enable paper IBKR adapter"
    ))
}

/// Hard stop for execution.
///
/// When this is armed (`PQBOT_KILL_SWITCH=1|true|yes|on`), the engine should
/// refuse to execute orders (and surface rejections) regardless of strategy.
pub fn is_trading_kill_switch_armed() -> bool {
    env_truthy("PQBOT_KILL_SWITCH")
}
