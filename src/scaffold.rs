use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};

pub struct ScaffoldResult {
    pub plugin_file: PathBuf,
    pub notes_file: PathBuf,
}

pub fn create_strategy_plugin_scaffold(
    plugin_id: &str,
    output_dir: impl AsRef<Path>,
    force: bool,
) -> Result<ScaffoldResult> {
    validate_plugin_id(plugin_id)?;
    let out_dir = output_dir.as_ref();
    fs::create_dir_all(out_dir)
        .with_context(|| format!("create scaffold dir failed: {}", out_dir.display()))?;

    let plugin_file = out_dir.join(format!("{plugin_id}.rs"));
    let notes_file = out_dir.join(format!("{plugin_id}_README.md"));

    if !force {
        if plugin_file.exists() {
            return Err(anyhow!(
                "scaffold target already exists: {}; use --force to overwrite",
                plugin_file.display()
            ));
        }
        if notes_file.exists() {
            return Err(anyhow!(
                "scaffold target already exists: {}; use --force to overwrite",
                notes_file.display()
            ));
        }
    }

    let code = render_plugin_template(plugin_id);
    fs::write(&plugin_file, code)
        .with_context(|| format!("write plugin scaffold failed: {}", plugin_file.display()))?;

    let notes = render_notes_template(plugin_id);
    fs::write(&notes_file, notes)
        .with_context(|| format!("write scaffold notes failed: {}", notes_file.display()))?;

    Ok(ScaffoldResult {
        plugin_file,
        notes_file,
    })
}

fn validate_plugin_id(plugin_id: &str) -> Result<()> {
    if plugin_id.is_empty() {
        return Err(anyhow!("plugin id cannot be empty"));
    }
    if !plugin_id
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
    {
        return Err(anyhow!(
            "plugin id must match [a-z0-9_]+ (example: value_momentum)"
        ));
    }
    Ok(())
}

fn render_plugin_template(plugin_id: &str) -> String {
    format!(
        r#"use std::collections::HashMap;

use private_quant_bot::{{
    model::Bar,
    portfolio::{{optimize_targets, PortfolioMethod, PortfolioOptimizerConfig, SignalCandidate}},
    strategy::StrategyPlugin,
}};

#[derive(Debug, Clone)]
pub struct {name} {{
    top_n: usize,
}}

impl {name} {{
    pub fn new(top_n: usize) -> Self {{
        Self {{ top_n }}
    }}
}}

impl StrategyPlugin for {name} {{
    fn id(&self) -> &'static str {{
        "{plugin_id}"
    }}

    fn target_notionals(
        &mut self,
        bars: &[Bar],
        market_budget: f64,
        current_notionals: &HashMap<String, f64>,
    ) -> HashMap<String, f64> {{
        let mut targets: HashMap<String, f64> =
            bars.iter().map(|b| (b.symbol.clone(), 0.0)).collect();

        // TODO: replace with your own cross-sectional alpha logic.
        let mut candidates = bars
            .iter()
            .map(|bar| SignalCandidate {{
                symbol: bar.symbol.clone(),
                alpha_score: bar.close,
                volatility: 0.02,
                returns: vec![0.0, 0.0, 0.0, 0.0],
            }})
            .collect::<Vec<_>>();
        candidates.sort_by(|a, b| b.alpha_score.total_cmp(&a.alpha_score));
        candidates.truncate(self.top_n.max(1));

        let optimized = optimize_targets(
            &candidates,
            current_notionals,
            market_budget,
            PortfolioOptimizerConfig {{
                method: PortfolioMethod::RiskParity,
                risk_parity_blend: 0.6,
                max_turnover_ratio: 0.35,
            }},
        );

        for (symbol, target) in optimized {{
            targets.insert(symbol, target);
        }}
        targets
    }}
}}
"#,
        name = plugin_struct_name(plugin_id),
    )
}

fn render_notes_template(plugin_id: &str) -> String {
    format!(
        r#"# Strategy Plugin Scaffold: `{plugin_id}`

## What this generated

- `{plugin_id}.rs`: strategy plugin skeleton implementing `StrategyPlugin`
- `{plugin_id}_README.md`: this notes file

## Integration steps

1. Move or copy `{plugin_id}.rs` into your source tree.
2. Register it in `src/strategy.rs` registry:
   - add plugin constant
   - add catalog entry in `strategy_plugin_catalog()`
   - route in `build_strategy(...)`
3. Add config validation in `src/config.rs` if needed.
4. Add tests under strategy module and run:

```bash
cargo test
```
"#
    )
}

fn plugin_struct_name(plugin_id: &str) -> String {
    let mut out = String::new();
    for segment in plugin_id.split('_').filter(|x| !x.is_empty()) {
        let mut chars = segment.chars();
        if let Some(first) = chars.next() {
            out.push(first.to_ascii_uppercase());
            for ch in chars {
                out.push(ch);
            }
        }
    }
    if out.is_empty() {
        "CustomPlugin".to_string()
    } else {
        out
    }
}

#[cfg(test)]
mod tests {
    use super::create_strategy_plugin_scaffold;

    #[test]
    fn scaffold_generates_files() {
        let dir = std::env::temp_dir().join("private_quant_bot_scaffold_test");
        if dir.exists() {
            std::fs::remove_dir_all(&dir).ok();
        }
        let out = create_strategy_plugin_scaffold("alpha_test", &dir, false).expect("scaffold");
        assert!(out.plugin_file.exists());
        assert!(out.notes_file.exists());
    }
}
