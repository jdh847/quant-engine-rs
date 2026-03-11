use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct SdkInitRequest {
    pub plugin_id: String,
    pub output_dir: PathBuf,
    pub force: bool,
    pub project_root: PathBuf,
}

#[derive(Debug, Clone)]
pub struct SdkInitReport {
    pub package_dir: PathBuf,
    pub cargo_toml: PathBuf,
    pub lib_rs: PathBuf,
    pub manifest_toml: PathBuf,
    pub readme_md: PathBuf,
}

#[derive(Debug, Clone)]
pub struct SdkCheckReport {
    pub package_dir: PathBuf,
    pub valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginManifest {
    pub plugin_id: String,
    pub sdk_version: String,
    pub created_at_utc: String,
    pub factory_fn: String,
    pub runtime_trait: String,
}

#[derive(Debug, Clone)]
pub struct SdkRegisterRequest {
    pub package_dir: PathBuf,
    pub display_name: Option<String>,
    pub description: Option<String>,
    pub registry_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct SdkRegisterReport {
    pub plugin_id: String,
    pub registry_path: PathBuf,
    pub created: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RegisteredSdkPlugin {
    pub plugin_id: String,
    pub name: String,
    pub description: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_min_price")]
    pub min_price: f64,
    #[serde(default = "default_alpha_scale")]
    pub alpha_volume_scale: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
struct SdkPluginRegistryFile {
    #[serde(default)]
    plugins: Vec<RegisteredSdkPlugin>,
}

fn default_enabled() -> bool {
    true
}

fn default_min_price() -> f64 {
    1.0
}

fn default_alpha_scale() -> f64 {
    0.00001
}

pub fn create_strategy_sdk(req: &SdkInitRequest) -> Result<SdkInitReport> {
    validate_plugin_id(&req.plugin_id)?;
    fs::create_dir_all(&req.output_dir)
        .with_context(|| format!("create sdk output dir failed: {}", req.output_dir.display()))?;

    let package_dir = req.output_dir.join(&req.plugin_id);
    if package_dir.exists() {
        if req.force {
            fs::remove_dir_all(&package_dir).with_context(|| {
                format!(
                    "remove existing sdk package failed: {}; try manually deleting it",
                    package_dir.display()
                )
            })?;
        } else {
            return Err(anyhow!(
                "sdk package already exists: {}; use --force to overwrite",
                package_dir.display()
            ));
        }
    }

    let src_dir = package_dir.join("src");
    fs::create_dir_all(&src_dir)?;

    let dep_path = relative_path_string(&package_dir, &req.project_root)
        .unwrap_or_else(|| req.project_root.display().to_string());

    let cargo_toml = package_dir.join("Cargo.toml");
    let lib_rs = src_dir.join("lib.rs");
    let manifest_toml = package_dir.join("plugin.toml");
    let readme_md = package_dir.join("README.md");

    fs::write(&cargo_toml, render_cargo_toml(&req.plugin_id, &dep_path))
        .with_context(|| format!("write Cargo.toml failed: {}", cargo_toml.display()))?;
    fs::write(&lib_rs, render_lib_template(&req.plugin_id))
        .with_context(|| format!("write lib.rs failed: {}", lib_rs.display()))?;

    let manifest = PluginManifest {
        plugin_id: req.plugin_id.clone(),
        sdk_version: "0.1.0".to_string(),
        created_at_utc: Utc::now().to_rfc3339(),
        factory_fn: "build_plugin".to_string(),
        runtime_trait: "private_quant_bot::strategy::StrategyPlugin".to_string(),
    };
    fs::write(
        &manifest_toml,
        toml::to_string_pretty(&manifest).context("serialize plugin manifest toml failed")?,
    )
    .with_context(|| format!("write plugin.toml failed: {}", manifest_toml.display()))?;

    fs::write(&readme_md, render_readme(&req.plugin_id, &dep_path))
        .with_context(|| format!("write README.md failed: {}", readme_md.display()))?;

    Ok(SdkInitReport {
        package_dir,
        cargo_toml,
        lib_rs,
        manifest_toml,
        readme_md,
    })
}

pub fn check_strategy_sdk(package_dir: impl AsRef<Path>) -> Result<SdkCheckReport> {
    let package_dir = resolve_package_dir(package_dir.as_ref())?;
    let mut errors = Vec::new();
    let mut warnings = Vec::new();

    let cargo_toml = package_dir.join("Cargo.toml");
    let manifest_toml = package_dir.join("plugin.toml");
    let lib_rs = package_dir.join("src").join("lib.rs");
    let readme = package_dir.join("README.md");

    for path in [&cargo_toml, &manifest_toml, &lib_rs, &readme] {
        if !path.exists() {
            errors.push(format!("missing required file: {}", path.display()));
        }
    }

    if manifest_toml.exists() {
        let text = fs::read_to_string(&manifest_toml)
            .with_context(|| format!("read plugin.toml failed: {}", manifest_toml.display()))?;
        match toml::from_str::<PluginManifest>(&text) {
            Ok(manifest) => {
                if manifest.factory_fn != "build_plugin" {
                    warnings.push(format!(
                        "factory_fn is '{}', expected 'build_plugin'",
                        manifest.factory_fn
                    ));
                }
                if manifest.runtime_trait != "private_quant_bot::strategy::StrategyPlugin" {
                    warnings.push(format!(
                        "runtime_trait is '{}', expected private_quant_bot::strategy::StrategyPlugin",
                        manifest.runtime_trait
                    ));
                }
                if let Err(err) = validate_plugin_id(&manifest.plugin_id) {
                    errors.push(format!("invalid plugin_id in manifest: {err}"));
                }
            }
            Err(err) => errors.push(format!("invalid plugin.toml format: {err}")),
        }
    }

    if lib_rs.exists() {
        let code = fs::read_to_string(&lib_rs)
            .with_context(|| format!("read lib.rs failed: {}", lib_rs.display()))?;
        if !code.contains("impl StrategyPlugin for") {
            errors.push("src/lib.rs missing `impl StrategyPlugin for ...`".to_string());
        }
        if !code.contains("pub fn build_plugin(") {
            errors.push("src/lib.rs missing `pub fn build_plugin(...)` factory".to_string());
        }
        if !code.contains("optimize_targets(") {
            warnings.push(
                "src/lib.rs does not use optimize_targets; consider keeping risk/turnover constraints"
                    .to_string(),
            );
        }
    }

    Ok(SdkCheckReport {
        package_dir,
        valid: errors.is_empty(),
        errors,
        warnings,
    })
}

pub fn register_strategy_sdk(req: &SdkRegisterRequest) -> Result<SdkRegisterReport> {
    let check = check_strategy_sdk(&req.package_dir)?;
    if !check.valid {
        return Err(anyhow!(
            "sdk package is invalid; run `sdk-check` first: {}",
            check.package_dir.display()
        ));
    }

    let package_dir = check.package_dir;
    let manifest = read_plugin_manifest(package_dir.join("plugin.toml"))?;
    validate_plugin_id(&manifest.plugin_id)?;

    let name = req
        .display_name
        .clone()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| display_name_from_plugin_id(&manifest.plugin_id));
    let description = req
        .description
        .clone()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| format!("SDK plugin generated from package `{}`", manifest.plugin_id));

    let registry_path = req
        .registry_path
        .clone()
        .unwrap_or(resolve_registry_path_for_write()?);
    if let Some(parent) = registry_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "create sdk registry parent directory failed: {}",
                parent.display()
            )
        })?;
    }

    let mut registry = read_registry_file(&registry_path)?;
    let mut created = true;
    if let Some(existing) = registry
        .plugins
        .iter_mut()
        .find(|p| p.plugin_id == manifest.plugin_id)
    {
        existing.name = name;
        existing.description = description;
        existing.enabled = true;
        created = false;
    } else {
        registry.plugins.push(RegisteredSdkPlugin {
            plugin_id: manifest.plugin_id.clone(),
            name,
            description,
            enabled: true,
            min_price: default_min_price(),
            alpha_volume_scale: default_alpha_scale(),
        });
        registry
            .plugins
            .sort_by(|a, b| a.plugin_id.cmp(&b.plugin_id));
    }

    fs::write(
        &registry_path,
        toml::to_string_pretty(&registry).context("serialize sdk registry failed")?,
    )
    .with_context(|| format!("write sdk registry failed: {}", registry_path.display()))?;

    Ok(SdkRegisterReport {
        plugin_id: manifest.plugin_id,
        registry_path,
        created,
    })
}

pub fn list_registered_sdk_plugins() -> Result<Vec<RegisteredSdkPlugin>> {
    let Some(path) = resolve_registry_path_for_read() else {
        return Ok(Vec::new());
    };
    let registry = read_registry_file(path)?;
    Ok(registry.plugins)
}

pub fn list_registered_sdk_plugins_or_empty() -> Vec<RegisteredSdkPlugin> {
    list_registered_sdk_plugins().unwrap_or_default()
}

pub fn is_registered_sdk_plugin(plugin_id: &str) -> bool {
    list_registered_sdk_plugins_or_empty()
        .iter()
        .any(|p| p.enabled && p.plugin_id == plugin_id)
}

fn resolve_package_dir(input: &Path) -> Result<PathBuf> {
    if input.is_absolute() {
        return Ok(input.to_path_buf());
    }

    let cwd_joined = std::env::current_dir()
        .context("read current_dir failed")?
        .join(input);
    if cwd_joined.exists() {
        return Ok(cwd_joined);
    }

    let manifest_joined = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(input);
    if manifest_joined.exists() {
        return Ok(manifest_joined);
    }

    Ok(cwd_joined)
}

fn read_plugin_manifest(path: impl AsRef<Path>) -> Result<PluginManifest> {
    let path = path.as_ref();
    let text = fs::read_to_string(path)
        .with_context(|| format!("read plugin manifest failed: {}", path.display()))?;
    toml::from_str(&text)
        .with_context(|| format!("parse plugin manifest failed: {}", path.display()))
}

fn read_registry_file(path: impl AsRef<Path>) -> Result<SdkPluginRegistryFile> {
    let path = path.as_ref();
    if !path.exists() {
        return Ok(SdkPluginRegistryFile::default());
    }

    let text = fs::read_to_string(path)
        .with_context(|| format!("read sdk registry failed: {}", path.display()))?;
    toml::from_str(&text).with_context(|| format!("parse sdk registry failed: {}", path.display()))
}

fn resolve_registry_path_for_read() -> Option<PathBuf> {
    if let Ok(raw) = std::env::var("PRIVATE_QUANT_SDK_REGISTRY") {
        let path = PathBuf::from(raw);
        if path.exists() {
            return Some(path);
        }
    }

    let cwd_path = std::env::current_dir()
        .ok()?
        .join("config")
        .join("sdk_plugins.toml");
    if cwd_path.exists() {
        return Some(cwd_path);
    }

    let manifest_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("config")
        .join("sdk_plugins.toml");
    if manifest_path.exists() {
        return Some(manifest_path);
    }

    None
}

fn resolve_registry_path_for_write() -> Result<PathBuf> {
    if let Ok(raw) = std::env::var("PRIVATE_QUANT_SDK_REGISTRY") {
        return Ok(PathBuf::from(raw));
    }

    Ok(std::env::current_dir()
        .context("read current_dir failed")?
        .join("config")
        .join("sdk_plugins.toml"))
}

fn display_name_from_plugin_id(plugin_id: &str) -> String {
    plugin_id
        .split('_')
        .filter(|x| !x.is_empty())
        .map(|segment| {
            let mut chars = segment.chars();
            let mut out = String::new();
            if let Some(first) = chars.next() {
                out.push(first.to_ascii_uppercase());
                out.extend(chars);
            }
            out
        })
        .collect::<Vec<_>>()
        .join(" ")
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

fn relative_path_string(from_dir: &Path, to_path: &Path) -> Option<String> {
    let from_abs = fs::canonicalize(from_dir).ok()?;
    let to_abs = fs::canonicalize(to_path).ok()?;

    let from_components: Vec<Component<'_>> = from_abs.components().collect();
    let to_components: Vec<Component<'_>> = to_abs.components().collect();
    let common_len = from_components
        .iter()
        .zip(to_components.iter())
        .take_while(|(a, b)| a == b)
        .count();

    let mut out = PathBuf::new();
    let up_count = from_components.len().saturating_sub(common_len);
    for _ in 0..up_count {
        out.push("..");
    }
    for comp in to_components.iter().skip(common_len) {
        out.push(comp.as_os_str());
    }
    Some(out.to_string_lossy().to_string())
}

fn render_cargo_toml(plugin_id: &str, dep_path: &str) -> String {
    format!(
        r#"[package]
name = "{plugin_id}_sdk"
version = "0.1.0"
edition = "2021"
description = "Strategy plugin SDK crate for private_quant_bot"

[lib]
name = "{plugin_id}_sdk"
path = "src/lib.rs"

[dependencies]
private_quant_bot = {{ path = "{dep_path}" }}
"#
    )
}

fn render_lib_template(plugin_id: &str) -> String {
    let struct_name = plugin_struct_name(plugin_id);
    format!(
        r#"use std::collections::HashMap;

use private_quant_bot::{{
    model::Bar,
    portfolio::{{optimize_targets, PortfolioMethod, PortfolioOptimizerConfig, SignalCandidate}},
    strategy::StrategyPlugin,
}};

#[derive(Debug, Clone)]
pub struct {struct_name} {{
    top_n: usize,
    min_price: f64,
}}

impl {struct_name} {{
    pub fn new(top_n: usize, min_price: f64) -> Self {{
        Self {{
            top_n: top_n.max(1),
            min_price,
        }}
    }}
}}

impl StrategyPlugin for {struct_name} {{
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

        // Replace this alpha with your own cross-sectional model.
        let mut candidates = bars
            .iter()
            .filter(|bar| bar.close >= self.min_price)
            .map(|bar| SignalCandidate {{
                symbol: bar.symbol.clone(),
                alpha_score: bar.close * (1.0 + (bar.volume + 1.0).ln() * 0.00001),
                volatility: 0.02,
                returns: vec![0.0; 8],
            }})
            .collect::<Vec<_>>();
        candidates.sort_by(|a, b| b.alpha_score.total_cmp(&a.alpha_score));
        candidates.truncate(self.top_n);

        let optimized = optimize_targets(
            &candidates,
            current_notionals,
            market_budget,
            PortfolioOptimizerConfig {{
                method: PortfolioMethod::RiskParity,
                risk_parity_blend: 0.7,
                max_turnover_ratio: 0.35,
            }},
        );

        for (symbol, target) in optimized {{
            targets.insert(symbol, target);
        }}
        targets
    }}
}}

pub fn build_plugin(top_n: usize) -> Box<dyn StrategyPlugin> {{
    Box::new({struct_name}::new(top_n, 1.0))
}}
"#
    )
}

fn render_readme(plugin_id: &str, dep_path: &str) -> String {
    format!(
        r#"# Strategy SDK Package: `{plugin_id}`

This crate was generated by:

```bash
cargo run -- sdk-init --id {plugin_id}
```

## Files

- `Cargo.toml` with a path dependency to `private_quant_bot` (`{dep_path}`)
- `src/lib.rs` plugin template
- `plugin.toml` SDK manifest

## Factory API

- `build_plugin(top_n: usize) -> Box<dyn StrategyPlugin>`

## Integration

1. Validate package structure:

```bash
cargo run -- sdk-check --package-dir {plugin_id}
```

2. Register plugin into runtime registry:

```bash
cargo run -- sdk-register --package-dir {plugin_id}
```

3. Run with this plugin id:

```bash
cargo run -- run --config config/bot.toml --strategy-plugin {plugin_id}
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
        "CustomSdkPlugin".to_string()
    } else {
        out
    }
}

#[cfg(test)]
mod tests {
    use super::{
        check_strategy_sdk, create_strategy_sdk, read_registry_file, register_strategy_sdk,
        SdkInitRequest, SdkRegisterRequest,
    };

    #[test]
    fn sdk_init_and_check_work() {
        let root = std::env::temp_dir().join("private_quant_bot_sdk_test");
        if root.exists() {
            std::fs::remove_dir_all(&root).ok();
        }
        std::fs::create_dir_all(&root).expect("mkdir");

        let report = create_strategy_sdk(&SdkInitRequest {
            plugin_id: "alpha_sdk".to_string(),
            output_dir: root.join("plugins"),
            force: false,
            project_root: root.clone(),
        })
        .expect("sdk init");
        assert!(report.cargo_toml.exists());
        assert!(report.lib_rs.exists());
        assert!(report.manifest_toml.exists());
        assert!(report.readme_md.exists());

        let check = check_strategy_sdk(&report.package_dir).expect("sdk check");
        assert!(check.valid, "errors={:?}", check.errors);
    }

    #[test]
    fn sdk_register_writes_registry() {
        let root = std::env::temp_dir().join("private_quant_bot_sdk_register_test");
        if root.exists() {
            std::fs::remove_dir_all(&root).ok();
        }
        std::fs::create_dir_all(&root).expect("mkdir");

        let report = create_strategy_sdk(&SdkInitRequest {
            plugin_id: "register_alpha".to_string(),
            output_dir: root.join("plugins"),
            force: false,
            project_root: root.clone(),
        })
        .expect("sdk init");

        let registry_path = root.join("config").join("sdk_plugins.toml");
        let reg = register_strategy_sdk(&SdkRegisterRequest {
            package_dir: report.package_dir.clone(),
            display_name: Some("Register Alpha".to_string()),
            description: None,
            registry_path: Some(registry_path),
        })
        .expect("sdk register");
        assert!(reg.registry_path.exists());

        let plugins = read_registry_file(&reg.registry_path)
            .expect("read registry")
            .plugins;
        assert!(
            plugins.iter().any(|p| p.plugin_id == "register_alpha"),
            "plugins={plugins:?}"
        );
    }
}
