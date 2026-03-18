use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tar::Archive;
use tar::Builder;

#[derive(Debug, Clone)]
pub struct BundleRequest {
    pub output_dir: PathBuf,
    pub bundle_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleFile {
    pub rel_path: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleManifest {
    pub created_at_utc: String,
    pub output_dir: String,
    pub files: Vec<BundleFile>,
    pub missing: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct BundleReport {
    pub bundle_path: PathBuf,
    pub manifest: BundleManifest,
}

pub fn create_run_bundle(req: &BundleRequest) -> Result<BundleReport> {
    let output_dir = fs::canonicalize(&req.output_dir).with_context(|| {
        format!(
            "canonicalize output_dir failed: {}",
            req.output_dir.display()
        )
    })?;
    if !output_dir.is_dir() {
        return Err(anyhow!(
            "output_dir is not a directory: {}",
            output_dir.display()
        ));
    }

    let bundle_path = req.bundle_path.clone();
    if let Some(parent) = bundle_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create bundle parent dir failed: {}", parent.display()))?;
    }

    // A "run bundle" is a curated set of key artifacts. Missing files are allowed,
    // but they are recorded in the manifest.
    let wanted = [
        "dashboard.html",
        "dashboard_share.html",
        "summary.txt",
        "config_used_redacted.toml",
        "audit_snapshot.json",
        "audit_snapshot_summary.txt",
        "data_quality_report.csv",
        "data_quality_summary.txt",
        "equity_curve.csv",
        "trades.csv",
        "rejections.csv",
        "factor_attribution.csv",
        "factor_attribution_summary.txt",
        "research_report.md",
        "research_report.html",
        "research_report.json",
        "research_report_summary.txt",
        "factor_decay.csv",
        "rolling_ic.csv",
        "regime_split.csv",
        "walk_forward_deep_dive.csv",
        "walk_forward/walk_forward_folds.csv",
        "walk_forward/walk_forward_summary.txt",
        "run_registry.csv",
        "run_registry.json",
        "run_registry_top.md",
    ];

    let mut files = Vec::new();
    let mut missing = Vec::new();

    for rel in wanted {
        let full = output_dir.join(rel);
        if !full.exists() {
            missing.push(rel.to_string());
            continue;
        }
        let canonical = fs::canonicalize(&full)
            .with_context(|| format!("canonicalize file failed: {}", full.display()))?;
        if !canonical.starts_with(&output_dir) {
            return Err(anyhow!(
                "refusing to bundle file outside output_dir: {}",
                canonical.display()
            ));
        }

        let (bytes, sha256) = hash_file(&canonical)?;
        files.push(BundleFile {
            rel_path: rel.to_string(),
            bytes,
            sha256,
        });
    }

    files.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    missing.sort();

    let manifest = BundleManifest {
        created_at_utc: chrono::Utc::now().to_rfc3339(),
        output_dir: output_dir.display().to_string(),
        files: files.clone(),
        missing: missing.clone(),
    };

    write_bundle(&output_dir, &bundle_path, &manifest)?;

    Ok(BundleReport {
        bundle_path,
        manifest,
    })
}

#[derive(Debug, Clone)]
pub struct BundleVerifyReport {
    pub manifest: BundleManifest,
    pub checked_files: usize,
}

pub fn verify_run_bundle(bundle_path: impl AsRef<Path>) -> Result<BundleVerifyReport> {
    let bundle_path = bundle_path.as_ref();
    let (manifest, file_bytes) = read_manifest_and_file_bytes(bundle_path)?;

    let mut checked = 0usize;
    for item in &manifest.files {
        let key = format!("bundle/files/{}", item.rel_path);
        let data = file_bytes.get(&key).ok_or_else(|| {
            anyhow!(
                "bundle missing file '{}' referenced in manifest",
                item.rel_path
            )
        })?;
        if data.len() as u64 != item.bytes {
            return Err(anyhow!(
                "bundle file size mismatch for '{}': expected {} got {}",
                item.rel_path,
                item.bytes,
                data.len()
            ));
        }
        let got = format!("{:x}", Sha256::digest(data));
        if got != item.sha256 {
            return Err(anyhow!(
                "bundle sha256 mismatch for '{}': expected {} got {}",
                item.rel_path,
                item.sha256,
                got
            ));
        }
        checked += 1;
    }

    Ok(BundleVerifyReport {
        manifest,
        checked_files: checked,
    })
}

#[derive(Debug, Clone)]
pub struct BundleExtractRequest {
    pub bundle_path: PathBuf,
    pub output_dir: PathBuf,
    pub force: bool,
}

pub fn extract_run_bundle(req: &BundleExtractRequest) -> Result<()> {
    // Safety: verify first, then extract only bundle/files/* into output_dir.
    let verify = verify_run_bundle(&req.bundle_path)?;
    if verify.checked_files == 0 {
        return Err(anyhow!("bundle contains no files"));
    }

    fs::create_dir_all(&req.output_dir).with_context(|| {
        format!(
            "create extract output dir failed: {}",
            req.output_dir.display()
        )
    })?;

    let f = File::open(&req.bundle_path).with_context(|| {
        format!(
            "open bundle for extract failed: {}",
            req.bundle_path.display()
        )
    })?;
    let dec = GzDecoder::new(f);
    let mut ar = Archive::new(dec);

    for entry in ar.entries().context("read bundle tar entries")? {
        let mut entry = entry?;
        let path = entry.path()?.to_path_buf();
        let path_str = path.to_string_lossy();
        if !path_str.starts_with("bundle/files/") {
            continue;
        }
        let rel = path_str.trim_start_matches("bundle/files/");
        if rel.is_empty() {
            continue;
        }
        let safe_rel = sanitize_rel_path(rel)?;
        let out_path = req.output_dir.join(&safe_rel);
        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if out_path.exists() && !req.force {
            return Err(anyhow!(
                "refusing to overwrite existing file (pass --force): {}",
                out_path.display()
            ));
        }
        entry
            .unpack(&out_path)
            .with_context(|| format!("extract failed: {} -> {}", path_str, out_path.display()))?;
    }

    Ok(())
}

fn write_bundle(output_dir: &Path, bundle_path: &Path, manifest: &BundleManifest) -> Result<()> {
    let f = File::create(bundle_path)
        .with_context(|| format!("create bundle failed: {}", bundle_path.display()))?;
    let enc = GzEncoder::new(f, Compression::default());
    let mut tar = Builder::new(enc);

    let readme = format!(
        "Private Quant Bot Run Bundle\n\n\
This is a curated archive of a single run output directory.\n\n\
created_at_utc={}\n\
output_dir={}\n\
files_included={}\n\
files_missing={}\n\n\
Notes:\n\
- The bundle contains hashes (SHA256) in bundle/manifest.json.\n\
- No network is required to open the dashboard (bundle/files/dashboard.html).\n",
        manifest.created_at_utc,
        manifest.output_dir,
        manifest.files.len(),
        manifest.missing.len()
    );
    append_bytes(&mut tar, "bundle/README.txt", readme.as_bytes(), 0o644)?;

    let manifest_json = serde_json::to_vec_pretty(manifest).context("serialize bundle manifest")?;
    append_bytes(&mut tar, "bundle/manifest.json", &manifest_json, 0o644)?;

    for item in &manifest.files {
        let src = output_dir.join(&item.rel_path);
        let dst = format!("bundle/files/{}", item.rel_path);
        append_file(&mut tar, &dst, &src)?;
    }

    let enc = tar.into_inner().context("finalize tar builder")?;
    enc.finish().context("finalize gzip stream")?;
    Ok(())
}

fn append_bytes(
    tar: &mut Builder<GzEncoder<File>>,
    path: &str,
    bytes: &[u8],
    mode: u32,
) -> Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(mode);
    header.set_cksum();
    tar.append_data(&mut header, path, bytes)
        .with_context(|| format!("append bytes failed: {path}"))?;
    Ok(())
}

fn append_file(tar: &mut Builder<GzEncoder<File>>, dst_path: &str, src_path: &Path) -> Result<()> {
    let mut f = File::open(src_path)
        .with_context(|| format!("open bundle source failed: {}", src_path.display()))?;
    let meta = f
        .metadata()
        .with_context(|| format!("stat bundle source failed: {}", src_path.display()))?;
    if !meta.is_file() {
        return Err(anyhow!("not a file: {}", src_path.display()));
    }
    let mut header = tar::Header::new_gnu();
    header.set_size(meta.len());
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(&mut header, dst_path, &mut f)
        .with_context(|| format!("append file failed: {} -> {}", src_path.display(), dst_path))?;
    Ok(())
}

fn hash_file(path: &Path) -> Result<(u64, String)> {
    let mut f =
        File::open(path).with_context(|| format!("open file failed: {}", path.display()))?;
    let mut h = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    let mut total = 0u64;
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        total += n as u64;
        h.update(&buf[..n]);
    }
    Ok((total, format!("{:x}", h.finalize())))
}

fn sanitize_rel_path(p: &str) -> Result<PathBuf> {
    let mut out = PathBuf::new();
    for comp in Path::new(p).components() {
        match comp {
            std::path::Component::Normal(c) => out.push(c),
            std::path::Component::CurDir => {}
            _ => {
                return Err(anyhow!("unsafe path in bundle: '{p}'"));
            }
        }
    }
    if out.as_os_str().is_empty() {
        return Err(anyhow!("empty path in bundle"));
    }
    Ok(out)
}

fn read_manifest_and_file_bytes(
    bundle_path: &Path,
) -> Result<(BundleManifest, std::collections::HashMap<String, Vec<u8>>)> {
    let f = File::open(bundle_path)
        .with_context(|| format!("open bundle failed: {}", bundle_path.display()))?;
    let dec = GzDecoder::new(f);
    let mut ar = Archive::new(dec);

    let mut manifest_bytes: Option<Vec<u8>> = None;
    let mut file_bytes: std::collections::HashMap<String, Vec<u8>> =
        std::collections::HashMap::new();

    for entry in ar.entries().context("read tar entries")? {
        let mut entry = entry?;
        let path = entry.path()?.to_path_buf();
        let p = path.to_string_lossy().to_string();
        let mut buf = Vec::new();
        entry.read_to_end(&mut buf)?;
        if p == "bundle/manifest.json" {
            manifest_bytes = Some(buf);
            continue;
        }
        if p.starts_with("bundle/files/") {
            file_bytes.insert(p, buf);
        }
    }

    let manifest_bytes =
        manifest_bytes.ok_or_else(|| anyhow!("bundle missing bundle/manifest.json"))?;
    let manifest: BundleManifest =
        serde_json::from_slice(&manifest_bytes).context("parse bundle manifest json")?;
    Ok((manifest, file_bytes))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        create_run_bundle, extract_run_bundle, verify_run_bundle, BundleExtractRequest,
        BundleRequest,
    };

    #[test]
    fn bundle_writes_tar_gz_with_manifest() {
        let dir = std::env::temp_dir().join("pqbot_bundle_test");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("mkdir");
        fs::write(dir.join("summary.txt"), "hello").expect("write");
        fs::write(dir.join("dashboard.html"), "<html/>").expect("write");

        let bundle = dir.join("out.tar.gz");
        let report = create_run_bundle(&BundleRequest {
            output_dir: dir.clone(),
            bundle_path: bundle.clone(),
        })
        .expect("bundle");
        assert!(report.bundle_path.exists());
        let bytes = fs::metadata(&bundle).expect("stat").len();
        assert!(bytes > 50);
    }

    #[test]
    fn bundle_verify_and_extract_work() {
        let dir = std::env::temp_dir().join("pqbot_bundle_test2");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("mkdir");
        fs::write(dir.join("summary.txt"), "hello").expect("write");
        fs::write(dir.join("dashboard.html"), "<html/>").expect("write");

        let bundle = dir.join("out.tar.gz");
        let _ = create_run_bundle(&BundleRequest {
            output_dir: dir.clone(),
            bundle_path: bundle.clone(),
        })
        .expect("bundle");
        let verify = verify_run_bundle(&bundle).expect("verify");
        assert!(verify.checked_files >= 2);

        let out = dir.join("extract");
        extract_run_bundle(&BundleExtractRequest {
            bundle_path: bundle,
            output_dir: out.clone(),
            force: false,
        })
        .expect("extract");
        assert!(out.join("summary.txt").exists());
        assert!(out.join("dashboard.html").exists());
    }
}
